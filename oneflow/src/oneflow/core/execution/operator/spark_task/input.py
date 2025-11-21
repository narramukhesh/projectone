from oneflow.core.schemas.input import SparkInput
from oneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
from oneflow.core.schemas.features import (
    FilterDataFeature,
    DropColumnsFeature,
    SelectColumnsFeature,
    SchemaInferenceFromRegistry,
    SchemaType,
    ChangeFeature,
    PostTaskExecutionFeature,
)
from oneflow.core.exception.validation import ChangeDataFeatureTypeParseError
from oneflow.core.schemas.state import ChangeDataCaptureState
from oneflow.core.exception.validation import SchemaInferenceFromRegistryError
from oneflow.core.schemas.event import SubscribedEventHandleEnum
from oneflow.core.event import get_event_handler_function
from oneflow.core.execution.spark_task.context import SparkTaskExecutionContext
import json
from oneflow.core.observability.logging import Logger
from pyspark.sql import DataFrame
from oneflow.core.types import F
from typing import Type, Any, cast
from oneflow.core.execution.operator import Operator, execute_step
from functools import partial

logger = Logger.get_logger(__name__)


class InputOperator(Operator):
    pre_do_step_features = ["change_data_feature"]
    post_do_step_features = [
        "filter_data_feature",
        "schema_inference_from_registry",
        "drop_columns_feature",
        "post_task_execution",
        "select_columns_feature",
    ]

    def __init__(self, inputConfig: SparkInput, ctx: SparkTaskExecutionContext):
        self.config = inputConfig
        self.context = ctx
        self.pre_steps = {}
        self.post_steps = {}
        self.result = None
        self.events = {}
        logger.info(f"Initializing the Input operator object for input {self.config} ")
        if inputConfig.features:
            for feature in inputConfig.features.model_dump():

                step_value = getattr(inputConfig.features, feature)
                if step_value is not None:
                    if feature in InputOperator.pre_do_step_features:
                        step_type = "pre"
                    else:
                        step_type = "post"
                    if step_value.resolve:
                        step_func = getattr(
                            inputConfig.source_class_obj, f"resolve_{feature}"
                        )
                        step = (
                            f"{step_type}_step_resolve_{feature}",
                            step_func,
                            step_value,
                        )
                    else:
                        step = (f"{step_type}_step_resolve_{feature}", None, step_value)

                    if feature in InputOperator.pre_do_step_features:
                        self.pre_steps[feature] = step
                    elif feature in InputOperator.post_do_step_features:
                        self.post_steps[feature] = step

        #### Event management code to handle the input configuration event handler
        if inputConfig.events:
            for event in inputConfig.events:
                if event.consumers is not None:
                    for consumer in event.consumers:
                        self.context.event_manager.subscribe(
                            event.type, get_event_handler_function(consumer)
                        )
                self.events[event.type] = event.handle
        ####
        self.reader_func = getattr(
            inputConfig.source_class_obj,
            f"read_{inputConfig.source_extract_type.value.lower()}",
        )
        logger.info(
            f"Completed the initializing the Input operator object for input {self.config.name} "
        )

    @execute_step("InputResult")
    def pre_step_resolve_change_data_feature(self, cdc_func: Type[F], value: Any):
        """
        This method resolve the change data capture feature

        Parameters
        ------------------------
        cdc_func: Type[F]
            source cdc function implementation
        value: Any
            cdc feature object
        """

        logger.debug(
            f"Started the executing change data feature for input {self.config.name} with cdc configuration {value}"
        )
        value = cast(ChangeFeature, value)
        if self.context.refresh_policy.type == SparkTaskRefreshTypes.backfill:
            if value.start_value is None or value.end_value is None:
                raise ChangeDataFeatureTypeParseError(
                    f"Input configuration {self.config.name} has configured with backfill refresh policy where start_value or end_value configured with None"
                )

        previous_cdc_value = json.loads(
            self.context.metadata.get(
                "sources", f"{self.config.name}_cdc_value", default="{}"
            )
        )
        previous_cdc_value = ChangeDataCaptureState.from_dict(previous_cdc_value)
        cdc_result = cdc_func(
            self.context.spark,
            self.config.path,
            self.config.source.value.lower(),
            self.config.source_type,
            self.context.refresh_policy.type,
            previous_cdc_value,
            value,
            self.config.options,
        )
        logger.debug(
            f"Completed the execution of the cdc feature for input {self.config.name}, after execution which returned the attribute:{cdc_result.attribute},start_value:{cdc_result.start_value},end_value:{cdc_result.end_value},filter_expression:{cdc_result.filter_expr},extra_info:{cdc_result.extra_info}"
        )
        if (cdc_result.filter_expr is not None) and isinstance(
            cdc_result.filter_expr, str
        ):
            previous_filter_value = self.post_steps.get(
                "filter_data_feature",
                ("post_step_resolve_filter_data_feature", None, FilterDataFeature()),
            )
            previous_filter_value[2].expression = (
                previous_filter_value[2].expression + " AND " + cdc_result.filter_expr
                if previous_filter_value[2].expression is not None
                else cdc_result.filter_expr
            )
            self.post_steps["filter_data_feature"] = previous_filter_value
        if (
            cdc_result.end_value.value is not None
            and cdc_result.start_value.value is not None
        ):
            self.cdc_state = ChangeDataCaptureState(
                attribute=cdc_result.attribute,
                next_value=cdc_result.end_value,
                start_value=cdc_result.start_value,
                extra_info=cdc_result.extra_info,
                load_type=self.context.refresh_policy.type,
            )

        self.config.options = cdc_result.options

        if cdc_result.path is not None:
            self.config.path = cdc_result.path

    @execute_step("InputResult")
    def post_step_resolve_filter_data_feature(
        self, df: DataFrame, filter_expression: FilterDataFeature
    ):
        """
        This function resolve the filter the data by the filter expression

        Parameters
        ---------------
        df: DataFrame
            this input dataframe which be used for transformation
        filter_expression: FilterDataFeature
            this is the filter expression used to filter the dataframe

        """
        expression = filter_expression.expression
        logger.debug(
            f"Filtering the input {self.config.name} data set with filter expression {expression}"
        )
        df = df.filter(expression)
        return df

    @execute_step("InputResult")
    def post_step_resolve_drop_columns_feature(
        self, df: DataFrame, drop_columns: DropColumnsFeature
    ):
        """
        This function resolve the column pruning

        Parameters
        ---------------
        df: DataFrame
            this input dataframe which be used for transformation
        drop_columns: DropColumnsFeature
            this is the columns pruned from source dataframe

        """
        columns = drop_columns.columns
        logger.debug(f"Pruning the input {self.config.name} data set columns {columns}")
        drop_columns = columns.split(",")
        df = df.drop(*drop_columns)
        return df

    @execute_step("InputResult")
    def post_step_resolve_select_columns_feature(
        self, df: DataFrame, select_columns: SelectColumnsFeature
    ):
        """
        This function resolve the column pruning

        Parameters
        ---------------
        df: DataFrame
            this input dataframe which be used for transformation
        select_column: SelectColumnsFeature
            this is the columns selected from source dataframe

        """
        columns = select_columns.columns
        logger.debug(f"Select the input {self.config.name} data set columns {columns}")
        select_columns = columns.split(",")
        df = df.select(*select_columns)
        return df

    @execute_step("InputResult")
    def post_step_resolve_schema_inference_from_registry(
        self, df: DataFrame, schema_registry_information: SchemaInferenceFromRegistry
    ):
        """
        This function resolve the schema inference with schema registry

        Parameters
        ---------------
        df: DataFrame
            this input dataframe which be used for transformation
        schema_registry_information: SchemaInferenceFromRegistry
            this is the schema registry information to be included by the

        """
        from oneflow.core.utils.spark import from_avro, from_json

        logger.debug(
            f"Schema inference for input {self.config.name} with configuration {schema_registry_information}"
        )

        if schema_registry_information.schema_type == SchemaType.avro:
            df = df.withColumn(
                schema_registry_information.target_column_name,
                from_avro(
                    data=schema_registry_information.source_column_name,
                    schemaRegistryAddress=schema_registry_information.schema_registry_credentials.schema_registry_address,
                    schemaRegistryOptions={
                        "clientId": schema_registry_information.schema_registry_credentials.schema_registry_user,
                        "clientSecret": schema_registry_information.schema_registry_credentials.schema_registry_pass,
                        "type": schema_registry_information.schema_registry_credentials.schema_registry_type,
                    },
                    schemaRegistrySubject=schema_registry_information.subject_name,
                    file=schema_registry_information.file_name,
                ),
            )
        elif schema_registry_information.schema_type == SchemaType.json:
            df = df.withColumn(
                schema_registry_information.target_column_name,
                from_json(
                    col=schema_registry_information.source_column_name,
                    schemaRegistryAddress=schema_registry_information.schema_registry_credentials.schema_registry_address,
                    schemaRegistryOptions={
                        "clientId": schema_registry_information.schema_registry_credentials.schema_registry_user,
                        "clientSecret": schema_registry_information.schema_registry_credentials.schema_registry_pass,
                        "type": schema_registry_information.schema_registry_credentials.schema_registry_type,
                    },
                    schemaRegistrySubject=schema_registry_information.subject_name,
                    file=schema_registry_information.file_name,
                ),
            )
        else:
            logger.exception(
                f"Not supported Schema Type {schema_registry_information.schema_type}"
            )
            raise SchemaInferenceFromRegistryError(
                f"Not supported Schema Type {schema_registry_information.schema_type}"
            )

        return df

    @execute_step("InputResult")
    def execute_read(self):
        """This function will execute the read function of the source"""
        options = (
            self.config.options.model_dump() if self.config.options is not None else {}
        )

        df = partial(self.reader_func)(
            self.context.spark,
            self.config.source.value.lower(),
            self.config.source_type,
            self.config.path,
            options,
        )

        return df

    @execute_step("InputResult")
    def post_step_resolve_post_task_execution(
        self,
        df: DataFrame,
        post_exec_func,
        post_execution_operation: PostTaskExecutionFeature,
    ):
        """
        This function resolve the column pruning

        Parameters
        ---------------
        df: DataFrame
            this input dataframe which be used for transformation
        post_execution_operation: PostTaskExecutionFeature
            this is the operation to be performed in the post execution

        """
        options = (
            self.config.options.model_dump() if self.config.options is not None else {}
        )
        post_exec_func(
            self.context.spark,
            self.config.path,
            self.config.source_type,
            options,
            post_execution_operation,
        )
        return df

    def execute(self):
        """This function is actual execution of the input operator"""

        # executing the presteps
        for pre, attr in self.pre_steps.items():
            logger.debug(
                f"Started the execution of the pre-step {pre} for input {self.config.name}"
            )
            pre_step_func = getattr(self, attr[0])
            self.result = pre_step_func(attr[1], attr[2])
            if self.result.exception:
                if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                    logger.warning(
                        f"Exception for the pre-step stage {pre} for input {self.config.name} with exception {self.result.exception}"
                    )
                    return self.result
                elif self.result.event_severity == SubscribedEventHandleEnum.CONTINUE:
                    logger.info(
                        f"Exception for the pre-step stage {pre} for input {self.config.name} with exception {self.result.exception} but continuing as configured by the events"
                    )
                    continue
                else:
                    logger.exception(
                        f"Exception for the pre-step stage {pre} for input {self.config.name} with exception {self.result.exception}"
                    )

                    return self.result
            logger.debug(
                f"Completed the execution of the pre-step {pre} for input {self.config.name} with result {self.result}"
            )

        logger.debug(
            f"Started the execution of the execution stage for input {self.config.name}"
        )
        self.result = self.execute_read()

        if self.result.exception:
            logger_write = logger.exception
            if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                logger_write = logger.warning
            logger_write(
                f"Exception for the execution stage for input {self.config.name} with exception {self.result.exception}"
            )
            return self.result
        logger.debug(
            f"Completed the execution of the execution stage for input {self.config.name} with result {self.result}"
        )

        for post, attr in self.post_steps.items():
            logger.debug(
                f"Started the execution of the post-step {post} for input {self.config.name}"
            )
            post_step_func = getattr(self, attr[0])
            if attr[1] is not None:
                self.result = post_step_func(self.result.result, attr[1], attr[2])
            else:
                self.result = post_step_func(self.result.result, attr[2])
            if self.result.exception:
                logger_write = logger.exception
                if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                    logger_write = logger.warning
                logger_write(
                    f"Exception for the post-step stage {post} for input {self.config.name} with exception {self.result.exception}"
                )
                return self.result
            logger.debug(
                f"completed the execution of the post-step {post} for input {self.config.name} with result {self.result}"
            )

        return self.result

    def __str__(self):
        """This method is used to return the string representation of the input operator"""
        config = self.config.to_json(safe=True)
        return json.dumps(config)
