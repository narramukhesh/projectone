from projectoneflow.core.schemas.output import SparkOutput
from projectoneflow.core.execution.spark_task.context import SparkTaskExecutionContext
from projectoneflow.core.observability.logging import Logger
from pyspark.sql import DataFrame
from projectoneflow.core.types import F
from projectoneflow.core.event import get_event_handler_function
from projectoneflow.core.schemas.event import SubscribedEventHandleEnum
from typing import Type, Any
from projectoneflow.core.execution.operator import Operator, execute_step
from projectoneflow.core.exception.sources import (
    WriteFunctionNotImplementedError,
)
import inspect
from functools import partial
from projectoneflow.core.schemas.sources import SinkType
import json

logger = Logger.get_logger(__name__)


class OutputOperator(Operator):
    pre_do_step_features = ["create_data_object_if_not_exists"]
    post_do_step_features = []

    def __init__(self, outputConfig: SparkOutput, ctx: SparkTaskExecutionContext):
        self.config = outputConfig
        self.context = ctx
        self.data = None
        self.pre_steps = {}
        self.post_steps = {}
        self.options = {}
        self.events = {}
        self.type = "stream" if ctx.type == "stream" else "batch"
        self.writer_stream_func = None
        logger.info(
            f"Initializing the Output operator object for output {self.config} "
        )
        if outputConfig.features:
            for feature in outputConfig.features.model_dump():

                step_value = getattr(outputConfig.features, feature)
                if step_value is not None:
                    if feature in OutputOperator.pre_do_step_features:
                        step_type = "pre"
                    else:
                        step_type = "post"
                    if step_value.resolve:
                        step_func = getattr(
                            outputConfig.sink_class_obj, f"resolve_{feature}"
                        )
                        step = (
                            f"{step_type}_step_resolve_{feature}",
                            step_func,
                            step_value,
                        )
                    else:
                        step = (f"{step_type}_step_resolve_{feature}", None, step_value)

                    if feature in OutputOperator.pre_do_step_features:
                        self.pre_steps[feature] = step
                    elif feature in OutputOperator.post_do_step_features:
                        self.post_steps[feature] = step

        writer_args = {}

        if ctx.type == "stream" and hasattr(
            outputConfig.sink_class_obj, "write_stream"
        ):
            self.writer_stream_func = getattr(
                outputConfig.sink_class_obj, "write_stream"
            )

        if outputConfig.write_type != "custom":
            try:
                writer_func = outputConfig.sink_class_obj.get_write_function(
                    outputConfig.write_type.value.lower()
                )
            except WriteFunctionNotImplementedError:
                logger.exception(
                    f"Write function is not defined for operator object for output {self.config.name} "
                )
                raise WriteFunctionNotImplementedError(
                    f"{outputConfig.write_type.value.lower()} write type function is not implemented"
                )

        else:
            writer_func = outputConfig.custom_function.execution_function

        writer_function_args = inspect.getfullargspec(writer_func).args
        if outputConfig.options is not None:
            option_values = outputConfig.options.model_dump()
            option_keys = list(option_values.keys())
            for argument in writer_function_args:
                if argument in option_keys:
                    writer_args[argument] = option_values[argument]

        if ctx.type == "stream":
            writer_args["trigger"] = (
                outputConfig.options.trigger.trigger
                if hasattr(outputConfig.options, "trigger")
                and hasattr(outputConfig.options.trigger, "trigger")
                else {"once": True}
            )
            writer_args["checkpointLocation"] = (
                ctx.metadata.state_location + f"/checkpoints/{outputConfig.name}"
                if (not hasattr(outputConfig.options, "checkpointLocation"))
                or (
                    hasattr(outputConfig.options, "checkpointLocation")
                    and getattr(outputConfig.options, "checkpointLocation") is None
                )
                else getattr(outputConfig.options, "checkpointLocation")
            )
        writer_args["table_name"] = self.config.path
        writer_args["user_metadata"] = json.dumps(
            {
                "pipeline_batch_id": self.context.batch_id,
                "pipeline_name": self.context.batch_name,
                "pipeline_output_name": self.config.name,
                "pipeline_output_load_type": self.config.write_type.value,
            }
        )

        self.writer_func = (writer_func, writer_args)

        #### Event management code to handle the input configuration event handler
        if outputConfig.events:
            for event in outputConfig.events:
                if event.consumers is not None:
                    for consumer in event.consumers:
                        self.context.event_manager.subscribe(
                            event.type, get_event_handler_function(consumer)
                        )
                self.events[event.type] = event.handle
        ####
        logger.info(
            f"Completed the initializing the ouput operator object for ouput {self.config.name} "
        )

    @execute_step("OutputResult")
    def pre_step_resolve_create_data_object_if_not_exists(
        self, df: DataFrame, create_data_object_func: Type[F], value: Any
    ):
        """This function resolve the change data capture feature"""

        create_data_object_func(
            self.context.spark,
            df,
            self.config.path,
            self.config.sink_type,
            value,
            self.config.options,
        )

    @execute_step("OutputResult")
    def execute_write(self, df: DataFrame):
        """This function will execute the write function of the source"""

        self.options = (
            self.config.options.model_dump()
            if not isinstance(self.config.options, dict)
            else self.config.options
        )
        self.options["format"] = self.config.sink.value.lower()
        if self.config.sink_type == SinkType.file:
            self.options["file"] = True

        else:
            self.options["table"] = True

        if self.type == "stream":
            if "extra_options" in self.options:
                if self.options["extra_options"] is not None:
                    self.options["extra_options"]["persist_dataset"] = True
                elif self.options["extra_options"] is None:
                    self.options["extra_options"] = {"persist_dataset": True}
            else:
                self.options["extra_options"] = {"persist_dataset": True}
        try:
            self.context.spark.sparkContext.setJobGroup(
                f"output_{self.config.name}",
                description=f"This is the job group where all jobs related to output {self.config.name} are executed",
            )
        except Exception as e:
            logger.warning(
                f"Failed to set the spark job group for output {self.config.name} failed with error {e}"
            )
        if self.type == "stream" and self.writer_stream_func is not None:
            query_result = self.writer_stream_func(
                name=self.config.name,
                target_df=df,
                trigger=self.writer_func[1]["trigger"],
                checkpoint_location=self.writer_func[1]["checkpointLocation"],
                writer_function=self.writer_func[0],
                options={
                    k: v
                    for k, v in self.writer_func[1].items()
                    if k not in ["trigger", "checkpointLocation"]
                },
            )
        elif self.type == "stream":
            logger.info(
                f"Starting the stream output query as a microbatch with the provided options : {self.options}"
            )
            query_result = (
                df.writeStream.queryName(self.config.name)
                .trigger(**self.writer_func[1]["trigger"])
                .option("checkpointLocation", self.writer_func[1]["checkpointLocation"])
                .foreachBatch(
                    lambda data, batchId: partial(
                        self.writer_func[0],
                        source_df=data,
                        batchId=batchId,
                        batchAppName=self.context.batch_name,
                        options=self.options,
                    )(
                        **{
                            k: v
                            for k, v in self.writer_func[1].items()
                            if k not in ["trigger", "checkpointLocation"]
                        }
                    )
                )
                .start()
            )
        else:
            query_result = partial(
                self.writer_func[0], source_df=df, options=self.options
            )(**self.writer_func[1])

        try:
            if self.type != "stream":
                self.context.spark.sparkContext._jsc.clearJobGroup()
        except Exception as e:
            logger.warning(
                f"Failed to clear the spark job group for output {self.config.name} failed with error {e}"
            )

        return query_result

    def execute(self, df):
        """This function is actual execution of the input operator"""

        # executing the presteps
        for pre, attr in self.pre_steps.items():
            logger.debug(
                f"Started the execution of the pre-step {pre} for output {self.config.name}"
            )
            pre_step_func = getattr(self, attr[0])
            self.result = pre_step_func(df, attr[1], attr[2])
            if self.result.exception:
                logger_write = logger.exception
                if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                    logger_write = logger.warning
                logger_write(
                    f"Exception for the pre-step stage {pre} for output {self.config.name} with exception {self.result.exception}"
                )
                return self.result
            logger.debug(
                f"Completed the execution of the pre-step {pre} for output {self.config.name} with result {self.result}"
            )

        logger.debug(
            f"Started the execution of the execution stage for output {self.config.name}"
        )
        self.result = self.execute_write(df)
        if self.result.exception:
            logger_write = logger.exception
            if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                logger_write = logger.warning
            logger_write(
                f"Exception for the execution stage for output {self.config.name} with exception {self.result.exception}"
            )
            return self.result
        logger.debug(
            f"Completed the execution of the execution stage for output {self.config.name} with result {self.result}"
        )

        for post, attr in self.post_steps.items():
            logger.debug(
                f"Started the execution of the post-step {post} for output {self.config.name}"
            )
            post_step_func = getattr(self, attr[0])
            self.result = post_step_func(attr[2])
            if self.result.exception:
                logger_write = logger.exception
                if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                    logger_write = logger.warning
                logger_write(
                    f"Exception for the post-step stage {post} for output {self.config.name} with exception {self.result.exception}"
                )
                return self.result
            logger.debug(
                f"completed the execution of the post-step {post} for output {self.config.name} with result {self.result}"
            )

        return self.result
