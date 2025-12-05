from projectoneflow.core.task import Task
from projectoneflow.core.schemas import ParentModel
from projectoneflow.core.schemas.refresh import (
    TaskRefreshPolicy as SparkTaskRefreshPolicy,
    TaskRefreshTypes as SparkTaskRefreshTypes,
)
from projectoneflow.core.schemas.input import SparkInput
from projectoneflow.core.schemas.output import SparkOutput
from projectoneflow.core.schemas.execution import SparkExecution
from projectoneflow.core.schemas.sources import SparkSourceExtractType
from projectoneflow.core.schemas.event import SubscribedEventHandleEnum
from projectoneflow.core.state.spark import SparkExecutionTaskState
from projectoneflow.core.secrets.spark_secret import SparkSecretManager, SECRETS_PATTERN
from projectoneflow.core.exception.validation import (
    SparkTaskValidationError,
    SparkTaskExecutionFunctionInitializationError,
    SparkTaskInputInitializationError,
    SparkTaskOuputInitializationError,
)
from projectoneflow.core.exception.execution import (
    SparkTaskExecutionError,
    SparkTaskInputExecutionError,
    SparkTaskOutputExecutionError,
    SparkTaskExecutionFunctionError,
    SparkTaskCreationError,
    SparkTaskSuccessExecutionError,
)
from pyspark.sql import SparkSession
from projectoneflow.core.execution.operator.spark_task.input import InputOperator
from projectoneflow.core.execution.operator.spark_task.execution import ExecutorOperator
from projectoneflow.core.execution.operator.spark_task.output import OutputOperator
from projectoneflow.core.execution.spark_task import SparkTaskExecutionContext
import json
from projectoneflow.core.types import CO
from typing import Type, Dict
from datetime import datetime
import re
from projectoneflow.core.observability import Logger
from projectoneflow.core.runtime import Runtime

logger = Logger.get_logger(__name__)


class SparkTask(Task):
    """This class is a implemention for the spark task"""

    class Builder:
        """This class is builder class for the Spark Task"""

        def __init__(self):
            self.sparkConfig = {}
            self.input = {}
            self.output = {}
            self.execution = None
            self.name = None
            self.metadata_location = f"/tmp/{self.name}"
            self.secret_file = None
            self.secret_scope = None
            self.cxt = None
            self.refresh_policy = SparkTaskRefreshPolicy()
            self.listener = True

        def setSparkconfig(self, key, value):
            """This method sets the spark config which accepts the config key and value"""
            self.sparkConfig[key] = value
            return self

        def setSparkconfigs(self, values: Dict[str, str]):
            """This method sets the spark config which accepts the config key and value"""
            if isinstance(values, dict):
                for k, v in values.items():
                    self.sparkConfig[k] = v
            return self

        def setListener(self, setting: bool = True):
            """This method is to setup the spark listeners"""
            self.listener = setting if isinstance(setting, bool) else True
            return self

        def setName(self, name: str):
            """This method set the name of the object"""
            self.name = name
            return self

        def setInput(self, input: SparkInput):
            """This method sets the input to task input task"""
            if isinstance(input, SparkInput):
                self.input[input.name] = input
            elif (
                isinstance(input, list)
                and len(input) > 0
                and isinstance(input[0], SparkInput)
            ):
                for inc in input:
                    self.input[inc.name] = inc
            return self

        def setExecution(self, execution: SparkExecution):
            """This method sets the execution function to check execution for input task"""
            self.execution = execution
            return self

        def setOutput(self, output: SparkOutput):
            """This method sets the output for output task"""
            if isinstance(output, SparkOutput):
                self.output[output.name] = output
            elif (
                isinstance(output, list)
                and len(output) > 0
                and isinstance(output[0], SparkOutput)
            ):
                for out in output:
                    self.output[out.name] = out
            return self

        def setRefreshPolicy(self, policy: SparkTaskRefreshPolicy):
            """This method sets the refresh policy"""
            if policy is not None and isinstance(policy, SparkTaskRefreshPolicy):
                self.refresh_policy = policy
            return self

        def setSecret(self, secret_file: str = None, secret_scope: str = None):
            """This method sets the secret manager"""
            if secret_file is not None:
                self.secret_file = secret_file
            elif secret_scope is not None:
                self.secret_scope = secret_scope
            return self

        def setMetadataLog(self, path: str):
            """This method sets the metadata location"""
            if path is not None:
                self.metadata_location = path
            return self

        def format_secrets(self, config: Type[CO]) -> Type[CO]:
            """
            This method will format all options with any environment, secrets, pipeline variables
            This method should run after validation , initialization of task context

            Parameters
            --------------------
            config: Type[CO]
                This parameter is used for the configuration to be formatted

            Returns
            -------------------
            Type[CO]
                source configuration returned after transformation
            """

            def format_string(string):
                if string is not None:
                    string = string.replace(
                        "{{RangeStart}}", self.cxt.refresh_policy.range_start
                    ).replace("{{RangeEnd}}", self.cxt.refresh_policy.range_end)

                    value = re.findall(SECRETS_PATTERN, string)
                    for v in value:
                        s = v.replace("{{", "").replace("}}", "").split("/")
                        secret_scope = s[0]
                        secret_key = s[1]
                        string = string.replace(
                            v,
                            self.cxt.secret_manager.resolve(
                                scope=secret_scope, key=secret_key
                            ),
                        )

                return string

            def format_dictionary(value):
                for key in value:
                    if isinstance(value[key], str):
                        value[key] = format_string(value[key])
                    elif isinstance(value[key], ParentModel):
                        value[key] = self.format_secrets(value[key])
                    elif isinstance(value[key], list):
                        value[key] = format_list(value[key])
                    elif isinstance(value[key], dict):
                        value[key] = format_dictionary(value[key])
                return value

            def format_list(value):
                for idx, key in enumerate(value):
                    if isinstance(key, str):
                        value[idx] = format_string(key)
                    elif isinstance(key, dict):
                        value[idx] = format_dictionary(key)
                    elif isinstance(key, list):
                        value[idx] = format_list(key)
                    elif isinstance(key, ParentModel):
                        value[idx] = self.format_secrets(key)
                return value

            for option in config.model_fields.keys():
                if isinstance(getattr(config, option), str):
                    setattr(config, option, format_string(getattr(config, option)))
                elif isinstance(getattr(config, option), dict):
                    setattr(config, option, format_dictionary(getattr(config, option)))
                elif isinstance(getattr(config, option), list):
                    setattr(config, option, format_list(getattr(config, option)))
                elif isinstance(getattr(config, option), ParentModel):
                    setattr(
                        config,
                        option,
                        self.format_secrets(getattr(config, option)),
                    )
            return config

        def validations(self):
            """This method does the validation for the input source"""
            logger.info(
                "Running the validation on Input, Output, Execution, Spark Task Configuration Provided"
            )
            if self.name is None:
                logger.exception("Spark Task Name is required")
                raise SparkTaskValidationError(
                    "Spark Task name need to be defined for observability"
                )
            if not self.input:
                logger.exception("Spark Input Configuration is not set")
                raise SparkTaskValidationError(
                    "Spark Task requires the input to read from and process"
                )
            if not self.output:
                logger.exception("Spark Output Configuration is not set")
                raise SparkTaskValidationError(
                    "Spark Task requires the output which writes the processed data"
                )
            if self.execution is None:
                logger.exception(
                    "Spark execution Configuration is not set, need to be defined for processing the input data"
                )
                raise SparkTaskValidationError(
                    "Spark Task execution function configuration need to be defined for processing the input data"
                )

            ### check whether input stream and output stream validation check
            if any(
                [
                    self.input[input].source_extract_type
                    == SparkSourceExtractType.stream
                    for input in self.input
                ]
            ):
                if self.refresh_policy.type != SparkTaskRefreshTypes.stream:
                    logger.exception(
                        "One of the Input provided as stream but task refresh policy specified as not stream which is againt the constraint"
                    )
                    raise SparkTaskValidationError(
                        "One of the Input provided as stream but task refresh policy specified as not stream which is againt the constraint"
                    )

                inputs_count = len(self.input.keys())
                output_count = len(self.output.keys())

                if (inputs_count != output_count) or (
                    inputs_count > output_count and output_count != 1
                ):
                    logger.exception(
                        "No.of Input Sources and Output Sink is mismatching which is againt the constraint"
                    )
                    raise SparkTaskValidationError(
                        "No.of Input Sources and Output Sink is mismatching which is againt the constraint"
                    )

        def create(self):
            """This method creates the spark task object"""
            try:
                logger.info("Started intializing the Spark Task object")
                self.validations()
                logger.info("Initialized the spark session object")
                spark = (
                    SparkSession.builder.appName(self.name)
                    .config(map=self.sparkConfig)
                    .getOrCreate()
                )
                metadata = SparkExecutionTaskState(
                    spark=spark, state_directory=self.metadata_location
                )
                logger.info(
                    f"Initialized the spark task state manager which was configured to path {metadata.state_location}"
                )
                secret_manager = SparkSecretManager(
                    spark=spark, scope=self.secret_scope, secret_file=self.secret_file
                )
                logger.info(
                    f"Initialized the spark task secret manager {secret_manager}"
                )
                self.cxt = SparkTaskExecutionContext(
                    spark=spark,
                    name=self.name,
                    metadata=metadata,
                    refresh_policy=self.refresh_policy,
                    secret_manager=secret_manager,
                )
                logger.info(f"Initialized the spark task context manager")
                if self.listener:
                    self.cxt.set_spark_listeners()
                    logger.info("Initialized the spark task listeners")
                try:
                    input = {}
                    for name, config in self.input.items():
                        config = self.format_secrets(config)
                        input[name] = InputOperator(config, self.cxt)
                except Exception as e:
                    raise SparkTaskInputInitializationError(
                        f"Problem with initialization of the spark task input failed with error {e}"
                    )
                try:
                    output = {}
                    for name, config in self.output.items():
                        config = self.format_secrets(config)
                        output[name] = OutputOperator(config, self.cxt)
                except Exception as e:
                    raise SparkTaskOuputInitializationError(
                        f"Problem with initialization of the spark task output failed with error {e}"
                    )
                try:
                    execution = ExecutorOperator(
                        self.execution, self.cxt, list(output.keys())
                    )
                except Exception as e:
                    raise SparkTaskExecutionFunctionInitializationError(
                        f"Problem with initialization of the spark task execution function failed with error {e}"
                    )
                return SparkTask(
                    input=input,
                    output=output,
                    execution=execution,
                    name=self.name,
                    cxt=self.cxt,
                )
            except Exception as e:
                logger.error(f"Spark Task Creation failed because of the error {e}")
                if hasattr(self, "cxt"):
                    self.cxt.stop()
                raise SparkTaskCreationError(
                    f"Failed to create the spark task due to error {e}"
                )

    builder = Builder()

    def __init__(
        self,
        name: str,
        input: Dict[str, InputOperator],
        output: Dict[str, OutputOperator],
        execution: ExecutorOperator,
        cxt: SparkTaskExecutionContext,
    ):
        """
        This is the initialization method for the spark task

        Parameters
        -------------
        name: str
            Name of the spark task
        input: Dict
            spark input configuration
        output: Dict
            spark output Configuration
        execution: SparkExecutionFunction
            spark execution function
        ctx: SparkTaskExecutionContext
            spark context manager for managing the state, metadata

        """
        if (
            (not isinstance(input, Dict))
            or (not isinstance(output, Dict))
            or (not isinstance(execution, ExecutorOperator))
            or (not isinstance(cxt, SparkTaskExecutionContext))
        ):
            raise SparkTaskValidationError(
                "One of the arguments mismatching with the spark input state"
            )

        self.input = input
        self.output = output
        self.execution = execution
        self.context = cxt
        self.name = name

    def task_process(self):
        """This is helper methods which executes the inputs, execution, output_result"""
        input_results = {}
        logger.info("Started the Spark Task Inputs Execution step")
        for input in self.input:
            input_results[input] = self.input[input].execute()
            if input_results[input].exception:
                if (
                    input_results[input].event_severity
                    == SubscribedEventHandleEnum.STOP_WH_FAIL
                ):
                    logger.warning(
                        f"Spark Task input execution failed for input name {input} with error {input_results[input].exception} but not failing the spark task because of the setting STOP_WH_FAIL"
                    )
                    raise SparkTaskSuccessExecutionError(
                        f"Spark Task input execution failed for input name {input} with error {input_results[input].exception} but not failing the spark task because of the setting STOP_WH_FAIL"
                    )
                else:
                    logger.exception(
                        f"Spark Task input execution failed for input name {input} with error {input_results[input].exception}"
                    )
                    raise SparkTaskInputExecutionError(
                        f"Spark Task input execution failed for input name {input} with error {input_results[input].exception}"
                    )
            try:
                result = input_results[input]
                result.input_configuration = self.input[input]
                self.context.metadata.set(
                    f"sources", f"{input}_result", result.to_json()
                )
                self.context.metadata.set(
                    f"sources",
                    f"{input}_result_schema",
                    json.dumps(input_results[input].result.schema.json()),
                )
            except Exception as e:
                logger.warning(
                    f"Failed to write the {input} input state result to pipeline state folder, failed with issue {e}"
                )
        logger.info("Completed the Spark Task Inputs Execution step")

        logger.info("Started the Spark Task Execution step")
        input_results_process = {i: input_results[i].result for i in input_results}
        execution_results = self.execution.execute(input_results_process)

        if execution_results.exception:

            if (
                execution_results.event_severity
                == SubscribedEventHandleEnum.STOP_WH_FAIL
            ):
                logger.warning(
                    f"Spark Task execution function failed with error {execution_results.exception} but not failing the spark task because of the setting STOP_WH_FAIL"
                )
                raise SparkTaskSuccessExecutionError(
                    f"Spark Task execution function failed with error {execution_results.exception} but not failing the spark task because of the setting STOP_WH_FAIL"
                )
            else:
                logger.exception(
                    f"Spark Task execution function failed with error {execution_results.exception}"
                )
                raise SparkTaskExecutionFunctionError(
                    f"Spark Task execution function failed with error {execution_results.exception}"
                )
        logger.info("Completed the Spark Task Execution step")

        logger.info("Started the Spark Task Output Execution step")
        output_results = {}
        for output in self.output:

            output_execution_result = (
                execution_results.result[output]
                if isinstance(execution_results.result, dict)
                else execution_results.result
            )
            try:
                self.context.metadata.set(
                    f"sink",
                    f"{output}_result_schema",
                    json.dumps(output_execution_result.schema.json()),
                )
            except Exception as e:
                logger.warning(
                    f"Failed to write the {output} output state result to pipeline state folder, failed with issue {e}"
                )

            output_results[output] = self.output[output].execute(
                df=output_execution_result
            )

            if output_results[output].exception:
                if (
                    execution_results.event_severity
                    == SubscribedEventHandleEnum.STOP_WH_FAIL
                ):
                    raise SparkTaskSuccessExecutionError(
                        f"Spark Task output execution failed for output name {output} with error {output_results[output].exception} but not failing the spark task because of the setting STOP_WH_FAIL"
                    )
                else:
                    raise SparkTaskOutputExecutionError(
                        f"Spark Task output execution failed for output name {output} with error {output_results[output].exception}"
                    )
            try:
                self.context.metadata.set(
                    f"sink", f"{output}_result", output_results[output].to_json()
                )
            except Exception as e:
                logger.warning(
                    f"Failed to write the {output} output state result to pipeline state folder, failed with issue {e}"
                )
        logger.info("Completed the Spark Task Output Execution step")
        return input_results, execution_results, output_results

    def batch_process(self):
        """
        This method process the batch workload
            Steps
                1. Build the input sources object which initilization input with cdc
                2. Build the output sink object which initialization output
                3. Executes the input object and get the input dataframes
                4. after getting the input dataframes check the input dataframe names and arguments in execution function mataches else error
                5. execute the transformation function which returns the single dataframe or results in dictionary
                6. pass the result to output
                7. save the input incremental cdc state
        """

        logger.info(f"Started execution of batch process for spark task {self.name}")

        input_results, execution_results, output_results = self.task_process()

        logger.info(
            "Writing the input state like cdc, schema into target inputs pipeline state file"
        )
        for input in input_results:
            if (self.input[input].config.features is not None) and (
                self.input[input].config.features.change_data_feature is not None
            ):
                if (
                    self.context.refresh_policy.type
                    in [
                        SparkTaskRefreshTypes.incremental,
                        SparkTaskRefreshTypes.backfill,
                    ]
                    and hasattr(self.input[input], "cdc_state")
                    and self.input[input].cdc_state is not None
                ):
                    cdc_state = self.input[input].cdc_state
                    cdc_state.batch_id = self.context.batch_id
                    cdc_state.batch_name = self.context.batch_name
                    try:
                        self.context.metadata.set(
                            f"sources",
                            f"{input}_cdc_value",
                            json.dumps(cdc_state.to_json()),
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to write the {input} input cdc state result to pipeline state folder, failed with issue {e}"
                        )
        logger.info(f"Completed execution of batch process for spark task {self.name}")

    def stream_wait_close(self, streaming_queries: dict):
        """This method check for the streaming queries to complete"""
        while streaming_queries:
            temp_queries = streaming_queries.copy()
            for table in temp_queries:

                if temp_queries[table].isActive:
                    continue
                else:
                    streaming_queries.pop(table)

    def stream_process(self):
        """
        This method process the stream workload
            Steps
                1. Build the input sources object which initilization input with stream
                2. Build the output sink object which initialization output
                3. Executes the input object and get the input dataframes
                4. after getting the input dataframes check the input dataframe names and arguments in execution function mataches else error
                5. execute the transformation function which returns the single dataframe or results in dictionary
                6. pass the result to output
                7. get the stream query object and passed to stream_wait_close to loop through each query completion
        """
        streaming_queries = {}
        logger.info(f"Started execution of stream process for spark task {self.name}")
        input_results, execution_results, output_results = self.task_process()
        for output in output_results:
            streaming_queries[output] = output_results[output].result
        self.stream_wait_close(streaming_queries)
        logger.info(f"Completed execution of stream process for spark task {self.name}")

    def execute(self):
        """This is the execution funtion which get executes the workload process"""
        process_function = getattr(self, self.context.type + "_process")
        logger.info(f"Started the execution of spark task {self.name}")
        try:
            process_function()
            if self.context.refresh_policy.type == SparkTaskRefreshTypes.backfill:
                self.context.metadata.set(
                    "state",
                    "pipeline_backfill_load_timestamp",
                    json.dumps(
                        {
                            "batch_id": self.context.batch_id,
                            "batch_name": self.context.batch_name,
                            "load_type": "backfill",
                            "load_timestamp": f"{datetime.now()}",
                            "start_date": f"{self.context.refresh_policy.range_start}",
                            "end_date": f"{self.context.refresh_policy.range_end}",
                        }
                    ),
                )
            elif self.context.refresh_policy.type == SparkTaskRefreshTypes.stream:
                self.context.metadata.set(
                    "state",
                    "pipeline_stream_load_timestamp",
                    json.dumps(
                        {
                            "batch_id": self.context.batch_id,
                            "batch_name": self.context.batch_name,
                            "load_type": "stream",
                            "load_timestamp": f"{datetime.now()}",
                            "start_date": f"{self.context.refresh_policy.range_start}",
                            "end_date": f"{datetime.now()}",
                        }
                    ),
                )
            else:
                self.context.metadata.set(
                    "state",
                    "pipeline_incremental_load_timestamp",
                    json.dumps(
                        {
                            "batch_id": self.context.batch_id,
                            "batch_name": self.context.batch_name,
                            "load_type": "incremental",
                            "load_timestamp": f"{datetime.now()}",
                            "start_date": f"{self.context.refresh_policy.range_start}",
                            "end_date": f"{self.context.refresh_policy.range_end}",
                        }
                    ),
                )
            logger.info(
                f"Completed execution of {self.context.refresh_policy.type.value} process for spark task {self.name}"
            )

        except Exception as e:
            self.context.metadata.set(
                "state",
                f"pipeline_{self.context.type.lower()}_exception",
                json.dumps(
                    {
                        "batch_id": self.context.batch_id,
                        "batch_name": self.context.batch_name,
                        "load_type": f"{self.context.type.lower()}",
                        "load_timestamp": f"{datetime.now()}",
                        "start_date": f"{self.context.refresh_policy.range_start}",
                        "end_date": f"{self.context.refresh_policy.range_end}",
                        "exception": f"{e}",
                    }
                ),
            )
            if isinstance(e, SparkTaskSuccessExecutionError):
                logger.warning(f"Spark Task Execution failed with error {e}")
            else:
                logger.exception(f"Spark Task Execution failed with error {e}")
                raise SparkTaskExecutionError(
                    f"Spark Task Execution failed with error {e}"
                )
        finally:
            self.context.stop()
            Runtime().cleanup()
