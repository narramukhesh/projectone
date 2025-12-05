from oneflow.core.execution.spark_task.context import SparkTaskExecutionContext
from oneflow.core.observability.logging import Logger
from pyspark.sql import DataFrame
from typing import List
from oneflow.core.execution.operator import Operator, execute_step
from oneflow.core.schemas.execution import SparkExecution, SparkExecutionTypes
from oneflow.core.schemas.event import SubscribedEventHandleEnum
from oneflow.core.event import get_event_handler_function
from oneflow.core.execution.spark_task import SparkExecutionFunction
from oneflow.core.exception.execution import (
    ExecutionFuncInitializeError,
    SparkTaskExecutionFunctionArgsMismatch,
)
from oneflow.core.exception.validation import SparkTaskResultError

logger = Logger.get_logger(__name__)


class ExecutorOperator(Operator):
    pre_do_step_features = ["validate_input_args"]
    post_do_step_features = ["validate_output_args"]

    def __init__(
        self,
        executionConfig: SparkExecution,
        ctx: SparkTaskExecutionContext,
        output: List[str],
    ):
        self.config = executionConfig
        self.context = ctx
        self.pre_steps = {}
        self.post_steps = {}
        self.events = {}
        self.output = set(output)
        logger.info(
            f"Initializing the Execution operator object for execution config {self.config} "
        )
        if executionConfig.features:
            for feature in executionConfig.features.model_dump():
                step_value = getattr(executionConfig.features, feature)
                if step_value is not None:
                    if feature in ExecutorOperator.pre_do_step_features:
                        step_type = "pre"
                    else:
                        step_type = "post"

                    step = (f"{step_type}_step_resolve_{feature}", None, step_value)

                    if feature in ExecutorOperator.pre_do_step_features:
                        self.pre_steps[feature] = step
                    elif feature in ExecutorOperator.post_do_step_features:
                        self.post_steps[feature] = step
        try:
            if executionConfig.type == SparkExecutionTypes.body:
                self.execution_function = SparkExecutionFunction(
                    func_name=self.config.name,
                    func_body=self.config.source,
                    extra_arguments=self.config.extra_arguments,
                )
            elif executionConfig.type == SparkExecutionTypes.module:
                self.execution_function = SparkExecutionFunction(
                    func_name=self.config.name,
                    func_module=self.config.source,
                    extra_arguments=self.config.extra_arguments,
                )
            elif executionConfig.type == SparkExecutionTypes.file:
                self.execution_function = SparkExecutionFunction(
                    func_name=self.config.name,
                    func_file=self.config.source,
                    extra_arguments=self.config.extra_arguments,
                )
            else:
                self.execution_function = SparkExecutionFunction(
                    func_name=self.config.name,
                    func_module=self.config.source,
                    extra_arguments=self.config.extra_arguments,
                )
        except Exception as e:
            logger.exception(
                f"Execution function Initialization error due to error {e}",
                exc_info=False,
            )
            raise ExecutionFuncInitializeError(
                f"Execution function Initialization error as specified in {e}"
            )

        #### Event management code to handle the input configuration event handler
        if executionConfig.events:
            for event in executionConfig.events:
                if event.consumers is not None:
                    for consumer in event.consumers:
                        self.context.event_manager.subscribe(
                            event.type, get_event_handler_function(consumer)
                        )
                self.events[event.type] = event.handle
        ####

        logger.info(
            f"Completed the initializing the Executor operator object for executor config {self.config.name} "
        )

    @execute_step("ExecutionResult")
    def pre_step_resolve_validate_input_args(self, dfs, value):
        if value.check:
            input_names = set([input for input in dfs])
            execution_fn_args = set(self.execution_function.execution_function_args)
            execution_fn_default_args = set(
                self.execution_function.execution_function_args_defaults.keys()
            )
            if (len(execution_fn_args - input_names) > 0) and (
                len(execution_fn_args - input_names - execution_fn_default_args)
            ) > 0:

                raise SparkTaskExecutionFunctionArgsMismatch(
                    "Mismatch of the execution function arguments with input names"
                )
        return dfs

    @execute_step("ExecutionResult")
    def post_step_resolve_validate_output_args(self, results, value):
        if value.check:
            if (not isinstance(results, DataFrame)) and (not isinstance(results, dict)):
                raise SparkTaskResultError("Error Not matching the output type")

            if isinstance(results, dict):
                if len(set(results.keys()) - self.output) > 0:
                    raise SparkTaskResultError(
                        "Output keys provided not matching returned keys"
                    )
        return results

    @execute_step("ExecutionResult")
    def execute_function(self, dfs):
        """This is used for executing the execution function"""
        result = self.execution_function.execution_function(
            **{
                **{
                    i: dfs.get(i, None)
                    for i in self.execution_function.execution_function_args
                },
                **self.execution_function.execution_function_args_defaults,
                **self.execution_function.execution_function_kwargs_defaults,
            }
        )
        return result

    def execute(self, dfs):
        """This function is actual execution of the execution operator"""
        # executing the presteps
        for pre, attr in self.pre_steps.items():
            logger.debug(
                f"Started the execution of the pre-step {pre} for excecution step"
            )
            pre_step_func = getattr(self, attr[0])
            self.result = pre_step_func(dfs, attr[2])
            if self.result.exception:
                logger_write = logger.exception
                if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                    logger_write = logger.warning
                logger_write(
                    f"Exception for the pre-step stage {pre} for execution step with exception {self.result.exception}"
                )
                return self.result
            logger.debug(
                f"Completed the execution of the pre-step {pre} for execution step with result {self.result}"
            )

        logger.debug(f"Started the execution of the execution stage for execution step")
        self.result = self.execute_function(dfs)

        if self.result.exception:
            logger_write = logger.exception
            if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                logger_write = logger.warning
            logger_write(
                f"Exception for the execution stage for execution step with exception {self.result.exception}"
            )
            return self.result
        logger.debug(
            f"Completed the execution of the execution stage for execution step with result {self.result}"
        )

        for post, attr in self.post_steps.items():
            logger.debug(
                f"Started the execution of the post-step {post} for execution step"
            )
            post_step_func = getattr(self, attr[0])
            self.result = post_step_func(self.result.result, attr[2])
            if self.result.exception:
                logger_write = logger.exception
                if self.result.event_severity == SubscribedEventHandleEnum.STOP_WH_FAIL:
                    logger_write = logger.warning
                logger_write(
                    f"Exception for the post-step stage {post} for execution step with exception {self.result.exception}"
                )
                return self.result
            logger.debug(
                f"completed the execution of the post-step {post} for  execution step with result {self.result}"
            )

        return self.result
