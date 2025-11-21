from typing import Type, Tuple, Any, List
from oneflow.core.types import C
from oneflow.core.schemas.deploy import (
    PipelineConfig,
    PipelineTaskTypes,
    PipelineTypes,
)
from oneflow.core.schemas.execution import SparkExecutionTypes
from oneflow.framework.validation import Run, Check, ResultEnum
import re
import importlib
import ast
import inspect

REQUIRED_PIPELINE_FOLDERS = set([])
OPTIONAL_PIPELINE_FOLDERS = set(["tasks", "docs", "tests", "execution"])
VALIDATIONS = {
    "pipeline": {"pipeline_name": "name", "pipeline_description": "description"},
    "spark_task": {
        "task_name": "name",
        "task_description": "description",
        "task_execution_type": "execution",
        "task_execution_dag": "execution",
        "task_cluster": "cluster",
    },
    "spark_pipeline_task": {
        "pipeline_task_name": "pipeline_name",
        "pipeline_task_name_dependency": "pipeline_name",
    },
}


class PipelineValidation:
    """This class is used to run the schema validation checks"""

    @staticmethod
    def check_pipeline_name(name: str, *args) -> Tuple:
        """This check validates where pipeline name is lower or not"""

        return name.islower(), None, ResultEnum.warning

    @staticmethod
    def check_pipeline_description(description: str, *args) -> Tuple:
        """This check validates where pipeline description shouldn't be empty and have atleast 15 characters"""

        return (
            (description is not None) and len(description) > 10,
            None,
            ResultEnum.warning,
        )

    @staticmethod
    def check_task_description(description: str, *args) -> Tuple:
        """This check validates where task description shouldn't be empty and have atleast 15 characters"""

        return (
            (description is not None) and len(description) > 10,
            None,
            ResultEnum.warning,
        )

    @staticmethod
    def check_task_name(name: str, *args) -> Tuple:
        """This check validates where pipeline name should be lower case"""

        return name.islower(), None, ResultEnum.warning

    @staticmethod
    def check_pipeline_task_name(pipeline_name: str, *args) -> Tuple:
        """This check validates where task name and pipeline name matches or not"""
        return (
            pipeline_name != args[5] if args[3] is None else True,
            "There is a cyclic dependency error detected, where pipeline name is referenced in spark pipeline task is same as task pipeline name. please check task configuration",
            ResultEnum.error,
        )

    @staticmethod
    def check_pipeline_task_name_dependency(pipeline_name: str, *args) -> Tuple:
        """This check validates where task pipeline name and pipeline name matches or not"""
        pipelines = []
        for i in args[4]:
            for j in i.split("||"):
                pipelines.append(j)
        return (
            pipeline_name in pipelines if args[3] is None else True,
            f"pipeline name {pipeline_name} referenced in spark pipeline task is not existing in current project contract pipelines {pipelines}, please provide the pipeline id or project pipeline name existing in project contract pipelines",
            ResultEnum.error,
        )

    @staticmethod
    def check_task_execution_type(execution: Any, *args) -> Tuple:
        """This check validates where task execution type should be module or not"""

        return execution.type == SparkExecutionTypes.module, None, ResultEnum.failed

    @staticmethod
    def check_task_cluster(cluster: str, *args) -> Tuple:
        """This check validates where task cluster name is defined in pipeline cluster list or not"""
        return (
            cluster in args[1] if cluster is not None else True,
            f"Cluster name {cluster} defined in task configuration is not defined in pipeline cluster configuration, where available cluster name are {list(args[1])}",
            ResultEnum.failed,
        )

    @staticmethod
    def check_task_execution_dag(execution: Any, *args) -> Tuple:
        """This check validates where task execution dag function can be resolvable or not and check any syntax issues"""
        pattern = r"execution\.(.*)"
        execution_source = re.match(pattern, execution.source)
        if execution_source is None:
            return (
                False,
                f"task execution dag module {execution.source} is not matching the pattern execution.*, it should be under folder execution folder where dag function module should be resolvable",
                ResultEnum.failed,
            )
        try:
            execution_module = importlib.import_module(
                f"project.{args[0]}.{execution.source}"
            )
        except Exception as e:
            return (
                False,
                f"task execution dag module {execution.source} can't be resolvable, please check the file should be under execution folder and any references to transform functions needs to be reference as pattern 'project.<project name>.<transform folder>'. check failed due to below error {e}",
                ResultEnum.failed,
            )

        try:
            func = getattr(execution_module, execution.name)
        except Exception as e:
            return (
                False,
                f"task execution dag function {execution.source}.{execution.name} can't be resolvable, it should be under pipeline execution folder. check failed due to below error {e}",
                ResultEnum.failed,
            )

        try:
            source_code = inspect.getsource(func)
            ast.parse(source_code)
        except OSError:
            return (
                False,
                f"Could not get source code for task execution dag function {execution.source}.{execution.name}.",
                ResultEnum.failed,
            )
        except SyntaxError as e:
            return (
                False,
                f"Having some syntax error while parsing the task execution dag function {execution.source}.{execution.name}, the syntax errors are {e}",
                ResultEnum.failed,
            )

        return True, None

    @classmethod
    def validate(
        cls: Type[C],
        run: Run,
        pipeline: PipelineConfig,
        docs: str,
        pipeline_module: str,
        name: str,
        object_location: str,
        pipelines: List[str] = [],
    ):
        """
        This class method validates all checks and returns the dictionary of the check results

        Parameters
        -----------------------
        run: Run
            run
        pipeline: PipelineConfig
            pipeline configuration to be validated
        name: str
            table name to be validated
        object_location: str
            table object where table object is stored
        """

        # run table validation checks
        for pipeline_validation, validation_attr in VALIDATIONS["pipeline"].items():
            check_name = f"check_{pipeline_validation}"
            cls_method = getattr(cls, check_name)
            attr = getattr(pipeline, validation_attr)
            result = cls_method(attr)
            run.append(
                Check(
                    name=check_name,
                    object_type="pipeline",
                    object_name=name,
                    description=cls_method.__doc__,
                    details=result[1],
                    result=result[2] if not result[0] else ResultEnum.passed,
                    location=object_location,
                )
            )

        if pipeline.type == PipelineTypes.spark:
            # task validation
            for task_name in pipeline.tasks:
                task = pipeline.tasks[task_name]
                for validation, validation_attr in VALIDATIONS[task.type.value].items():
                    check_name = f"check_{validation}"
                    cls_method = getattr(cls, check_name)
                    attr = getattr(task, validation_attr)
                    result = cls_method(
                        attr,
                        pipeline_module,
                        pipeline.clusters.keys(),
                        (
                            getattr(task, "pipeline_name")
                            if hasattr(task, "pipeline_name")
                            else None
                        ),
                        (
                            getattr(task, "pipeline_id")
                            if hasattr(task, "pipeline_id")
                            else None
                        ),
                        pipelines,
                        name,
                    )
                    run.append(
                        Check(
                            name=check_name,
                            object_type="task",
                            object_name=f"{name}:{task.name}",
                            description=cls_method.__doc__,
                            details=result[1],
                            result=result[2] if not result[0] else ResultEnum.passed,
                            location=object_location,
                        )
                    )

        return
