from oneflow.framework.runner import PipelineRunner, JobOutput, TaskOutput
from oneflow.framework.contract.config.objects import PipelineContractObject
from oneflow.core.schemas import ParentEnum, ParentModel
from oneflow.core.schemas.deploy import PipelineTaskTypes
from oneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
from oneflow.framework.exception.runner import LocalJobRunFetchError
from pydantic import Field
import datetime
import time
from typing import Optional, List, Any, Dict
import json
import subprocess
import os
import sys
import tempfile
import uuid
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from oneflow.framework.contract.env import Environment
from oneflow.framework.utils import remove_color_codes
from oneflow.core.schemas.sources import SinkType

MAX_WORKERS = int(os.cpu_count() * (2 / 3))
TASK_LOG_PATH = os.path.join(tempfile.gettempdir(), "tasks", "logs")
JOB_FAILED_MESSAGE = "Job Failed because one of the task failed"
THREE_DOT_NAMESPACE = r"(\w+\.\w+\.\w+)"


REF_EXTERNAL_PATTERN = r"#ref\(([^,]+,[^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\)"


def replace_ref_external_object(string):
    """This method will returns terraform databricks object"""
    match = re.match(REF_EXTERNAL_PATTERN, string)
    if match is not None:
        ref_file = match.group(3)
        return ref_file
    else:
        return string


REF_OBJECT_PATTERN = {REF_EXTERNAL_PATTERN: replace_ref_external_object}


class LocalTaskStatus(ParentEnum):
    """This is the enum definition for the possible definition local task result status"""

    failed = "FAILED"
    success = "SUCCESS"
    running = "RUNNING"
    skipped = "SKIPPED"
    not_started = "NOT STARTED"


class LocalTaskConfig(ParentModel):
    """This is schema definition for the local task configuration"""

    config: Any = Field(..., description="Configuration fo the local task")
    status: LocalTaskStatus = (
        Field(LocalTaskStatus.not_started, description="task status"),
    )
    dependencies: Optional[List[str]] = Field([], description="dependent tasks")


class LocalTaskOutput(TaskOutput):
    """This class is the schema definition for the task output"""

    name: str = Field(..., description="This is the task name for the databricks task")
    end_time: Optional[float] = Field(
        None, description="This is the run end time for the databricks job task"
    )
    start_time: Optional[float] = Field(
        None, description="This is the run start time for the databricks job task"
    )
    result_file: Optional[Any] = Field(
        None, description="This is the result of the databricks task"
    )
    error: Optional[str] = Field(
        None, description="This is the error description for the databricks task output"
    )

    def __str__(self):
        """This method is the override of the string representation of the class"""
        return self.to_string()

    def to_string(self):
        """This is the method to return the string representation of the databricks task result"""

        result = f"""{'='*10}\nTask name: {self.name}\nTask Start Time:{datetime.datetime.fromtimestamp(self.start_time)}\n"""
        if self.error is not None:
            if self.result_file is not None:
                result += f"""Task failed Log File:{self.result_file}\n"""
            else:
                result += f"""Task failed with error: {self.error}\n"""
        else:
            result += f"""Task Result Log File: {self.result_file}\n"""
        result += f"""Tasks completed execution at {datetime.datetime.fromtimestamp(self.end_time)}\n{'='*10}\n"""
        return result


def task_executor(
    task_configuration: Dict[str, Any], task_type: str, env: Dict[str, str]
):
    """This method is used to execute the task with provided configuration and type"""
    start_time = time.time()
    cmd = [
        "python",
        "-m",
        "oneflow.core.task.cli",
        "--task_configuration",
        json.dumps(task_configuration),
        "--task_type",
        task_type,
    ]
    task_exec = subprocess.Popen(
        cmd,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
        cwd=task_configuration["extra_spark_configuration"].get(
            "spark.sql.warehouse.dir", tempfile.gettempdir()
        ),
    )
    out, err = task_exec.communicate()
    ret_code = task_exec.returncode
    end_time = time.time()
    out = remove_color_codes(out.decode())
    err = remove_color_codes(err.decode())
    task_output = LocalTaskOutput(
        name=task_configuration["name"], start_time=start_time, end_time=end_time
    )
    task_file = os.path.join(TASK_LOG_PATH, f"{uuid.uuid1().hex}.log")
    if ret_code != 0:
        if (out is not None) or (err is not None):
            result_data = ""
            if out is not None:
                result_data = f"{result_data}\nSTDOUTPUT: {out}"
            if err is not None:
                result_data = f"{result_data}\nSTDERROR: {err}"
            with open(task_file, "w") as f:
                f.write(result_data)
            task_output.result_file = task_file

        task_output.error = err
    else:

        with open(task_file, "w") as f:
            f.write(out)
        task_output.result_file = task_file
    return task_output


class LocalJobOutput(JobOutput):
    """This class is the schema definition for the job output"""

    name: str = Field(..., description="Name of the job")
    start_time: float = Field(..., description="start time of the job")
    end_time: float = Field(..., description="end time of the job")
    error: Optional[str] = Field(
        None, description="This is the run error for the databricks job"
    )
    tasks: Optional[List[LocalTaskOutput]] = Field(
        [], description="This is the list of the tasks for the databricks job"
    )

    def __str__(self):
        """This method is the override of the string representation of the class"""
        return self.to_string()

    def to_string(self):
        """This is the method to return the string representation of the databricks task result"""

        result = f"""\nJob {self.name}:\n\tRun Details:\n\t\t Run Start Time:{datetime.datetime.fromtimestamp(self.start_time)}\n\t\t Run End Time:{datetime.datetime.fromtimestamp(self.end_time)}\n"""
        if len(self.tasks) > 0:
            result += """Tasks Result:\n"""
            for task in self.tasks:
                result += task.to_string()
        return result

    def error_message(self):
        """This is the error message to return in string representation"""
        error_message = f"\nJob {self.name} started at {datetime.datetime.fromtimestamp(self.start_time)} and completed at {datetime.datetime.fromtimestamp(self.end_time)} failed "
        if self.error is not None:
            if len(self.tasks) > 0:
                error_message += """Tasks Errors:\n"""
                for task in self.tasks:
                    if task.error is not None:
                        error_message += task.to_string()
        return error_message


class LocalRunner(PipelineRunner):
    """This class is used for running the pipeline locally"""

    def __init__(self):
        """This method is used for the initialization"""
        super().__init__()

        if not os.path.exists(TASK_LOG_PATH):
            os.makedirs(name=TASK_LOG_PATH, mode=777, exist_ok=True)

    def resolve_references(self, configuration: Dict[str, Any]):
        """This method solves the reference in the configuration"""

        def string_format_variables(str_object):
            for pattern in REF_OBJECT_PATTERN:
                reference_match = re.match(pattern, str_object)
                if reference_match is not None:
                    replacable_string = reference_match.group(0)
                    replaced_string = REF_OBJECT_PATTERN[pattern](replacable_string)
                    str_object = str_object.replace(replacable_string, replaced_string)
            return str_object

        def list_formatting_variables(list_object):
            local_result = []
            for ele in list_object:
                if isinstance(ele, dict):
                    local_result.append(self.resolve_references(ele))
                elif isinstance(ele, str):
                    ele = string_format_variables(ele)
                    local_result.append(ele)
                elif isinstance(ele, list):
                    local_result.extend(list_formatting_variables(ele))
            return local_result

        for option in configuration.keys():
            key_option = option
            if isinstance(configuration[key_option], str):
                configuration[key_option] = string_format_variables(
                    configuration[key_option]
                )
            elif isinstance(configuration[key_option], dict):
                configuration[key_option] = self.resolve_references(
                    configuration[key_option],
                )
            elif isinstance(configuration[key_option], list):
                configuration[key_option] = list_formatting_variables(
                    configuration[key_option]
                )
        return configuration

    def run(self, pipeline: PipelineContractObject):
        """This method is used to run the job in local mode"""
        environment = Environment()
        pipeline_type = pipeline.type
        task_config_type = pipeline_type.value.split("_")[0]
        tasks = {}
        not_executed_tasks = set()
        task_outputs = {}
        pipeline_configuration = pipeline.deploy_asset()
        if pipeline_configuration is None:
            raise LocalJobRunFetchError(
                "Provided pipeline configuration deployable object is not produced, please check the provided pipeline configuration"
            )

        ###
        ### setting local warehouse directory to place the data and setting the default catalog and schema
        spark_warehouse_dir = os.path.abspath(
            os.path.join(
                environment.DF_MODE_RUN_LOCAL_SPARK_CATALOG_LOCATION,
                "spark_catalog",
            )
        )
        for task in pipeline_configuration.tasks:
            task_configuration = pipeline_configuration.tasks[task].to_json()
            if pipeline_configuration.tasks[task].type == PipelineTaskTypes.spark_task:
                if environment.DF_MODE_RUN_LOCAL_SECRET_FILE is not None:
                    task_configuration["secret_file_path"] = os.path.abspath(
                        environment.DF_MODE_RUN_LOCAL_SECRET_FILE
                    )
                if environment.DF_MODE_RUN_PIPELINE_STATE_PREFIX:
                    task_configuration["metadata_location_path"] = (
                        f"{environment.DF_MODE_RUN_PIPELINE_STATE_PREFIX}/{pipeline_configuration.name}/{pipeline_configuration.tasks[task].name}"
                    )
                    if (
                        pipeline_configuration.tasks[task].refresh_policy.type
                        == SparkTaskRefreshTypes.stream
                    ):
                        for index, output in enumerate(task_configuration["output"]):
                            out = output.copy()
                            out["options"][
                                "checkpointLocation"
                            ] = f"{environment.DF_MODE_RUN_PIPELINE_STATE_PREFIX}/{pipeline_configuration.name}/{pipeline_configuration.tasks[task].name}/checkpoint/{out['name']}"

                            task_configuration["output"][index] = out
                ### Temporary solution for the eliminating the three dot namespace because currently spark doesn't support that and in local mode its harder to create schemas so mentioning the schema as the folder path
                for index, output in enumerate(task_configuration["output"]):
                    out = output.copy()
                    if (
                        (out["sink_type"] == SinkType.table.value)
                        and (out["sink"] in ["delta"])
                        and (("/" not in out["path"]) and ("\\" not in out["path"]))
                    ):
                        three_dot = re.findall(THREE_DOT_NAMESPACE, out["path"])
                        if len(three_dot) > 0:
                            namespaces = out["path"].split(".")
                            out["path"] = f"default.{namespaces[1]}_{namespaces[2]}"
                        else:
                            namespaces = out["path"].split(".")
                            out["path"] = f"default.{namespaces[0]}_{namespaces[1]}"
                    if (out["features"] is not None) and (
                        out["features"].get("create_data_object_if_not_exists", None)
                        is not None
                    ):
                        if (
                            out["features"]["create_data_object_if_not_exists"].get(
                                "table", None
                            )
                            is not None
                        ):
                            out["features"]["create_data_object_if_not_exists"][
                                "table"
                            ][
                                "table_name"
                            ] = f'{out["features"]["create_data_object_if_not_exists"]["table"]["schema_name"]}_{out["features"]["create_data_object_if_not_exists"]["table"]["table_name"]}'
                            out["features"]["create_data_object_if_not_exists"][
                                "table"
                            ]["schema_name"] = "default"
                            out["features"]["create_data_object_if_not_exists"][
                                "table"
                            ]["catalog"] = None
                    task_configuration["output"][index] = out

                if not os.path.exists(spark_warehouse_dir):
                    os.makedirs(spark_warehouse_dir, mode=777, exist_ok=True)
                task_configuration["extra_spark_configuration"][
                    "spark.sql.defaultCatalog"
                ] = "spark_catalog"
                task_configuration["extra_spark_configuration"][
                    "spark.sql.catalog.spark_catalog.defaultDatabase"
                ] = "default"
                task_configuration["extra_spark_configuration"][
                    "spark.sql.warehouse.dir"
                ] = spark_warehouse_dir
                task_configuration["extra_spark_configuration"][
                    "spark.sql.catalogImplementation"
                ] = "hive"
                ### resolve references
                task_configuration = self.resolve_references(task_configuration)
                ###
                tasks[task] = LocalTaskConfig(
                    config=task_configuration,
                    dependecies=pipeline_configuration.tasks[task].depends_on,
                    status=LocalTaskStatus.not_started,
                )
                not_executed_tasks.update([task])
            elif (
                pipeline_configuration.tasks[task].type
                == PipelineTaskTypes.spark_pipeline_task
            ):
                tasks[task] = LocalTaskConfig(
                    config=task_configuration,
                    dependecies=pipeline_configuration.tasks[task].depends_on,
                    status=LocalTaskStatus.skipped,
                )
                task_outputs[task] = LocalTaskOutput(
                    name=task,
                    start_time=0,
                    end_time=0,
                    error="This Task is Skipped because currently running the child pipeline is not supported in local runner",
                )
        start_time = time.time()

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while len(not_executed_tasks) > 0:
                current_batch = set()
                for task in not_executed_tasks:
                    if tasks[task].status == LocalTaskStatus.not_started:
                        if (tasks[task].config["depends_on"] is not None) and len(
                            tasks[task].config["depends_on"]
                        ) > 0:
                            if any(
                                [
                                    tasks[d].status == LocalTaskStatus.failed
                                    for d in tasks[task].config["depends_on"]
                                ]
                            ):
                                tasks[task].status = LocalTaskStatus.failed
                                task_outputs[task] = LocalTaskOutput(
                                    name=task,
                                    start_time=0,
                                    end_time=0,
                                    error="This Task is Skipped because of failure of the dependent tasks",
                                )
                            elif all(
                                [
                                    tasks[d].status == LocalTaskStatus.success
                                    for d in tasks[task].config["depends_on"]
                                ]
                            ):
                                current_batch.update([task])
                        else:
                            current_batch.update([task])

                if environment.DF_MODE_RUN_LOCAL_TASK_PARALLEL:
                    current_run_batch = current_batch
                else:
                    current_run_batch = [[i] for i in current_batch]
                for batch in current_run_batch:
                    futures = {
                        executor.submit(
                            task_executor,
                            tasks[task].config,
                            task_config_type,
                            {
                                **os.environ,
                                **{
                                    "PYTHONPATH": os.environ.get("PYTHONPATH", "")
                                    + ":".join(sys.path)
                                },
                            },
                        ): task
                        for task in batch
                    }

                    for future in as_completed(futures):

                        task = futures[future]

                        task_output = future.result()

                        tasks[task].status = (
                            LocalTaskStatus.failed
                            if task_output.error is not None
                            else LocalTaskStatus.success
                        )

                        task_outputs[task] = task_output
                        current_batch.remove(task)
                        not_executed_tasks.remove(task)

        end_time = time.time()

        job_output = LocalJobOutput(
            name=pipeline_configuration.name,
            start_time=start_time,
            end_time=end_time,
            tasks=task_outputs.values(),
        )
        if any(tasks[i].status == LocalTaskStatus.failed for i in tasks):
            job_output.error = JOB_FAILED_MESSAGE
        return job_output
