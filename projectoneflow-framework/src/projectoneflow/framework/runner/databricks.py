from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookOutput, SqlOutput
from databricks.sdk.service import jobs
from projectoneflow.framework.contract.config import DatabricksServerDetails
from projectoneflow.framework.runner import JobOutput, TaskOutput, JobError
from projectoneflow.framework.exception.runner import DatabricksJobRunFetchError
from projectoneflow.framework.runner import PipelineRunner
from projectoneflow.framework.connector.databricks import DatabricksConnector
from pydantic import Field
from typing import List, Optional, Any, Union
import datetime


class DatabricksJobError(JobError):
    """This is the class definition for the job error result"""

    error: str = Field(..., description="This is the job error description")


class DatabricksTaskOutput(TaskOutput):
    """This is the class definition for the task output"""

    name: str = Field(..., description="This is the task name for the databricks task")
    end_time: Optional[int] = Field(
        None, description="This is the run end time for the databricks job task"
    )
    start_time: Optional[int] = Field(
        None, description="This is the run start time for the databricks job task"
    )
    result: Optional[Any] = Field(
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

        result = f"""{'='*10}\nTask name: {self.name}\n"""
        if self.error is not None:
            result += f"""Task failed with error: {self.error}\n"""
        else:
            if isinstance(self.result, NotebookOutput):
                result += f"""Notebook result: {self.result.result}\n"""
            elif isinstance(self.result, SqlOutput):
                result += f"""Sql result: {self.result.query_output}\n"""
        result += f"""Tasks completed execution at {datetime.datetime.fromtimestamp(self.end_time/1000)}\n{'='*10}\n"""
        return result


class DatabricksJobOutput(JobOutput):
    """This is the class definition for the job output"""

    name: str = Field(..., description="This is the job name for the databricks job")
    job_id: Union[str, int] = Field(
        ..., description="This is the job id for the databricks job"
    )
    run_id: Union[str, int] = Field(
        ..., description="This is the run id for the databricks job"
    )
    run_page_url: str = Field(
        ..., description="This is the run page url  databricks job"
    )
    start_time: Optional[int] = Field(
        None, description="This is the run start date time for the databricks job"
    )
    end_time: Optional[int] = Field(
        None, description="This is the run end time for the databricks job"
    )
    error: Optional[str] = Field(
        None, description="This is the run error for the databricks job"
    )
    tasks: Optional[List[DatabricksTaskOutput]] = Field(
        [], description="This is the list of the tasks for the databricks job"
    )

    def __str__(self):
        """This method is the override of the string representation of the class"""
        return self.to_string()

    def to_string(self):
        """This is the method to return the string representation of the databricks task result"""
        result = f"""Job {self.name} with job_id:{self.job_id}\n\tRun Details:\n\t\t Run Url:{self.run_page_url}\n\t\t Run Start Time:{datetime.datetime.fromtimestamp(self.start_time/1000)}\n\t\t Run End Time:{datetime.datetime.fromtimestamp(self.end_time/1000)}\n"""
        if len(self.tasks) > 0:
            result += """Tasks Result:\n"""
            for task in self.tasks:
                result += task.to_string()
        return result

    def error_message(self):
        """This is the error message to return in string representation"""
        error_message = f"Job {self.name} with job_id:{self.job_id} and Run can be found at {self.run_page_url} started at {datetime.datetime.fromtimestamp(self.start_time/1000)} and completed at {datetime.datetime.fromtimestamp(self.end_time/1000)} failed "
        if self.error is not None:

            error_message += f" with error {self.error}\n"
            if len(self.tasks) > 0:
                error_message += """Tasks Errors:\n"""
                for task in self.tasks:
                    if task.error is not None:
                        error_message += task.to_string()
        return error_message


class DatabricksRunner(PipelineRunner, DatabricksConnector):
    """This class is the databricks runing the pipeline"""

    def run(self, job_id: str, timeout: int = 10):
        """
        This method used to run the job with specified job id

        Parameters
        ----------------
        job_id: str
            this is job/workflow id used for running in databricks
        """
        waiter = self.client.jobs.run_now(job_id=job_id)
        err = None
        try:
            run = waiter.result(timeout=datetime.timedelta(minutes=timeout))
        except Exception as e:
            err = e

        if err:
            return DatabricksJobError(error=f"{err}")
        if run.state.life_cycle_state == jobs.RunLifeCycleState.SKIPPED:
            return DatabricksJobOutput(
                name=run.run_name,
                job_id=run.job_id,
                run_id=run.run_id,
                run_page_url=run.run_page_url,
                start_time=run.start_time,
                end_time=run.end_time,
                tasks=[],
                error=f"Run skipped: {run.state.state_message}",
            )

        match run.state.result_state:
            case jobs.RunResultState.CANCELED:
                return DatabricksJobOutput(
                    name=run.run_name,
                    job_id=run.job_id,
                    run_id=run.run_id,
                    run_page_url=run.run_page_url,
                    start_time=run.start_time,
                    end_time=run.end_time,
                    tasks=[],
                    error=f"Run cancelled: {run.state.state_message}",
                )

            case jobs.RunResultState.FAILED:
                job_output = DatabricksJobOutput(
                    name=run.run_name,
                    job_id=run.job_id,
                    run_id=run.run_id,
                    run_page_url=run.run_page_url,
                    start_time=run.start_time,
                    end_time=run.end_time,
                    tasks=[],
                    error=f"Run Failed: {run.state.state_message}",
                )
                for task in run.tasks:
                    task_result = self.client.jobs.get_run_output(task.run_id)
                    if task_result.error is not None:
                        job_output.tasks.append(
                            DatabricksTaskOutput(
                                name=task.task_key,
                                end_time=task.end_time,
                                start_time=task.start_time,
                                result=task_result,
                                error=task_result.error,
                            )
                        )
                return job_output
            case jobs.RunResultState.SUCCESS:
                job_output = DatabricksJobOutput(
                    name=run.run_name,
                    job_id=run.job_id,
                    run_id=run.run_id,
                    run_page_url=run.run_page_url,
                    start_time=run.start_time,
                    end_time=run.end_time,
                    tasks=[],
                )

                for task in run.tasks:
                    task_result = self.client.jobs.get_run_output(task.run_id)
                    job_output.tasks.append(
                        DatabricksTaskOutput(
                            name=task.task_key,
                            end_time=task.end_time,
                            start_time=task.start_time,
                            result=task_result,
                            error=task_result.error,
                        )
                    )

                return job_output
            case jobs.RunResultState.TIMEDOUT:
                return DatabricksJobOutput(
                    name=run.run_name,
                    job_id=run.job_id,
                    run_id=run.run_id,
                    run_page_url=run.run_page_url,
                    start_time=run.start_time,
                    end_time=run.end_time,
                    tasks=[],
                    error=f"Run Timedout: {run.state.state_message}",
                )

        return None

    def get_job_output(self, run_id: str):
        """
        This method used to get the job result

        Parameters
        ---------------
        run_id: str
            this is job
        """
        err = None
        try:
            job_run = self.client.jobs.get_run(run_id=run_id)
        except Exception as e:
            err = DatabricksJobRunFetchError(
                f"Problem with getting the status of the run {run_id}, failed with error: {e}"
            )
            return None, err

        job_output = DatabricksJobOutput(
            name=job_run.run_name,
            job_id=job_run.job_id,
            run_id=job_run.run_id,
            run_page_url=job_run.run_page_url,
            start_time=job_run.start_time,
            end_time=job_run.end_time,
            tasks=[],
        )

        for task in job_run.tasks:
            task_result = self.client.jobs.get_run_output(task.run_id)
            job_output.tasks.append(
                DatabricksTaskOutput(
                    name=task.task_key,
                    end_time=task.end_time,
                    start_time=task.start_time,
                    result=task_result,
                    error=task_result.error,
                )
            )

        return job_output, err
