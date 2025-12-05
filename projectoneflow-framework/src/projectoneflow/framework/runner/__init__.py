from oneflow.core.schemas import ParentModel
from typing import Protocol, runtime_checkable


class JobOutput(ParentModel):
    """This is a parent model to represent the runner job output"""

    def to_string(self):
        """This method is used to get the string representation of the job result"""


class TaskOutput(ParentModel):
    """This is a parent model to represent the runner job output"""

    def to_string(self):
        """This method is used to get the string representation of the job result"""


class JobError(ParentModel):
    """This is a parent model to represent the runner job error"""


@runtime_checkable
class PipelineRunner(Protocol):
    """This class is used as the base class for the pipeline runner job"""

    def run(self):
        """This method used to run the pipeline"""

    def monitor(self):
        """This method monitors the job result"""

    def get_run_result(self):
        """This method returns the result from job run"""
