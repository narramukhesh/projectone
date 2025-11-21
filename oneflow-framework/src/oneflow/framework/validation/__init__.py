from oneflow.core.schemas import ParentModel, ParentEnum
from typing import List, Optional
from pydantic import Field
from uuid import UUID, uuid4
from datetime import datetime, timezone


class ResultEnum(ParentEnum):
    passed = "passed"
    warning = "warning"
    failed = "failed"
    error = "error"
    unknown = "unknown"


class Check(ParentModel):
    name: str = Field(..., description="check name")
    object_type: str = Field(..., description="object type name")
    object_name: Optional[str] = Field(None, description="object name")
    description: str = Field(..., description="check description name")
    details: Optional[str] = Field(None, description="check validation details")
    location: Optional[str] = Field(None, description="object location")
    result: ResultEnum = Field(..., description="result from check")

    def mark_up_result(self):
        """This method will change the color of the result"""
        if self.result == ResultEnum.passed:
            return "[green]passed[/green]"
        if self.result == ResultEnum.warning:
            return "[yellow]warning[/yellow]"
        if self.result == ResultEnum.failed:
            return "[red]failed[/red]"
        if self.result == ResultEnum.error:
            return "[red]error[/red]"
        return self.result


class Run(ParentModel):
    Id: UUID = Field(..., description="unique id of the run id")
    timestamp_start: datetime = Field(..., description="timestamp start the run")
    timestamp_end: datetime = Field(..., description="timestamp end the run")
    checks: List[Check] = Field(..., description="checks to be validated")
    result: ResultEnum = Field(ResultEnum.unknown, description="result of the run")

    def has_passed(self):
        self.calculate_result()
        return self.result == ResultEnum.passed

    def finish(self):
        self.timestamp_end = datetime.now(timezone.utc)
        self.calculate_result()

    def calculate_result(self):
        if any(check.result == ResultEnum.error for check in self.checks):
            self.result = ResultEnum.error
        elif any(check.result == ResultEnum.failed for check in self.checks):
            self.result = ResultEnum.failed
        elif any(check.result == ResultEnum.warning for check in self.checks):
            self.result = ResultEnum.warning
        elif any(check.result == ResultEnum.passed for check in self.checks):
            self.result = ResultEnum.passed
        else:
            self.result = ResultEnum.unknown

    def append(self, check: Check):
        self.checks.append(check)

    def extend(self, checks: List[Check]):
        self.checks.extend(checks)

    @classmethod
    def create_run(cls):
        """
        Factory method to create a new Run instance.

        Returns
        ------------
        Run
            instance
        """
        run_id = uuid4()
        now = datetime.now(timezone.utc)
        return cls(
            Id=run_id,
            timestamp_start=now,
            timestamp_end=now,
            checks=[],
        )
