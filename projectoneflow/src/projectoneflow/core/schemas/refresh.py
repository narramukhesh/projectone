from projectoneflow.core.schemas import ParentModel, ParentEnum, DateFormatTypes
from typing import Optional
from pydantic import model_validator, Field


class TaskRefreshTypes(ParentEnum):
    """This is schema definition for possible values for spark refresh types"""

    stream = "stream"
    incremental = "incremental"
    backfill = "backfill"


class TaskRefreshInterval(ParentEnum):
    """This is schema definition for possible values for spark refresh interval"""

    day = "day"
    week = "week"
    month = "month"
    year = "year"


class TaskRefreshPolicy(ParentModel):
    """This is schema definition for spark refresh policy"""

    type: Optional[TaskRefreshTypes] = Field(
        TaskRefreshTypes.incremental, description="Refresh policy incremental"
    )
    interval: Optional[TaskRefreshInterval] = Field(
        TaskRefreshInterval.day, description="Task Refresh policy interval"
    )
    start_value: Optional[str] = Field(
        None, description="Start value of the spark task policy"
    )
    end_value: Optional[str] = Field(
        None, description="end value of the spark task policy"
    )
    format: Optional[DateFormatTypes] = Field(
        DateFormatTypes.timestamp, description="date format of the spark refresh policy"
    )
    range_start: Optional[str] = Field(
        None,
        description="Range Start of the spark task policy which is calculated on run-time when pipeline is started",
    )
    range_end: Optional[str] = Field(
        None,
        description="Range End of the spark task policy which is calculated on run-time when pipeline is started",
    )

    @model_validator(mode="after")
    def validate(self):
        self.range_start = self.start_value
        self.range_end = self.end_value
        return self
