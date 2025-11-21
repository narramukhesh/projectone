from pyspark.sql import SparkSession
from typing import Type
from oneflow.core.types import ST, RP, SM
from uuid import uuid1
from oneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
from oneflow.core.utils.spark import is_in_databricks_runtime
import json
from oneflow.core.utils import DateUtils
from oneflow.core.observability.spark_listener import (
    SparkListener,
    SparkQueryListener,
    SparkStreamListener,
)
from oneflow.core.event.manager import EventManager
from oneflow.core.event import EndEvent


class SparkTaskExecutionContext:
    """This class is context manager for the spark task execution"""

    def __init__(
        self,
        spark: SparkSession,
        name: str,
        metadata: Type[ST],
        refresh_policy: Type[RP],
        secret_manager: Type[SM],
    ):
        """
        This is the initialization method

        Parameters
        ---------------
        spark:SparkSession
            This is the spark session object for entry point to spark cluster
        name: str
            This is the name of the
        metadata: Type[ST]
            This is the task state implementation object
        refresh_policy: Type[RP]
            This is the refresh policy schema definition
        secret_manager: Type[SM]
            This is the secret manager class obj
        """
        self.spark = spark
        self.metadata = metadata
        self.batch_id = uuid1().hex
        self.batch_name = name
        self.refresh_policy = refresh_policy
        self.secret_manager = secret_manager
        self.type = (
            "stream"
            if self.refresh_policy.type == SparkTaskRefreshTypes.stream
            else "batch"
        )
        self.get_range_values()
        self.__listerner = []
        self.event_manager = EventManager()
        self.start()

    def get_range_values(self):
        """
        This method will populate the range start and range end for pipeline current run
        """
        if self.refresh_policy.type != SparkTaskRefreshTypes.backfill:
            previous_value = self.metadata.get(
                source="state", key="pipeline_load_timestamp"
            )
            previous_value = (
                json.loads(previous_value)["end_date"]
                if previous_value is not None
                else self.refresh_policy.start_value
            )
            (
                self.refresh_policy.range_start,
                self.refresh_policy.range_end,
            ) = DateUtils.get_date_to_current_value(
                previous_value, self.refresh_policy.format
            )
        else:
            (
                self.refresh_policy.range_start,
                self.refresh_policy.range_end,
            ) = DateUtils.format_date_value(
                self.refresh_policy.format,
                previous_value,
                self.refresh_policy.end_value,
            )

    def start(self):
        """
        This function used for the  starting the objects in the context
        """
        self.event_manager.start()
        return self

    def stop(self):
        """
        This method is the teardown function for context
        """
        for listener in self.__listerner:
            listener.stop()
        if not is_in_databricks_runtime():
            self.spark.stop()

        self.event_manager.push(EndEvent())
        self.event_manager.stop()

    def set_spark_listeners(self):
        """This methods initializes the spark listerner and register into spark context"""
        spark_core_listener = SparkListener(self)
        spark_query_listener = SparkQueryListener(self)

        self.__listerner.append(spark_core_listener)
        self.__listerner.append(spark_query_listener)

        if self.type == "stream":
            stream_listener = SparkStreamListener(self)
            self.__listerner.append(stream_listener)
