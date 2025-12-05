from projectoneflow.core.state import TaskState
from pyspark.sql import SparkSession
from projectoneflow.core.observability.logging import Logger
from typing import Any
import json
from pathlib import Path
import os
import io


logger = Logger.get_logger(__name__)


class IO(io.FileIO):
    """IO wrapper implements the FileIO for read and write operations"""

    def readAllBytes(self):
        return self.read()


class FileSystem:
    """This file system is wrapper for fallback to operting system access"""

    def exists(self, path: Path):
        return path.exists()

    def mkdirs(self, path: Path):
        os.makedirs(name=path.__str__(), mode=777, exist_ok=True)

    def create(self, path: Path):
        return IO(path.__str__(), "wb")

    def open(self, path: Path):
        return IO(path.__str__(), "rb")


class SparkExecutionTaskState(TaskState):
    """This class is implementation for holding the task state for the spark task execution"""

    def __init__(self, spark: SparkSession, state_directory: str = "."):
        """
        This is the initialization of the spark-execution task

        Parameters
        --------------
        spark: SparkSession
            Spark Session object
        state_directory: str
            state directory where state like pipeline source state, sink state, checkpoints stored

        Returns
        --------------
        None
        """
        self.spark = spark
        self.__checkpoint_directory = state_directory
        try:
            self.fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            )
            self.path = spark._jvm.org.apache.hadoop.fs.Path
            self.fs.exists(self.path(self.__checkpoint_directory))

        except Exception:
            logger.warning("Falling back to local file system implementation")
            self.fs = FileSystem()
            self.path = Path
        self.__initialize_input()
        self.__initialize_output()
        self.__initialize_state()

    @property
    def state_location(self):
        """This is property to returns state location"""
        return self.__checkpoint_directory

    def __initialize_input(self):
        """Creates the sources directory if not exists for storing information related to input"""
        self.fs.mkdirs(self.path(self.__checkpoint_directory, "sources"))

    def __initialize_output(self):
        """Creates the sink directory if not exists for storing information related to output"""
        self.fs.mkdirs(self.path(self.__checkpoint_directory, "sink"))

    def __initialize_state(self):
        """Creates the state directory if not exists for storing information related to pipeline state"""
        self.fs.mkdirs(self.path(self.__checkpoint_directory, "state"))

    def set(self, source: str, key: str, value: str):
        """
        This is set implementation for setting key in target

        Parameters
        -----------
        source: str
            source name or folder name where to store the value
        key: str
            key name to store the value/ created a file with name of the key under source folder
        value: str
            value to be stored under the key specified
        """
        key_dir_path = self.path(self.__checkpoint_directory, f"{source}/{key}/")
        key_path = self.path(self.__checkpoint_directory, f"{source}/{key}/{key}")
        version = 0
        if not self.fs.exists(key_dir_path):
            self.fs.mkdirs(key_dir_path)

        else:
            if self.fs.exists(key_path):
                output_stream = self.fs.open(key_path)
                previous_value = output_stream.readAllBytes().decode("utf-8")
                output_stream.close()
                previous_version = json.loads(previous_value)["version"]

                previous_path = self.path(
                    self.__checkpoint_directory,
                    f"{source}/{key}/{key}_v{previous_version}",
                )
                outputStream = self.fs.create(previous_path)
                outputStream.write(str(previous_value).encode("utf-8"))
                outputStream.close()
                version = previous_version + 1

        final_value = json.dumps({"version": version, "value": value})
        outputStream = self.fs.create(key_path)
        outputStream.write(str(final_value).encode("utf-8"))
        outputStream.close()

    def append(self, source: str, key: str, value: str):
        """
        This is append implementation for setting key in target

        Parameters
        -----------
        source: str
            source name or folder name where to store the value
        key: str
            key name to store the value/ created a file with name of the key under source folder
        value: str
            value to be stored under the key specified
        """
        source_dir = source.split("_")[0]
        key_dir = "_".join(source.split("_")[1:])
        key_dir_path = self.path(
            self.__checkpoint_directory, f"{source_dir}/{key_dir}/"
        )
        key_path = self.path(
            self.__checkpoint_directory, f"{source_dir}/{key_dir}/{key}"
        )

        if not self.fs.exists(key_dir_path):
            self.fs.mkdirs(key_dir_path)

        outputStream = self.fs.create(key_path)
        outputStream.write(str(value).encode("utf-8"))
        outputStream.close()

    def get(self, source: str, key: str, default: str = None) -> Any:
        """
        This is get implementation for getting key value from file based key-store

        Parameters
        -----------
        source: str
            source name or folder name where to get the value
        key: str
            key name to store the value/ created a file with name of the key under source folder
        default: str
            return default value if value is not specified
        """
        path = self.path(self.__checkpoint_directory, f"{source}/{key}/{key}")
        if self.fs.exists(path):
            output_stream = self.fs.open(path)
            value = output_stream.readAllBytes().decode("utf-8")
            output_stream.close()
            value_json = json.loads(value)
            final_value = value_json["value"]
            return final_value
        else:
            return default
