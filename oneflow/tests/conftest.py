"""This file will have the list of the fixture to be used in the following pytests"""

import logging
import pytest
import os
import shutil
from typing import Dict, Any
from pyspark.sql import SparkSession
import tempfile
from datetime import datetime
from copy import deepcopy

logging.basicConfig(
    format="%(levelname)s:%(created)f:%(funcName)s:%(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def setup_teardown():
    folder = f"{tempfile.gettempdir()}/pytests/oneflow/integration_tests/"
    logger.info(f"Setting up the integration_test tests folder at {folder}")
    if os.path.exists(folder):
        shutil.rmtree(f"{folder}", ignore_errors=True)

    os.makedirs(f"{folder}", mode=0o777)
    yield folder
    logger.info(f"""tearing down the integration test directory {folder}""")
    shutil.rmtree(f"{folder}", ignore_errors=True)


@pytest.fixture(scope="module")
def get_example_spark_pipeline_config():
    """This fixture will be used for the spark pipeline configuration"""
    return [
        {
            "name": "test_spark_pipeline_job",
            "description": "This configuration to check the whether the provided configuration parses to spark configuration and has dependency in test_spark_pipeline_config",
            "clusters": {"spark_test": {}},
            "refresh_policy": {"cron_expression": "46 0 3 * * ?"},
            "type": "spark",
            "tasks": {
                "test_spark_task": {
                    "name": "test_spark_task",
                    "input": [
                        {
                            "name": "input",
                            "path": "input",
                            "source": "delta",
                            "source_type": "file",
                            "source_extract_type": "batch",
                        }
                    ],
                    "execution": {
                        "name": "data",
                        "type": "module",
                        "source": "execution.test_execution",
                    },
                    "output": [
                        {
                            "name": "output",
                            "path": "output",
                            "sink_type": "file",
                            "write_type": "append",
                            "sink": "delta",
                        }
                    ],
                    "type": "spark_task",
                    "cluster": "spark_test",
                    "refresh_policy": {"type": "incremental"},
                }
            },
        },
        {
            "name": "test_spark_pipeline_config",
            "description": "This configuration to check the whether the provided configuration parses to spark configuration",
            "clusters": {"test": {}},
            "refresh_policy": {"cron_expression": "46 0 3 * * ?"},
            "type": "spark",
            "tasks": {
                "test_spark_task": {
                    "name": "test_spark_task",
                    "input": [
                        {
                            "name": "input",
                            "path": "input",
                            "source": "delta",
                            "source_type": "file",
                            "source_extract_type": "batch",
                        }
                    ],
                    "execution": {
                        "name": "data",
                        "type": "module",
                        "source": "execution.test_execution",
                    },
                    "output": [
                        {
                            "name": "output",
                            "path": "output",
                            "sink_type": "file",
                            "write_type": "append",
                            "sink": "delta",
                        }
                    ],
                    "type": "spark_task",
                    "cluster": "task",
                    "refresh_policy": {"type": "incremental"},
                },
                "test_spark_pipeline_task": {
                    "name": "test_spark_pipeline_task",
                    "pipeline_name": "test_spark_pipeline_job",
                    "type": "spark_pipeline_task",
                },
            },
        },
    ]


@pytest.fixture(scope="module")
def spark_context():
    """This fixture will setups the spark context which is used for this module test cases"""
    logger.info(f"Initializing the spark context")

    class SparkSessionConstruct:
        def __init__(self):
            """initialize method"""
            self._spark = None

        @property
        def spark(self):
            if SparkSession.getActiveSession() is None:
                self._spark = (
                    SparkSession.builder.config(
                        "spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1"
                    )
                    .config(
                        "spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension",
                    )
                    .config(
                        "spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    )
                    .getOrCreate()
                )
            else:
                self._spark = SparkSession.getActiveSession()
            return self._spark

        def stop(self):
            if self._spark:
                self._spark.stop()

    spark = SparkSessionConstruct()
    yield spark
    logger.info(f"closing the spark context")
    spark.stop()


@pytest.fixture(scope="module")
def sample_data():
    """This fixture will setups the sample data used for this test module"""

    class Data:
        def __init__(self, extra=False, order_timestamp=False):
            self.data = {
                "id": [1, 2, 3, 4, 5, 6],
                "name": ["oskar", "name", "scott", "bob", "fob", "fog"],
                "offset": [101, 102, 103, 104, 105, 106],
            }

            self.n_cols = 3
            self.n_rows = 6
            self.extra = extra
            self.order_timestamp = order_timestamp
            if extra:
                self.data["change"] = [0, 0, 0, 0, 0, 0]
                self.n_cols = self.n_cols + 1

            if order_timestamp:
                self.data["timestamp"] = [
                    datetime.strptime("2025-01-01", "%Y-%m-%d")
                ] * 6
                self.n_cols = self.n_cols + 1

        def get_data_with_extra_col(self, extra=False, order_timestamp=False):

            return self.__class__(extra, order_timestamp)

        def create_table(
            self,
            data_folder,
            write_type="append",
            metadata_column_mapping: Dict[str, Any] = None,
        ):
            """This property creates the sample table to be used in tests"""
            columns_schema = [
                {
                    "name": "id",
                    "type": "long",
                    "description": "Primary key of the table",
                    "nullable": False,
                },
                {
                    "name": "name",
                    "type": "string",
                    "description": "Value of the table",
                    "nullable": False,
                },
                {
                    "name": "offset",
                    "type": "long",
                    "description": "value of the offset",
                    "nullable": False,
                },
                {
                    "name": "__metadata_valid_to_ts__",
                    "type": "timestamp",
                    "description": "Metadata column for the valid upto",
                },
            ]
            if self.extra:
                columns_schema.append(
                    {
                        "name": "change",
                        "type": "long",
                        "description": "column for change tracking",
                    }
                )
            if self.order_timestamp:
                columns_schema.append(
                    {
                        "name": "timestamp",
                        "type": "timestamp",
                        "description": "ordered column for the source timestamps",
                        "nullable": False,
                    }
                )
            if write_type.startswith("scd"):
                columns_schema.extend(
                    [
                        {
                            "name": "__metadata_key_hash__",
                            "type": "string",
                            "description": "value of the all key hash to compare the result",
                        },
                        {
                            "name": "__metadata_data_hash__",
                            "type": "string",
                            "description": "value of all data column compare the result",
                        },
                        {
                            "name": "__metadata_valid_from_ts__",
                            "type": "timestamp",
                            "description": "metadata valid from ts for the result",
                        },
                    ]
                )
            if write_type == "scd2":

                columns_schema.append(
                    {
                        "name": "__metadata_active__",
                        "type": "string",
                        "description": "metadata active flag for the result",
                    }
                )
            if write_type == "scd3":
                columns_schema.extend(
                    [
                        {
                            "name": "prev_name",
                            "type": "string",
                            "description": "old Value of the table",
                            "nullable": True,
                        },
                        {
                            "name": "prev_offset",
                            "type": "long",
                            "description": "old value of the offset",
                            "nullable": True,
                        },
                        {
                            "name": "__metadata_column_key_hash__",
                            "type": "string",
                            "description": "metadata column key hash for the result",
                        },
                    ]
                )

            if metadata_column_mapping is not None:
                for idx, _ in enumerate(columns_schema):
                    column_schema = columns_schema[idx]
                    column_schema["name"] = metadata_column_mapping.get(
                        column_schema["name"], column_schema["name"]
                    )
                    columns_schema[idx] = column_schema

            return {
                "column_schema": columns_schema,
                "comment": "This table is a dummy sample test",
                "location": data_folder,
            }

        def set(self, id, name, offset, timestamp=None):
            _data = deepcopy(self.data)
            _data["id"] = _data["id"] + [id]
            _data["name"] = _data["name"] + [name]
            _data["offset"] = _data["offset"] + [offset]
            if self.extra:
                _data["change"] = _data["change"] + [0]
            if self.order_timestamp:
                _data["timestamp"] = _data["timestamp"] + [timestamp]
            return _data

        def bulk_set(self, set=[]):
            _data = deepcopy(self.data)
            for i in set:

                _data["id"] = _data["id"] + [i["id"]]
                _data["name"] = _data["name"] + [i["name"]]
                _data["offset"] = _data["offset"] + [i["offset"]]
                if self.extra:
                    _data["change"] = _data["change"] + [0]
                if self.order_timestamp:
                    _data["timestamp"] = _data["timestamp"] + [i.get("timestamp", None)]
            return _data

        def update(self, id, name, offset):
            _data = deepcopy(self.data)
            index = _data["id"].index(id)
            _data["name"][index] = name
            _data["offset"][index] = offset
            return _data

        @property
        def cdc_data(self):
            self._cdc_data = {
                "id": [1, 2, 3, 4, 5, 6],
                "name": ["oskar", "name", "scott", "bob", "fob", "fog"],
                "offset": [101, 102, 103, 104, 105, 106],
                "value": [
                    '{"id": 1, "version": 2}',
                    '{"id": 1, "version": 2}',
                    '{"id": 1, "version": 2}',
                    '{"id": 1, "version": 2}',
                    '{"id": 1, "version": 2}',
                    '{"id": 1, "version": 2}',
                ],
                "timestamp": [datetime.now()] * 6,
            }
            return self._cdc_data

        def set_cdc_data(self, id, name, offset, timestamp=datetime.now()):
            _data = self._cdc_data.copy()
            _data["id"] = _data["id"] + [id]
            _data["name"] = _data["name"] + [name]
            _data["offset"] = _data["offset"] + [offset]
            _data["value"] = _data["value"] + [None]
            _data["timestamp"] = _data["timestamp"] + [timestamp]
            return _data

    return Data(extra=False, order_timestamp=False)
