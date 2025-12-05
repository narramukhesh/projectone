from projectoneflow.core.sources import SparkSource
from typing import Optional
from pydantic import Field


class JdbcSource(SparkSource):
    read_supported = ["file"]
    read_extract_supported = ["batch"]
    write_supported = None
    read_features_supported = [
        "drop_columns_feature",
        "schema_inference_from_registry",
        "filter_data_feature",
    ]

    class ReadOptions(SparkSource.ReadOptions):
        """This class is file source specific read options"""

        user: Optional[str] = Field(
            None,
            description="Username for the autentication",
            json_schema_extra={"secret": True},
        )
        password: Optional[str] = Field(
            None,
            description="Password for the autentication",
            json_schema_extra={"secret": True},
        )
        query: Optional[str] = Field(None, description="Query to execute on the target")
        url: Optional[str] = Field(
            None, description="Server Host address for fetching the data"
        )
        dbtable: Optional[str] = Field(
            None, description="table address to access from the database"
        )
        prepareQuery: Optional[str] = Field(
            None, description="query to be used for the "
        )
        partitionColumn: Optional[str] = Field(
            None,
            description="Parition column used in conjunction with the lowerbound and upperbound",
        )
        lowerbound: Optional[str] = Field(
            None,
            description="Parition column used in conjunction with the lowerbound and upperbound",
        )
        upperbound: Optional[str] = Field(
            None,
            description="Parition column used in conjunction with the lowerbound and upperbound",
        )
        numPartitions: Optional[str] = Field(
            None,
            description="No. of parallel tasks  running in parallel to fetch the data",
        )
        queryTimeout: Optional[str] = Field(
            None, description="query timeout for the single partition"
        )
        connectTimeout: Optional[str] = Field(
            "10000", description="connection timeout for the single partition"
        )
        socketTimeout: Optional[str] = Field(
            "10000", description="socket timeout for the single partition"
        )
        driver: Optional[str] = Field(
            None,
            description="driver to be specified to be run for the java class to be loaded",
        )

        def model_dump(self, *args, **kwargs):
            """This is the custom model_dump used to create the model process"""
            exclude = set()
            for attr in [
                "driver",
                "queryTimeout",
                "numPartitions",
                "upperbound",
                "lowerbound",
                "partitionColumn",
            ]:
                attr_value = getattr(self, attr)
                if attr_value is None:
                    exclude.add(attr)
            if len(exclude) > 0:
                kwargs["exclude"] = exclude
            return super().model_dump(*args, **kwargs)
