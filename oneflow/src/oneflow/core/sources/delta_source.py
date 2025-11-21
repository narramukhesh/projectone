from oneflow.core.sources import SparkSource
from typing import Optional, Any, Type
from pydantic import Field
from oneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
from oneflow.core.schemas.sources import SparkSourceType
from oneflow.core.schemas.features import ChangeDataFeatureType
from oneflow.core.schemas.state import ChangeFeatureValue
from oneflow.core.exception.sources import (
    DeltaSparkSourceCDCInitializationError,
    WriteFunctionNotImplementedError,
)
from delta import DeltaTable
from oneflow.core.utils.delta import DeltaUtils
from oneflow.core.types import F
from pyspark.sql import SparkSession, DataFrame
from oneflow.core.schemas.result import ChangeDataCaptureResult


class DeltaSource(SparkSource):
    read_supported = ["file", "table"]
    read_extract_supported = ["batch", "stream"]
    write_supported = ["stream", "file", "table"]
    write_features_supported = ["create_data_object_if_not_exists"]
    write_type_supported = ["append", "overwrite", "scd1", "scd2", "scd3"]
    read_features_supported = [
        "change_data_feature",
        "drop_columns_feature",
        "schema_inference_from_registry",
        "filter_data_feature",
    ]

    class ReadOptions(SparkSource.ReadOptions):
        readChangeFeed: Optional[str] = Field(
            "false",
            description="This change feed is used for the change data capture enabled from source",
        )
        startingVersion: Optional[str] = Field(
            "0",
            description="This configuration is used for the get the interval start for the change data capture",
        )
        endingVersion: Optional[str] = Field(
            "0",
            description="This configuration is used for the get the interval ending for the change data capture",
        )

    @classmethod
    def get_write_function(cls, write_type: str) -> Type[F]:
        """
        This method returns the write function obj for specific write type

        Parameters
        ------------------
        cls: class
        write_type: str
            This will specify the write type to get the return write object
        """
        from oneflow.core.execution.write import scd1, scd2, scd3, append, overwrite

        if write_type == "append":
            return append
        elif write_type == "scd1":
            return scd1
        elif write_type == "scd2":
            return scd2
        elif write_type == "scd3":
            return scd3
        elif write_type == "overwrite":
            return overwrite
        else:
            raise WriteFunctionNotImplementedError(
                f"{write_type} write type function is not implemented"
            )

    @staticmethod
    def resolve_create_data_object_if_not_exists(
        spark: SparkSession,
        df: DataFrame,
        path: str,
        sink_type: str,
        create_data_object_feature: Any,
        options: Any,
    ):
        """
        This is a method is a implementation for resolving the create table feature

        Parameters
        ------------------
        spark:SparkSession
            spark Session object to be used in connecting the source
        df: DataFrame
            dataframe used for the any futher processing
        path:str
            path location for delta input source
        sink_type:str
            sink type whether file/ table
        create_data_object_feature:Any
            create_table_feature coinfiguration for this source
        options:Any
            delta input sources options

        Returns
        -------------
        Any
            returns options which is used for the calling method
        """
        if create_data_object_feature.table is not None:
            create_table_feature = create_data_object_feature.table
            column_information = create_table_feature.column_schema

            column_schema, properties = DeltaUtils.get_column_schema(
                column_information, create_table_feature.properties
            )
            table_name = None
            if create_table_feature.table_name is not None:
                table_name = create_table_feature.table_name

            if (create_table_feature.schema_name is not None) and (
                table_name is not None
            ):
                table_name = f"{create_table_feature.schema_name}.{table_name}"

            if (create_table_feature.catalog is not None) and (table_name is not None):
                table_name = f"{create_table_feature.catalog}.{table_name}"

            DeltaUtils.create_table_if_not_exists(
                spark,
                tableName=table_name,
                column_information=column_schema,
                properties=properties,
                comment=create_table_feature.comment,
                partition_by=create_table_feature.partition_by,
                cluster_by=create_table_feature.cluster_by,
                location=create_table_feature.location,
            )
        return options

    @staticmethod
    def resolve_change_data_feature(
        spark: SparkSession,
        path: str,
        source: str,
        source_type: "SparkSourceType",
        refresh_policy_type: "SparkTaskRefreshTypes",
        previous_cdc_value: Any,
        cdc: Any,
        options: "ReadOptions",
    ):
        """
        This is a method is a implementation for resolving the change table feature

        This Delta source supports two types of cdc
            1. Delta native supported cdc columns
                For this type of load this framework captures startVersion and endVersion of the current load
                please refer the delta cdc documentation and extra fields populated with source input
                    https://docs.delta.io/latest/delta-change-data-feed.html
            2. This framework supported incremental


        Parameters
        ------------------
        spark:SparkSession
            spark Session object to be used in connecting the source
        path:str
            path location for delta input source
        source:str
            source name of the input source
        source_type:str
            source type name whether file/table
        refresh_policy_type:SparkTaskRefreshTypes
            refresh policy type name for the processing the spark cdc
        previous_cdc_value: Any
            This is the parameter to be processed to the further cdc processing source
        cdc:Any
            cdc configuration for this source
        options:Any
            delta input sources options

        Returns
        -------------
        Any
            returns options which is used for the calling method
        """
        attribute = ""
        start_value = ChangeFeatureValue(
            value=cdc.start_value, value_type=cdc.value_type
        )
        end_value = ChangeFeatureValue(value=cdc.end_value, value_type=cdc.value_type)
        extra_info = None
        filter_expr = None
        if refresh_policy_type in [
            SparkTaskRefreshTypes.incremental,
            SparkTaskRefreshTypes.backfill,
        ]:
            if refresh_policy_type == SparkTaskRefreshTypes.incremental:

                previous_value = previous_cdc_value.next_value

                if cdc.change_feature_type == ChangeDataFeatureType.delta_cdc_feed:
                    try:
                        if source_type == SparkSourceType.table:
                            input = DeltaTable.forName(spark, path)
                        elif source_type == SparkSourceType.file:
                            input = DeltaTable.forPath(spark, path)
                        else:
                            raise DeltaSparkSourceCDCInitializationError(
                                "Delta table initialization error because of the unsupported spark source type"
                            )
                        if (
                            input.detail()
                            .collect()[0]
                            .properties.get("delta.enableChangeDataFeed", "false")
                            == "true"
                        ):

                            start_value = (
                                (
                                    ChangeFeatureValue(
                                        value=input.history()
                                        .select("version")
                                        .orderBy(F.col("version").asc())
                                        .limit(1)
                                        .collect()[0]
                                        .version,
                                        value_type=cdc.value_type,
                                    )
                                    if start_value.get_python_value() is None
                                    else start_value
                                )
                                if previous_value is None
                                else previous_value
                            )
                            end_value = ChangeFeatureValue(
                                value=input.history()
                                .select("version")
                                .orderBy(F.col("version").desc())
                                .limit(1)
                                .collect()[0]
                                .version,
                                value_type=cdc.value_type,
                            )

                            options.startingVersion = start_value.get_python_value()
                            options.endingVersion = end_value.get_python_value()
                            options.readChangeFeed = "true"
                            attribute = "readChangeFeed"

                    except Exception as e:
                        raise DeltaSparkSourceCDCInitializationError(
                            f"Delta source cdc initialization error because of the error {e}"
                        )
                else:
                    try:
                        if source_type == SparkSourceType.table:
                            input = spark.read.format("delta").table(path)
                        elif source_type == SparkSourceType.file:
                            input = spark.read.format("delta").load(path)
                        else:
                            raise DeltaSparkSourceCDCInitializationError(
                                f"Delta source cdc initialization error because of the unsupported spark source type"
                            )
                        prov_start_value = start_value.model_copy(deep=True)
                        start_value = (
                            ChangeFeatureValue(
                                value=input.selectExpr(
                                    f"min({cdc.attribute})"
                                ).collect()[0][0],
                                value_type=cdc.value_type,
                            )
                            if (
                                previous_value is None
                                and prov_start_value.get_python_value() is None
                            )
                            else (
                                previous_value
                                if previous_value is not None
                                else prov_start_value
                            )
                        )
                        end_value = (
                            ChangeFeatureValue(
                                value=input.selectExpr(
                                    f"max({cdc.attribute})"
                                ).collect()[0][0],
                                value_type=cdc.value_type,
                            )
                            if (
                                previous_value is None
                                and prov_start_value.get_python_value() is None
                            )
                            else (
                                ChangeFeatureValue(
                                    value=input.filter(
                                        f"{cdc.attribute}>{previous_value.get_spark_string_value()}"
                                    )
                                    .selectExpr(f"max({cdc.attribute})")
                                    .collect()[0][0],
                                    value_type=cdc.value_type,
                                )
                                if previous_value is not None
                                else ChangeFeatureValue(
                                    value=input.filter(
                                        f"{cdc.attribute}>={prov_start_value.get_spark_string_value()}"
                                    )
                                    .selectExpr(f"max({cdc.attribute})")
                                    .collect()[0][0],
                                    value_type=cdc.value_type,
                                )
                            )
                        )

                        if (
                            start_value.value is not None
                            and end_value.value is not None
                        ):
                            filter_expr = (
                                f"{cdc.attribute} >= {start_value.get_spark_string_value()} and {cdc.attribute} <= {end_value.get_spark_string_value()}"
                                if previous_value is None
                                else (
                                    f"{cdc.attribute} > {start_value.get_spark_string_value()} and {cdc.attribute} <= {end_value.get_spark_string_value()}"
                                    if start_value.get_python_value()
                                    != end_value.get_python_value()
                                    else f"{cdc.attribute} = {start_value.get_spark_string_value()}"
                                )
                            )

                            attribute = cdc.attribute
                        else:
                            filter_expr = f"1=2"

                    except Exception as e:
                        raise DeltaSparkSourceCDCInitializationError(
                            f"Delta source cdc initialization error because of the error {e}"
                        )
            else:
                if cdc.change_feature_type == ChangeDataFeatureType.delta_source:
                    options.startingVersion = start_value.get_python_value()
                    options.endingVersion = end_value.get_python_value()
                    options.readChangeFeed = "true"

                    attribute = "readChangeFeed"
                else:
                    filter_expr = f"{cdc.attribute} >= {start_value.get_spark_string_value()} and {cdc.attribute} <= {end_value.get_spark_string_value()}"
                    attribute = cdc.attribute
        return ChangeDataCaptureResult(
            attribute=attribute,
            start_value=start_value,
            end_value=end_value,
            extra_info=extra_info,
            filter_expr=filter_expr,
            options=options,
            path=path,
        )
