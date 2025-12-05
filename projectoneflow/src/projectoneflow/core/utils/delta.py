from delta import DeltaTable
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from projectoneflow.core.schemas.data_objects import TableColumnSchema
from projectoneflow.core.exception.sources import ColumnTypeParsingError
from typing import Dict, List


class DeltaUtils:
    @staticmethod
    def get_column_schema(
        columns_schema: TableColumnSchema, properties: Dict[str, str]
    ) -> T.StructType:

        delta_column_schema = T.StructType()
        for column_schema in columns_schema:
            try:
                data_type = T._parse_datatype_string(column_schema.type)
            except Exception:
                ColumnTypeParsingError(
                    f"Cannot parse type {column_schema.type}, Please check the the column types at https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html "
                )
            metadata = {}
            if column_schema.generate_expr is not None:
                metadata["delta.generationExpression"] = column_schema.generate_expr
            if column_schema.identity:
                data_type = T.LongType()
                metadata["delta.identity.allowExplicitInsert"] = True
                metadata["delta.identity.start"] = column_schema.identity_start
                metadata["delta.identity.step"] = column_schema.identity_step
            elif column_schema.default is not None:
                metadata["default"] = column_schema.default
                metadata["CURRENT_DEFAULT"] = column_schema.default
                properties["delta.feature.allowColumnDefaults"] = "supported"
            if column_schema.description:
                metadata["comment"] = column_schema.description
            delta_column_schema.add(
                field=column_schema.name,
                data_type=data_type,
                nullable=column_schema.nullable,
                metadata=metadata,
            )
        return delta_column_schema, properties

    @staticmethod
    def delta_max_version(spark: SparkSession, tableName: str, nameType: str = "table"):
        """
        This method returns the latest version of delta table

        Parameters
        -------------
        spark: SparkSession
            spark session object
        tableName: str
            delta table to be queried
        nameType: str
            delta table type whether it is file path/name
        """

        if nameType == "file":
            return (
                DeltaTable.forPath(spark, tableName)
                .history(1)
                .select("version")
                .orderBy(F.col("version").desc())
                .limit(1)
                .collect()[0]
                .version
            )

        else:
            return (
                DeltaTable.forName(spark, tableName)
                .history(1)
                .select("version")
                .orderBy(F.col("version").desc())
                .limit(1)
                .collect()[0]
                .version
            )

    @staticmethod
    def create_table_if_not_exists(
        spark: SparkSession,
        column_information: T.StructType,
        tableName: str = None,
        properties: dict = {},
        comment: str = None,
        partition_by: List[str] = None,
        location: str = None,
        cluster_by: List[str] = None,
    ):
        """
        This method create the delta table if not exists

        Parameters
        --------------
        spark:SparkSession
            spark session object
        tableName:str
            delta table to be created
        column_information:StructType
            column schema to be inserted into delta table
        properties:dict
            properties to be specified for the delta table
        comment:str
            table description to be specified for the delta table
        partition_by:str
            table partioning for the delta table separated by the comma
        location:str
            external location to be specified for table target location
        cluster_by:str
            cluster the delta table specified by comma separated
        """

        table = (
            DeltaTable.createIfNotExists(spark)
            .addColumns(column_information)
            .comment(comment)
        )
        for property, value in properties.items():
            table = table.property(property, value)
        if tableName is not None:
            table = table.tableName(tableName)
        if partition_by is not None:
            table = table.partitionedBy(partition_by)
        if cluster_by is not None:
            table = table.clusterBy(cluster_by)
        if location is not None:
            table = table.location(location)
        table.execute()

    @staticmethod
    def delta_max_version_stats(
        spark: SparkSession, tableName: str, nameType: str = "table"
    ):
        """
        This method returns the latest version of delta table

        Parameters
        -------------
        spark: SparkSession
            spark session object
        tableName: str
            delta table to be queried
        nameType: str
            delta table type whether it is file path/name
        """

        if nameType == "file":
            target = DeltaTable.forPath(spark, tableName)

        else:
            target = DeltaTable.forName(spark, tableName)

        return (
            target.history(1)
            .selectExpr(
                "operation",
                "operationParameters",
                "operationMetrics",
                "readVersion",
                "version as writeVersion",
            )
            .collect()[0]
            .asDict()
        )
