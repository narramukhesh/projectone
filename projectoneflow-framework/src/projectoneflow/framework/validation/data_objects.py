from oneflow.core.schemas.data_objects import Table, Schema
from oneflow.core.types import C
from typing import Type, Tuple
from pyspark.sql.types import _parse_datatype_string
from oneflow.framework.validation import Run, Check, ResultEnum
from pyspark.sql import SparkSession
import sqlparse

VALID_TABLE_FORMAT = ["delta", "csv", "parquet", "json", "orc", "jdbc"]
VALIDATIONS = {
    "table": {
        "table_schema": "schema_name",
        "table_description": "comment",
        "table_name": "table_name",
        "table_format": "format",
    },
    "view": {
        "table_schema": "schema_name",
        "table_description": "comment",
        "table_name": "name",
        "table_query": "query",
    },
    "schema": {"schema_name": "name", "schema_description": "comment"},
    "column": {
        "column_name": "name",
        "column_description": "description",
        "column_datatype": "type",
    },
}


class SchemaValidation:
    """This class is used to run the schema validation checks"""

    @staticmethod
    def check_schema_name(name: str) -> Tuple:
        """This check validates where schema name is lower or not"""
        return name.islower(), None, ResultEnum.failed

    @staticmethod
    def check_schema_description(description: str) -> Tuple:
        """This check validates where schema description shouldn't be empty and have atleast 15 characters"""

        return (
            (description is not None) and len(description) > 10,
            None,
            ResultEnum.failed,
        )

    @classmethod
    def validate(
        cls: Type[C], run: Run, schema: Schema, name: str, object_location: str
    ):
        """
        This class method validates all checks and returns the dictionary of the check results

        Parameters
        -----------------------
        run: Run
            run
        schema: Schema
            schema name to be validated
        name: str
            table name to be validated
        object_location: str
            table object where table object is stored
        """

        # run table validation checks
        for schema_validation, validation_attr in VALIDATIONS["schema"].items():
            check_name = f"check_{schema_validation}"
            cls_method = getattr(cls, check_name)
            attr = getattr(schema, validation_attr)
            result = cls_method(attr)
            run.append(
                Check(
                    name=check_name,
                    object_type="schema",
                    object_name=name,
                    description=cls_method.__doc__,
                    details=result[1],
                    result=result[2] if not result[0] else ResultEnum.passed,
                    location=object_location,
                )
            )
        return


class TableValidation:
    """This class is used to run the table validations checks"""

    @staticmethod
    def check_column_name(name: str) -> Tuple:
        """This check validates where column name is lower or not"""

        return name.islower(), None, ResultEnum.failed

    @staticmethod
    def check_column_description(description: str) -> Tuple:
        """This check validates where description shouldn't be empty and have atleast 10 characters"""
        return (
            (description is not None) and len(description) > 10,
            None,
            ResultEnum.warning,
        )

    @staticmethod
    def check_column_datatype(type: str) -> Tuple:
        """This check validates whether column type is valid or not"""
        details = None
        try:
            SparkSession.builder.getOrCreate()
            _parse_datatype_string(type.lower())
            return True, None, ResultEnum.failed
        except Exception as e:
            details = f"{e}"
        return False, details, ResultEnum.failed

    @staticmethod
    def check_table_name(name: str) -> Tuple:
        """This check validates where table name is lower or not"""

        return (name is not None) and name.islower(), None, ResultEnum.failed

    @staticmethod
    def check_table_description(description: str) -> Tuple:
        """This check validates where table description shouldn't be empty and have atleast 15 characters"""

        return (
            (description is not None) and len(description) > 10,
            None,
            ResultEnum.failed,
        )

    @staticmethod
    def check_table_query(query: str) -> Tuple:
        """This check validates where table query is empty or not"""
        if len(query) == 0:
            return False, "Query should be non-empty", ResultEnum.failed
        try:
            sqlparse.parse(query)
            return True, None, ResultEnum.warning
        except Exception as e:
            return (
                False,
                "Something is wrong with Sql query provide, can't parse",
                ResultEnum.failed,
            )

    @staticmethod
    def check_table_schema(schema: str) -> Tuple:
        """This check validates whether schema name shouldn't be empty"""
        return (
            (schema is not None)
            and len(schema) > 0
            and len(schema.replace(" ", "")) > 0,
            None,
            ResultEnum.warning,
        )

    @staticmethod
    def check_table_format(format: str) -> Tuple:
        """This check validates whether table format valid or not"""

        return (
            (format is not None) and (format.lower() in VALID_TABLE_FORMAT),
            None,
            ResultEnum.failed,
        )

    @classmethod
    def validate(
        cls: Type[C],
        run: Run,
        table: Table,
        name: str,
        object_location: str,
        table_type: str = "table",
    ):
        """
        This class method validates all checks and returns the dictionary of the check results

        Parameters
        -----------------------
        run: Run
            run
        table: Table
            table schema to be validated
        name: str
            table name to be validated
        object_location: str
            table object where table object is stored
        """
        # run table/view validation checks
        for table_validation, validation_attr in VALIDATIONS[table_type].items():
            check_name = f"check_{table_validation}"
            cls_method = getattr(cls, check_name)
            attr = getattr(table, validation_attr)
            result = cls_method(attr)
            run.append(
                Check(
                    name=check_name,
                    object_type=table_type,
                    object_name=(
                        table.table_name if table_type == "table" else table.name
                    ),
                    description=cls_method.__doc__,
                    details=result[1],
                    result=result[2] if not result[0] else ResultEnum.passed,
                    location=object_location,
                )
            )
        if table_type == "table":
            # run table validation checks
            for col in table.column_schema:
                for column_validation, validation_attr in VALIDATIONS["column"].items():
                    check_name = f"check_{column_validation}"
                    cls_method = getattr(cls, check_name)
                    attr = getattr(col, validation_attr)
                    result = cls_method(attr)
                    run.append(
                        Check(
                            name=check_name,
                            object_type="column",
                            object_name=f"{table.table_name}.{col.name}",
                            description=cls_method.__doc__,
                            details=result[1],
                            result=result[2] if not result[0] else ResultEnum.passed,
                            location=object_location,
                        )
                    )

        return
