from typing import Union, Optional, Mapping, Dict, Any, TYPE_CHECKING
from pyspark.sql.types import (
    StringType,
    StructType,
    IntegerType,
    ArrayType,
    LongType,
    MapType,
    FloatType,
    TimestampType,
    BooleanType,
    DateType,
    DoubleType,
    DecimalType,
)
from pyspark.sql.column import Column
from pyspark.sql import SparkSession
from projectoneflow.core.schemas.features import SchemaRegistryType
from projectoneflow.core.exception import SchemaRegistryRequestParsingError
import json
import os
import re

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


def json_schema_to_spark(
    schema_root: str,
    fields: list,
    type_mapping: Dict[str, Any],
    initial_schema: Dict[str, Any],
):
    """
    This is utility function which recieves the schema and recursively constructs the spark schema

    Parameters
    ---------------
    schema_root:str
        spark schema root struct type object
    fields:list
        json schema fileds list name
    type_mapping:Dict[str,Any]
        type mapping between json schema to spark schema
    initial_schema:Dict[str,Any]
        json schema
    """
    for j in fields:
        i = schema_root[j]
        dtype = (
            i.get("type", "string") if i.get("format", None) is None else i["format"]
        )
        metadata = i.get("description", None)
        dtype_obj = type_mapping[dtype]

        if dtype == "object":
            dtype_obj = dtype_obj()
            schema_property = i["properties"]
            fields_keys = schema_property.keys()
            json_schema_to_spark(schema_property, fields_keys, type_mapping, dtype_obj)

        elif dtype == "array":
            items = i["items"]
            item_type = (
                items.get("type", "string")
                if items.get("format", None) is None
                else items["format"]
            )
            item_type_obj = type_mapping[item_type]()
            if item_type == "object":
                schema_property = items["properties"]
                fields_keys = schema_property.keys()
                json_schema_to_spark(
                    schema_property, fields_keys, type_mapping, item_type_obj
                )
            dtype_obj = dtype_obj(item_type_obj)
        else:
            dtype_obj = dtype_obj()

        initial_schema.add(j, dtype_obj, metadata={"comment": metadata})


def convert_json_schema_to_spark(schema: str) -> StructType:
    """
    This is utility function which recevies the schema string and converts to spark struct schema

    Parameters
    ---------------
    schema: str
        This is the schema string

    Returns
    ------------
    StructType
        This is the schema for the
    """
    type_mapping = {
        "object": StructType,
        "array": ArrayType,
        "number": FloatType,
        "string": StringType,
        "long": LongType,
        "double": DoubleType,
        "boolean": BooleanType,
        "integer": IntegerType,
        "date-time": TimestampType,
        "date": DateType,
    }
    initial_schema = StructType()
    fields = schema["properties"].keys()
    schema_root = schema["properties"]
    json_schema_to_spark(schema_root, fields, type_mapping, initial_schema)
    return initial_schema


def read_schema_from_file(path: str) -> StructType:
    """
    This is a utility function to read schema from input file

    Parameters
    --------------
    path: str
        local schema path location

    Returns
    ----------------
    StructType
        This returns the spark schema
    """

    try:
        ### commented because facing error for the multi line json
        # if SparkSession.getActiveSession:
        #     spark = SparkSession.getActiveSession()
        #     json_schema = spark.read.text(path).collect()[0].value
        # else:
        with open(path, "r") as read_file:
            json_schema = read_file.read()
        return json_schema

    except Exception as e:
        raise SchemaRegistryRequestParsingError(f"Schema Parsing failed with error {e}")


def read_from_schema_registry(
    subject: str, registry_address: str, registry_key: str, registry_pass: str
):
    """
    This is utility function to get the schema from confluent schema registry

    Parameters
    -------------
    subject:str
        subject name to get the schema
    registry_address:str
        registry address for fetching the schema
    registry_key:str
        registry key for autentication
    registry_pass:str
        registry pass for autentication

    """
    from confluent_kafka.schema_registry import SchemaRegistryClient

    schema_registry_conf = {
        "url": registry_address,
        "basic.auth.user.info": "{}:{}".format(registry_key, registry_pass),
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    try:
        json_schema = schema_registry_client.get_latest_version(
            subject
        ).schema.schema_str

        return json_schema
    except Exception as e:
        raise SchemaRegistryRequestParsingError(e)


def from_json(
    col: "ColumnOrName",
    schema: Optional[Union[ArrayType, StructType, Column, str]] = None,
    options: Optional[Mapping[str, str]] = None,
    schemaRegistryAddress: Optional[str] = None,
    schemaRegistryOptions: Optional[Mapping[str, str]] = None,
    schemaRegistrySubject: Optional[str] = None,
    file: Optional[str] = None,
) -> Column:
    import pyspark.sql.functions as F

    if (schemaRegistryAddress is not None) and (schemaRegistrySubject is not None):
        if schemaRegistryOptions.get("type", None) == SchemaRegistryType.confluent:
            json_schema = read_from_schema_registry(
                schemaRegistrySubject,
                schemaRegistryAddress,
                schemaRegistryOptions.get("clientId"),
                schemaRegistryOptions.get("clientSecret"),
            )
            schema = convert_json_schema_to_spark(json.loads(json_schema))
    elif file is not None:
        json_schema = read_schema_from_file(file)
        schema = convert_json_schema_to_spark(json.loads(json_schema))

    return F.from_json(F.col(col).cast("string"), schema, options)


def from_avro(
    data: "ColumnOrName",
    jsonFormatSchema: str = None,
    options: Optional[Dict[str, str]] = None,
    schemaRegistryAddress: Optional[str] = None,
    schemaRegistryOptions: Optional[Mapping[str, str]] = None,
    schemaRegistrySubject: Optional[str] = None,
    file: Optional[str] = None,
) -> Column:
    import pyspark.sql.avro.functions as f
    import pyspark.sql.functions as F

    if (schemaRegistryAddress is not None) and (schemaRegistrySubject is not None):
        if is_in_databricks_runtime() and (
            schemaRegistryOptions.get("type", None) == SchemaRegistryType.confluent
        ):

            options = {} if options is None else options
            options["confluent.schema.registry.basic.auth.credentials.source"] = (
                "USER_INFO"
            )
            options["confluent.schema.registry.basic.auth.user.info"] = "{}:{}".format(
                schemaRegistryOptions.get("clientId"),
                schemaRegistryOptions.get("clientSecret"),
            )
            return f.from_avro(
                data=F.col(data),
                options=options,
                subject=schemaRegistrySubject,
                schemaRegistryAddress=schemaRegistryAddress,
            )
        else:
            raise SchemaRegistryRequestParsingError(
                "Currently fetching from schema registry is only supported in databricks platform for avro records"
            )
    elif file is not None:
        json_schema = read_schema_from_file(file)
        jsonFormatSchema = json.loads(json_schema)
    return f.from_avro(data=F.col(data), jsonFormatSchema=jsonFormatSchema)


def is_in_databricks_runtime():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def check_valid_type(data_type: str) -> bool:
    """This utitlity function to valid whether provided spark type is valid or not"""
    valid_types = {
        "string": StringType,
        "integer": IntegerType,
        "long": LongType,
        "double": DoubleType,
        "float": FloatType,
        "boolean": BooleanType,
        "timestamp": TimestampType,
        "date": DateType,
        "decimal": DecimalType,
        "array": ArrayType,
        "map": MapType,
        "struct": StructType,
    }

    data_type = data_type.lower().strip()

    # Directly match simple types from valid_types
    if data_type in valid_types:
        return True

    # Regex to match MapType
    map_match = re.match(r"map<(.+),(.+)>", data_type)
    if map_match:
        key_type = map_match.group(1).strip()
        value_type = map_match.group(2).strip()

        return check_valid_type(key_type) and check_valid_type(value_type)

    # Regex to match ArrayType
    array_match = re.match(r"array<(.+)>", data_type)
    if array_match:
        element_type = array_match.group(1).strip()

        return check_valid_type(element_type)

    # Regex to match StructType
    struct_match = re.match(r"struct<(.+)>", data_type)
    if struct_match:

        fields_str = struct_match.group(1).strip()
        fields = fields_str.split(",")
        for field in fields:
            field_name, field_type = field.split(":")
            field_type = field_type.strip()

            if not check_valid_type(field_type):
                return False
        return True

    return False
