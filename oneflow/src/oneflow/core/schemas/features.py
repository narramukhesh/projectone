from oneflow.core.schemas import ParentEnum, ParentModel
from pydantic import Field, model_validator
from typing import Optional, Union
from oneflow.core.exception.validation import (
    CreateTableValidationError,
    SchemaInferenceFromRegistryError,
    CreateDataObjectValidationError,
    ChangeDataFeatureTypeParseError,
)
from oneflow.core.schemas.data_objects import Table, View


class Feature(ParentModel):
    """This class is parent schema definition for all features"""

    @property
    def resolve(self):
        return False


class FilterDataFeature(Feature):
    """This is a schema definition for the filter data feature"""

    expression: str = Field(
        None, description="This is expression to be used in filtering the data"
    )


class DropColumnsFeature(Feature):
    """This is a schema definition for the drop column feature"""

    columns: str = Field(None, description="This is columns list as string")


class SelectColumnsFeature(Feature):
    """This is a schema definition for the select column feature"""

    columns: str = Field(None, description="This is columns list as string")


class SchemaType(ParentEnum):
    json = "json"
    avro = "avro"


class SchemaRegistryType(ParentEnum):
    """This class is the enum definition for all schema registry types"""

    confluent = "confluent"


class SchemaRegistryCredentials(ParentModel):
    """This is a schema definition for the schema registry credentials"""

    schema_registry_address: Union[str, None] = Field(
        None, description="Schema registry url address"
    )
    schema_registry_type: Union[SchemaRegistryType, None] = Field(
        SchemaRegistryType.confluent, description="Schema registry platform name"
    )
    schema_registry_user: Union[str, None] = Field(
        None,
        description="Schema registry user for autentication",
        json_schema_extra={"safe": True},
    )
    schema_registry_pass: Union[str, None] = Field(
        None,
        description="Schema registry password for autentication",
        json_schema_extra={"safe": True},
    )


class SchemaInferenceFromRegistry(Feature):
    """This is a schema definition for the column value schema inference for the column supplied as values"""

    source_column_name: str = Field(
        ..., description="This column which is column to inferred"
    )
    target_column_name: str = Field(
        ..., description="This column which is column to generate"
    )
    schema_type: Optional[SchemaType] = Field(
        SchemaType.json, description="This is schema type where value is provided"
    )
    schema_registry_credentials: Optional[SchemaRegistryCredentials] = Field(
        SchemaRegistryCredentials(), description="Schema registry credentials"
    )
    subject_name: Optional[Union[str, None]] = Field(
        None, description="Subject name from where schema to be pulled from the source"
    )
    file_name: Optional[Union[str, None]] = Field(
        None, description="schema file name to get the schema inferred from the source"
    )

    @model_validator(mode="after")
    def validate(self):
        if self.subject_name is not None:
            if self.schema_registry_credentials.schema_registry_address is None:
                raise SchemaInferenceFromRegistryError(
                    "Error caused because subject is defined but missing the schema registry address"
                )
        return self

    def __str__(self):
        return f"Schema inference feature configured to parse the column {self.source_column_name} to column {self.target_column_name} using from_{self.schema_type.value} util function"


class ChangeFeatureValueType(ParentEnum):
    """This is schema definition for the possible change data capture feature value types"""

    integer = "integer"
    date = "date"
    timestamp = "timestamp"


class ChangeDataFeatureType(ParentEnum):
    """This is schema definition for the possible change data capture feature types"""

    delta_cdc_feed = "delta_cdc_feed"
    file_path_cdc_feed = "file_path_cdc_feed"


class ChangeFeature(Feature):
    """This is schema definition for the change feature"""

    attribute: str = Field(
        ..., description="Change Data capture column name on which filter the data for"
    )
    start_value: Optional[Union[str, int]] = Field(
        None, description="Start value specified for the cdc to start from"
    )
    end_value: Optional[Union[str, int]] = Field(
        None, description="ending value used for backfilling purposes"
    )
    value_type: Optional[ChangeFeatureValueType] = Field(
        None, description="parsing the input value for filtering purposes"
    )
    change_feature_type: Optional[ChangeDataFeatureType] = Field(
        None, description="selecting the delta lake features purposes"
    )
    value_format: Optional[str] = Field(
        None, description="format the value which is configured"
    )

    @property
    def resolve(self):
        return True

    def __str__(self):
        return f"Change data feature with attributes {self.to_json()}"

    @model_validator(mode="after")
    def validate(self):
        """validation for the change data capture features"""
        if self.change_feature_type == ChangeDataFeatureType.delta_cdc_feed:
            if (
                self.value_type is not None
                and self.value_type != ChangeFeatureValueType.integer
            ):
                raise ChangeDataFeatureTypeParseError(
                    "Provided delta cdc feed and value type are mismatch combination where value type should be integer type"
                )
            else:
                self.value_type = ChangeFeatureValueType.integer
        elif self.change_feature_type not in [
            ChangeDataFeatureType.delta_cdc_feed,
            ChangeDataFeatureType.file_path_cdc_feed,
        ]:
            if self.value_type is None:
                raise ChangeDataFeatureTypeParseError(
                    "Provided cdc feed feature require value_type attribute to parse the attribute"
                )
        return self


class PostTaskExecutionOperationType(ParentEnum):
    """This is the possibles definition for the operation"""

    delete = "delete"


class PostTaskExecutionFeature(ParentModel):
    """This is the schema definition for the file operation"""

    operation: Optional[PostTaskExecutionOperationType] = Field(
        None, description="operation to be executed after task executed"
    )
    target_path: Optional[str] = Field(
        None, description="target path to be provided for operation like copy"
    )

    @property
    def resolve(self):
        return True


class InputFeatureOptions(ParentModel):
    """This is schema definition for the input feature options"""

    change_data_feature: Optional[Union[ChangeFeature, None]] = Field(
        None, description="Change data capture class to define the input features"
    )
    filter_data_feature: Optional[Union[FilterDataFeature, None]] = Field(
        None, description="Filter expression used to filter the dataset"
    )
    drop_columns_feature: Optional[Union[DropColumnsFeature, None]] = Field(
        None, description="Drop the subset columns from the dataframe"
    )
    schema_inference_from_registry: Optional[
        Union[SchemaInferenceFromRegistry, None]
    ] = Field(
        None,
        description="When specified fetches the schema from schema registry and applies to the dataset",
    )
    post_task_execution: Optional[Union[PostTaskExecutionFeature, None]] = Field(
        None, description="Post task execution operation to be executed for input"
    )
    select_columns_feature: Optional[Union[SelectColumnsFeature, None]] = Field(
        None, description="Select the subset columns from the dataset"
    )


class CreateDataObjectIfNotExists(Feature):
    """This is schema definition for the create table if not exists"""

    table: Optional[Table] = Field(None, description="Table definition to be created")
    view: Optional[View] = Field(None, description="View definition to be created")

    @model_validator(mode="after")
    def validate(self):
        if self.table is None and self.view is None:
            raise CreateDataObjectValidationError(
                f"Either view or table definition need to be defined"
            )
        if self.table.table_name is None and self.table.location is None:
            raise CreateTableValidationError(
                f"Either Table name or location should be present"
            )
        return self

    @property
    def resolve(self):
        return True


class OutputFeatureOptions(ParentModel):
    """This is schema definition for the output feature options"""

    create_data_object_if_not_exists: Optional[CreateDataObjectIfNotExists] = Field(
        ..., description="Create data object schema definition to be defined"
    )


class ValidateInputArgs(Feature):
    check: bool = Field(
        True, description=" This is just a temporary flag for validating the input args"
    )


class ValidateOutputArgs(Feature):
    check: bool = Field(
        True, description=" This is just a temporary flag for validating the input args"
    )


class ExecutionFeatureOptions(ParentModel):
    """This is schema definition for the input feature options"""

    validate_input_args: ValidateInputArgs = Field(
        ValidateInputArgs(),
        description="Change data capture class to define the input features",
    )
    validate_output_args: ValidateOutputArgs = Field(
        ValidateOutputArgs(),
        description="Change data capture class to define the input features",
    )
