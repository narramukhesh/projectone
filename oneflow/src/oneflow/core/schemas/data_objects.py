from oneflow.core.schemas import ParentModel
from typing import Optional, List, Dict, Union
from pydantic import Field


class Schema(ParentModel):
    """This is the schema definition for the schema/database"""

    name: str = Field(..., description="schema name identifier")
    properties: Optional[Dict[str, str]] = Field(
        {}, description="properties for the schema"
    )
    catalog: Optional[str] = Field(None, description="catalog name of the schema")
    comment: Optional[str] = Field(
        None, description="comment for the schema definition"
    )
    location: Optional[str] = Field(
        None, description="storage root location where schema should be stored"
    )
    tags: Optional[Dict[str, str]] = Field(
        None, description="tags associated by the schema object"
    )

    def get_resolution_name(self):
        return f"{self.catalog}.{self.name}"


class TableColumnSchema(ParentModel):
    """This is schema definition for the table column schema"""

    name: str = Field(..., description="Column name to be displayed")
    type: str = Field(..., description="Column type to be applied for data")
    description: Optional[str] = Field(
        None, description="Column description for data", alias="comment"
    )
    identity: Optional[bool] = Field(
        False, description="Whether column is identity column"
    )
    nullable: Optional[bool] = Field(
        False, description="Whether column is identity column"
    )
    generate_expr: Optional[str] = Field(
        None, description="Whether column is identity column"
    )
    identity_start: Optional[int] = Field(
        1, description="Setting the identity start value"
    )
    identity_step: Optional[int] = Field(
        1, description="setting the identity step value"
    )
    default: Optional[str] = Field(
        None, description="Setting the default value for default value"
    )

    position: Optional[int] = Field(
        None, description="provides the position of the table column"
    )
    tags: Optional[Dict[str, str]] = Field(
        None, description="tags associated by the column object"
    )


class Table(ParentModel):
    """This is class definition for the table data object"""

    table_name: Optional[str] = Field(None, description="table name identifier")
    schema_name: Optional[str] = Field(
        None,
        description="schema name where table will be created under",
        alias="schema",
    )
    catalog: Optional[str] = Field(
        None, description="catalog name of the table created"
    )
    column_schema: List[TableColumnSchema] = Field(
        ..., description="column schema to be defined"
    )
    format: str = Field(None, description="format of the table to be created")
    properties: Optional[Dict[str, str]] = Field(
        {}, description="properties for the table"
    )
    comment: Optional[str] = Field(
        None, description="comment description for the table"
    )
    partition_by: Optional[List[str]] = Field(
        None, description="partition by columns for the table"
    )
    cluster_by: Optional[List[str]] = Field(
        None, description="cluster by columns for the table"
    )
    location: Optional[str] = Field(
        None, description="location of the data to be stored for the table"
    )
    tags: Optional[Dict[str, str]] = Field(
        None, description="tags associated by the schema object"
    )


class View(ParentModel):
    """This is class definition for the view data object"""

    name: str = Field(..., description="view name identifier")
    schema_name: Optional[str] = Field(
        None,
        description="schema name where table will be created under",
        alias="schema",
    )
    catalog: Optional[str] = Field(
        None, description="schema name where table will be created under catalog"
    )
    query: str = Field(..., description="query where view will be created")
    comment: Optional[str] = Field(
        None, description="comment description for the table"
    )


class VolumeFile(ParentModel):
    """This is class definition for the volume file data object"""

    name: str = Field(
        ...,
        description="File name with extention which is used to upload the file to volume",
    )
    source_file_name: str = Field(
        ..., description="File name to be passed for source path"
    )
    source_path: str = Field(
        ..., description="source path to be used to upload to the target location"
    )


class Volume(ParentModel):
    """This is class definition for the volume data object"""

    name: str = Field(..., description="volume name identifier")
    schema_name: str = Field(
        ..., description="schema name where table will be created under", alias="schema"
    )
    catalog: Optional[Union[str, None]] = Field(
        None, description="schema name where table will be created under catalog"
    )
    storage_location: Optional[Union[str, None]] = Field(
        None, description="storage location where volume is deployed"
    )
    comment: Optional[Union[str, None]] = Field(
        None, description="comment description for the table"
    )
    files: Optional[Union[List[VolumeFile], None]] = Field(
        None, description="volume files which are managed with this volume"
    )


class DataObject(ParentModel):
    """This is class definition to represent the hierarchy database->tables, views"""

    database: Schema = Field(
        ...,
        description="schema definition for the corresponding schema object",
        alias="schema",
    )
    tables: Optional[List[Table]] = Field(
        [], description="tables to be defined for the target schema object"
    )
    views: Optional[List[View]] = Field(
        [],
        description="View to be created target location for the target schema object",
    )
    volumes: Optional[List[Volume]] = Field(
        [],
        description="volumes created at target location for the target schema object",
    )
