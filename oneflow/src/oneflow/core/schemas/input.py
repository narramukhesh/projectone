from oneflow.core.schemas import ParentModel
from pydantic import Field, ConfigDict, model_validator
from oneflow.core.schemas.sources import (
    SparkSource,
    SparkSourceExtractType,
    SparkSourceType,
    ReadOptions,
)
from oneflow.core.schemas.features import InputFeatureOptions
from typing import Optional, Union, List
from oneflow.core.exception.validation import SparkInputValidationError
from oneflow.core.sources import SourceProxy
from oneflow.core.utils import is_file_path_like, is_table_path_like
from oneflow.core.schemas.event import SubscribedEvent


class SparkInput(ParentModel):
    """This class is a schema definition for the spark task input configuration"""

    name: str = Field(..., description="Name of the input source")
    path: str = Field(..., description="Path to the input source specified")
    source: SparkSource = Field(..., description="source name for the input specified")
    source_type: SparkSourceType = Field(
        ..., description="source type for the input specified"
    )
    source_extract_type: SparkSourceExtractType = Field(
        ..., description="source extract type for the input specified"
    )
    features: Optional[Union[InputFeatureOptions, None]] = Field(
        None,
        description="Input Source features which are applied at pre-steps, post-steps",
    )
    options: Optional[Union[ReadOptions, None]] = Field(
        None, description="Input Source reader options"
    )
    events: Optional[Union[List[SubscribedEvent], None]] = Field(
        None, description="events to be handled specific to operator"
    )
    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def validation(self):
        """This method validation the initialized objects following the spark task inputs"""
        self.source_class_obj = SourceProxy.get_source_class(self.source.value)
        if self.source_type.value not in self.source_class_obj.read_supported:
            raise SparkInputValidationError(
                f"Input Validataion Error because source_type `{self.source_type}` provided is not matching with source supported types {self.source_class_obj.read_supported}"
            )
        try:
            options_class = getattr(self.source_class_obj, f"ReadOptions")
            self.options = (
                options_class(**self.options.model_dump())
                if self.options is not None
                else options_class()
            )
        except Exception as e:
            raise SparkInputValidationError(
                f"Input Validation Error because options provided is not parse to source options provided with error {e}"
            )
        if self.source_type == SparkSourceType.table and (
            not is_table_path_like(self.path)
        ):
            raise SparkInputValidationError(
                f"Input Validation Error because provided source type as {self.source_type} and provided path {self.path} doesn't look like table"
            )
        if self.source_type == SparkSourceType.file and (
            not is_file_path_like(self.path)
        ):
            raise SparkInputValidationError(
                f"Input Validation Error because provided source type as {self.source_type} and provided path {self.path} doesn't look like file path"
            )
        return self

    def __str__(self):
        return f"Input with name {self.name} at path {self.path} of type {self.source.value} with load type {self.source_extract_type.value}"
