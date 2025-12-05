from oneflow.core.schemas import ParentModel
from pydantic import Field, ConfigDict, model_validator
from oneflow.core.schemas.sources import WriteOptions, Sink, SinkType, WriteType
from oneflow.core.schemas.features import OutputFeatureOptions
from typing import Optional, Any, Union, List
from oneflow.core.exception.validation import SparkOutputValidationError
from oneflow.core.sources import SourceProxy
from oneflow.core.utils import is_file_path_like, is_table_path_like
from oneflow.core.schemas.event import SubscribedEvent


class SparkOutput(ParentModel):
    """This class is a schema definition for the spark task output configuration"""

    name: str = Field(
        ...,
        description="Name of the output source should matches the execution function output",
    )
    path: str = Field(..., description="Path to the output source specified")
    sink: Sink = Field(..., description="sink name for the output specified")
    sink_type: SinkType = Field(..., description="sink type for the output specified")
    write_type: WriteType = Field(
        ..., description="write type for the output specified"
    )
    features: Optional[Union[OutputFeatureOptions, None]] = Field(
        None, description="Output features for the sink"
    )
    options: Optional[Union[WriteOptions, None]] = Field(
        WriteOptions(), description="Sink write options"
    )

    events: Optional[Union[List[SubscribedEvent], None]] = Field(
        None, description="events to be handled specific to operator"
    )

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def validation(self):
        """This method validation the initialized objects following the spark task outputs"""
        self.sink_class_obj = SourceProxy.get_source_class(self.sink.value)
        if self.features is not None:
            features = set(
                [
                    o
                    for o in self.features.dict()
                    if getattr(self.features, o) is not None
                ]
            )
            if (
                len(
                    features.difference(
                        set(self.sink_class_obj.write_features_supported)
                    )
                )
                > 0
            ):
                raise SparkOutputValidationError(
                    f"Output provided features {self.features} are not supported"
                )
        try:
            options_class = getattr(self.sink_class_obj, f"WriteOptions")
            self.options = (
                options_class(**self.options.model_dump())
                if self.options is not None
                else None
            )
        except Exception as e:
            raise SparkOutputValidationError(
                f"Output Config Validation Error because options provided is not parse to source options provided with error {e}"
            )
        if self.sink_type == SinkType.table and (not is_table_path_like(self.path)):
            raise SparkOutputValidationError(
                f"Output config Validation Error because provided source type as {self.sink_type} and provided path {self.path} doesn't look like table"
            )
        if self.sink_type == SinkType.file and (not is_file_path_like(self.path)):
            raise SparkOutputValidationError(
                f"Output config Validation Error because provided source type as {self.sink_type} and provided path {self.path} doesn't look like file path"
            )
        return self

    def __str__(self):
        return f"Ouput with name {self.name} at path {self.path} of type {self.sink.value} with write type {self.write_type.value}"
