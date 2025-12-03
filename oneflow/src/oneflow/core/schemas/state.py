from oneflow.core.schemas import ParentModel
from pydantic import Field
from typing import Optional, Dict, Any, Union
from oneflow.core.schemas.refresh import TaskRefreshTypes
from oneflow.core.schemas.features import ChangeFeatureValueType
import pandas as pd
from oneflow.core.exception.validation import (
    IncorrectChangeFeatureValueType,
    IncorrectChangeDataCaptureStateValue,
)


class ChangeFeatureValue(ParentModel):
    """This is the value object definition to be used as the start and next value for the cdc process"""

    value: Any = Field(..., description="this field represent the cdc feed value")
    value_type: Union[ChangeFeatureValueType, None] = Field(
        ..., description="this field represent the cdc feed value type"
    )

    def to_json(self, safe=False):
        """Method used for the converting the object into the json object"""
        value = None
        if self.value_type == ChangeFeatureValueType.integer:
            if self.value is None:
                value = None
            value = int(self.value)
        elif self.value_type in [
            ChangeFeatureValueType.date,
            ChangeFeatureValueType.timestamp,
        ]:
            if self.value is None:
                value = None
            value = f"{pd.to_datetime(pd.Series([self.value]))[0]}"
        elif self.value_type is None:
            value = value
        else:
            raise IncorrectChangeFeatureValueType(
                f"Provided value {self.value} can't be serialized with value type {self.value_type} to python type, Please check the provided change feature value type and provided value"
            )
        return {"value": value, "value_type": self.value_type.value}

    def get_python_value(self):
        """This method used by enum object to get the python object type representation of the value type"""
        if self.value_type == ChangeFeatureValueType.integer:
            if self.value is None:
                return None
            return int(self.value)
        elif self.value_type in [
            ChangeFeatureValueType.date,
            ChangeFeatureValueType.timestamp,
        ]:
            if self.value is None:
                return None
            return pd.to_datetime(pd.Series([self.value]))[0]
        elif self.value_type is None:
            return self.value
        else:
            raise IncorrectChangeFeatureValueType(
                f"Provided value {self.value} can't be serialized with value type {self.value_type} to python type, Please check the provided change feature value type and provided value"
            )

    def get_spark_string_value(self):
        """This method used by enum object to get the string representation of the value type"""
        if self.value_type == ChangeFeatureValueType.integer:
            return f"{self.get_python_value()}"
        elif self.value_type == ChangeFeatureValueType.date:
            return f"'{self.get_python_value().strftime('%Y-%m-%d')}'"
        elif self.value_type == ChangeFeatureValueType.timestamp:
            return f"'{self.get_python_value()}'"
        else:
            raise IncorrectChangeFeatureValueType(
                f"Provided value {self.value} can't be serialized with value type {self.value_type} to string type, Please check the provided change feature value type and provided value"
            )


class ChangeDataCaptureState(ParentModel):
    """This is the state definition for the cdc feed defined for the source defined"""

    attribute: Union[str, None] = Field(
        ..., description="Attribute name where cdc is defined"
    )
    next_value: Union[ChangeFeatureValue, None] = Field(
        ..., description="Next value for cdc feed to be used for next run"
    )
    start_value: Union[ChangeFeatureValue, None] = Field(
        ..., description="Start value for cdc feed to be defined for the current run"
    )
    load_type: TaskRefreshTypes = Field(
        TaskRefreshTypes.incremental,
        description="Type of the task source refresh policy",
    )
    extra_info: Optional[Dict[str, Any]] = Field(
        None, description="extra information to be used for the cdc feed run"
    )
    batch_id: Optional[str] = Field(None, description="Batch id of the pipeline run")
    batch_name: Optional[str] = Field(
        None, description="Batch name of the pipeline run"
    )

    @classmethod
    def from_dict(cls, value={}):
        """This method creates the change data capture state from the dictionary passed as the attribute"""
        if len(value) == 0 and isinstance(value, dict):
            return cls(attribute=None, next_value=None, start_value=None)
        elif isinstance(value, dict):
            return cls(**value)
        else:
            raise IncorrectChangeDataCaptureStateValue(
                "Provided value is not in dictionary format which can be serialized to Change data capture state"
            )
