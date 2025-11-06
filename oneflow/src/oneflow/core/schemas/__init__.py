from pydantic import BaseModel, ConfigDict, Field
from typing import Any, List, Dict, Type, Optional
from oneflow.core.types import C
from enum import Enum


class ParentEnum(Enum):
    """This class is parent implementation of Enum"""

    def to_json(self, safe=False):
        return self.value

    @classmethod
    def to_list(cls):
        return list(map(lambda c: c.value, cls))


class ParentModel(BaseModel):
    """This class is parent implementation of BaseModel"""

    model_config = ConfigDict(
        populate_by_name=True,
    )

    def to_dict(self):
        return (
            {**self.model_extra, **self.__dict__}
            if self.model_extra is not None
            else self.__dict__
        )

    def model_dump(self, *args, **kwargs) -> Dict[str, Any]:
        """This method is used for converting the schematic definition to json"""
        by_alias = True
        if "by_alias" in kwargs:
            by_alias = kwargs["by_alias"]
        kwargs = {k: v for k, v in kwargs.items() if k != "by_alias"}

        return super().model_dump(*args, by_alias=by_alias, **kwargs)

    def to_json(self, safe=False):
        """This is used for iterative class model definition"""
        final_result = {}
        for k, v in self.__dict__.items():
            if safe:
                field = self.model_fields[k]
                if hasattr(field, "json_schema_extra") and isinstance(
                    getattr(field, "json_schema_extra"), dict
                ):
                    if field.json_schema_extra.get("secret", False):
                        continue
            v_final = v
            if isinstance(v, ParentModel) or isinstance(v, ParentEnum):
                v_final = v.to_json(safe)
            elif isinstance(v, List):
                v_final = []
                for i in v:
                    if isinstance(i, ParentModel) or isinstance(i, ParentEnum):
                        v_final.append(i.to_json(safe))
                    else:
                        v_final.append(i)
            elif isinstance(v, Dict):
                v_final = {}
                for i, j in v.items():
                    if isinstance(j, ParentModel) or isinstance(j, ParentEnum):
                        v_final[i] = j.to_json(safe)
                    else:
                        v_final[i] = j

            final_result[k] = v_final
        return final_result


def cast(dest: Type["C"], src: ParentModel) -> ParentModel:
    """This method copies the src parent model object to destination provided parent model class obj"""
    json_obj = src.to_json(safe=False)
    m = dest(**json_obj)

    return m


class DateFormatTypes(ParentEnum):
    """This is schema definition for all possible for date format types"""

    date = "%Y-%m-%d"
    timestamp = "%Y-%m-%dT%H:%M:%S"
    date_integer = "%Y%m%d"


class BaseCredentials(ParentModel):
    """This is the schema definition to be used for credentials to be used for the all preceeding modules"""

    client_id: Optional[str] = Field(
        None,
        description="client id/user for autentication",
        json_schema_extra={"secret": True},
    )
    client_secret: Optional[str] = Field(
        None,
        description="client secret/password for autentication",
        json_schema_extra={"secret": True},
    )
    client_certificate: Optional[str] = Field(
        None, description="client certificate for autentication"
    )
