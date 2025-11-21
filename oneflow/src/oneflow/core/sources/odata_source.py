"""This module is to implement the Creatio Source Connector"""

from oneflow.core.schemas.sources import ReadOptions
from oneflow.core.sources import SparkSource
from pydantic import Field
from typing import Optional, Union


class OdataSource(SparkSource):
    """This class is implementation of the creatio based source"""

    read_supported = ["file"]
    read_extract_supported = ["batch", "stream"]
    read_features_supported = [
        "drop_columns_feature",
        "schema_inference_from_registry",
        "filter_data_feature",
    ]
    write_supported = []
    write_features_supported = []
    write_type_supported = []

    class ReadOptions(ReadOptions):
        """This class is creatio source read options"""

        clientID: str = Field(
            ...,
            description="client id for the creatio autentication",
            json_schema_extra={"secret": True},
        )
        clientSecret: str = Field(
            ...,
            description="client secret for the creatio  autentication",
            json_schema_extra={"secret": True},
        )
        identityUrl: str = Field(
            ...,
            description="Identity url to request the access token for autentication",
        )
        instanceUrl: str = Field(
            ..., description="Instance Url to request the data for the creatio endpoint"
        )
        row_per_page_fetch: Optional[str] = Field(
            "10000", description="Rows per page to fetch for each request"
        )
        max_pages_to_process: Optional[str] = Field(
            "5",
            description=" No. of Pages of the data to be processed for each request",
        )
        read_connection_timeout: Optional[str] = Field(
            "1800000", description="read connection timeout in milliseconds"
        )
        fields_to_be_selected: Optional[Union[str, None]] = Field(
            None, description="Fields selected from the string of the columns"
        )
        earliest_time: Optional[Union[str, None]] = Field(
            "946684800000",
            description="earliest time to be specified for the micro batch stream",
        )
        predicates: Optional[Union[str, None]] = Field(
            "", description="Predicates to be used to push down on the source"
        )

        def model_dump(self, *args, **kwargs):
            """This is the custom model_dump used to create the model process"""
            exclude = set()
            for attr in ["earliest_time", "fields_to_be_selected", "predicates"]:
                attr_value = getattr(self, attr)
                if attr_value is None:
                    exclude.add(attr)
            if len(exclude) > 0:
                kwargs["exclude"] = exclude
            return super().model_dump(*args, **kwargs)
