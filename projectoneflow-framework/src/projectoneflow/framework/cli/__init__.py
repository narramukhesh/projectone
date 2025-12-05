from projectoneflow.core.schemas import ParentModel, ParentEnum
from typing import Optional, Union, Dict, Any
from pydantic import Field
from rich.table import Table
import pandas as pd


class CliOutTypes(ParentEnum):
    """This is the enum definition for the cli out type supported by the Cli json"""

    exception = "EXCEPTION"
    output = "OUTPUT"


class CliOutput(ParentModel):
    """This is the class definition for the cli output"""

    type: CliOutTypes = Field(CliOutTypes.output, description="Type of the cli output")
    message: Optional[Union[str, None]] = Field(
        None, description="message specified to be displayed for the exception"
    )
    result: Optional[Union[Dict[str, Any], str, None]] = Field(
        None, description="result of the output"
    )
    contract_name: Optional[str] = Field(
        None, description="Contract name where cli out is specified"
    )


class FormattingTable(Table):
    """This is extention to console table to print the results in formatted format"""

    def to_json(self):
        """This method converts the table rows and columns into json format"""
        data = {i.header: i._cells for i in self.columns}
        return pd.DataFrame(data).to_dict("records")
