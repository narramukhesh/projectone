from projectoneflow.core.schemas import ParentModel, ParentEnum
from pydantic import Field, ConfigDict, model_validator
from projectoneflow.core.schemas.features import ExecutionFeatureOptions
from typing import Dict, Any, Optional, Union, List
from projectoneflow.core.schemas.event import SubscribedEvent


class SparkExecutionTypes(ParentEnum):
    """This class is the all possible execution spark types"""

    file = "file"
    body = "body"
    module = "module"


class SparkExecution(ParentModel):
    """This class is a schema definition for the spark task execution function configuration"""

    name: str = Field(..., description="Name of the execution function")
    type: SparkExecutionTypes = Field(
        SparkExecutionTypes.module,
        description="type of the input of the execution function",
    )
    source: str = Field(..., description="from where to get the input source")
    extra_arguments: Optional[Dict[str, Any]] = Field(
        None, description="Extra arguments to be passed to the execution function"
    )
    features: ExecutionFeatureOptions = Field(
        ExecutionFeatureOptions(),
        description="Feature to be applied on the execution function",
    )
    events: Optional[Union[List[SubscribedEvent], None]] = Field(
        None, description="events to be handled specific to operator"
    )
    model_config = ConfigDict(extra="allow")

    def __str__(self):
        return f"Execution Configuration with name {self.name} from source  {self.source} function object will be instantiated"
