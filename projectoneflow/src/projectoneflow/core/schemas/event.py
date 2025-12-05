from projectoneflow.core.schemas import ParentModel, ParentEnum, BaseCredentials
from pydantic import Field
from typing import Dict, List, Optional, Union


class EventType(ParentEnum):
    """This is the event types which corresponds to the execution model"""

    ASAP = "ASAP"
    DELAYED = "DELAYED"
    END = "END"


#### Event handler specific schema objects
class SubscribedEventConsumerEnum(ParentEnum):
    """This is the list of the avaiable event handle to say how to handle the events"""

    NOTIFICATION = "notification"
    CUSTOM = "custom"


class APIAuthType(ParentEnum):
    """This is the definition to hold list of choices of the api types"""

    BASIC = "basic"
    OAUTH = "oauth"


class APICredentials(BaseCredentials):
    """This is the schema definition for the api credentials to be used by the notification"""

    auth_url: Optional[Union[None, str]] = Field(
        None,
        description=" authentication url to be requested by the server to ge the auth token",
    )
    request_url: str = Field(
        ..., description="Request url to be included to be requested by the server"
    )
    auth_type: Optional[APIAuthType] = Field(
        APIAuthType.BASIC,
        description="This is the auth type to be used to send the request",
    )
    headers: Optional[Union[None, Dict[str, str]]] = Field(
        None, description="The headers to be passed with the requesting url"
    )


class SubscribedEventConsumer(ParentModel):
    """This event consumer schmea defintion to be used for every pattern"""

    type: SubscribedEventConsumerEnum = Field(
        SubscribedEventConsumerEnum.NOTIFICATION,
        description="The type of the consumer and corresponding handler to be executed",
    )
    credentials: Optional[None | APICredentials] = Field(
        None,
        description="credentials to be used for auth for corresponding api/custom function",
    )
    fn: Optional[None | str] = Field(
        None, description="this is the function to be executed to handle the events"
    )
    message: Optional[None | str] = Field(
        None, description="The message to be passed with the event handler"
    )
    condition: Optional[None | str] = Field(
        None, description="condition when this event should be handled"
    )


class SubscribedEventHandleEnum(ParentEnum):
    """This is the list of the avaiable event handle to say how to handle the events"""

    CONTINUE = "CONTINUE"
    FAIL = "FAIL"
    STOP_WH_FAIL = "STOP_WH_FAIL"


class SubscribedEventHandle(ParentModel):
    """This is the list of the avaiable event handle to say how to handle the events"""

    fn: str = Field(
        ..., description="this is the function to be executed to handle the events"
    )


class SubscribedEvent(ParentModel):
    """This class is definition of the subscribed events to be used for the stages in the operators"""

    consumers: Union[List[SubscribedEventConsumer], None] = Field(
        None,
        description="subscriber consumer to run indepedent to the task flow, here not major operation to be handled. only notifications or any custom operation to be completed",
    )

    handle: Union[SubscribedEventHandleEnum, SubscribedEventHandle] = Field(
        SubscribedEventHandleEnum.STOP_WH_FAIL,
        description="defines how to handle the subscribed events from the operator",
    )
    type: str = Field(
        ...,
        description="Event which can be anything in the framework which excepts and handles the function",
    )


####
