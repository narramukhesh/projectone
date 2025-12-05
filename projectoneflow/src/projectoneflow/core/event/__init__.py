from typing import Protocol, Type
from projectoneflow.core.schemas.event import (
    EventType,
    SubscribedEventConsumer,
    SubscribedEventConsumerEnum,
)
from projectoneflow.core.utils import post_webhook_api
from projectoneflow.core.observability.logging import Logger
from functools import partial
from projectoneflow.core.types import F

logger = Logger.get_logger(__name__)


class EventSubscription:
    """This is the event subscription model class"""

    def __init__(self):

        self.no_subscribers = 0
        self.subscribers = {}

    def register(self, fn: callable) -> int:
        """
        This method is to register the subscriber and append to end of the list and return the id for the subcribers

        Parameters
        --------------------------
        fn: Callable
            callable function to be used to register for event subscription

        Returns
        ------------------------
        int: Unique id for the subscription list
        """
        self.no_subscribers += 1
        subscribe_function = id(fn)
        self.subscribers[subscribe_function] = fn

        return subscribe_function

    def unregister(self, subscriber_id: int):
        """
        This method is to deregister the subscriber and delete from the end of the list

        Parameters
        --------------------------
        subscriber_id: int
            id of the callable function
        """
        del_value = self.subscribers.pop(subscriber_id, None)
        if del_value is not None:
            self.no_subscribers -= 1

    def get(self):
        """
        This method is to get the subscribers from the registered subscribers list, this is the generator function

        Returns
        ------------------------
        Callable: handler function to be executed
        """
        for h in self.subscribers.values():
            yield h


class Event(Protocol):
    """This is the protocol definition for the event where this class will be used for all the package exceptions and results"""

    @property
    def etype(self):
        """This property used to define the event execution style whether to execute the asap,delayed,end"""

    @property
    def name(self):
        """This property used to define the event execution style whether to execute the asap,delayed,end"""


class EndEvent(Event):
    """This event is end event to stop the consumer thread from the event manager"""

    @property
    def etype(self):
        """This property used to define the event execution style whether to execute the asap,delayed,end"""
        return EventType.END

    @property
    def name(self):
        """This property used to define the event execution style whether to execute the asap,delayed,end"""
        return "EndEvent"


class ExceptionEvent(EndEvent):
    """This event is the exception event to be used with the"""

    def __init__(self, exception: Exception):
        super().__init__()
        self.exception = exception
        self.__name = exception.__class__.__name__

    @property
    def etype(self):
        """This property used to define the event execution style whether to execute the asap,delayed,end"""
        return EventType.ASAP

    @property
    def name(self):
        """This property used to define the event execution style whether to execute the asap,delayed,end"""
        self.__name


def event_handler_custom(consumer: SubscribedEventConsumer, event: Event):
    return


def event_handler_notification(consumer: SubscribedEventConsumer, event: Event):
    """
    This is a function to send the webhook notification

    Parameters
    -------------------
        consumer:SubscribedEventConsumer
            subscribed consumer to be passed as first argument which has the information about the event handler consumer
        event: Event
            calling event to be passed to function to be used by the function
    """
    try:
        rep = post_webhook_api(
            consumer.credentials.request_url, consumer.credentials.message
        )
    except Exception as e:
        logger.warning(
            f"Sending the notification failed for {event.name} with error: {e}"
        )


def get_event_handler_function(consumer: SubscribedEventConsumer) -> Type[F]:
    """
    This is the event handler funtion to be used to get the event handler for the subscibed consumer

    Parameters
    -------------------
        consumer:SubscribedEventConsumer
            subscribed consumer to be passed as first argument which has the information about the event handler consumer
    Returns
    -------------------
    FunctionType
        return function object
    """

    if consumer.type == SubscribedEventConsumerEnum.NOTIFICATION:
        return partial(event_handler_notification, consumer=consumer)
    else:
        return partial(event_handler_custom, consumer=consumer)
