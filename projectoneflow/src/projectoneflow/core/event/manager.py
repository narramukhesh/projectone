from projectoneflow.core.event import Event, EndEvent, EventSubscription
from projectoneflow.core.schemas.event import EventType
import threading
from queue import Queue
from projectoneflow.core.observability.logging import Logger

logger = Logger.get_logger(__name__)


class EventManager:
    """This class is used for the managing the events"""

    EVENT_MGR_TH_DESC = "EventManagerListner"

    def __init__(self):
        """Initialization method to manages the events to be created"""
        self._subscriptions = {}
        self.__event_bus = Queue()
        self.__end_task_queue = []
        self.__delay_task_queue = []
        self.__internal_thread = threading.Thread(target=self.run, daemon=True)
        self.__internal_thread.name = self.__class__.EVENT_MGR_TH_DESC
        logger.debug("Initializing the Event Manager Listener")

    def subscribe(self, event: Event, fn: callable):
        """
        This is the method to subscribe to the event with the callable function

        Parameters
        -------------------
        event: Event
            Event to which it should be subscribed
        fn: callable
            callable function to which it should be executed

        Returns
        --------------------
        int: returns the subscriber id
        """
        event_subscription = self._subscriptions.get(event, EventSubscription())
        self._subscriptions[event] = event_subscription
        subscriber_id = event_subscription.register(fn)
        return subscriber_id

    def unsubscribe(self, event: Event, subscribed_id: int):
        """
        This is the method to subscribe to the event with the callable function

        Parameters
        -------------------
        event: Event
            Event to which it should be subscribed
        subscribed_id: int
            subscribed id to which to be unsubscribed
        """
        event_subscription = self._subscriptions.get(event, None)
        if event_subscription is not None:
            event_subscription.unregister(subscribed_id)

    def start(self):
        """This method starts the event manager which starts to listening the events"""
        logger.debug("Starting the Event Listener deamon thread")
        self.__internal_thread.start()

    def stop(self):
        """This method stops the event manager which end to stop the internal daemon thread"""
        self.__internal_thread.join()
        for de_th in self.__delay_task_queue:
            if not de_th.finished():
                de_th.join()
        logger.debug("Stopped the Event Listener deamon thread")

    def push(self, event: Event):
        """This is the events to be pushed to the queue"""
        self.__event_bus.put(event)

    def execute_handlers(self, event: Event):
        """This method executes the handler register to the event manager"""
        event_handlers = self._subscriptions.get(event.name, None)
        if event_handlers is not None:
            for h in event_handlers.get():
                h(event)

    def dispatch(self, event: EventType, end: bool = False):
        """This method dispatches the event created by the any event causing incident"""

        if event.etype == EventType.END:
            if end:
                self.execute_handlers(event)
            else:
                self.__end_task_queue.append(event)
        elif event.etype == EventType.DELAYED:
            delay_thread = threading.Timer(
                event.get_interval(), self.execute_handlers, (event,)
            )
            delay_thread.daemon(True)
            self.__delay_task_queue(delay_thread)
            delay_thread.start()
        else:
            self.execute_handlers(event)

    def run(self):
        """This method run which will be the consumer/listens to the event bus"""
        while True:
            event = self.__event_bus.get()
            if isinstance(event, EndEvent):
                for e in self.__end_task_queue:
                    self.dispatch(e, end=True)
                self.__event_bus.task_done()
                break
            self.dispatch(event)
            self.__event_bus.task_done()
