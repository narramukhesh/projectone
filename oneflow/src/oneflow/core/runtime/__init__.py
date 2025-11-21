from oneflow.core.utils.patterns import Singleton
from oneflow.core.observability.logging import Logger
import threading

logger = Logger.get_logger(__name__)


class Runtime(metaclass=Singleton):
    """This class object is created and holds the task cleanup functions at exit of task and will extended for future purposes"""

    def __init__(self):
        self.exit_functions = {}

    def atexit(self, func, *args):
        """This method is used to capture the function and arguments and helpful to clear when task runtime is successful"""
        current_thread = threading.current_thread().ident
        self.exit_functions[current_thread] = self.exit_functions.get(
            current_thread, []
        ) + [(func, *args)]

    def cleanup(self):
        """This is the method to run the functions in the queue"""
        current_thread = threading.current_thread().ident
        funcs = self.exit_functions.get(current_thread, [])

        for func in funcs:
            try:
                func[0](*func[1:])
            except Exception as e:
                logger.debug(
                    f"Problem with the executing the cleanup function registered {func}, failed with error {e} "
                )
                continue
