from abc import ABC, abstractmethod
from typing import Any


class TaskState(ABC):
    """This is a abstract class interface for implementing the task key-value state store"""

    @abstractmethod
    def set(self, source: str, key: str, value: str):
        """This method used for the setting the key and value"""

    @abstractmethod
    def get(self, source: str, key: str, default: str = None) -> Any:
        """This method used for the getting the value for specific key"""
