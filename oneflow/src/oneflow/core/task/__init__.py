from abc import ABC


class Task(ABC):
    class Builder:
        """This class is used to build the task input, output and execution"""

    def execute(self):
        "This is the entry-point for execution"
