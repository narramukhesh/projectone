import json
from typing import Any
from projectoneflow.core.schemas.state import ChangeFeatureValue


class Result:
    def __init__(
        self,
        load_type: Any,
        status: str,
        exception: Any,
        result: Any,
        start_time: Any,
        end_time: Any,
        batch_id: str,
        batch_name: str,
        event_severity: Any,
    ):
        """
        Initialization method to initialize the Output result

        Parameters
        ------------
        load: Any
            this arguments define the type of load processed whether the batch/stream
        status: str
            this argument to define status of the result whether success/failure
        exception: Any
            this argument to exception if anything occured
        result: str
            this argument to result to processed by further process

        """
        self.status = status
        self.exception = exception
        self.result = result
        self.load_type = load_type
        self.start_time = start_time
        self.end_time = end_time
        self.batch_id = batch_id
        self.batch_name = batch_name
        self.event_severity = event_severity

    def to_json(self):
        """This method is converts the class state to json"""
        return json.dumps({k: v.__str__() for k, v in self.__dict__.items()})


class InputResult(Result):
    """This class is output result response shared returned by the spark task input result output"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ExecutionResult(Result):
    """This class is output result response shared returned by the spark task Execution output"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class OutputResult(Result):
    """This class is output result response shared returned by the spark task output"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ChangeDataCaptureResult(Result):
    """This class is the result response from the change data capture"""

    def __init__(
        self,
        path: str,
        attribute: str,
        start_value: ChangeFeatureValue,
        end_value: ChangeFeatureValue,
        extra_info: str,
        filter_expr: str,
        options: str,
    ):
        """
        Initialization method to initialize the ChangeDataCaptureResult

        Parameters
        ------------
        path: Any
            Path of the table/file data source
        attribute: str
            the attribute of the cdc output
        start_value: ChangeFeatureValue
            this argument to specify the start value of the cdc
        end_value: ChangeFeatureValue
            this argument to specify the end value of the cdc
        extra_info: str
            this argument to specify the extra information for the source cdc
        end_value: str
            this argument to specify the end value of the cdc

        """
        self.path = path
        self.attribute = attribute
        self.start_value = start_value
        self.end_value = end_value
        self.extra_info = extra_info
        self.filter_expr = filter_expr
        self.options = options
