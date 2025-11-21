from typing import Protocol
from oneflow.core.types import F, R
from oneflow.core.event import ExceptionEvent
from typing import Type
from datetime import datetime
import importlib


class Operator(Protocol):
    def execute(self):
        """This is the execution of the pre-step, execute, post-steps"""


def execute_step(result_type: Type[F]):
    result_attr = getattr(
        importlib.import_module("oneflow.core.schemas.result"), result_type
    )

    def __execute_step(function: Type[F]) -> Type[F]:
        def __inner(*args, **kwargs) -> Type[R]:
            exception = None
            status = "Success"
            load_type = args[0].context.type
            batch_id = args[0].context.batch_id
            batch_name = args[0].context.batch_name
            start_time = datetime.now()
            func_result = None
            event_severity = None
            try:
                func_result = function(*args, **kwargs)
            except Exception as e:
                exception = e
                exc = e
                while exc:
                    if exc.__class__.__name__ in args[0].events:
                        event = ExceptionEvent(exc)
                        args[0].context.event_manager.push(event)
                        event_severity = args[0].events[exc.__class__.__name__]
                    exc = exc.__cause__ or exc.__context__
                status = "Failure"
            end_time = datetime.now()
            result = result_attr(
                load_type=load_type,
                result=func_result,
                status=status,
                exception=exception,
                start_time=start_time,
                end_time=end_time,
                batch_id=batch_id,
                batch_name=batch_name,
                event_severity=event_severity,
            )
            return result

        return __inner

    return __execute_step
