from dataclasses import dataclass
from functools import wraps
from inspect import signature
from typing import Callable, List, Optional, Any


@dataclass
class TaskDef:

    name: str
    depends_on: Optional[List[str]]
    pure: bool
    # task: Callable[[dict, dict], Any]


class InvalidTaskDefinitionError(Exception):
    pass


def task(_func=None, *, depends_on: str = None, pure: bool = True):

    def decorator_task(func: Callable):

        sig = signature(func)
        param_names = sig.parameters.keys()
        if 'previous_results' not in param_names:
            raise InvalidTaskDefinitionError('Task function must be defined as taking a `previous_results` argument.')

        @wraps(func)
        def task_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        task_wrapper.task_def = TaskDef(**{
            'name': func.__name__,
            'depends_on': depends_on,
            'pure': pure,
        })

        return task_wrapper

    if _func is None:
        return decorator_task
    else:
        return decorator_task(_func)
