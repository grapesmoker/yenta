from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from inspect import signature
from typing import Callable, List, Dict, Optional


class ParameterType(int, Enum):

    PIPELINE_RESULTS = 1
    EXPLICIT = 2


class ResultType(str, Enum):

    VALUE = 'values'
    ARTIFACT = 'artifacts'


@dataclass
class ResultSpec:

    result_task_name: str
    result_var_name: Optional[str] = None


@dataclass
class ParameterSpec:

    param_name: str
    param_type: ParameterType
    result_spec: Optional[ResultSpec] = None
    selector: Optional[Callable] = None


@dataclass
class TaskDef:

    name: str
    depends_on: Optional[List[str]]
    pure: bool
    param_specs: List[ParameterSpec] = field(default_factory=list)


class InvalidTaskDefinitionError(Exception):
    pass


def build_parameter_spec(func, depends_on: List[str]):

    sig = signature(func)
    param_names = list(sig.parameters.keys())

    if len(param_names) < len(depends_on):

        raise InvalidTaskDefinitionError(
            f'Insufficient number of parameters ({len(param_names)}) defined '
            f'for a task that depends on {len(depends_on)} inputs.')
    elif len(param_names) == 0:
        spec = []
    else:
        spec = []
        for dependency, param_name in zip(depends_on, param_names):
            if '.' not in dependency:
                task_dep_name = dependency
                var_dep_name = None
                param_type = ParameterType.PIPELINE_RESULTS
            else:
                task_dep_name, var_dep_name = dependency.split('.')
                param_type = ParameterType.EXPLICIT
            result_spec = ResultSpec(task_dep_name, var_dep_name)
            spec.append(ParameterSpec(param_name, param_type, result_spec))

    return spec


def task(_func=None, *, depends_on: Optional[List[str]] = None, pure: bool = True, selectors=None):

    depends_on = depends_on or []

    def decorator_task(func: Callable):

        @wraps(func)
        def task_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        setattr(task_wrapper, 'task_def', TaskDef(
            name=func.__name__,
            depends_on=depends_on,
            pure=pure,
            param_specs=build_parameter_spec(func, depends_on)
        ))

        setattr(task_wrapper, '_yenta_task', True)

        return task_wrapper

    if _func is None:
        return decorator_task
    else:
        return decorator_task(_func)
