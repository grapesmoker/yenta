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
    result_type: ResultType
    result_var_name: str


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


def build_parameter_spec(func, selectors: Dict[str, Callable] = None):

    sig = signature(func)
    param_names = list(sig.parameters.keys())

    # three options available:
    # 1. a single parameter which will receive the full intermediate pipeline state
    # 2. any number of parameters annotated with a string of the form:
    #   '<task_name>__<values|artifacts>__<value_name|artifact_name>'
    # 3. any number of parameters accompanied by a selector dictionary, of the form:
    #   {parameter_name: callable} where the callable takes the previous state as a
    #   parameter and produces an arbitrary value
    # note the double underbars like in the django query language

    # TODO: fix bug where a single parameter cannot be underbar-referenced

    err_format = '<task_name>__<values|artifacts>__<value_name|artifact_name>'

    if len(param_names) == 0:
        spec = []
    elif len(param_names) == 1 and '__' not in param_names[0] and not selectors:
        spec = [ParameterSpec(param_names[0], ParameterType.PIPELINE_RESULTS)]
    else:
        spec = []
        for name in param_names:
            param = sig.parameters[name]
            if selectors:
                selector = selectors[name]
                spec.append(ParameterSpec(name, ParameterType.EXPLICIT, None, selector))
            elif isinstance(param.annotation, str):
                annot = param.annotation.split('__')
                if len(annot) != 3:
                    raise InvalidTaskDefinitionError(
                        f'Invalid function annotation for parameter {name}.'
                        f'Function parameters must be annotated using the following format:'
                        f'\n{err_format}')

                spec.append(ParameterSpec(name, ParameterType.EXPLICIT, ResultSpec(*annot)))
            else:
                raise InvalidTaskDefinitionError(
                    f'Annotation string or selector missing for variable {name}.'
                    f'Function parameters must be annotated using the following format:'
                    f'\n{err_format}')

    return spec


def task(_func=None, *, depends_on: Optional[List[str]] = None, pure: bool = True, selectors=None):

    def decorator_task(func: Callable):

        @wraps(func)
        def task_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        setattr(task_wrapper, 'task_def', TaskDef(
            name=func.__name__,
            depends_on=depends_on,
            pure=pure,
            param_specs=build_parameter_spec(func, selectors)
        ))

        setattr(task_wrapper, '_yenta_task', True)

        return task_wrapper

    if _func is None:
        return decorator_task
    else:
        return decorator_task(_func)
