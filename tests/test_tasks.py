import pytest

from yenta.tasks import (
    task, build_parameter_spec, TaskDef, InvalidTaskDefinitionError, ParameterSpec,
    ParameterType, ResultSpec, ResultType
)


def test_build_param_spec():

    @task
    def foo(previous_results):
        pass

    spec = build_parameter_spec(foo)
    expected_spec = [ParameterSpec('previous_results', ParameterType.PIPELINE_RESULTS)]

    assert(spec == expected_spec)

    @task
    def bar(x: 'foo__values__x', y: 'foo__artifacts__y'):
        pass

    spec = build_parameter_spec(bar)
    expected_spec = [
        ParameterSpec('x', ParameterType.EXPLICIT, ResultSpec('foo', ResultType.VALUE, 'x')),
        ParameterSpec('y', ParameterType.EXPLICIT, ResultSpec('foo', ResultType.ARTIFACT, 'y'))
    ]

    assert(spec == expected_spec)

    selector = lambda res: res
    selectors = {'x': selector}

    @task(selectors={'x': selector})
    def baz(x):
        pass

    spec = build_parameter_spec(baz, selectors)
    expected_spec = [ParameterSpec('x', ParameterType.EXPLICIT, None, selector)]

    assert(spec == expected_spec)


def test_invalid_param_spec():

    with pytest.raises(InvalidTaskDefinitionError) as ex:

        @task
        def foo(x: 'bar__values__x', y: int):
            pass

        _ = build_parameter_spec(foo)

    assert('Annotation string or selector missing' in str(ex.value))

    with pytest.raises(InvalidTaskDefinitionError) as ex:

        @task
        def foo(x: 'bar__values__x', y: 'bad_annotation'):
            pass

        _ = build_parameter_spec(foo)

    assert ('Invalid function annotation' in str(ex.value))


def test_task_definition():

    @task
    def foo(previous_results):
        pass

    expected_def = TaskDef('foo', None, True, [ParameterSpec('previous_results', ParameterType.PIPELINE_RESULTS)])

    assert(foo.task_def == expected_def)

