import pytest

from yenta.tasks import (
    task, build_parameter_spec, TaskDef, InvalidTaskDefinitionError, ParameterSpec,
    ParameterType, ResultSpec, ResultType
)


def test_build_param_spec():

    @task(depends_on=['bar'])
    def foo(bar_result):
        pass

    spec = build_parameter_spec(foo, ['bar'])
    expected_spec = [ParameterSpec('bar_result',
                                   ParameterType.PIPELINE_RESULTS,
                                   result_spec=ResultSpec(result_task_name='bar'))]

    assert(spec == expected_spec)

    @task
    def bar(x, y):
        pass

    spec = build_parameter_spec(bar, ['foo.x', 'foo.y'])
    expected_spec = [
        ParameterSpec('x', ParameterType.EXPLICIT, ResultSpec('foo', 'x')),
        ParameterSpec('y', ParameterType.EXPLICIT, ResultSpec('foo', 'y'))
    ]

    assert(spec == expected_spec)


def test_invalid_param_spec():

    with pytest.raises(InvalidTaskDefinitionError) as ex:

        @task
        def foo(x, y):
            pass

        _ = build_parameter_spec(foo, depends_on=['bar.x', 'bar.y', 'bar.z'])

    assert('Insufficient number of parameters' in str(ex.value))


def test_task_definition():

    @task
    def foo(previous_results):
        pass

    expected_def = TaskDef('foo', [], True, [])

    assert(foo.task_def == expected_def)

