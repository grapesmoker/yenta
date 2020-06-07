import pytest

from yenta.tasks.Task import task, TaskDef, InvalidTaskDefinitionError, TASK_LIST


def test_task_definition():

    @task
    def foo(values=None, artifacts=None):
        pass

    expected_def = TaskDef('foo', None, True, foo)

    assert(len(TASK_LIST) == 1)
    assert(expected_def == TASK_LIST[0])


def test_improper_task_def():

    with pytest.raises(InvalidTaskDefinitionError) as ex:
        @task
        def foo():
            pass

    assert('values' in str(ex.value) and 'artifacts' in str(ex.value))
