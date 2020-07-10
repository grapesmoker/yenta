import json
import pytest
import networkx as nx

from datetime import datetime
from pathlib import Path

from yenta.config import settings
from yenta.tasks.Task import task
from yenta.pipeline import Pipeline, TaskResult, PipelineResult, InvalidTaskResultError
from yenta.values import Value
from yenta.artifacts import FileArtifact

settings.YENTA_JSON_STORE_PATH = Path('tests/tmp/pipeline.json')


def test_pipeline_creation():

    @task
    def foo(previous_results=None):
        pass

    @task
    def bar():
        pass

    @task(depends_on=['foo', 'bar'])
    def baz(previous_results):
        pass

    pipeline = Pipeline(foo, bar, baz)
    assert(pipeline.task_graph.has_edge('foo', 'baz'))
    assert(pipeline.task_graph.has_edge('bar', 'baz'))
    assert(not pipeline.task_graph.has_edge('foo', 'bar'))


def test_pipeline_with_cycles():

    @task(depends_on=['baz'])
    def foo(previous_results=None):
        pass

    @task(depends_on=['foo'])
    def bar():
        pass

    @task(depends_on=['bar'])
    def baz(previous_results):
        pass

    with pytest.raises(nx.NetworkXUnfeasible):
        pipeline = Pipeline(foo, bar, baz)


def test_run_pipeline_with_past_results():

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo(previous_results=None) -> TaskResult:
        return TaskResult({'x': Value(1)}, {})

    @task
    def bar():
        return TaskResult({'y': Value(2)}, {})

    @task(depends_on=['foo', 'bar'])
    def baz(previous_results: PipelineResult):

        x = previous_results.values('foo', 'x')
        y = previous_results.values('bar', 'y')

        result = x + y

        return TaskResult({'sum': Value(result)}, {})

    pipeline = Pipeline(foo, bar, baz)
    result = pipeline.run_pipeline()

    sum = result.values('baz', 'sum')
    assert(sum == 3)

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()


def test_pipeline_run_with_explicit_params():

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo():
        return {'values': {'x': Value(1)}}

    @task
    def bar():
        return TaskResult({'y': Value(2)}, {})

    @task(depends_on=['foo', 'bar'])
    def baz(x: 'foo__values__x', y: 'bar__values__y'):

        result = x + y

        return TaskResult({'sum': Value(result)}, {})

    pipeline = Pipeline(foo, bar, baz)
    result = pipeline.run_pipeline()
    sum = result.values('baz', 'sum')

    assert (sum == 3)
    assert (pipeline._tasks_executed == {'foo', 'bar', 'baz'})
    assert (pipeline._tasks_reused == set())

    raw_1 = json.load(open(settings.YENTA_JSON_STORE_PATH, 'r'))
    t1 = settings.YENTA_JSON_STORE_PATH.stat().st_mtime

    result = pipeline.run_pipeline()
    sum = result.values('baz', 'sum')
    assert (sum == 3)
    assert (pipeline._tasks_reused == {'foo', 'bar', 'baz'})

    raw_2 = json.load(open(settings.YENTA_JSON_STORE_PATH, 'r'))
    t2 = settings.YENTA_JSON_STORE_PATH.stat().st_mtime

    assert(raw_1 == raw_2)
    assert(t1 != t2)

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()


def test_pipeline_run_with_artifacts():

    foo_file = './foo.dat'
    bar_file = './bar.dat'
    baz_file = './baz.dat'

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo() -> TaskResult:
        with open(foo_file, 'w') as f:
            f.write('foo')
        return TaskResult({'x': 1},
                          {'foo_file': FileArtifact(foo_file, str(datetime.now()))})

    @task
    def bar():
        with open(bar_file, 'w') as f:
            f.write('bar')
        return TaskResult({'y': Value(2)},
                          {'bar_file': FileArtifact(bar_file, str(datetime.now()))})

    @task(depends_on=['foo', 'bar'])
    def baz(x: 'foo__values__x', y: 'bar__values__y',
            foo_artifact: 'foo__artifacts__foo_file', bar_artifact: 'bar__artifacts__bar_file'):
        sum_x_y = x + y
        with open(foo_artifact.location, 'r') as f:
            foo_data = f.read()
        with open(bar_artifact.location, 'r') as f:
            bar_data = f.read()
        with open(baz_file, 'w') as f:
            f.write(foo_data + bar_data)
        return TaskResult({'sum': Value(sum_x_y)},
                          {'baz_file': FileArtifact(baz_file, str(datetime.now()))})

    pipeline = Pipeline(foo, bar, baz)

    result = pipeline.run_pipeline()
    sum = result.values('baz', 'sum')
    assert (sum == 3)

    baz_artifact: FileArtifact = result.artifacts('baz', 'baz_file')

    with open(baz_artifact.location, 'r') as f:
        baz_data = f.read()

    assert(baz_data == 'foobar')

    Path(foo_file).unlink()
    Path(bar_file).unlink()
    Path(baz_file).unlink()

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()


def test_pipeline_with_non_scalar_values():

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo() -> TaskResult:
        return TaskResult({'x': Value([1, 2, 3])})

    @task
    def bar():
        return TaskResult({'y': [4, 5, 6]})

    @task(depends_on=['foo', 'bar'])
    def baz(x: 'foo__values__x', y: 'bar__values__y'):
        sum_x_y = x + y
        return TaskResult({'result': Value(sum_x_y)})

    pipeline = Pipeline(foo, bar, baz)

    result = pipeline.run_pipeline()
    answer = result.values('baz', 'result')
    assert (answer == [1, 2, 3, 4, 5, 6])

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()


def test_pipeline_run_with_selectors():

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo() -> TaskResult:
        return TaskResult({'x': Value([1, 2, 3])})

    @task
    def bar():
        return TaskResult({'y': [4, 5, 6]})

    def foo_x_selector(result: PipelineResult):
        return sum(result.values('foo', 'x'))

    def bar_y_selector(result: PipelineResult):
        return sum(result.values('bar', 'y'))

    @task(depends_on=['foo', 'bar'], selectors={'x': foo_x_selector, 'y': bar_y_selector})
    def baz(x, y):
        sum_x_y = x + y
        return TaskResult({'result': Value(sum_x_y)})

    pipeline = Pipeline(foo, bar, baz)

    result = pipeline.run_pipeline()
    answer = result.values('baz', 'result')
    assert (answer == 21)

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()


def test_wrap_value():

    v = Value(1)
    assert v == TaskResult._wrap_as_value(v)
    assert v == TaskResult._wrap_as_value({'value': 1})
    assert v == TaskResult._wrap_as_value(1)

    with pytest.raises(ValueError) as ex:
        TaskResult._wrap_as_value({1})

    assert f'Can not wrap {set([1])} in a Value'


def test_wrap_task_output():

    t = TaskResult(values={'v': 1}, artifacts={})
    assert t == Pipeline._wrap_task_output({'values': {'v': 1}}, 'task_name')
    assert t == Pipeline._wrap_task_output(t, 'task_name')

    with pytest.raises(InvalidTaskResultError) as ex:
        Pipeline._wrap_task_output([1], 'task_name')

    assert "Task task_name returned invalid result of type <class 'list'>, " \
           "expected either a dict or a TaskResult" in str(ex.value)


def test_non_serializable_result():

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo() -> TaskResult:
        return TaskResult({'x': 1})

    # a task with a non-serializable result
    @task(depends_on=['foo'])
    def bar():
        return TaskResult({'y': Path('abc')})

    @task(depends_on=['bar'])
    def baz():
        return TaskResult({'z': 2})

    pipeline = Pipeline(foo, bar)

    # we'll have an exception that will prevent baz from running
    result = pipeline.run_pipeline()

    with open(settings.YENTA_JSON_STORE_PATH, 'r') as f:
        cached_result = json.load(f)
        assert result == PipelineResult(**cached_result)
        assert 'baz' not in cached_result
