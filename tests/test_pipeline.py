import networkx as nx
import matplotlib.pyplot as plt
import json

from datetime import datetime
from pathlib import Path

from yenta.config import settings
from yenta.tasks.Task import task
from yenta.pipeline.Pipeline import Pipeline, TaskResult, PipelineResult
from yenta.values.Value import Value
from yenta.artifacts.Artifact import Artifact, FileArtifact

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


def test_run_pipeline_with_past_results():

    if settings.YENTA_JSON_STORE_PATH.exists():
        settings.YENTA_JSON_STORE_PATH.unlink()

    @task
    def foo(previous_results=None) -> TaskResult:
        return TaskResult({'x': Value(1)}, {})

    @task
    def bar(previous_results=None):
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
    def foo() -> TaskResult:
        return TaskResult({'x': Value(1)}, {})

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
    # import ipdb
    # ipdb.set_trace()
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
        return TaskResult({'x': Value(1)},
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
