import networkx as nx
import matplotlib.pyplot as plt

from yenta.tasks.Task import task
from yenta.pipeline.Pipeline import Pipeline, TaskResult, PipelineResult
from yenta.values.Value import Value
from yenta.artifacts.Artifact import Artifact


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

    @task
    def foo(previous_results=None) -> TaskResult:
        return TaskResult({'x': Value('x', 1)}, {})

    @task
    def bar(previous_results=None):
        return TaskResult({'y': Value('y', 2)}, {})

    @task(depends_on=['foo', 'bar'])
    def baz(previous_results: PipelineResult):

        x = previous_results.values('foo', 'x')
        y = previous_results.values('bar', 'y')

        result = x + y

        return TaskResult({'sum': Value('sum', result)}, {})

    pipeline = Pipeline(foo, bar, baz)
    result = pipeline.run_pipeline()

    sum = result.values('baz', 'sum')
    assert(sum == 3)


def test_pipeline_run_with_explicit_params():

    @task
    def foo() -> TaskResult:
        return TaskResult({'x': Value('x', 1)}, {})

    @task
    def bar():
        return TaskResult({'y': Value('y', 2)}, {})

    @task(depends_on=['foo', 'bar'])
    def baz(x: 'foo__values__x', y: 'bar__values__y'):

        result = x + y

        return TaskResult({'sum': Value('sum', result)}, {})

    pipeline = Pipeline(foo, bar, baz)
    result = pipeline.run_pipeline()

    sum = result.values('baz', 'sum')
    assert (sum == 3)
