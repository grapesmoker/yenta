import networkx as nx
import matplotlib.pyplot as plt

from yenta.tasks.Task import task
from yenta.pipeline.Pipeline import Pipeline, TaskResult, PipelineResult
from yenta.values.Value import Value
from yenta.artifacts.Artifact import Artifact


def test_pipeline_creation():

    @task
    def foo(values=None, artifacts=None):
        pass

    @task
    def bar(values=None, artifacts=None):
        pass

    @task(depends_on=['foo', 'bar'])
    def baz(values=None, artifacts=None):
        pass

    pipeline = Pipeline()
    pipeline.build_task_graph()


def test_pipeline_run():

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
