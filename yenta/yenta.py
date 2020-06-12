"""Main module."""
from yenta.tasks.Task import task
from yenta.pipeline.Pipeline import TaskResult
from yenta.values.Value import Value
from yenta.artifacts.Artifact import FileArtifact


print('asdfasdfasdfas')

@task
def foo():
    print('hello world')
