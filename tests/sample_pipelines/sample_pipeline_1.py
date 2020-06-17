from yenta.tasks.Task import task
from yenta.pipeline.Pipeline import TaskResult
from yenta.values.Value import Value
from yenta.artifacts.Artifact import FileArtifact


@task
def foo():
    print('hello from foo task')
    return TaskResult({'result': Value('hello world')})


@task
def bar():
    raise ValueError('oh noes')
    print('hello from bar task')
    # return TaskResult({'result': Value('result', 1)})
    #


@task(depends_on=['foo', 'bar'])
def baz():
    print('hello from baz task')
    return TaskResult({'result': Value('result', 1)})