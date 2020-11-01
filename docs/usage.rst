=====
Usage
=====

Yenta can be used as a library or invoked from the command line.

Library
-------

It is straightforward to use Yenta as a library.

.. code-block:: python

    from yenta.tasks import task
    from yenta.pipeline import Pipeline, TaskResult
    from datetime import datetime

    @task
    def foo():
        return {'values': {'x': 1}}

    @task
    def bar():
        return {'values': {'y': 2}, 'artifacts': {}}

    @task(depends_on['foo', 'bar'])
    def baz(u: 'foo__values__x', v: 'bar__values__y'):
        with open('baz_results', 'w') as f:
            f.write(u + v)
        return TaskResult(values={'sum': u + v},
                          artifacts={'sum_file': FileArtifact(location='baz_results',
                                                              date_created=str(datetime.now()))})

    pipeline = Pipeline(foo, bar, baz)
    result = pipeline.run()

    print(result.values('baz', 'sum'))

    >>> 3

    with open('baz_results', 'r') as f:
        results = f.read()

    print(results)

    >>> 3

Task Signatures
+++++++++++++++

Tasks are defined by decorating functions with the :code:`@task` decorator. The decorator optionally accepts an
arguments called :code:`depends_on` which lists the names of the tasks on which that task depends. Above, the
tasks :code:`foo` and :code:`bar` do not depend on anything, while the task :code:`baz` depends on both of them.

The signature of a task function is a key feature of Yenta. Tasks can receive their arguments using three different
strategies: they can either receive the results of task execution of their dependencies as one state blob, they
can select slices of the state via annotations, or they can select slices of state via selector functions. In the
above example, the :code:`baz` task takes as its arguments two values, :code:`u` and :code:`v`, which are annotated
as paths into the state. The format of the annotation is:

.. code-block:: python

    '<task_name>__<values|artifacts>__<value_name|artifact_name>'


Thus, in the above example, the argument :code:`u` will take the value named :code:`x` from the result obtained by
executing task :code:`foo`, and likewise the :code:`v` argument will take the value named :code:`y` obtained by
executing task :code:`bar`. Note that the names in the annotations refer to the `output` names in the
:code:`TaskResults` returned by the dependencies; they do not have to match the names of the parameters of the task
being executed. Note further the use of double-underbars as a separator, which should be familiar to anyone
who has worked with Django. If we wanted to instead get the entire state slice corresponding to the results of the
two dependencies of :code:`baz`, we could have written:

.. code-block:: python

    @task(depends_on['foo', 'bar'])
    def baz(past_results: PipelineResult):
        u = past_results.values('foo', 'x')
        v = past_results.values('bar', 'y')
        with open('baz_results', 'w') as f:
            f.write(u + v)
        return TaskResult(values={'sum': u + v},
                          artifacts={'sum_file': FileArtifact(location='baz_results',
                                                              date_created=str(datetime.now()))})

Currently the two parameter styles cannot be mixed, so you must choose at task definition time whether you want
some specific slice of the previous state or the whole thing. If we had a fourth task that depended on :code:`baz`,
we could retrieve, say, the :code:`baz` artifact like so:

.. code-block:: python

    @task(depends_on['baz'])
    def glorp(artifact: 'baz__artifacts__sum_file'):
        # do whatever


or equivalently

.. code-block:: python

    @task(depends_on['baz'])
    def glorp(past_results):
        artifact = past_results.artifacts('baz', 'sum_file')
        # do whatever


.. warning::

    A task will only receive those slices of state which are indicated as part of its dependency chain. If you want
    state for a given task, your downstream task must have that other task as a dependency.

Finally, you can also use selector functions to select pieces of state and possibly so something with them before
passing them as arguments to the downstream task. To see how this is accomplished, consider the following snippet:

.. code-block:: python

    @task
    def foo() -> TaskResult:
        return TaskResult({'x': [1, 2, 3]})

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
        return TaskResult({'sum': sum_x_y})

    pipeline = Pipeline(foo, bar, baz)
    result = pipeline.run()

    print(result.values('baz', 'sum'))

    >>> 21

The :code:`selectors` argument to the task decorator above is a dictionary whose keys are parameter names on the
receiving task and whose values are functions which are to be called on the previous state. These functions receive
the previous state in the form of a :class:`~yenta.pipeline.Pipeline.PipelineResult` object and can return any value
at all. As the above example demonstrates, selectors can optionally perform some operations on the slice of state
they extract.

.. warning::

    Selectors must be pure functions, i.e. they must not modify the state. If supplied, selectors will take precedence
    over annotations. Although you `can` do meaningful work in the selector function, you mostly `should not`; the
    purpose of selectors is to reshape the state into the form expected by the downstream task, but of course you
    can always do that inside the task anyway.

Return Values
+++++++++++++

Tasks can return their results in two ways, both of which are shown above. The first way is as a simple dictionary
whose keys are the names of the returned values, and whose values are... the values. Each value must have its own
name in the result set. A second way is to return the task result directly via a
:class:`~yenta.pipeline.Pipeline.TaskResult` object. In general, the second way is preferable since it is the most
explicit; however, under the hood, Yenta transforms the first format into
:class:`~yenta.pipeline.Pipeline.TaskResult` anyway.

The results of a Yenta task come in two flavors: values and Artifacts. A value is any basic Python value that is
computed during the execution of the task and should be returned to the pipeline. Any Python object that can be pickled
can be a value. Artifacts represent any modifications to external stores that might be created by the task; an example
(currently the only example) of an Artifact is the :class:`~yenta.artifacts.Artifact.FileArtifact`, which represents an
external file generated during the task execution.

.. warning::

    Values must be picklable by Python. The usual caveats about unpickling untrusted code apply. In the previous
    version of Yenta, you could only use JSON-serializable values, but that restriction has been lifted.


Caching TaskResults and "Functional" Pipelines
++++++++++++++++++++++++++++++++++++++++++++++

The first time that Yenta runs, it will execute every task and, assuming task execution succeeds, serialize the
results to the directory indicated by :data:`~yenta.config.settings.YENTA_STORE_PATH`. If you run the pipeline
a second time, the graphical output will show a yellow bar next to the task names, indicating that the previous
results of the run have been reused. This is a key feature of Yenta.

In a loose sense, every Yenta task can be thought of as the reducer from Redux: the job of a task is to take the state,
some parameters, and produce the next state. If the input to the task is identical to the input it received the last
time it was run, then the task `should` produce the same output, and Yenta assumes that it does. Therefore, in such
cases Yenta will simply pass the previous results to the next stage of the pipeline without invoking the task. This
is, more or less, a flavor of referential transparency, with the caveat that "the state" of the pipeline includes
any external artifacts that are generated by the tasks but which are not themselves "stored in" the pipeline cache.

Obviously, some tasks will not fit this paradigm. One example is any task that relies on random numbers, unless
care is taken to explicitly reuse the same seed each time the task is run. Another issue where you might need to take
extra care is floating point computations, which, depending on the precise software doing the math and configuration
thereof may not be deterministically rounded the same way each time.

Named Pipelines
+++++++++++++++

As of version 0.3.0, Yenta supports the use of multiple pipelines in a single project, which can be distinguished by
their :code:`name` parameter, specified at creation, e.g. :code:`pipeline = Pipeline(*tasks, name='my_pipeline')`.
If you use a single pipeline, by default it will have the name `default`, but you can use as many pipelines as you
like and they will all operate independently of each other. Task dependency between pipelines is not currently
supported.

Command Line Usage
------------------

Yenta may also be invoked via the command line script :code:`yenta`, which takes a number of commands and options.

::

    Usage: yenta [OPTIONS] COMMAND [ARGS]...

    Options:
      --config-file PATH  The config file from which to read settings.
      --pipeline PATH     The file to which the pipeline will be cached.
      --entry-point PATH  The file containing the task definitions.
      --log-file PATH     The file to which the logs should be written.
      --help              Show this message and exit.

    Commands:
      dump-task-graph  Dump the task graph to a file; requires Matplotlib.
      list-tasks       List all available tasks.
      rm               Remove a task from the pipeline cache.
      run              Run the pipeline.
      show-config      Show the current configuration.
      task-info        Show information about a specific task.

Most of these options are self-explanatory. The most important one is the :code:`--entry-point` option, which tells
Yenta where to find your task definitions. Currently, all task definitions must reside in a single file.

.. warning::

    Removing a task from the cache only removes its results; if the task generated any artifacts, they will not
    be removed.
