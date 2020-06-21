Questions and Answers
=====================

Why is this taskrunner different from other taskrunners?
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

I work in bioinformatics on a project that requires building out a pipeline of different operations, a quite
common task in the field. I spent quite a bit of time researching different frameworks for doing this, but none
of them quite fit my usecase or preferences. This isn't at all to say that these are bad projects, just that if
your preferences align with mine you might find Yenta useful. All that follows is just my opinion derived from
either using the framework in question or reading its documentation and is in no way intended as an authoritative
evaluation.

1. `Snakemake <https://snakemake.readthedocs.io/en/stable/>`_
-------------------------------------------------------------

Snakemake is quite popular in bioinformatics. It is essentially a rule-based execution engine for which one writes
configuration files in a language very much resembling Python. The configuration describes the tasks, their inputs
and outputs, and their dependencies on each other. It's very feature complete, so if you need to run your pipeline
in a distributed environment or require interop with CWL, you should probably use Snakemake instead of Yenta.

What I did not like about Snakemake was the necessity of learning another configuration language and the fact that
it seems to lean too much on "scripts" as the unit of execution. If you're writing a pipeline in Python with only
computationally intensive (as opposed to glue) parts outsourced to C/C++, then it is preferable in my view to stay in
Python all the time and write a complete project in it rather than having individual scripts invoked by the execution
engine. Snakemake is heavily focused on calling things from the command line, but many times what I actually want
to do is to perform some custom operation and Snakemake did not seem to make that easy.

2. `Nextflow <https://nextflow.io>`_
------------------------------------

I'll be honest: I try to avoid the JVM ecosystem when I can. The fact that it's another custom DSL for task
configuration `and` is in Java is enough to scare me away from it. Also, the Nextflow DSL example appears to require
you to write Python code inline in the configuration file, which, no thanks. I'm sure it's nice.

3. `doit <https://pydoit.org/>`_
--------------------------------

I'm currently using :code:`doit` in my project, which has some benefits in my view over the previous two frameworks.
First, doit is pure python, which means that all of the code to set up and run tasks can be unified in a single project.
Second, it has a very nice CLI which makes it possible to selectively run tasks, clear their results, display
information, and so on. The Yenta CLI is partially inspired by doit.

Nonetheless, doit has several shortcomings from my perspective. One major issue that I've had to work around is that
a :code:`task_do_thing` function in the doit framework is not actually the task itself; it's merely a setup that
returns the definition of a task. This becomes a real problem when, in order to define a task, you have to know
what output is going to be ahead of time, and has forced me into some unfortunate gymnastics to accommodate the
framework. Also, when you have a task that might generates hundreds or even thousands of files, doit really bogs
down.

4. `Prefect <https://prefect.io>`_
----------------------------------

The last option I explored was Prefect, which seems like a pretty sophisticated orchestration tool. Prefect supports
parallel execution and integrates directly with Dask, which is pretty cool. It also uses Python functions as the
basic task unit, which is a definite plus. It also appears to have great support for resuming tasks when they error out,
visualization, and a ton of other things. I have not used Prefect directly, so I don't know what its pain points are,
but overall it appears to be quite sophisticated and full-featured. Perhaps the only downside is figuring out which
of those features you actually need. The Prefect library is the open source part of a commercial orchestration product,
so obviously it's going to be very professional. In the end I went with doit because Prefect seemed to be too complex
for my specific use case.


Why should I use Yenta?
+++++++++++++++++++++++

Short answer: you probably shouldn't.

Long answer: I started working on Yenta because I was trying to come up with something relatively simple, Python-native,
and not subject to the inconveniences I experienced working with doit. I would say that Yenta might be a good fit
for any situation in which you need to sequence a bunch of tasks but are not overly concerned with parallelism (see
next question), don't need a ton of bells and whistles, and are not concerned with cloud environments. In that case,
Yenta could be a useful library, but ultimately you're taking a chance on a solo developer's personal project. If you
like living on the edge, I guess that might be a good reason too.

Does Yenta support any sort of parallelism?
+++++++++++++++++++++++++++++++++++++++++++

There's currently nothing in the framework to make it possible to run independent tasks in parallel. It is a goal of
mine to implement this sort of capability, but parallelism is notoriously tricky and I think there are other features
that I'd like to work on before I commit to fully parallelizing this project. However, since Yenta tasks are just
regular Python functions, nothing prevents you from implementing parallelism `within` the individual tasks. Async tasks
are currently not supported.

What's up with the weird annotations?
+++++++++++++++++++++++++++++++++++++

Every task in Yenta needs to know the result of the execution of any of its dependencies. There are two ways to get
that information: either pass the whole state blob corresponding to those results to the task and let the function
sort it out, or have a kind of shortcut which slices out the relevant bit of state that the downstream task cares
about. Since Python annotations can be literally any legal object, annotating a task argument according to the
specified format signals to the task definition wrapper that it should slice out that bit of state from the previous
pipeline result to feed it to the task. You can think of this annotation as a kind of selector into the pipeline
state, akin to selectors in frameworks like Redux.

What features are planned?
++++++++++++++++++++++++++

In no particular order, I'd like to at some point add the following:

* a more robust selector logic, allowing arbitrary state configurations to be passed to tasks
* more serialization options for how to store state than just as JSON blob
* support for async tasks
* better, fancier logging
* more CLI options
* better visualization of state

In the longer term I'd like to add:

* true parallelism suitable for use in distributed environments
* some sort of web interface for monitoring pipeline state
