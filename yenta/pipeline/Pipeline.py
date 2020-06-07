import networkx as nx

from collections import defaultdict
from dataclasses import dataclass, field
from pprint import pprint
from typing import List, Dict, Any

from yenta.tasks.Task import TaskDef
from yenta.artifacts.Artifact import Artifact
from yenta.values.Value import Value


class ImproperTaskResultError(Exception):
    pass


@dataclass
class TaskResult:
    """ Holds the result of a specific task execution """
    values: Dict[str, Value] = field(default_factory=dict)
    artifacts: Dict[str, Artifact] = field(default_factory=dict)


class PipelineValues:

    def __get__(self, instance, owner):

        self._task_results: Dict[str, TaskResult] = instance.task_results
        return self

    def __getitem__(self, item):
        if not isinstance(item, tuple) and len(item) != 2:
            raise TypeError(f'Attempt to access values with invalid keys: {item}, '
                            f'expected tuple (task_name, value_name)')
        return self._task_results[item[0]].values[item[1]].value


@dataclass
class PipelineResult:
    """ Holds the intermediate results of a step in the pipeline, where the keys of the dicts
        are the names of the tasks that have been executed and the values are TaskResults"""
    # values: Dict[str, TaskResult] = field(default_factory=dict)
    # artifacts: Dict[str, TaskResult] = field(default_factory=dict)
    task_results: Dict[str, TaskResult] = field(default_factory=dict)
    values = PipelineValues()


class Pipeline:

    def __init__(self, tasks):

        self._tasks = tasks
        self.task_graph = nx.DiGraph()
        self.execution_order = []

        self.build_task_graph()

    def build_task_graph(self):

        for task in self._tasks:
            self.task_graph.add_node(task.task_def.name, task=task)
            for dependency in (task.task_def.depends_on or []):
                self.task_graph.add_edge(dependency, task.task_def.name)

        self.execution_order = list(nx.algorithms.dag.lexicographical_topological_sort(self.task_graph))

    def run_pipeline(self):

        result = PipelineResult()

        for node in self.execution_order:
            task = self.task_graph.nodes[node]['task']
            args = PipelineResult()
            for dependency in (task.task_def.depends_on or []):
                args.task_results[dependency] = result.task_results[dependency]

            raw_output = task(previous_results=args)
            if isinstance(raw_output, dict):
                output: TaskResult = TaskResult(**raw_output)
            elif isinstance(raw_output, TaskResult):
                output = raw_output
            else:
                raise ImproperTaskResultError(f'Task {node} returned invalid result of type {type(raw_output)}, '
                                              f'expected either a dict or a TaskResult')
            result.task_results[task.task_def.name] = output

        return result
