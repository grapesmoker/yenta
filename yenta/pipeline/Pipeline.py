import json
import networkx as nx

from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Dict, Any

from yenta.tasks.Task import TaskDef, ParameterType, ResultSpec, ResultType
from yenta.artifacts.Artifact import Artifact
from yenta.values.Value import Value


class InvalidTaskResultError(Exception):
    pass


class InvalidParameterError(Exception):
    pass


@dataclass
class TaskResult:
    """ Holds the result of a specific task execution """
    values: Dict[str, Value] = field(default_factory=dict)
    artifacts: Dict[str, Artifact] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """ Holds the intermediate results of a step in the pipeline, where the keys of the dicts
        are the names of the tasks that have been executed and the values are TaskResults"""
    task_results: Dict[str, TaskResult] = field(default_factory=dict)
    task_inputs: Dict[str, dict] = field(default_factory=dict)

    def values(self, task_name: str, value_name: str):
        return self.task_results[task_name].values[value_name].value

    def artifacts(self, task_name: str, artifact_name: str):
        return self.task_results[task_name].values[artifact_name].value

    def from_spec(self, spec: ResultSpec):
        func = getattr(self, spec.resut_type)
        return func(spec.result_task_name, spec.result_var_name)


class Pipeline:

    def __init__(self, *tasks):

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

    @staticmethod
    def _wrap_task_output(raw_output, task_name):

        if isinstance(raw_output, dict):
            output: TaskResult = TaskResult(**raw_output)
        elif isinstance(raw_output, TaskResult):
            output = raw_output
        else:
            raise InvalidTaskResultError(f'Task {task_name} returned invalid result of type {type(raw_output)}, '
                                          f'expected either a dict or a TaskResult')

        return output

    def build_args_dict(self, task, args: PipelineResult):

        task_def: TaskDef = task.task_def
        args_dict = {}

        for spec in task_def.param_specs:
            if spec.param_type == ParameterType.PIPELINE_RESULTS:
                args_dict[spec.param_name] = args
            elif spec.param_type == ParameterType.EXPLICIT:
                args_dict[spec.param_name] = args.from_spec(spec.result_spec)

        return args_dict

    def invoke_task(self, task, **kwargs):

        output = task(**kwargs)
        return self._wrap_task_output(output, task.task_def.name)

    def cache_pipeline_result(self, cache_file: Path, result: PipelineResult):

        with open(cache_file, 'w') as f:
            json.dump(asdict(result), f, indent=4)

    def run_pipeline(self):

        result = PipelineResult()

        for node in self.execution_order:
            task = self.task_graph.nodes[node]['task']
            args = PipelineResult()
            for dependency in (task.task_def.depends_on or []):
                args.task_results[dependency] = result.task_results[dependency]

            # output = self._wrap_task_output(task(previous_results=args), node)
            args_dict = self.build_args_dict(task, args)
            output = self.invoke_task(task, **args_dict)

            result.task_results[task.task_def.name] = output
            result.task_inputs[task.task_def.name] = args_dict

        self.cache_pipeline_result(Path('./pipeline.json').resolve(), result)
        return result
