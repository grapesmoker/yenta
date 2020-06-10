import json
import networkx as nx

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Dict, Any

from yenta.config.settings import YENTA_JSON_STORE_PATH
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

    def __post_init__(self):

        values = {k: Value(**v) for k, v in self.values.items() if not isinstance(v, Value)}
        artifacts = {k: Artifact(**v) for k, v in self.artifacts.items() if not isinstance(v, Artifact)}

        self.values.update(values)
        self.artifacts.update(artifacts)


@dataclass
class PipelineResult:
    """ Holds the intermediate results of a step in the pipeline, where the keys of the dicts
        are the names of the tasks that have been executed and the values are TaskResults"""
    task_results: Dict[str, TaskResult] = field(default_factory=dict)
    task_inputs: Dict[str, 'PipelineResult'] = field(default_factory=dict)

    def __post_init__(self):

        task_results = {k: TaskResult(**v) for k, v in self.task_results.items() if not isinstance(v, TaskResult)}
        task_inputs = {k: PipelineResult(**v) for k, v in self.task_inputs.items() if not isinstance(v, PipelineResult)}

        self.task_results.update(task_results)
        self.task_inputs.update(task_inputs)

    def values(self, task_name: str, value_name: str):
        return self.task_results[task_name].values[value_name].value

    def artifacts(self, task_name: str, artifact_name: str):
        return self.task_results[task_name].artifacts[artifact_name]

    def from_spec(self, spec: ResultSpec):
        func = getattr(self, spec.resut_type)
        return func(spec.result_task_name, spec.result_var_name)


class Pipeline:

    def __init__(self, *tasks):

        self._tasks = tasks
        self.task_graph = nx.DiGraph()
        self.execution_order = []

        self.build_task_graph()

        self._tasks_executed = set()
        self._tasks_reused = set()

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

    @staticmethod
    def build_args_dict(task, args: PipelineResult):

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

    @staticmethod
    def cache_pipeline_result(cache_file: Path, result: PipelineResult):

        with open(cache_file, 'w') as f:
            json.dump(asdict(result), f, indent=4)

    def load_pipeline(self):

        if YENTA_JSON_STORE_PATH.exists():
            with open(YENTA_JSON_STORE_PATH, 'r') as f:
                pipeline = PipelineResult(**json.load(f))
        else:
            pipeline = PipelineResult()
        return pipeline

    @staticmethod
    def reuse_inputs(task_name, previous_result: PipelineResult, args: PipelineResult):

        previous_inputs = previous_result.task_inputs.get(task_name, None)
        if previous_inputs:
            return previous_inputs == args

        return False

    def run_pipeline(self):

        previous_result: PipelineResult = self.load_pipeline()
        result = PipelineResult()

        for task_name in self.execution_order:
            task = self.task_graph.nodes[task_name]['task']
            args = PipelineResult()
            for dependency in (task.task_def.depends_on or []):
                args.task_results[dependency] = result.task_results[dependency]

            if self.reuse_inputs(task_name, previous_result, args):
                self._tasks_reused.add(task_name)
                output = previous_result.task_results[task_name]
            else:
                self._tasks_executed.add(task_name)
                args_dict = self.build_args_dict(task, args)
                output = self.invoke_task(task, **args_dict)

            result.task_results[task_name] = output
            result.task_inputs[task_name] = args

            self.cache_pipeline_result(YENTA_JSON_STORE_PATH, result)

        return result
