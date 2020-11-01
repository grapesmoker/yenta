#!/usr/bin/env python3
"""Console script for yenta."""
import sys
import click
import configparser
import importlib.util
import more_itertools
import shutil
import os

from networkx.drawing.nx_pydot import to_pydot
from colorama import init, Fore, Style
from pathlib import Path
from yenta.config import settings
from yenta.config import logging
from yenta.pipeline.Pipeline import Pipeline, TaskStatus

import logging

logger = logging.getLogger(__name__)


def load_tasks(entry_file):
    spec = importlib.util.spec_from_file_location('main', entry_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    tasks = [func for _, func in module.__dict__.items()
             if callable(func) and hasattr(func, '_yenta_task')]

    return tasks


@click.group()
@click.option('--config-file', default=settings.YENTA_CONFIG_FILE, type=Path,
              help='The config file from which to read settings.')
@click.option('--pipeline-store', type=Path, help='The directory to which the pipeline will be cached.')
@click.option('--entry-point', type=Path, help='The file containing the task definitions.')
@click.option('--log-file', type=Path, help='The file to which the logs should be written.')
def yenta(config_file, pipeline_store, entry_point, log_file):

    init()

    # append the local path we're running from so that we can allow
    # the project to import normally when running via CLI
    sys.path.append(os.getcwd())

    cf = configparser.ConfigParser()
    cf.read(config_file or settings.YENTA_CONFIG_FILE)
    if 'yenta' not in cf:
        cf['yenta'] = {}

    settings.YENTA_ENTRY_POINT = entry_point or \
                                 cf['yenta'].get('entry_point', None) or \
                                 settings.YENTA_ENTRY_POINT

    pipeline_file = cf['yenta'].get('pipeline_store', None)
    pipeline_path = Path(pipeline_file).resolve() if pipeline_file else None
    settings.YENTA_STORE_PATH = pipeline_store or \
                                pipeline_path or \
                                settings.YENTA_STORE_PATH
    conf_log_file = cf['yenta'].get('log_file', None)
    conf_log_path = Path(conf_log_file).resolve() if log_file else None
    settings.YENTA_LOG_FILE = log_file or \
                              conf_log_path or \
                              settings.YENTA_LOG_FILE


@yenta.command(help='List all available tasks.')
@click.option('--pipeline-name', default='default', help='The name of the pipeline to display.')
def list_tasks(pipeline_name='default'):

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline_data = Pipeline.load_pipeline(settings.YENTA_STORE_PATH / pipeline_name)

    print(Fore.WHITE + Style.BRIGHT + 'The following tasks are available:')
    for task in tasks:
        task_name = task.task_def.name
        task_result = pipeline_data.task_results.get(task_name, None)
        marker = ' '
        if task_result and task_result.status == TaskStatus.SUCCESS:
            marker = Fore.GREEN + u'\u2714' + Fore.WHITE
        elif task_result and task_result.status == TaskStatus.FAILURE:
            marker = Fore.RED + u'\u2718' + Fore.WHITE

        print(Fore.WHITE + Style.BRIGHT + f'[{marker}] {task_name}')


@yenta.command(help='Show the current configuration.')
def show_config():

    print(Fore.WHITE + Style.BRIGHT + 'Yenta is using the following configuration:')
    print('The entrypoint for Yenta is ' + Fore.GREEN + str(settings.YENTA_ENTRY_POINT) + Fore.WHITE)
    print('Pipelines will be cached in ' + Fore.GREEN + str(settings.YENTA_STORE_PATH) + Fore.WHITE)
    if settings.YENTA_LOG_FILE:
        print('Log output will be written to ' + Fore.GREEN + str(settings.YENTA_LOG_FILE) + Fore.WHITE)
    else:
        print('No log output configured')


@yenta.command(help='Show information about a specific task.')
@click.argument('task-name')
@click.option('--pipeline-name', default='default', help='The name of the pipeline to display.')
def task_info(task_name, pipeline_name='default'):

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline_data = Pipeline.load_pipeline(settings.YENTA_STORE_PATH / pipeline_name)
    try:
        task = more_itertools.one(filter(lambda t: t.task_def.name == task_name, tasks))
        print(Fore.WHITE + Style.BRIGHT + 'Information for task ' + Fore.GREEN + task_name + Fore.WHITE + ':')
        deps = ', '.join(task.task_def.depends_on) if task.task_def.depends_on else 'None'
        print('Dependencies:', Fore.GREEN + deps + Fore.WHITE)
        task_result = pipeline_data.task_results.get(task_name, None)
        marker = 'Did not run'
        if task_result and task_result.status == TaskStatus.SUCCESS:
            marker = Fore.GREEN + u'\u2714' + Fore.WHITE
        elif task_result and task_result.status == TaskStatus.FAILURE:
            marker = Fore.RED + u'\u2718' + ' ' + task_result.error + Fore.WHITE
        print(f'Previous status: {marker}')
        if task_result and task_result.status == TaskStatus.SUCCESS:
            print('Previous result: ', task_result)
        else:
            print('Previous result: ' + Fore.GREEN + 'None' + Fore.WHITE)

    except ValueError:
        print(Fore.WHITE + Style.BRIGHT + 'Unknown task ' + Fore.RED + task_name + Fore.WHITE + ' specified.')


@yenta.command(help='Remove a task from the pipeline cache.')
@click.argument('task-name')
@click.option('--pipeline-name', default='default', help='The name of the pipeline to display.')
def rm(task_name, pipeline_name='default'):

    task_path = settings.YENTA_STORE_PATH / pipeline_name / task_name

    if task_path.exists():
        shutil.rmtree(task_path)
    else:
        print(Fore.WHITE + Style.BRIGHT + 'Unknown task ' + Fore.RED + task_name + Fore.WHITE + ' specified.')


@yenta.command(help='Dump the task graph to a DOT file.')
@click.argument('filename', type=click.Path())
def dump_task_graph(filename: Path):

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline = Pipeline(*tasks)
    pydot_graph = to_pydot(pipeline.task_graph)
    pydot_graph.write(filename)


@yenta.command(help='Run the pipeline.')
@click.option('--up-to', help='Optionally run the pipeline up to and including a given task.')
@click.option('--force-rerun', '-f', multiple=True, default=[], help='Force specified tasks to rerun.')
@click.option('--pipeline-name', default='default', help='The name of the pipeline to run.')
def run(up_to=None, force_rerun=None, pipeline_name='default'):

    logger.info('Running the pipeline')
    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline = Pipeline(*tasks, name=pipeline_name)
    result = pipeline.run_pipeline(up_to, force_rerun)


if __name__ == "__main__":
    sys.exit(yenta())  # pragma: no cover
