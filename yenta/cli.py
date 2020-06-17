#!/usr/bin/env python3
"""Console script for yenta."""
import sys
import click
import configparser
import importlib.util
import more_itertools
import json
import networkx as nx
import matplotlib.pyplot as plt

from dataclasses import asdict
from colorama import init, Fore, Back, Style
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
@click.option('--config-file', default=settings.YENTA_CONFIG_FILE, type=Path)
@click.option('--pipeline', type=Path)
@click.option('--entry-point', type=Path)
@click.option('--log-file', type=Path)
@click.option('--verbose', default=False, type=bool)
def yenta(config_file, pipeline, entry_point, log_file, verbose):

    init()

    cf = configparser.ConfigParser()
    cf.read(config_file or settings.YENTA_CONFIG_FILE)
    if 'yenta' not in cf:
        cf['yenta'] = {}

    settings.YENTA_ENTRY_POINT = entry_point or \
                                 cf['yenta'].get('entry_point', None) or \
                                 settings.YENTA_ENTRY_POINT

    pipeline_file = cf['yenta'].get('pipeline_store', None)
    pipeline_path = Path(pipeline_file).resolve() if pipeline_file else None
    settings.YENTA_JSON_STORE_PATH = pipeline or \
                                     pipeline_path or \
                                     settings.YENTA_JSON_STORE_PATH
    conf_log_file = cf['yenta'].get('log_file', None)
    conf_log_path = Path(conf_log_file).resolve() if log_file else None
    settings.YENTA_LOG_FILE = log_file or \
                              conf_log_path or \
                              settings.YENTA_LOG_FILE
    settings.VERBOSE = verbose or bool(cf['yenta'].get('verbose', False)) or settings.VERBOSE


@yenta.command()
def list_tasks():

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline_data = Pipeline.load_pipeline()

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


@yenta.command()
def show_config():

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline = Pipeline(*tasks)

    print(Fore.WHITE + Style.BRIGHT + 'Yenta is using the following configuration:')
    print('The entrypoint for Yenta is ' + Fore.GREEN + str(settings.YENTA_ENTRY_POINT) + Fore.WHITE)
    print('Pipeline will be cached in ' + Fore.GREEN + str(settings.YENTA_JSON_STORE_PATH) + Fore.WHITE)
    if settings.YENTA_LOG_FILE:
        print('Log output will be written to ' + Fore.GREEN + str(settings.YENTA_LOG_FILE) + Fore.WHITE)
    else:
        print('No log output configured')
    print('Tasks will be executed in the following order: ' + Fore.GREEN + ', '.join(pipeline.execution_order))


@yenta.command()
@click.argument('task-name')
def task_info(task_name):

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline_data = Pipeline.load_pipeline()
    try:
        task = more_itertools.one(filter(lambda t: t.task_def.name == task_name, tasks))
        print(Fore.WHITE + Style.BRIGHT + 'Information for task ' + Fore.GREEN + task_name + Fore.WHITE + ':')
        deps = ', '.join(task.task_def.depends_on) if task.task_def.depends_on else 'None'
        print('Dependencies:', Fore.GREEN, deps, Fore.WHITE)
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


@yenta.command()
@click.argument('task-name')
def rm(task_name):

    pipeline_data = Pipeline.load_pipeline()

    if task_name in pipeline_data.task_results:
        del pipeline_data.task_results[task_name]
    else:
        print(Fore.WHITE + Style.BRIGHT + 'Unknown task ' + Fore.RED + task_name + Fore.WHITE + ' specified')
    if task_name in pipeline_data.task_inputs:
        del pipeline_data.task_inputs[task_name]

    with open(settings.YENTA_JSON_STORE_PATH, 'w') as f:
        json.dump(asdict(pipeline_data), f, indent=4)


@yenta.command()
@click.argument('filename', type=click.Path())
def dump_task_graph(filename):

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline = Pipeline(*tasks)
    nx.draw_networkx(pipeline.task_graph)
    plt.savefig(filename)


@yenta.command()
@click.option('--up-to')
@click.option('--force-rerun', '-f', multiple=True, default=[])
def run(up_to=None, force_rerun=None):

    logger.info('Running the pipeline')
    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline = Pipeline(*tasks)
    result = pipeline.run_pipeline(up_to, force_rerun)


if __name__ == "__main__":
    sys.exit(yenta())  # pragma: no cover
