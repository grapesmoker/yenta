#!/usr/bin/env python3
"""Console script for yenta."""
import sys
import click
import configparser
import importlib.util

from colorama import init, Fore, Back, Style
from pathlib import Path
from yenta.config import settings
from yenta.pipeline.Pipeline import Pipeline, TaskStatus


def load_tasks(entry_file):
    spec = importlib.util.spec_from_file_location('main', settings.YENTA_ENTRY_POINT)
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

    settings.YENTA_ENTRY_POINT = entry_point or \
                                 cf['yenta'].get('entry_point', None) or \
                                 settings.YENTA_ENTRY_POINT
    settings.YENTA_JSON_STORE_PATH = pipeline or \
                                     Path(cf['yenta'].get('pipeline_store', None)).resolve() or \
                                     settings.YENTA_JSON_STORE_PATH
    conf_log_file = cf['yenta'].get('log_file', None)
    conf_log_path = Path(log_file).resolve() if log_file else None
    settings.YENTA_LOG_FILE = log_file or \
                              conf_log_path or \
                              settings.YENTA_LOG_FILE
    settings.VERBOSE = verbose or bool(cf['yenta'].get('verbose', False)) or settings.VERBOSE


@yenta.command()
def list_tasks():

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline_data = Pipeline.load_pipeline()

    print(Fore.WHITE + Style.BRIGHT + 'The following tasks are available: ')
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
def run():

    tasks = load_tasks(settings.YENTA_ENTRY_POINT)
    pipeline = Pipeline(*tasks)
    result = pipeline.run_pipeline()
    print('the result was', result.values('foo', 'result'))


if __name__ == "__main__":
    sys.exit(yenta())  # pragma: no cover
