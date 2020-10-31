#!/usr/bin/env python

"""Tests for `yenta` package."""

import pytest
import json
import shutil

from pathlib import Path
from click.testing import CliRunner

from yenta import cli
from yenta.tasks import task
from yenta.pipeline import Pipeline, PipelineResult
from yenta.config import settings


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


@pytest.fixture
def store_path(monkeypatch):

    monkeypatch.setattr(settings, 'YENTA_STORE_PATH', Path('tests/tmp/pipeline'))
    yield settings.YENTA_STORE_PATH
    for path in settings.YENTA_STORE_PATH.iterdir():
        shutil.rmtree(path)


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.yenta)
    assert result.exit_code == 0
    for cmd in ['dump-task-graph', 'list-tasks', 'rm', 'run', 'show-config', 'task-info']:
        assert cmd in result.output


def test_list_tasks(store_path):

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', store_path, 'list-tasks'])

    output_lines = result.output.split('\n')
    assert result.exit_code == 0
    assert output_lines[0] == 'The following tasks are available:'
    assert output_lines[1] == '[ ] foo'
    assert output_lines[2] == '[ ] bar'
    assert output_lines[3] == '[ ] baz'


def test_run_tasks(store_path):

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', store_path, 'run'])

    assert result.exit_code == 0

    # bar errors, foo runs, baz never gets called
    output_lines = result.output.split('\n')
    ind = output_lines.index('[\u2718] bar')
    assert ind >= 0
    assert output_lines[ind + 1] == 'hello from foo task'
    assert output_lines[ind + 2] == '[\u2714] foo'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', store_path, 'run'])

    # bar still errors, foo recycles the old value, baz still not called
    output_lines = result.output.split('\n')
    ind = output_lines.index('[\u2718] bar')
    assert ind >= 0
    assert output_lines[ind + 1] == '[\u2014] foo'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', store_path, 'list-tasks'])

    output_lines = result.output.split('\n')
    ind = output_lines.index('The following tasks are available:')
    assert output_lines[ind + 1] == '[\u2714] foo'
    assert output_lines[ind + 2] == '[\u2718] bar'
    assert output_lines[ind + 3] == '[ ] baz'


def test_show_config(store_path):

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', store_path, 'show-config'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Yenta is using the following configuration:'
    assert output_lines[1] == f'The entrypoint for Yenta is {entry_point}'
    assert output_lines[2] == f'Pipelines will be cached in {store_path}'
    assert output_lines[3] == 'No log output configured'
    print(result.output)


def test_task_info(store_path):

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'task-info', 'nonexistent-task'])

    assert result.exit_code == 0
    assert result.output == 'Unknown task nonexistent-task specified.\n'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'run'])

    assert result.exit_code == 0
    assert Path(store_path / 'default').exists()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'task-info', 'foo'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Information for task foo:'
    assert output_lines[1] == 'Dependencies: None'
    assert output_lines[2] == 'Previous status: \u2714'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'task-info', 'bar'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Information for task bar:'
    assert output_lines[1] == 'Dependencies: None'
    assert output_lines[2] == 'Previous status: \u2718 oh noes'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'task-info', 'baz'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Information for task baz:'
    assert output_lines[1] == 'Dependencies: foo, bar'
    assert output_lines[2] == 'Previous status: Did not run'


def test_rm_task(store_path):

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'run'])

    assert result.exit_code == 0
    assert Path(store_path / 'default').exists()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'rm', 'foo'])

    assert result.exit_code == 0
    assert Path(store_path / 'default').exists()

    pipeline = Pipeline.load_pipeline(store_path / 'default')

    with pytest.raises(KeyError) as ex:
        _ = pipeline.values('foo', 'whatever')

    assert 'foo' in str(ex.value)

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', store_path,
                                       'rm', 'nonexistent-task'])

    assert result.exit_code == 0
    assert result.output == 'Unknown task nonexistent-task specified.\n'


def test_dump_task_graph():

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'
    pipeline_store = 'tests/sample_pipelines/sample_pipeline_1.json'
    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()

    task_graph = 'tests/sample_pipelines/sample_task_graph.png'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'dump-task-graph', task_graph])

    assert result.exit_code == 0
    assert Path(task_graph).exists()

    Path(task_graph).unlink()
