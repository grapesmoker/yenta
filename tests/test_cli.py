#!/usr/bin/env python

"""Tests for `yenta` package."""

import pytest
import json

from pathlib import Path
from click.testing import CliRunner

from yenta import cli
from yenta.pipeline import PipelineResult


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


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.yenta)
    assert result.exit_code == 0
    for cmd in ['dump-task-graph', 'list-tasks', 'rm', 'run', 'show-config', 'task-info']:
        assert cmd in result.output
#
#     # Path('pipeline.json').unlink()
#     # help_result = runner.invoke(cli.yenta, ['--help'])
#     # assert help_result.exit_code == 0
#     # assert '--help  Show this message and exit.' in help_result.output


def test_list_tasks():

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'
    pipeline_store = 'tests/sample_pipelines/sample_pipeline_1.json'
    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', pipeline_store, 'list-tasks'])

    output_lines = result.output.split('\n')
    assert result.exit_code == 0
    assert output_lines[0] == 'The following tasks are available:'
    assert output_lines[1] == '[ ] foo'
    assert output_lines[2] == '[ ] bar'
    assert output_lines[3] == '[ ] baz'

    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()


def test_run_tasks():

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'
    pipeline_store = 'tests/sample_pipelines/sample_pipeline_1.json'
    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', pipeline_store, 'run'])

    assert result.exit_code == 0

    # bar errors, foo runs, baz never gets called
    output_lines = result.output.split('\n')
    assert output_lines[0] == '[\u2718] bar'
    assert output_lines[1] == 'hello from foo task'
    assert output_lines[2] == '[\u2714] foo'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', pipeline_store, 'run'])

    # bar still errors, foo recycles the old value, baz still not called
    output_lines = result.output.split('\n')
    assert output_lines[0] == '[\u2718] bar'
    assert output_lines[1] == '[\u2014] foo'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', pipeline_store, 'list-tasks'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'The following tasks are available:'
    assert output_lines[1] == '[\u2714] foo'
    assert output_lines[2] == '[\u2718] bar'
    assert output_lines[3] == '[ ] baz'

    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()


def test_show_config():

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'
    pipeline_store = 'tests/sample_pipelines/sample_pipeline_1.json'
    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point, '--pipeline', pipeline_store, 'show-config'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Yenta is using the following configuration:'
    assert output_lines[1] == f'The entrypoint for Yenta is {entry_point}'
    assert output_lines[2] == f'Pipeline will be cached in {pipeline_store}'
    assert output_lines[3] == 'No log output configured'
    assert output_lines[4] == 'Tasks will be executed in the following order: bar, foo, baz'
    print(result.output)


def test_task_info():

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'
    pipeline_store = 'tests/sample_pipelines/sample_pipeline_1.json'
    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'task-info', 'nonexistent-task'])

    assert result.exit_code == 0
    assert result.output == 'Unknown task nonexistent-task specified.\n'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'run'])

    assert result.exit_code == 0
    assert Path(pipeline_store).exists()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'task-info', 'foo'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Information for task foo:'
    assert output_lines[1] == 'Dependencies: None'
    assert output_lines[2] == 'Previous status: \u2714'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'task-info', 'bar'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Information for task bar:'
    assert output_lines[1] == 'Dependencies: None'
    assert output_lines[2] == 'Previous status: \u2718 oh noes'

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'task-info', 'baz'])

    output_lines = result.output.split('\n')
    assert output_lines[0] == 'Information for task baz:'
    assert output_lines[1] == 'Dependencies: foo, bar'
    assert output_lines[2] == 'Previous status: Did not run'

    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()


def test_rm_task():

    runner = CliRunner()
    entry_point = 'tests/sample_pipelines/sample_pipeline_1.py'
    pipeline_store = 'tests/sample_pipelines/sample_pipeline_1.json'
    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'run'])

    assert result.exit_code == 0
    assert Path(pipeline_store).exists()

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'rm', 'foo'])

    assert result.exit_code == 0
    assert Path(pipeline_store).exists()

    with open(pipeline_store, 'r') as f:
        pipeline = PipelineResult(**json.load(f))

    with pytest.raises(KeyError) as ex:
        _ = pipeline.values('foo', 'whatever')

    assert 'foo' in str(ex.value)

    result = runner.invoke(cli.yenta, ['--entry-point', entry_point,
                                       '--pipeline', pipeline_store,
                                       'rm', 'nonexistent-task'])

    assert result.exit_code == 0
    assert result.output == 'Unknown task nonexistent-task specified.\n'

    if Path(pipeline_store).exists():
        Path(pipeline_store).unlink()


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
