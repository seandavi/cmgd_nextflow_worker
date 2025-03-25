import pytest
import asyncio
from cmgd_nextflow_worker.activities import ActivityContainer

from cmgd_nextflow_worker.models import CommandResult

activities = ActivityContainer(None) # type: ignore

def test_run_cmd():
    cmd = "echo 'hello world'"
    result = asyncio.run(activities.run_cmd(cmd))
    assert isinstance(result, CommandResult)
    assert result.returncode == 0
    assert result.stdout == b'hello world\n'
    assert result.stderr == b''
    