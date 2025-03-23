from temporalio import activity
from models import HelloWorldInput
import asyncio

@activity.defn
async def say_hello(input: HelloWorldInput) -> str:
    return f"Hello, {input.sample_id}, {input.run_ids}!"

@activity.defn
async def run1(cmd) -> str:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    print(f'[{cmd!r} exited with {proc.returncode}]')
    if stdout:
        print(f'[stdout]\n{stdout.decode()}')
    if stderr:
        print(f'[stderr]\n{stderr.decode()}')
        
    return stdout.decode()