import asyncio
import json
from temporalio import activity
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from .external_db import SlurmBatchJobResult
from .models import CMGDJobInput, CommandResult


# The ActivityContainer class is a container for all the activities
# Such a class is the recommended way to inject dependencies into activities
class ActivityContainer:
    def __init__(self, db_engine: sa.engine.Engine):
        self.engine = db_engine
    
    async def run_cmd(self, cmd: str) -> CommandResult:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        returncode = proc.returncode
        
        return CommandResult(returncode, stdout, stderr)
    
    @activity.defn
    async def sbatch_submit(self, cmd: str) -> int:
        result = await self.run_cmd(cmd)
        
        if result.returncode != 0:
            raise Exception(f"Error submitting job: {result.stderr}")
        
        try: 
            job_id = int(result.stdout.decode('utf-8').strip().split()[-1])
            return job_id
        except ValueError:
            raise Exception(f"Error parsing job id: {result.stdout}")
        
    async def get_sacct_json_by_job_id(self, job_id: int) -> dict:
        cmd = f"sacct -j {job_id} --json"
        result = await self.run_cmd(cmd)
        json_result = result.stdout.decode('utf-8').strip()
        result_dict = json.loads(json_result)
        return result_dict
    
    
    
    
    @activity.defn
    async def get_and_store_final_sacct_details(self, job_id: int) -> dict:
        result_dict = await self.get_sacct_json_by_job_id(job_id)
        async_session = sa.orm.sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                sacct_result = SlurmBatchJobResult(
                    job_id=job_id,
                    job_metadata=result_dict['jobs'][0],
                    sacct_result=result_dict
                )
                session.add(sacct_result)
        return result_dict

    async def get_sacct_job_status(self, job_id: int) -> str:
        result = await self.get_sacct_json_by_job_id(job_id)
        try:
            state = result['jobs'][0]['state']['current']
            return state
        except (IndexError, KeyError):
            return "PENDING"

    @activity.defn
    async def get_job_status_activity(self, job_id: int) -> str:
        current_state = await self.get_sacct_job_status(job_id)

        async def heartbeat():
            while True:
                await asyncio.sleep(10)
                activity.heartbeat({'state': current_state})
            
        heartbeat_task = asyncio.create_task(heartbeat())
        
        while True:
            await asyncio.sleep(60)
            if current_state in ["COMPLETED", "FAILED"]:
                heartbeat_task.cancel()
                return current_state
            current_state = await self.get_sacct_job_status(job_id)

if __name__ == "__main__":
    activities = ActivityContainer(None)
    res = asyncio.run(activities.get_sacct_json_by_job_id(12448090))
    print(res)