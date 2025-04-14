import asyncio
from dataclasses import dataclass
from temporalio.client import Client
from temporalio.worker import Worker
from .workflow import WorkflowConcurrencyTracker

from temporalio.testing import WorkflowEnvironment

class Settings:
    TEMPORALIO_ADDRESS: str
    TEMPORAL_TLS: bool
    
settings = Settings()
settings.TEMPORALIO_ADDRESS = "localhost:7233"
settings.TEMPORAL_TLS = False

async def main():
    # Create a Temporal worker
    print(settings.TEMPORALIO_ADDRESS)
    print(settings.TEMPORAL_TLS)
    
    # Using an activity container allows us to
    # include shared resources across activies
    # activities = ActivityContainer(engine)
    
    async with await WorkflowEnvironment.start_local() as env:
        local_client = env.client
        
        async with Worker(
            client=local_client,
            task_queue="alpine-task-queue",
            workflows=[WorkflowConcurrencyTracker],
        ):
            print("Worker started. Waiting for workflows...")
            # start a workflow
            
            # Start the workflow
            handle = await local_client.start_workflow(
                WorkflowConcurrencyTracker.run,
                task_queue="alpine-task-queue",
                id='tracker-workflow'
            )
            
            
            for i in range(10):
                # Register a job
                await handle.signal(WorkflowConcurrencyTracker.register_job, f"job-{i}")
                print(f"Registered job-{i}")
                
                # Wait for the workflow to complete
                # result = await handle.result()
                await asyncio.sleep(0.1)
                # Check the job count
                job_count = await handle.query(WorkflowConcurrencyTracker.job_count)
                print(f"Job count: {job_count}")
                
                # Complete a job
                await handle.signal(WorkflowConcurrencyTracker.complete_job, f"job-{i}")
                print(f"Completed job-{i}")
            
            # Wait for the workflow to complete
            result = await handle.result()
         

if __name__ == "__main__":
    asyncio.run(main())