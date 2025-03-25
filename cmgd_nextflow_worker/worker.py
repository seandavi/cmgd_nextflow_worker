import asyncio
import temporalio.client
from temporalio.worker import Worker
from cmgd_nextflow_worker.workflow import SlurmBatchJobWorkflow
from cmgd_nextflow_worker.activities import ActivityContainer
from cmgd_nextflow_worker.config import settings
from .external_db import engine

async def main():
    # Create a Temporal worker
    print(settings.TEMPORALIO_ADDRESS)
    print(settings.TEMPORAL_TLS)
    
    # Using an activity container allows us to
    # include shared resources across activies

    activities = ActivityContainer(engine)
    
    async with Worker(
        client=await temporalio.client.Client.connect(settings.TEMPORALIO_ADDRESS, tls=settings.TEMPORAL_TLS),
        task_queue="alpine-task-queue",
        workflows=[SlurmBatchJobWorkflow],
        activities=[
            activities.sbatch_submit, 
            activities.get_job_status_activity, 
            activities.get_and_store_final_sacct_details
        ],
    ):
        print("Worker started. Waiting for workflows...")
        await asyncio.Event().wait()  # Keep the worker running

if __name__ == "__main__":
    asyncio.run(main())