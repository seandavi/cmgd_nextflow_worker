import asyncio
import temporalio.client
from temporalio.worker import Worker
from cmgd_nextflow_worker.workflow import SlurmBatchJobWorkflow
from cmgd_nextflow_worker.activities import ActivityContainer
from cmgd_nextflow_worker.config import settings
from .external_db import engine

from temporalio.client import Client
from cmgd_nextflow_worker.models import CMGDJobInput

async def main(input: CMGDJobInput):
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
        # start a workflow
        
        client = await Client.connect(settings.TEMPORALIO_ADDRESS, tls = settings.TEMPORAL_TLS)

        # Start the workflow
        handle = await client.execute_workflow(
            SlurmBatchJobWorkflow.run,
            CMGDJobInput(sample_id=input.sample_id, run_ids=input.run_ids),
            task_queue="alpine-task-queue",
            id=f'{input.sample_id}-cmgd-workflow'
        )
         

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start a Temporal workflow")
    parser.add_argument("--sample-id", type=str, help="The sample ID", required=True)
    parser.add_argument("--run-ids", type=str, nargs="+", help="The run IDs", required=True)
    args = parser.parse_args()
    asyncio.run(main(CMGDJobInput(sample_id=args.sample_id, run_ids=args.run_ids)))