import asyncio
from temporalio.client import Client
from cmgd_nextflow_worker.workflow import SlurmBatchJobWorkflow
from cmgd_nextflow_worker.models import CMGDJobInput

from cmgd_nextflow_worker.config import settings

async def main(input: CMGDJobInput):
    # Connect to the Temporal server
    client = await Client.connect(settings.TEMPORALIO_ADDRESS, tls = settings.TEMPORAL_TLS)

    # Start the workflow
    handle = await client.start_workflow(
        SlurmBatchJobWorkflow.run,
        CMGDJobInput(sample_id=input.sample_id, run_ids=input.run_ids),
        task_queue="alpine-task-queue",
        id=f'{input.sample_id}-cmgd-workflow'
    )
    print(f"Workflow started with ID: {handle.id}")

    # Wait for a few seconds and send the signal
    # await asyncio.sleep(5)
    # await handle.signal("continue_signal")
    # print("Signal sent to workflow.")

    # # Wait for the workflow to complete
    # result = await handle.result()
    # print(f"Workflow result: {result}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start a Temporal workflow")
    parser.add_argument("--sample-id", type=str, help="The sample ID", required=True)
    parser.add_argument("--run-ids", type=str, nargs="+", help="The run IDs", required=True)
    args = parser.parse_args()
    asyncio.run(main(CMGDJobInput(sample_id=args.sample_id, run_ids=args.run_ids)))