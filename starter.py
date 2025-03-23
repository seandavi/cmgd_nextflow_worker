import asyncio
from temporalio.client import Client
from workflow import HelloWorldWorkflow
from models import HelloWorldInput

from config import settings

async def main():
    # Connect to the Temporal server
    client = await Client.connect(settings.TEMPORALIO_ADDRESS, tls = settings.TEMPORAL_TLS)

    # Start the workflow
    handle = await client.start_workflow(
        HelloWorldWorkflow.run,
        HelloWorldInput(sample_id="sample-id", run_ids=["SRR000237"]),
        task_queue="hello-world-task-queue",
        id="hello-world-workflow-id",
    )
    print(f"Workflow started with ID: {handle.id}")

    # Wait for a few seconds and send the signal
    await asyncio.sleep(5)
    await handle.signal("continue_signal")
    print("Signal sent to workflow.")

    # Wait for the workflow to complete
    result = await handle.result()
    print(f"Workflow result: {result}")

if __name__ == "__main__":
    asyncio.run(main())