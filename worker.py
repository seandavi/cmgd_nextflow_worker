import asyncio
import temporalio.client
from temporalio.worker import Worker
from workflow import HelloWorldWorkflow
from activities import say_hello, run1
from config import settings

async def main():
    # Create a Temporal worker
    async with Worker(
        client=await temporalio.client.Client.connect(settings.TEMPORALIO_ADDRESS, tls=settings.TEMPORAL_TLS),
        task_queue="hello-world-task-queue",
        workflows=[HelloWorldWorkflow],
        activities=[say_hello, run1],
    ):
        print("Worker started. Waiting for workflows...")
        await asyncio.Event().wait()  # Keep the worker running

if __name__ == "__main__":
    asyncio.run(main())