from temporalio import workflow
from temporalio.exceptions import ApplicationError
import asyncio

@workflow.defn
class WorkflowConcurrencyTracker():
    """Serves as a semaphore to limit the number of concurrent jobs
    """
    def __init__(self, max_concurrent_jobs: int=400):
        self.queue = asyncio.Queue()
        self.running_jobs = {}
        self.max_concurrent_jobs = max_concurrent_jobs
        self.job_status_changed: bool = False
        
    @workflow.signal
    async def register_job(self, job_id: str):
        """Register a job id to the queue
        """
        await self.queue.put(job_id)
        self.job_status_changed = True
        workflow.logger.info(f"Job {job_id} registered")
        return True

    @workflow.signal
    async def complete_job(self, job_id: str):
        """Unregister a job id from the queue
        """
        if job_id in self.running_jobs:
            del self.running_jobs[job_id]
            self.job_status_changed = True
            workflow.logger.info(f"Job {job_id} completed")
        #else:
        #    raise ApplicationError(f"Job {job_id} not found in running jobs")
        return True
    
    @workflow.query
    async def job_count(self):
        """Get the number of jobs in the queue
        """
        return self.queue.qsize()
    
    @workflow.run
    async def run(self):
        """Run the workflow
        """
        while True:
            self.job_status_changed = False
            
            # Wait for a job to be registered
            await workflow.wait_condition(lambda: self.job_status_changed)
            
            
            # log the number of jobs in the queue
            workflow.logger.info(f"Jobs in queue: {self.queue.qsize()}")
            # log the number of running jobs
            workflow.logger.info(f"Running jobs: {len(self.running_jobs)}") 
            
            # check if the number of running jobs is less than the max concurrent jobs
            if len(self.running_jobs) < self.max_concurrent_jobs:
                job_id = await self.queue.get()
                self.running_jobs[job_id] = True
                workflow.logger.info(f"Starting job {job_id}")
                # start the job
                # run the job here
                workflow.logger.info(f"Job {job_id} started")
            else:
                workflow.logger.info("Max concurrent jobs reached")

# worker code for testing 