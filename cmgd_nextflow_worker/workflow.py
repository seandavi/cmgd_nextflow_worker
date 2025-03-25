from temporalio import workflow

# Define the signal name
CONTINUE_SIGNAL = "continue_signal"
from datetime import timedelta
from cmgd_nextflow_worker.models import CMGDJobInput

with workflow.unsafe.imports_passed_through():
    pass
    # from cmgd_nextflow_worker.activities import ActivityContainer


@workflow.defn
class SlurmBatchJobWorkflow:
    def __init__(self):
        self.signal_received = False

    @workflow.run
    async def run(self, input: CMGDJobInput) -> dict:
        self.input = input
        # Log the start of the workflow
        workflow.logger.info("Workflow started. Waiting for signal...")

        # Wait for the signal to be received
        # await workflow.wait_condition(lambda: self.signal_received)

        # Proceed after receiving the signal
        # workflow.logger.info("Signal received. Proceeding with workflow...")
        command = f"sbatch --export=NONE submit_alpine.sh '{';'.join(input.run_ids)}' {input.sample_id}"
        # for testing
        # command = "sbatch --export=NONE submit_test.sh"
        
        job_id = await workflow.execute_activity(
            "sbatch_submit",
            command,
            start_to_close_timeout=timedelta(seconds=10),
        )
        
        final_state = await workflow.execute_activity(
            "get_job_status_activity",
            job_id,
            start_to_close_timeout=timedelta(days = 2),
            heartbeat_timeout=timedelta(seconds=30),
        )
        
        final_sacct_details = await workflow.execute_activity(
            "get_and_store_final_sacct_details",
            job_id,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=30),
        )


        return final_sacct_details
        
        

    @workflow.signal(name=CONTINUE_SIGNAL)
    def continue_signal(self):
        self.signal_received = True