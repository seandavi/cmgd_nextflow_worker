from temporalio import workflow

# Define the signal name
CONTINUE_SIGNAL = "continue_signal"
from datetime import timedelta
from models import HelloWorldInput

with workflow.unsafe.imports_passed_through():
    from activities import say_hello, run1


@workflow.defn
class HelloWorldWorkflow:
    def __init__(self):
        self.signal_received = False

    @workflow.run
    async def run(self, input: HelloWorldInput) -> str:
        self.input = input
        # Log the start of the workflow
        workflow.logger.info("Workflow started. Waiting for signal...")

        # Wait for the signal to be received
        await workflow.wait_condition(lambda: self.signal_received)

        # Proceed after receiving the signal
        workflow.logger.info("Signal received. Proceeding with workflow...")
        hello_result = await workflow.execute_activity(
            say_hello,
            self.input,
            start_to_close_timeout=timedelta(seconds=100),
        )
        
        run1_result = await workflow.execute_activity(
            run1,
            "ls -l",
            start_to_close_timeout=timedelta(seconds=100),
        )
        
        return f"{hello_result}, {run1_result}"

    @workflow.signal(name=CONTINUE_SIGNAL)
    def continue_signal(self):
        self.signal_received = True