# prompt: A function that used postgresql (ayncpg) select for update skip locked to get a value from a table, do something with it, and then commit an update if the action is successful. 

import asyncio
import asyncpg
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


@dataclass
class CommandResult:
    returncode: int | None
    stdout: bytes
    stderr: bytes
    

async def run_cmd(cmd: str) -> CommandResult:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate()
    returncode = proc.returncode
    
    return CommandResult(returncode, stdout, stderr)

async def sbatch_submit(cmd: str) -> int:
    result = await run_cmd(cmd)
    
    if result.returncode != 0:
        raise Exception(f"Error submitting job: {result.stderr}")
    
    try: 
        job_id = int(result.stdout.decode('utf-8').strip().split()[-1])
        return job_id
    except ValueError:
        raise Exception(f"Error parsing job id: {result.stdout}")
        
async def get_running_or_pending_job_count():
    """Use squeue to get the number of running or pending jobs"""
    cmd = "squeue -u $USER | wc -l"
    result = await run_cmd(cmd)
    if result.returncode != 0:
        raise Exception(f"Error getting job count: {result.stderr}")
    
    try:
        job_count = int(result.stdout.decode('utf-8').strip())-1  # Subtract 1 for the header line
        return job_count
    except ValueError:
        raise Exception(f"Error parsing job count: {result.stdout}")

async def process_queue_item(pg_dsn):
    """
    Selects a queue item for processing, performs an action, and updates the queue.
    """
    try:
        connection = await asyncpg.connect(dsn=pg_dsn)
        async with connection.transaction():
            try:
                # Select a row for update, skipping locked rows
                row = await connection.fetchrow(
                    "SELECT sample_id, run_ids, status FROM cmgd_queue WHERE status = 'QUEUED' ORDER BY _created_at LIMIT 1 FOR UPDATE SKIP LOCKED"
                )
                if not row:
                    print("No items in the queue.")
                    return
                print(f"Processing row: {row}")
                sample_id = row["sample_id"]
                run_ids = row["run_ids"]

                # Do something with the data (replace with your actual logic)
                cmd = f"sbatch --export=NONE submit_alpine.sh '{run_ids}' {sample_id}"
                job_id = await sbatch_submit(cmd)

                await connection.execute(
                    "UPDATE cmgd_queue SET status = 'PROCESSED', _updated_at=now(), batch_id = $2 WHERE sample_id = $1", sample_id, job_id
                )
                logger.info(f"Processed sample_id {sample_id} with job_id {job_id}")

            except Exception as e:
                logger.error(f"Error processing queue item: {e}")
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Error connecting to database: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await connection.close()
                    
                    
async def main(PG_DSN):
    
    while True:
        # check the number of running or pending jobs
        job_count = await get_running_or_pending_job_count()
        if job_count >= 400:
            await asyncio.sleep(60)
            continue
        for _ in range(400-job_count):
            try:
                # Replace with your actual DSN
                await process_queue_item(PG_DSN)
            except Exception as e:
                print(f"Error in main loop: {e}")
            await asyncio.sleep(5)



async def create_database_table():
    """
    Create the database table if it doesn't exist.
    """
    PG_DSN = os.getenv("POSTGRES_DSN")
    try:
        connection = await asyncpg.connect(dsn=PG_DSN)
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS cmgd_queue (
                id SERIAL PRIMARY KEY,
                sample_id TEXT,
                run_ids TEXT,
                status TEXT,
                _created_at TIMESTAMP DEFAULT NOW(),
                _updated_at TIMESTAMP DEFAULT NOW(),
                batch_id INT
            )
        """)
        logger.info("Table created successfully.")
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Error creating table: {e}")
    finally:
        await connection.close()


if __name__ == "__main__":
    # Load environment variables from .env file
    import os
    from dotenv import load_dotenv
    load_dotenv()
    PG_DSN = os.getenv("POSTGRES_DSN")  # Assuming PG_DSN is defined as before
    
    asyncio.run(main(PG_DSN))
