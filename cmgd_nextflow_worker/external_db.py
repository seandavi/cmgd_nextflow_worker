from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import argparse
import asyncio
from dataclasses import dataclass

from .config import settings

QUEUED="QUEUED"
PROCESSING="PROCESSING"
COMPLETED="COMPLETED"
FAILED="FAILED"

print(settings.POSTGRES_DSN)

# Create async engine
engine = create_async_engine(
    settings.POSTGRES_DSN,
    echo=True,  # SQL logging
    pool_size=10,  # Number of connections
    max_overflow=2,  # Max connections
    pool_pre_ping=True,  # Check connection validity
)

# Create async session factory
async_session = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession
)

Base = declarative_base()

# Define the models
class SlurmBatchJobResult(Base):
    __tablename__ = "slurm_batch_job_results"

    id = sa.Column(sa.Integer, primary_key=True)
    job_id = sa.Column(sa.Integer)
    job_metadata = sa.Column(sapg.JSONB)
    sacct_result = sa.Column(sapg.JSONB)
    _inserted_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    _updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now(), server_default=sa.func.now())


class JobQueueRecord(Base):
    __tablename__ = "job_queue_records"

    id = sa.Column(sa.Integer, primary_key=True)
    job_name = sa.Column(sa.String)
    job_metadata = sa.Column(sapg.JSONB)
    job_priority = sa.Column(sa.Integer)
    job_status = sa.Column(sa.String)
    _inserted_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    _updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now(), server_default=sa.func.now())
    

@dataclass
class JobQueueRecordData:
    """Dataclass for job queue record data
    
    Used to supply the job data to insert into the job queue
    """
    job_name: str
    job_metadata: dict
    job_priority: int
    job_status: str
    
@dataclass
class JobRecord(JobQueueRecordData):
    """Dataclass for job record data
    
    Used to pass the job data back from the database deque
    """
    id: int
    _inserted_at: str
    _updated_at: str
    
async def enqueue_job(record: JobQueueRecordData) -> None:
    """Enqueue a job into the job queue"""
    async with async_session() as session:
        new_job = JobQueueRecord(
            job_name=record.job_name,
            job_metadata=record.job_metadata,
            job_priority=record.job_priority,
            job_status=QUEUED,
        )
        session.add(new_job)
        await session.commit()

async def dequeue_job() -> JobRecord | None:
    """Dequeue a job from the job queue"""
    async with async_session() as session:
        result = await session.execute(
            sa.select(JobQueueRecord)
            .where(JobQueueRecord.job_status == QUEUED)
            .order_by(JobQueueRecord.job_priority)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        job = result.scalars().first()
        if job:
            job.job_status = PROCESSING
            return_job = JobRecord(
                id=job.id,
                job_name=job.job_name,
                job_metadata=job.job_metadata,
                job_priority=job.job_priority,
                job_status=job.job_status,
                _inserted_at=job._inserted_at,
                _updated_at=job._updated_at
            )
            session.commit()
            return return_job
        return None
    
async def update_job_status(job_id: int, status: str) -> None:
    """Update the status of a job"""
    async with AsyncSession(engine) as session:
        result = await session.execute(
            sa.select(JobQueueRecord).where(JobQueueRecord.id == job_id)
        )
        job = result.scalars().first()
        if job:
            job.job_status = status
            await session.commit()
            return job
        else:
            print(f"Job with ID {job_id} not found")
            return None
    
async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def drop_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        
def get_db_info(dsn: str) -> tuple[str, str]:
    """Extract database name and admin DSN from connection string"""
    parts = dsn.split("/")
    db_name = parts[-1]
    admin_dsn = "/".join(parts[:-1]+["postgres"])
    return db_name, admin_dsn

# Using the postgres dsn,
# create the database if it does not exist
async def create_database() -> None:
    dsn = settings.POSTGRES_DSN
    db_name, admin_dsn = get_db_info(dsn)
    
    engine = create_async_engine(admin_dsn, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            # Check if database exists first
            result = await conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :database"),
                {"database": db_name}
            )
            exists = result.scalar()
            
            if not exists:
                # Using execute to run the text SQL statement
                await conn.execute(sa.text(f"CREATE DATABASE {db_name}"))
                print(f"Database {db_name} created successfully")
            else:
                print(f"Database {db_name} already exists")
    except sa.exc.OperationalError as e:
        print(f"Database connection error: {e}")
    except sa.exc.ProgrammingError as e:
        print(f"SQL syntax error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        await engine.dispose()

async def drop_database() -> None:
    dsn = settings.POSTGRES_DSN
    db_name, admin_dsn = get_db_info(dsn)
    
    engine = create_async_engine(admin_dsn, isolation_level="AUTOCOMMIT")
    try:
        # Using "connect" instead of "begin" to avoid transaction
        async with engine.connect() as conn:
            # Terminate any existing connections to the database
            await conn.execute(
                sa.text(
                    f"SELECT pg_terminate_backend(pg_stat_activity.pid) "
                    f"FROM pg_stat_activity "
                    f"WHERE pg_stat_activity.datname = '{db_name}' "
                    f"AND pid <> pg_backend_pid()"
                )
            )
            
            # Drop the database (must be outside transaction)
            await conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))
            print(f"Database {db_name} dropped successfully")
    except sa.exc.OperationalError as e:
        print(f"Database connection error: {e}")
    except sa.exc.ProgrammingError as e:
        print(f"SQL syntax error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the database")
    parser.add_argument("action", choices=["create_db", "drop_db", "create_tables", "drop_tables"])
    args = parser.parse_args()
    
    if args.action == "create_db":
        asyncio.run(create_database())
    elif args.action == "drop_db":
        asyncio.run(drop_database())
    elif args.action == "create_tables":
        asyncio.run(create_tables())
    elif args.action == "drop_tables":
        asyncio.run(drop_tables())
    else:
        print("Invalid action")

async def test_queue():
    """Test the job queue"""
    job = JobQueueRecordData(
        job_name="test_job",
        job_metadata={"key": "value"},
        job_priority=1,
        job_status=QUEUED,
    )
    
    jobin = await enqueue_job(job)
    dequeued_job = await dequeue_job()
    print(dequeued_job)
    if dequeued_job:
        print(f"Dequeued job: {dequeued_job.job_name}")
    else:
        print("No job found in the queue")
    await update_job_status(dequeued_job.id, COMPLETED)
    print(f"Updated job {dequeued_job.id} to COMPLETED")

if __name__ == "__main__":
    asyncio.run(test_queue())