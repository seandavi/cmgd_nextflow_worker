from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from .config import settings

print(settings.POSTGRES_DSN)

# Create async engine
engine = create_async_engine(
    settings.POSTGRES_DSN,
    echo=True,  # SQL logging
    pool_size=10,  # Number of connections
    max_overflow=2,  # Max connections
)

# Create async session factory

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
    
async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def drop_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        
# Using the postgres dsn,
# create the database if it does not exist
async def create_database():
    dsn = settings.POSTGRES_DSN
    parts = dsn.split("/")
    db_name = parts[-1]
    temp_dsn = "/".join(parts[:-1]+["postgres"])
    engine = sa.create_engine(temp_dsn)
    conn = engine.connect()
    try:
        conn.execute(sa.text(f"create database {db_name}"))
    except Exception as e:
        print(e)
    conn.close()

async def drop_database(): 
    dsn = settings.POSTGRES_DSN
    parts = dsn.split("/")
    db_name = parts[-1]
    temp_dsn = "/".join(parts[:-1]+["postgres"])
    engine = sa.create_engine(temp_dsn)
    conn = engine.connect()
    try:
        conn.execute(sa.text(f"drop database {db_name}"))
    except Exception as e:
        print(e)
    conn.close()


# Create a simple CLI for managing the database
# Use argparse to parse the command line arguments
import argparse
if __name__ == "__main__":
    import asyncio
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