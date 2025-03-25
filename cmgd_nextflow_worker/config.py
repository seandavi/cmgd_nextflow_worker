import os
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()

@dataclass
class Settings:
    TEMPORALIO_ADDRESS: str = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    TEMPORAL_TLS: bool = bool(os.getenv("TEMPORAL_TLS", False))
    POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "postgresql+asyncpg://postgres:password@localhost:5432/postgres")
    
settings = Settings()