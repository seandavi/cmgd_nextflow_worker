import os
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()

@dataclass
class Settings:
    TEMPORALIO_ADDRESS: str = os.getenv("TEMPORALIO_URL", "localhost:7233")
    TEMPORAL_TLS: bool = bool(os.getenv("TEMPORAL_TLS", True))
    
settings = Settings()