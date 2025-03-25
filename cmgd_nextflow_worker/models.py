from dataclasses import dataclass

@dataclass
class CMGDJobInput:
    sample_id: str
    run_ids: list[str]
    
@dataclass
class CommandResult:
    returncode: int | None
    stdout: bytes
    stderr: bytes
    