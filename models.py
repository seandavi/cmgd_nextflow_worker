from dataclasses import dataclass

@dataclass
class HelloWorldInput:
    sample_id: str
    run_ids: list[str]