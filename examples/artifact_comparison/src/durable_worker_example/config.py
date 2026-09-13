from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str = f"sqlite:///{Path.cwd() / 'example.db'}"
    build_workers: int = 8
    comparison_workers: int = 8
    claim_lease_seconds: int = 30
    poll_seconds: float = 0.25
    max_poll_seconds: float = 10
    comparison_page_size: int = 50


settings = Settings()
