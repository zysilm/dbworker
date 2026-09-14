import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str = os.environ.get("DBWORKER_DATABASE_URL", f"sqlite:///{Path.cwd() / 'example.db'}")
    import_root: Path = Path(os.environ.get("DBWORKER_IMPORT_ROOT", Path.cwd())).expanduser().resolve()
    build_workers: int = int(os.environ.get("DBWORKER_BUILD_WORKERS", "2"))
    comparison_workers: int = int(os.environ.get("DBWORKER_COMPARISON_WORKERS", "2"))
    claim_lease_seconds: int = 60
    poll_seconds: float = 0.25
    max_poll_seconds: float = 2
    comparison_page_size: int = int(os.environ.get("DBWORKER_COMPARISON_PAGE_SIZE", "250"))


settings = Settings()
