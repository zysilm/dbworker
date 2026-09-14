import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str = os.environ.get("IMAGE_DATABASE_URL", f"sqlite:///{Path.cwd() / 'example.db'}")
    import_root: Path = Path(os.environ.get("IMAGE_IMPORT_ROOT", Path.cwd())).expanduser().resolve()
    broker_url: str = os.environ.get("IMAGE_BROKER_URL", "redis://127.0.0.1:6379/0")
    comparison_page_size: int = int(os.environ.get("IMAGE_COMPARISON_PAGE_SIZE", "250"))
    dependency_wait_seconds: float = float(os.environ.get("IMAGE_DEPENDENCY_WAIT_SECONDS", "1"))
    outbox_batch_size: int = 1000

    def __post_init__(self) -> None:
        if self.comparison_page_size < 1 or self.dependency_wait_seconds <= 0:
            raise ValueError("Page size and dependency wait must be positive")


settings = Settings()
