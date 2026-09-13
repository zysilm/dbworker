from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import Enum, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .engine import Base


class ExecutionStatus(StrEnum):
    WORKING = "working"
    UNFINISHED = "unfinished"
    FINISHED = "finished"
    FAILED = "failed"


def status_column() -> Enum:
    return Enum(ExecutionStatus, native_enum=False,
                values_callable=lambda members: [member.value for member in members], validate_strings=True)


class Workspace(Base):
    __tablename__ = "workspace"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ImageAsset(Base):
    __tablename__ = "image_asset"
    id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    name: Mapped[str] = mapped_column(String(500))
    file_path: Mapped[str] = mapped_column(Text)


class FeatureArtifact(Base):
    __tablename__ = "feature_artifact"
    id: Mapped[int] = mapped_column(primary_key=True)
    execution_status: Mapped[ExecutionStatus | None] = mapped_column(status_column(), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    image_id: Mapped[int] = mapped_column(ForeignKey("image_asset.id"), unique=True)
    hash_value: Mapped[str | None] = mapped_column(String(16), nullable=True)


class ComparisonRequest(Base):
    __tablename__ = "comparison_request"
    id: Mapped[int] = mapped_column(primary_key=True)
    execution_status: Mapped[ExecutionStatus | None] = mapped_column(status_column(), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    query_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"))
    max_distance: Mapped[int] = mapped_column(Integer, default=10)
    retained_max_k: Mapped[int] = mapped_column(Integer, default=10)
    candidates_scored_count: Mapped[int] = mapped_column(Integer, default=0)


class TopComparison(Base):
    __tablename__ = "top_comparison"
    id: Mapped[int] = mapped_column(primary_key=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("comparison_request.id"), index=True)
    candidate_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"))
    distance: Mapped[int] = mapped_column(Integer)


class ScoredCandidate(Base):
    """Completion ledger, including candidates discarded from the top-K."""

    __tablename__ = "scored_candidate"
    request_id: Mapped[int] = mapped_column(ForeignKey("comparison_request.id"), primary_key=True)
    candidate_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"), primary_key=True)


class OutboxMessage(Base):
    """Task publication committed with application writes; deleted after send."""

    __tablename__ = "outbox_message"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=lambda: uuid4().hex)
    source_revision: Mapped[int] = mapped_column(Integer)
    task_name: Mapped[str] = mapped_column(String(100))
    source_id: Mapped[int] = mapped_column(Integer)
    available_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
