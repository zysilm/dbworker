from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .engine import Base


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
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    image_id: Mapped[int] = mapped_column(ForeignKey("image_asset.id"), unique=True)
    hash_value: Mapped[str | None] = mapped_column(String(16), nullable=True)


class ComparisonRequest(Base):
    __tablename__ = "comparison_request"
    id: Mapped[int] = mapped_column(primary_key=True)
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
