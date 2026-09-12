from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .engine import Base


class Workspace(Base):
    __tablename__ = "workspace"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Document(Base):
    __tablename__ = "document"
    id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    name: Mapped[str] = mapped_column(String(500))
    text: Mapped[str] = mapped_column(Text)


class FeatureArtifact(Base):
    __tablename__ = "feature_artifact"
    id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("document.id"), unique=True)
    feature_json: Mapped[dict[str, int] | None] = mapped_column(JSON(none_as_null=True), nullable=True)


class ComparisonRequest(Base):
    __tablename__ = "comparison_request"
    id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    query_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"))
    retained_max_k: Mapped[int] = mapped_column(Integer, default=10)
    candidates_scored_count: Mapped[int] = mapped_column(Integer, default=0)


class TopComparison(Base):
    __tablename__ = "top_comparison"
    id: Mapped[int] = mapped_column(primary_key=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("comparison_request.id"), index=True)
    candidate_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"))
    score: Mapped[float] = mapped_column(Float)


class ScoredCandidate(Base):
    """Completion ledger, including candidates discarded from the top-K."""

    __tablename__ = "scored_candidate"
    request_id: Mapped[int] = mapped_column(ForeignKey("comparison_request.id"), primary_key=True)
    candidate_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"), primary_key=True)
