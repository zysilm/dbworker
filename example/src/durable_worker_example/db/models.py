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
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    feature_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    claim_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class ComparisonRequest(Base):
    __tablename__ = "comparison_request"
    id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspace.id"), index=True)
    query_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"))
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    retained_max_k: Mapped[int] = mapped_column(Integer, default=10)
    candidate_cursor_artifact_id: Mapped[int] = mapped_column(Integer, default=0)
    candidates_scored_count: Mapped[int] = mapped_column(Integer, default=0)
    claim_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class TopComparison(Base):
    __tablename__ = "top_comparison"
    id: Mapped[int] = mapped_column(primary_key=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("comparison_request.id"), index=True)
    candidate_artifact_id: Mapped[int] = mapped_column(ForeignKey("feature_artifact.id"))
    score: Mapped[float] = mapped_column(Float)
