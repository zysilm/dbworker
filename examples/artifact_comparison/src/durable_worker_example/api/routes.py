from dbworker import Coordinator, ExecutionStatus

from pathlib import Path
from typing import TypedDict, cast

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from durable_worker_example.db.models import ComparisonRequest, Document, FeatureArtifact, TopComparison, Workspace

router = APIRouter()


class WorkspaceInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ImportInput(BaseModel):
    directory: str


class ComparisonInput(BaseModel):
    retained_max_k: int = Field(default=10, ge=1, le=100)


class WorkspaceResponse(TypedDict):
    id: int
    name: str


class ImportResponse(TypedDict):
    imported_documents: int


class BuildResponse(TypedDict):
    artifacts_available_for_build: int


class ArtifactResponse(TypedDict):
    id: int
    document_id: int
    execution_status: ExecutionStatus | None
    error: str | None


class ComparisonCreatedResponse(TypedDict):
    id: int
    execution_status: ExecutionStatus | None


class ComparisonResponse(ComparisonCreatedResponse):
    candidates_scored_count: int
    error: str | None


class ComparisonResultResponse(TypedDict):
    candidate_artifact_id: int
    score: float


def _session_factory(request: Request) -> sessionmaker[Session]:
    return cast(sessionmaker[Session], request.app.state.session_factory)


@router.post("/workspaces")
def create_workspace(body: WorkspaceInput, request: Request) -> WorkspaceResponse:
    with _session_factory(request).begin() as session:
        workspace = Workspace(name=body.name)
        session.add(workspace)
        session.flush()
        return {"id": workspace.id, "name": workspace.name}


@router.post("/workspaces/{workspace_id}/imports")
def import_text_files(workspace_id: int, body: ImportInput, request: Request) -> ImportResponse:
    directory = Path(body.directory)
    if not directory.is_dir():
        raise HTTPException(400, "directory must be an existing directory")
    files = sorted(directory.glob("*.txt"))
    with _session_factory(request).begin() as session:
        if session.get(Workspace, workspace_id) is None:
            raise HTTPException(404, "workspace not found")
        for file in files:
            document = Document(workspace_id=workspace_id, name=file.name, text=file.read_text(encoding="utf-8", errors="replace"))
            session.add(document)
            session.flush()
            session.add(FeatureArtifact(workspace_id=workspace_id, document_id=document.id))
    return {"imported_documents": len(files)}


@router.post("/workspaces/{workspace_id}/build-all")
def build_all(workspace_id: int, request: Request) -> BuildResponse:
    """The coordinator already polls; this endpoint makes failed artifacts retryable."""
    with _session_factory(request).begin() as session:
        artifacts = list(session.scalars(select(FeatureArtifact).where(FeatureArtifact.workspace_id == workspace_id)))
        if not artifacts and session.get(Workspace, workspace_id) is None:
            raise HTTPException(404, "workspace not found")
        for artifact in artifacts:
            request.app.state.build_worker.reset_failed(session, artifact.id)
    return {"artifacts_available_for_build": len(artifacts)}


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: int, request: Request) -> ArtifactResponse:
    with _session_factory(request)() as session:
        artifact = session.get(FeatureArtifact, artifact_id)
        if artifact is None:
            raise HTTPException(404, "artifact not found")
        coordinator = cast(Coordinator, request.app.state.coordinator)
        execution_status = coordinator.execution_status(session, worker="artifact_build", source_id=artifact_id)
        state = coordinator.workers["artifact_build"].state(session, artifact_id) if execution_status is ExecutionStatus.FAILED else None
        return {"id": artifact.id, "document_id": artifact.document_id,
                "execution_status": execution_status, "error": state["error"] if state else None}


@router.post("/comparisons/{query_artifact_id}")
def create_comparison(query_artifact_id: int, body: ComparisonInput, request: Request) -> ComparisonCreatedResponse:
    with _session_factory(request).begin() as session:
        artifact = session.get(FeatureArtifact, query_artifact_id)
        if artifact is None:
            raise HTTPException(404, "artifact not found")
        comparison = ComparisonRequest(workspace_id=artifact.workspace_id, query_artifact_id=artifact.id, retained_max_k=body.retained_max_k)
        session.add(comparison)
        session.flush()
        return {"id": comparison.id, "execution_status": None}


@router.get("/comparisons/{request_id}")
def get_comparison(request_id: int, request: Request) -> ComparisonResponse:
    with _session_factory(request)() as session:
        coordinator = cast(Coordinator, request.app.state.coordinator)
        execution_status = coordinator.execution_status(session, worker="comparison", source_id=request_id)
        # Read progress after execution status: observing FINISHED must not be
        # paired with a count loaded before the handler's atomic final commit.
        comparison = session.get(ComparisonRequest, request_id)
        if comparison is None:
            raise HTTPException(404, "comparison request not found")
        state = coordinator.workers["comparison"].state(session, request_id) if execution_status is ExecutionStatus.FAILED else None
        return {"id": comparison.id, "execution_status": execution_status,
                "candidates_scored_count": comparison.candidates_scored_count, "error": state["error"] if state else None}


@router.get("/comparisons/{request_id}/results")
def get_results(request_id: int, request: Request) -> list[ComparisonResultResponse]:
    with _session_factory(request)() as session:
        if session.get(ComparisonRequest, request_id) is None:
            raise HTTPException(404, "comparison request not found")
        rows = list(session.scalars(select(TopComparison).where(TopComparison.request_id == request_id).order_by(TopComparison.score.desc())))
        return [{"candidate_artifact_id": row.candidate_artifact_id, "score": row.score} for row in rows]
