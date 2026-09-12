from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from durable_worker_example.db.models import ComparisonRequest, Document, FeatureArtifact, TopComparison, Workspace

router = APIRouter()


class WorkspaceInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ImportInput(BaseModel):
    directory: str


class ComparisonInput(BaseModel):
    retained_max_k: int = Field(default=10, ge=1, le=100)


def _sessions(request: Request):
    return request.app.state.sessions


@router.post("/workspaces")
def create_workspace(body: WorkspaceInput, request: Request):
    with _sessions(request).begin() as session:
        workspace = Workspace(name=body.name)
        session.add(workspace)
        session.flush()
        return {"id": workspace.id, "name": workspace.name}


@router.post("/workspaces/{workspace_id}/imports")
def import_text_files(workspace_id: int, body: ImportInput, request: Request):
    directory = Path(body.directory)
    if not directory.is_dir():
        raise HTTPException(400, "directory must be an existing directory")
    files = sorted(directory.glob("*.txt"))
    with _sessions(request).begin() as session:
        if session.get(Workspace, workspace_id) is None:
            raise HTTPException(404, "workspace not found")
        for file in files:
            document = Document(workspace_id=workspace_id, name=file.name, text=file.read_text(encoding="utf-8", errors="replace"))
            session.add(document)
            session.flush()
            session.add(FeatureArtifact(workspace_id=workspace_id, document_id=document.id, status="pending"))
    return {"imported_documents": len(files)}


@router.post("/workspaces/{workspace_id}/build-all")
def build_all(workspace_id: int, request: Request):
    """The coordinator already polls; this endpoint makes failed artifacts retryable."""
    with _sessions(request).begin() as session:
        artifacts = list(session.scalars(select(FeatureArtifact).where(FeatureArtifact.workspace_id == workspace_id)))
        if not artifacts and session.get(Workspace, workspace_id) is None:
            raise HTTPException(404, "workspace not found")
        for artifact in artifacts:
            if artifact.status == "failed":
                artifact.status, artifact.error = "pending", None
    return {"artifacts_available_for_build": len(artifacts)}


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: int, request: Request):
    with _sessions(request)() as session:
        artifact = session.get(FeatureArtifact, artifact_id)
        if artifact is None:
            raise HTTPException(404, "artifact not found")
        return {"id": artifact.id, "document_id": artifact.document_id, "status": artifact.status, "error": artifact.error}


@router.post("/comparisons/{query_artifact_id}")
def create_comparison(query_artifact_id: int, body: ComparisonInput, request: Request):
    with _sessions(request).begin() as session:
        artifact = session.get(FeatureArtifact, query_artifact_id)
        if artifact is None:
            raise HTTPException(404, "artifact not found")
        comparison = ComparisonRequest(workspace_id=artifact.workspace_id, query_artifact_id=artifact.id, retained_max_k=body.retained_max_k)
        session.add(comparison)
        session.flush()
        return {"id": comparison.id, "status": comparison.status}


@router.get("/comparisons/{request_id}")
def get_comparison(request_id: int, request: Request):
    with _sessions(request)() as session:
        comparison = session.get(ComparisonRequest, request_id)
        if comparison is None:
            raise HTTPException(404, "comparison request not found")
        return {"id": comparison.id, "status": comparison.status, "candidates_scored_count": comparison.candidates_scored_count, "error": comparison.error}


@router.get("/comparisons/{request_id}/results")
def get_results(request_id: int, request: Request):
    with _sessions(request)() as session:
        if session.get(ComparisonRequest, request_id) is None:
            raise HTTPException(404, "comparison request not found")
        rows = list(session.scalars(select(TopComparison).where(TopComparison.request_id == request_id).order_by(TopComparison.score.desc())))
        return [{"candidate_artifact_id": row.candidate_artifact_id, "score": row.score} for row in rows]
