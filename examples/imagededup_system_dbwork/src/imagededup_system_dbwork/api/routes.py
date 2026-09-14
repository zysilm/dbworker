from dbworker import Coordinator, ExecutionStatus

from pathlib import Path
from typing import TypedDict, cast

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from imagededup_system_dbwork.db.models import ComparisonRequest, ImageAsset, FeatureArtifact, TopComparison, Workspace

router = APIRouter()


class WorkspaceInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ImportInput(BaseModel):
    directory: str
    limit: int = Field(default=25000, ge=1, le=25000)


class ComparisonInput(BaseModel):
    max_distance: int = Field(default=10, ge=0, le=64)
    retained_max_k: int = Field(default=10, ge=1, le=100)


class WorkspaceResponse(TypedDict):
    id: int
    name: str


class ImportResponse(TypedDict):
    imported_images: int
    artifact_ids: list[int]


class BuildResponse(TypedDict):
    artifacts_available_for_build: int


class ArtifactResponse(TypedDict):
    id: int
    image_id: int
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
    distance: int


def _session_factory(request: Request) -> sessionmaker[Session]:
    return cast(sessionmaker[Session], request.app.state.session_factory)


def _import_directory(directory_input: str, request: Request) -> Path:
    import_root = cast(Path, request.app.state.import_root).resolve()
    requested = Path(directory_input).expanduser()
    directory = (requested if requested.is_absolute() else import_root / requested).resolve()
    if not directory.is_relative_to(import_root):
        raise HTTPException(400, "directory must be within the configured import root")
    if not directory.is_dir():
        raise HTTPException(400, "directory must be an existing image directory")
    return directory


@router.post("/workspaces")
def create_workspace(body: WorkspaceInput, request: Request) -> WorkspaceResponse:
    with _session_factory(request).begin() as session:
        workspace = Workspace(name=body.name)
        session.add(workspace)
        session.flush()
        return {"id": workspace.id, "name": workspace.name}


@router.post("/workspaces/{workspace_id}/imports")
def import_images(workspace_id: int, body: ImportInput, request: Request) -> ImportResponse:
    directory = _import_directory(body.directory, request)
    files = sorted(file for file in directory.iterdir()
                   if file.is_file() and file.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"})[:body.limit]
    artifact_ids: list[int] = []
    with _session_factory(request).begin() as session:
        if session.get(Workspace, workspace_id) is None:
            raise HTTPException(404, "workspace not found")
        existing = set(session.scalars(select(ImageAsset.file_path).where(ImageAsset.workspace_id == workspace_id)))
        for file in files:
            if str(file) in existing:
                continue
            image = ImageAsset(workspace_id=workspace_id, name=file.name, file_path=str(file))
            session.add(image)
            session.flush()
            artifact = FeatureArtifact(workspace_id=workspace_id, image_id=image.id)
            session.add(artifact)
            session.flush()
            artifact_ids.append(artifact.id)
    return {"imported_images": len(artifact_ids), "artifact_ids": artifact_ids}


@router.get("/workspaces/{workspace_id}/artifacts")
def list_artifacts(workspace_id: int, request: Request, after_id: int = 0,
                   limit: int = Query(default=100, ge=1, le=1000)) -> list[ArtifactResponse]:
    with _session_factory(request)() as session:
        if session.get(Workspace, workspace_id) is None:
            raise HTTPException(404, "workspace not found")
        ids = list(session.scalars(select(FeatureArtifact.id).where(
            FeatureArtifact.workspace_id == workspace_id, FeatureArtifact.id > after_id,
        ).order_by(FeatureArtifact.id).limit(limit)))
    return [get_artifact(key, request) for key in ids]


@router.get("/artifacts/{artifact_id}/image")
def get_image(artifact_id: int, request: Request) -> FileResponse:
    with _session_factory(request)() as session:
        image = session.scalar(select(ImageAsset).join(FeatureArtifact).where(FeatureArtifact.id == artifact_id))
        if image is None:
            raise HTTPException(404, "artifact not found")
        path = Path(image.file_path)
        if not path.is_file():
            raise HTTPException(404, "image file no longer exists")
        return FileResponse(path)


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
        return {"id": artifact.id, "image_id": artifact.image_id,
                "execution_status": execution_status, "error": state["error"] if state else None}


@router.post("/comparisons/{query_artifact_id}")
def create_comparison(query_artifact_id: int, body: ComparisonInput, request: Request) -> ComparisonCreatedResponse:
    with _session_factory(request).begin() as session:
        artifact = session.get(FeatureArtifact, query_artifact_id)
        if artifact is None:
            raise HTTPException(404, "artifact not found")
        comparison = ComparisonRequest(workspace_id=artifact.workspace_id, query_artifact_id=artifact.id, retained_max_k=body.retained_max_k, max_distance=body.max_distance)
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
        rows = list(session.scalars(select(TopComparison).where(TopComparison.request_id == request_id).order_by(TopComparison.distance.asc(), TopComparison.candidate_artifact_id.asc())))
        return [{"candidate_artifact_id": row.candidate_artifact_id, "distance": row.distance} for row in rows]
