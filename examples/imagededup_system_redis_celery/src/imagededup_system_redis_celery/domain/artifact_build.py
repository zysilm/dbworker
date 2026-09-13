"""One image per Celery task; only IDs cross the Redis broker."""

from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker

from imagededup_system_redis_celery.db.models import ExecutionStatus, FeatureArtifact, ImageAsset


def build_artifact(artifact_id: int, revision: int, session_factory: sessionmaker[Session]) -> None:
    with session_factory.begin() as session:
        artifact = session.get(FeatureArtifact, artifact_id)
        if artifact is None or artifact.revision != revision or artifact.execution_status in (
            ExecutionStatus.FINISHED, ExecutionStatus.FAILED,
        ):
            return
        image = session.get(ImageAsset, artifact.image_id)
        if image is None:
            raise ValueError("Artifact image no longer exists")
        file_path = image.file_path
        session.execute(update(FeatureArtifact).where(
            FeatureArtifact.id == artifact_id, FeatureArtifact.revision == revision,
        ).values(execution_status=ExecutionStatus.WORKING))
    # No SQL connection is held while decoding and hashing. Imagededup imports
    # occur only in build children; comparison/API processes do not load Torch.
    from imagededup.methods import PHash  # type: ignore[import-untyped]

    hash_value = PHash(verbose=False).encode_image(image_file=file_path)
    if hash_value is None:
        raise ValueError(f"Cannot decode image: {file_path}")
    with session_factory.begin() as session:
        session.execute(update(FeatureArtifact).where(
            FeatureArtifact.id == artifact_id, FeatureArtifact.revision == revision,
        ).values(hash_value=hash_value, execution_status=ExecutionStatus.FINISHED,
                 error=None, revision=revision + 1))
