"""Build one imagededup perceptual hash in a DBWorker child process."""

from sqlalchemy import update
from sqlalchemy.orm import Session

from dbworker import Finished
from imagededup_system_dbwork.db.models import ImageAsset, FeatureArtifact


def build_artifact(artifact: FeatureArtifact, session: Session) -> Finished:
    artifact_id = artifact.id
    image = session.get(ImageAsset, artifact.image_id)
    if image is None:
        raise ValueError("Artifact image no longer exists")
    file_path = image.file_path
    session.rollback()
    # Import the image stack only in handlers; API startup stays lightweight.
    from imagededup.methods import PHash  # type: ignore[import-untyped]

    hash_value = PHash(verbose=False).encode_image(image_file=file_path)
    if hash_value is None:
        raise ValueError(f"Cannot decode image: {file_path}")
    session.execute(update(FeatureArtifact).where(FeatureArtifact.id == artifact_id).values(hash_value=hash_value))
    return Finished()
