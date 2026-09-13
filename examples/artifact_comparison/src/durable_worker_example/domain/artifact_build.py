"""Build and persist document features."""

import re
from collections import Counter

from sqlalchemy import update
from sqlalchemy.orm import Session

from dbworker import Finished
from durable_worker_example.db.models import Document, FeatureArtifact


def build_artifact(artifact: FeatureArtifact, session: Session) -> Finished:
    artifact_id = artifact.id
    document = session.get(Document, artifact.document_id)
    if document is None:
        raise ValueError("Artifact document no longer exists")
    text = document.text
    # Copy values before rollback expires ORM objects. No connection is held
    # while computing; the next SQL statement starts the final transaction.
    session.rollback()
    features = dict(Counter(re.findall(r"[a-z0-9]+", text.lower())))
    session.execute(update(FeatureArtifact).where(FeatureArtifact.id == artifact_id).values(feature_json=features))
    return Finished()
