"""Supply the reviewed task-identity context to the unchanged bound body."""


def initialize():
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "paperless_ngx" / "src"))
    import django
    django.setup()


def execute(payload: dict) -> dict:
    import hashlib
    import inspect
    from pathlib import Path
    from types import SimpleNamespace
    from documents.data_models import ConsumableDocument, DocumentMetadataOverrides, DocumentSource
    from documents.models import Document
    from documents.tasks import consume_file

    body = consume_file.run.__func__
    if list(inspect.signature(body).parameters) != ["self", "input_doc", "overrides"]:
        raise RuntimeError("The reviewed consumer boundary changed")
    input_doc = ConsumableDocument(source=DocumentSource(payload["source"]),
                                   original_file=Path(payload["original_file"]))
    context = SimpleNamespace(request=SimpleNamespace(id=payload["request_identity"]))
    result = body(context, input_doc, DocumentMetadataOverrides(**payload.get("overrides", {})))
    if not result or "document_id" not in result:
        raise RuntimeError(f"Document consumption did not succeed: {result}")
    document = Document.objects.get(pk=result["document_id"])
    if not document.source_path.is_file() or not document.thumbnail_path.is_file():
        raise RuntimeError("The consumed document's durable original or thumbnail is missing")
    from documents.parsers import get_default_thumbnail
    if document.thumbnail_path.read_bytes() == get_default_thumbnail().read_bytes():
        raise RuntimeError("Thumbnail generation fell back to the generic placeholder")
    from documents.search import get_backend
    import time
    deadline = time.monotonic() + 10
    while document.pk not in get_backend().search_ids(payload["search_term"], None):
        if time.monotonic() >= deadline:
            break
        time.sleep(.1)
    else:
        deadline = None
    if deadline is not None:
        raise RuntimeError("The consumed document is absent from the real search index")
    return {"content_sha256": hashlib.sha256(document.content.encode()).hexdigest(),
            "content": document.content, "checksum": document.checksum, "title": document.title,
            "original_sha256": hashlib.sha256(document.source_path.read_bytes()).hexdigest(),
            "search_verified": True, "thumbnail_present": True}
