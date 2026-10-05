"""Reuse product querying and CSV generation; preserve independent ORM writes."""


def initialize():
    import django
    django.setup()
    # Match URL/system-check initialization order to avoid upstream filter/type
    # cycles when the export function is imported by a background worker.
    import saleor.graphql.api  # noqa: F401


def execute(payload: dict) -> dict:
    import hashlib
    from types import SimpleNamespace
    from saleor.core.db.connection import allow_writer
    from saleor.csv.models import ExportFile
    from saleor.csv.tasks import export_products_task

    with allow_writer():
        export_file = ExportFile.objects.select_related("app", "user").get(pk=payload["export_file_id"])
    arguments = (export_file.pk, payload["scope"], payload["export_info"],
                 payload["file_type"], payload.get("delimiter", ","))
    # Reuse the upstream task body and lifecycle hooks as ordinary synchronous
    # callables. No Celery eager execution or broker publication is involved.
    try:
        result = export_products_task.run(*arguments)
    except Exception as exc:
        with allow_writer():
            export_products_task.on_failure(exc, str(export_file.pk), arguments, {},
                                           SimpleNamespace(type=type(exc)))
        raise
    with allow_writer():
        export_products_task.on_success(result, str(export_file.pk), arguments, {})
    export_file.refresh_from_db()
    if not export_file.content_file:
        raise RuntimeError("Saleor produced no export artifact")
    with export_file.content_file.open("rb") as stream:
        content = stream.read()
    return {"content_sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content),
            "status": export_file.status,
            "events": list(export_file.events.order_by("pk").values_list("type", flat=True))}
