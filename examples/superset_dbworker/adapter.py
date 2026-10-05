"""Invoke SQL Lab without publishing or eagerly executing a Celery task."""

from functools import cache


@cache
def initialize():
    from superset.app import create_app
    return create_app()


def execute(payload: dict) -> dict:
    from superset import db
    from superset.models.sql_lab import Query
    from superset.sql_lab import get_sql_results

    with initialize().app_context():
        result = get_sql_results.run(**payload)
        query = db.session.get(Query, payload["query_id"])
        if query is None or query.status != "success" or not result or result.get("status") != "success":
            raise RuntimeError(f"SQL Lab failed: {result}")
        # IDs, timing and result keys vary across fresh application databases.
        return {"data": result.get("data"), "columns": result.get("columns"), "status": query.status,
                "rows": query.rows}
