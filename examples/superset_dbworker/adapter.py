"""Initialize the original application for the SQL Lab executor variation."""

from functools import cache


@cache
def initialize():
    from superset.tasks.celery_app import flask_app
    return flask_app
