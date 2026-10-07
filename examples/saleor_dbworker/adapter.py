"""Initialize the unchanged Saleor application for durable workflow workers."""


def initialize():
    import django
    django.setup()
    # Match URL/system-check initialization order to avoid upstream filter/type
    # cycles when the export function is imported by a background worker.
    import saleor.graphql.api  # noqa: F401
