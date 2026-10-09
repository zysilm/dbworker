"""Call Saleor's authenticated GraphQL export producer without recreating it."""
from __future__ import annotations

import base64

EXPORT_PRODUCTS_MUTATION = """
mutation BenchmarkExportProducts($input: ExportProductsInput!) {
  exportProducts(input: $input) {
    exportFile { id }
    errors { field code message }
  }
}
"""


def export_input(product_ids):
    """Use public GraphQL IDs and enums; upstream normalizes task arguments."""
    return {"scope": "IDS", "ids": list(product_ids),
            "exportInfo": {"fields": ["NAME", "PRODUCT_TYPE", "VARIANT_SKU"]},
            "fileType": "CSV"}


def post_export(client, token, product_ids):
    """Run the original URL/view/middleware and return its complete response."""
    response = client.post(
        "/graphql/", {"query": EXPORT_PRODUCTS_MUTATION,
                      "variables": {"input": export_input(product_ids)}},
        content_type="application/json", HTTP_HOST="localhost",
        HTTP_AUTHORIZATION=f"JWT {token}")
    if response.status_code != 200:
        raise AssertionError(f"Saleor GraphQL endpoint returned HTTP {response.status_code}")
    return response.json()


def export_key(content):
    """Require producer success and decode the original returned ExportFile ID."""
    if content.get("errors"):
        raise AssertionError("Saleor GraphQL producer returned protocol/authentication errors")
    payload = (content.get("data") or {}).get("exportProducts")
    if not isinstance(payload, dict) or payload.get("errors"):
        raise AssertionError("Saleor GraphQL export mutation returned business errors")
    identity = (payload.get("exportFile") or {}).get("id")
    if not isinstance(identity, str):
        raise AssertionError("Saleor GraphQL producer did not return an export ID")
    try:
        kind, key = base64.b64decode(identity, validate=True).decode().split(":", 1)
    except (ValueError, UnicodeDecodeError) as error:
        raise AssertionError("Invalid GraphQL export identity") from error
    if kind != "ExportFile" or not key.isdecimal() or int(key) <= 0:
        raise AssertionError("GraphQL producer returned another object type or invalid key")
    return int(key)
