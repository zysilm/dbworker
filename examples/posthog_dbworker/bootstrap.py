"""Load the real notification slice without booting unrelated analytics products.

The business module, models and templates are loaded unchanged from the pinned
checkout. Selected support exports are compiled from their original AST nodes;
no algorithm, model field or SMTP operation is replaced. Both backends use this
same isolated application configuration and record the source projection.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import sys
import types
from functools import cache
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "posthog/posthog"
PROJECTIONS = {
    "posthog.utils": ("utils.py", ["PotentialSecurityProblemException", "absolute_uri", "compact_number", "str_to_bool"]),
    "posthog.settings.utils": ("settings/utils.py", ["get_from_env", "get_list"]),
    "posthog.models.utils": ("models/utils.py", ["UUIDTModel"]),
    "posthog.security.url_validation": ("security/url_validation.py", ["has_ambiguous_authority", "has_authority_bypass_chars", "ENCODED_AUTHORITY_TERMINATORS"]),
    "posthog.settings.base_variables": ("settings/base_variables.py", ["TEST"]),
}


def namespace(name: str, path: Path) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    module.__file__ = str(path / "__init__.py")
    module.__spec__ = importlib.util.spec_from_loader(name, loader=None, is_package=True)
    sys.modules[name] = module
    return module


def project(name: str, relative: str, exports: list[str], imports: str) -> types.ModuleType:
    path = SOURCE / relative
    parsed = ast.parse(path.read_text(), filename=str(path))
    def names(node):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            return {node.name}
        if isinstance(node, ast.Assign):
            return {target.id for target in node.targets if isinstance(target, ast.Name)}
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            return {node.target.id}
        return set()
    nodes = [node for node in parsed.body if names(node) & set(exports)]
    if set().union(*(names(node) for node in nodes)) != set(exports):
        raise RuntimeError(f"Upstream projection boundary changed: {name}")
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__spec__ = importlib.util.spec_from_loader(name, loader=None)
    sys.modules[name] = module
    exec(compile(imports, str(path), "exec"), module.__dict__)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), module.__dict__)
    return module


def load_model_support() -> None:
    project("posthog.models.utils", "models/utils.py", ["UUIDTModel"],
            "from django.db import models\nfrom posthog.uuidt import UUIDT\n")


@cache
def initialize() -> None:
    namespace("posthog", SOURCE)
    namespace("posthog.models", SOURCE / "models")
    namespace("posthog.helpers", SOURCE / "helpers")
    settings = namespace("posthog.settings", SOURCE / "settings")
    namespace("posthog.security", SOURCE / "security")
    project("posthog.security.url_validation", "security/url_validation.py",
            ["has_ambiguous_authority", "has_authority_bypass_chars", "ENCODED_AUTHORITY_TERMINATORS"],
            "import urllib.parse as urlparse\n")
    project("posthog.utils", "utils.py", PROJECTIONS["posthog.utils"][1],
            "from typing import Any, Optional, Union\nfrom urllib.parse import urlparse, urljoin\n"
            "from django.conf import settings\nfrom posthog.security.url_validation import has_ambiguous_authority\n")
    project("posthog.settings.utils", "settings/utils.py", ["get_from_env", "get_list"],
            "import os\nfrom typing import Any, Optional\nfrom collections.abc import Callable\n"
            "from django.core.exceptions import ImproperlyConfigured\nfrom posthog.utils import str_to_bool\n")
    helpers = sys.modules["posthog.settings.utils"]
    helpers.str_to_bool = sys.modules["posthog.utils"].str_to_bool
    project("posthog.settings.base_variables", "settings/base_variables.py", ["TEST"],
            "import sys\nfrom posthog.settings.utils import get_from_env, str_to_bool\n")
    from posthog.settings.dynamic_settings import CONSTANCE_CONFIG, CONSTANCE_DATABASE_PREFIX
    settings.CONSTANCE_CONFIG = CONSTANCE_CONFIG
    settings.CONSTANCE_DATABASE_PREFIX = CONSTANCE_DATABASE_PREFIX
    os.environ["DJANGO_SETTINGS_MODULE"] = "examples.posthog_dbworker.settings"
    import django
    django.setup()
