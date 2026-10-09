"""Bind persisted Celery identities to reviewed, pinned native registrations.

File membership is insufficient: another genuine function in an original file
does not implement the declared task. These bindings are an admission contract,
independent of result-provided names and checksums. An upstream revision change
requires reviewing and updating this contract, as well as the checkout pin.
"""
from __future__ import annotations

import ast
import hashlib
from collections.abc import Mapping
from pathlib import Path

from benchmarks.common.native_admission import NativeAdmissionError


# Paths are relative to the existing live-origin source roots. Module names are
# Python import names, not necessarily filesystem-relative paths (Paperless).
BINDINGS = {
    'superset': {
        'sql_lab.get_sql_results': ('superset/sql_lab.py', 'superset.sql_lab', 'get_sql_results', 'celery_app.task'),
    },
    'saleor': {
        'export-products': ('saleor/csv/tasks.py', 'saleor.csv.tasks', 'export_products_task', 'app.task'),
        'saleor.plugins.admin_email.tasks.send_email_with_link_to_download_file_task': (
            'saleor/plugins/admin_email/tasks.py', 'saleor.plugins.admin_email.tasks',
            'send_email_with_link_to_download_file_task', 'app.task'),
    },
    'paperless_ngx': {
        'documents.tasks.consume_file': ('src/documents/tasks.py', 'documents.tasks', 'consume_file', 'shared_task'),
    },
    'posthog': {
        'posthog.tasks.email.send_two_factor_auth_enabled_email': (
            'posthog/tasks/email.py', 'posthog.tasks.email', 'send_two_factor_auth_enabled_email', 'shared_task'),
        'posthog.email._send_email': ('posthog/email.py', 'posthog.email', '_send_email_now', 'shared_task'),
    },
    'sentry': {
        'sentry.tasks.email.send_email': ('sentry/tasks/email.py', 'sentry.tasks.email', 'send_email', 'instrumented_task'),
        'sentry.tasks.email.send_email_control': (
            'sentry/tasks/email.py', 'sentry.tasks.email', 'send_email_control', 'instrumented_task'),
    },
    'imagededup': {
        'images.build': ('imagededup_system_redis_celery/tasks.py', 'imagededup_system_redis_celery.tasks', 'build', 'app.task'),
        'images.compare': ('imagededup_system_redis_celery/tasks.py', 'imagededup_system_redis_celery.tasks', 'compare', 'app.task'),
        'images.dispatch': ('imagededup_system_redis_celery/tasks.py', 'imagededup_system_redis_celery.tasks', 'dispatch', 'app.task'),
    },
}

SOURCE_SHA256 = {
    ('superset', 'superset/sql_lab.py'): '57afb23c405755cc9c9143e65f1bc06b87cd90ba2a3bf5a7934d7096b4f373b3',
    ('saleor', 'saleor/csv/tasks.py'): 'c50dc236429f914f6d6f8cd238f91d4727364b14a3dbe0cfa28f9c9345376716',
    ('saleor', 'saleor/plugins/admin_email/tasks.py'): 'c0ff70ca30ee42c547194c9c0115f29b0d31c4c7c3c385e2961a42add1f43e3e',
    ('paperless_ngx', 'src/documents/tasks.py'): 'e916133fdf8b447a70194c2768d10d0cd1880089d0af962bc65037ca24d194ee',
    ('posthog', 'posthog/tasks/email.py'): 'fa988a5e7e993ce3fb08cdb3d3c8b2fa0d08d3a5966048b9786702aa14e60c4e',
    ('posthog', 'posthog/email.py'): '6e47575cc000547ed7954230d0b3f3ce883119e99fb958758fb37f0113f2a557',
    ('sentry', 'sentry/tasks/email.py'): 'df98adb24ad29526c6b7efcf905b143cb2865767a28e7b0a15b698d446ed23af',
    ('sentry', 'sentry/tasks/base.py'): '6ce063cf9a18ec4f21bf6b37b898d96856181532b64c31b38d128a4bc68071e1',
    ('imagededup', 'imagededup_system_redis_celery/tasks.py'): '29f4529ab989b36051c914af72ebe42837ebba8518b1eaf04a1071354aac4d67',
}


def _pinned_tree(suite, root, relative):
    root = Path(root).resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise NativeAdmissionError(f'{suite}: native registration source is missing or unsafe: {relative}')
    content = path.read_bytes()
    expected = SOURCE_SHA256.get((suite, relative))
    if expected is None or hashlib.sha256(content).hexdigest() != expected:
        raise NativeAdmissionError(f'{suite}: native registration source differs from reviewed pin: {relative}')
    return ast.parse(content, filename=str(path))


def _dotted(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted(node.value)
        return f'{parent}.{node.attr}' if parent else None
    return None


def _mapping_keys(suite, root, tree, node):
    """Prove an expanded keyword mapping cannot override the task's name."""
    if isinstance(node, ast.Dict):
        keys = set()
        for key, value in zip(node.keys, node.values):
            if key is None:
                keys.update(_mapping_keys(suite, root, tree, value))
            elif isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.add(key.value)
            else:
                raise NativeAdmissionError('Native task registration has dynamic keyword keys')
        return keys
    if isinstance(node, ast.Name):
        assignments = [item.value for item in tree.body if isinstance(item, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == node.id for target in item.targets)]
        if len(assignments) == 1:
            return _mapping_keys(suite, root, tree, assignments[0])
        # The notification imports this original mapping; inspect its separately
        # pinned definition rather than assuming **kwargs never includes name.
        if suite == 'posthog' and node.id == 'EMAIL_TASK_KWARGS':
            imports = [item for item in tree.body if isinstance(item, ast.ImportFrom)
                       and item.module == 'posthog.email' and item.level == 0
                       and any(alias.name == 'EMAIL_TASK_KWARGS' and alias.asname is None for alias in item.names)]
            if len(imports) == 1:
                imported = _pinned_tree(suite, root, 'posthog/email.py')
                return _mapping_keys(suite, root, imported, node)
    raise NativeAdmissionError('Native task registration has unproved keyword expansion')


def _registered_name(suite, root, tree, decorator, module, function):
    if not isinstance(decorator, ast.Call):
        return f'{module}.{function}'
    if decorator.args:
        raise NativeAdmissionError('Native task registration has an unsupported positional name')
    names = [keyword.value for keyword in decorator.keywords if keyword.arg == 'name']
    for keyword in decorator.keywords:
        if keyword.arg is None and 'name' in _mapping_keys(suite, root, tree, keyword.value):
            raise NativeAdmissionError('Native task registration has an expanded task name')
    if len(names) > 1 or names and not (isinstance(names[0], ast.Constant) and isinstance(names[0].value, str)):
        raise NativeAdmissionError('Native task registration has a dynamic or duplicate name')
    return names[0].value if names else f'{module}.{function}'


def validate_task_binding(suite: str, task_name: str, evidence: Mapping, source_root: Path) -> None:
    """Reject false task/function associations without importing the application.

    This is persisted producer-origin admission, not a worker-process code
    attestation. Live callable/bytecode and original framework-wrapper admission
    remains the responsibility of check_original_tasks.
    """
    binding = BINDINGS.get(suite, {}).get(task_name)
    if binding is None:
        raise NativeAdmissionError(f'{suite}: unreviewed native task identity: {task_name}')
    relative, module, function, registration_factory = binding
    if (not isinstance(evidence, Mapping) or evidence.get('source') != relative
            or evidence.get('function') != function
            or evidence.get('source_sha256') != SOURCE_SHA256[(suite, relative)]):
        raise NativeAdmissionError(f'{suite}: task evidence differs from original registration: {task_name}')
    tree = _pinned_tree(suite, source_root, relative)
    definitions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and node.name == function]
    if len(definitions) != 1:
        raise NativeAdmissionError(f'{suite}: original task function is missing or ambiguous: {task_name}')
    registrations = []
    for decorator in definitions[0].decorator_list:
        factory = decorator.func if isinstance(decorator, ast.Call) else decorator
        if _dotted(factory) == registration_factory:
            registrations.append(decorator)
    # PostHog registers its undecorated SMTP callable at module scope.
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        application = node.value
        if (isinstance(application.func, ast.Call) and _dotted(application.func.func) == registration_factory
                and len(application.args) == 1 and isinstance(application.args[0], ast.Name)
                and application.args[0].id == function and not application.keywords):
            registrations.append(application.func)
    if len(registrations) != 1:
        raise NativeAdmissionError(f'{suite}: original task registration is missing or ambiguous: {task_name}')
    registered_name = _registered_name(suite, source_root, tree, registrations[0], module, function)
    if registered_name != task_name:
        raise NativeAdmissionError(f'{suite}: original registered task name differs: {task_name}')
    if suite == 'sentry':
        # instrumented_task wraps the callable before app.task registration. Its
        # reviewed implementation and forwarding of name are separately pinned.
        _pinned_tree(suite, source_root, 'sentry/tasks/base.py')
