"""Static and live origin checks for original application Celery entry points."""
from __future__ import annotations

import ast
import hashlib
import inspect
import importlib
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APPLICATIONS = {
    'superset': 'superset.tasks.celery_app:app',
    'saleor': 'saleor.celeryconf:app',
    'paperless_ngx': 'paperless.celery:app',
    'posthog': 'posthog.celery:app',
    'sentry': 'sentry.celery:app',
    'imagededup': 'imagededup_system_redis_celery.celery_app:app',
}


class NativeAdmissionError(ValueError):
    pass


def _code_signature(code):
    """Compare executed function code to a fresh compilation of the pinned file."""
    constants = tuple(_code_signature(value) if isinstance(value, types.CodeType) else value
                      for value in code.co_consts)
    return (code.co_code, constants, code.co_names, code.co_varnames, code.co_freevars,
            code.co_cellvars, code.co_argcount, code.co_posonlyargcount,
            code.co_kwonlyargcount, code.co_flags, getattr(code, 'co_exceptiontable', b''))


def _verify_file_code(candidate, path, name):
    code = getattr(candidate, '__code__', None)
    if code is None:
        raise NativeAdmissionError(f'No inspectable function code: {name}')
    compiled = compile(path.read_bytes(), str(path), 'exec', dont_inherit=True)
    pending = [compiled]
    while pending:
        current = pending.pop()
        if (current.co_name == code.co_name and current.co_firstlineno == code.co_firstlineno
                and _code_signature(current) == _code_signature(code)):
            return
        pending.extend(value for value in current.co_consts if isinstance(value, types.CodeType))
    raise NativeAdmissionError(f'Runtime task code differs from pinned source: {name}')


def _reachable_nodes(tree):
    """Exclude unused local functions and statically dead command decoys.

    Public class methods are entry points because image runtime classes are
    instantiated by a separate module. Static admission is supplemented by
    live origin checks and trace replay; it cannot prove a command was launched.
    """
    functions = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.setdefault(node.name, []).append(node)
    pending, visited, result = [tree], set(), []

    def walk(node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return
        if isinstance(node, ast.ClassDef):
            pending.extend(child for child in node.body
                           if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)))
            return
        result.append(node)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            pending.extend(functions.get(node.func.id, []))
        if isinstance(node, ast.If) and isinstance(node.test, ast.Constant):
            for child in node.body if node.test.value else node.orelse:
                walk(child)
            return
        for field, value in ast.iter_fields(node):
            if isinstance(value, list):
                for child in value:
                    if isinstance(child, ast.AST):
                        walk(child)
                        if field == 'body' and isinstance(child, (ast.Return, ast.Raise)):
                            break
            elif isinstance(value, ast.AST):
                walk(value)

    while pending:
        current = pending.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in current.body:
                walk(child)
                if isinstance(child, (ast.Return, ast.Raise)):
                    break
        else:
            walk(current)
    return result


def check_worker_source(source, suite):
    """Inspect actual AST command lists, not comments or textual substrings."""
    tree = ast.parse(source)
    expected = APPLICATIONS[suite]
    admitted = []
    celery_constructors = {'Celery'}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'celery':
            celery_constructors.update(alias.asname or alias.name for alias in node.names if alias.name == 'Celery')
    assignments = {}
    reachable = _reachable_nodes(tree)
    mutated_names = set()
    for node in reachable:
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
                    mutated_names.add(target.value.id)
                if isinstance(node, ast.AugAssign) and isinstance(target, ast.Name):
                    mutated_names.add(target.id)
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.attr in ('append', 'extend', 'insert', 'pop', 'remove', 'clear', 'reverse', 'sort')):
            mutated_names.add(node.func.value.id)
    for node in reachable:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments.setdefault(target.id, []).append(node.value)

    def command_values(node, seen=()):
        if isinstance(node, (ast.List, ast.Tuple)):
            values = [[]]
            for element in node.elts:
                segments = command_values(element.value, seen) if isinstance(element, ast.Starred) else [[element.value if isinstance(element, ast.Constant) else None]]
                values = [left + right for left in values for right in segments]
            return values
        if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Slice):
            bounds = (node.slice.lower, node.slice.upper, node.slice.step)
            if all(bound is None or isinstance(bound, ast.Constant) and isinstance(bound.value, int) for bound in bounds):
                selected = slice(*(bound.value if bound is not None else None for bound in bounds))
                return [values[selected] for values in command_values(node.value, seen)]
        if isinstance(node, ast.Name) and node.id not in seen:
            resolved = [values for assigned in assignments.get(node.id, [])
                        for values in command_values(assigned, (*seen, node.id))]
            if node.id in mutated_names and any('worker' in values or 'celery' in values for values in resolved):
                raise NativeAdmissionError(f'Mutated worker command cannot be statically admitted: {node.id}')
            return resolved
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return [left + right for left in command_values(node.left, seen)
                    for right in command_values(node.right, seen)]
        return []

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name for alias in node.names] + [getattr(node, 'module', '') or '']
            if any('benchmarks.upstream.celery_app' in name for name in names):
                raise NativeAdmissionError('Benchmark-owned Celery wrapper import')
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and 'benchmarks.upstream.celery_app' in node.value:
            raise NativeAdmissionError('Benchmark-owned Celery worker command')
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id in celery_constructors:
            raise NativeAdmissionError('Benchmark source constructs a replacement Celery application')
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in ('task', 'shared_task', 'apply'):
                raise NativeAdmissionError('Benchmark source defines replacement tasks or invokes eager execution')
            if node.func.attr == 'run' and not (isinstance(node.func.value, ast.Name) and node.func.value.id == 'subprocess'):
                raise NativeAdmissionError('Benchmark source directly invokes a task body')
        if node not in reachable:
            continue
        function = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else ''
        if function not in ('launch', 'start_process', 'Popen', 'run', 'run_command'):
            continue
        for argument in node.args:
            for values in command_values(argument):
                native_bootstrap = 'examples.sentry_dbworker.native_worker' in values
                if native_bootstrap:
                    bootstrap_path = ROOT / 'examples/sentry_dbworker/native_worker.py'
                    expected_bootstrap = '''from sentry.runner import configure
def main():
    configure(skip_service_validation=True)
    from celery.__main__ import main as celery_main
    celery_main()
if __name__ == "__main__":
    main()
'''
                    bootstrap_tree = ast.parse(bootstrap_path.read_text())
                    if (bootstrap_tree.body and isinstance(bootstrap_tree.body[0], ast.Expr)
                            and isinstance(bootstrap_tree.body[0].value, ast.Constant)
                            and isinstance(bootstrap_tree.body[0].value.value, str)):
                        bootstrap_tree.body.pop(0)
                    if ast.dump(bootstrap_tree) != ast.dump(ast.parse(expected_bootstrap)):
                        raise NativeAdmissionError('Sentry bootstrap changes native Celery CLI behavior')
                if 'celery' not in values and not native_bootstrap and not any(isinstance(value, str) and 'from celery.__main__ import main' in value for value in values):
                    continue
                if 'worker' not in values and 'beat' not in values:
                    raise NativeAdmissionError('Native application command does not start a worker')
                for position, value in enumerate(values):
                    if value in ('-A', '--app') and position + 1 < len(values):
                        app = values[position + 1]
                    elif isinstance(value, str) and value.startswith('--app='):
                        app = value.split('=', 1)[1]
                    else:
                        continue
                    if app != expected:
                        raise NativeAdmissionError(f'Unexpected Celery application: {app}')
                    if 'worker' in values:
                        admitted.append(app)
    if not admitted:
        raise NativeAdmissionError('No statically verifiable native Celery worker command')
    return {'passed': True, 'worker_app': expected, 'check': 'worker_command_ast'}


def check_original_tasks(app, tasks, source_root, *, expected_application, configuration=None):
    """Verify registered task bodies originate in the pristine pinned checkout.

    Call after importing the original application, before publication. The runner
    separately verifies the entire checkout's commit and clean status. These live
    checks reject replacement task bodies living in benchmark or adapter files.
    """
    source_root = Path(source_root).resolve()
    module_name, attribute = expected_application.split(":", 1)
    native_app = getattr(importlib.import_module(module_name), attribute)
    if app is not native_app:
        raise NativeAdmissionError("Worker application is not the original exported app")
    evidence = {}
    for name in tasks:
        task = app.tasks[name]
        candidate = task.run
        framework_wrappers = []
        while True:
            physical_source = inspect.getsourcefile(candidate)
            if physical_source is None:
                raise NativeAdmissionError(f"Uninspectable task wrapper: {name}")
            if not Path(physical_source).resolve().is_relative_to(source_root):
                # Celery installs this wrapper for the application's original
                # autoretry_for declaration. Verify the actual factory, closure
                # and original callable; an arbitrary wraps() replacement fails.
                try:
                    from celery.app.autoretry import add_autoretry_behaviour
                except ImportError as error:
                    raise NativeAdmissionError(f"Non-native task wrapper: {name}") from error
                factory_source = Path(inspect.getsourcefile(add_autoretry_behaviour)).resolve()
                closure_task = inspect.getclosurevars(candidate).nonlocals.get("task")
                if (Path(physical_source).resolve() != factory_source
                        or getattr(candidate.__code__, 'co_qualname', 'add_autoretry_behaviour.<locals>.' + candidate.__code__.co_name)
                           != "add_autoretry_behaviour.<locals>.run"
                        or closure_task is not task
                        or getattr(candidate, "__wrapped__", None) is not getattr(task, "_orig_run", None)):
                    raise NativeAdmissionError(f"Non-native task wrapper: {name}: {physical_source}")
                framework_wrappers.append({"module": "celery.app.autoretry",
                                           "source_sha256": hashlib.sha256(factory_source.read_bytes()).hexdigest()})
                _verify_file_code(candidate, factory_source, name)
            else:
                _verify_file_code(candidate, Path(physical_source).resolve(), name)
            if not hasattr(candidate, "__wrapped__"):
                break
            candidate = candidate.__wrapped__
        body = inspect.unwrap(task.run)
        source = inspect.getsourcefile(body)
        if source is None:
            raise NativeAdmissionError(f'Uninspectable original task: {name}')
        path = Path(source).resolve()
        if not path.is_relative_to(source_root):
            raise NativeAdmissionError(f'Replaced task body: {name}: {path}')
        if getattr(task, 'name', None) != name:
            raise NativeAdmissionError(f'Renamed native task: {name}')
        tree = ast.parse(path.read_text())
        code = getattr(body, '__code__', None)
        if code is None:
            raise NativeAdmissionError(f'No native function code: {name}')
        defined = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == code.co_name]
        if not defined:
            raise NativeAdmissionError(f'Native AST function definition absent: {name}')
        evidence[name] = {'source': str(path.relative_to(source_root)),
                          'source_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                          'function': code.co_name,
                          'task_class': type(task).__module__ + '.' + type(task).__qualname__,
                          'framework_wrappers': framework_wrappers}
    if not evidence:
        raise NativeAdmissionError('Native task evidence is empty')
    return {'passed': True, 'worker_app': expected_application, 'observer_only': True,
            'task_names': list(tasks), 'tasks': evidence, 'configuration': configuration or {},
            'check': 'live_task_origin_and_ast'}
