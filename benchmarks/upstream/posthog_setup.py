"""Cache a complete pristine native PostgreSQL schema within one benchmark run.

The cache is untimed setup only. Every arm restores into a fresh owned cluster,
checks the original migration graph and creates its own original user fixtures.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import platform
import subprocess
import sys
import uuid
from pathlib import Path


class SetupCacheRejected(ValueError):
    pass


def cache_key(provenance):
    return hashlib.sha256(json.dumps(provenance, sort_keys=True, allow_nan=False).encode()).hexdigest()


def native_provenance(source_root):
    import django
    from django.conf import settings
    source_root = Path(source_root)
    commit = subprocess.check_output(['git', '-C', str(source_root), 'rev-parse', 'HEAD'], text=True).strip()
    return {'schema_version': 1, 'source_commit': commit,
            'upstream_uv_lock_sha256': hashlib.sha256((source_root / 'uv.lock').read_bytes()).hexdigest(),
            'python_version': platform.python_version(), 'python_implementation': sys.implementation.name,
            'python_cache_tag': sys.implementation.cache_tag, 'django_version': django.get_version(),
            'postgres_version': subprocess.check_output(['postgres', '--version'], text=True).strip(),
            'pg_dump_version': subprocess.check_output(['pg_dump', '--version'], text=True).strip(),
            'installed_apps': list(settings.INSTALLED_APPS)}


def validate_cache(directory, provenance):
    directory = Path(directory)
    archive, manifest_path = directory / 'schema.dump', directory / 'manifest.json'
    if not archive.is_file() or not manifest_path.is_file():
        raise SetupCacheRejected('Incomplete native schema cache; explicit rebuild is required')
    manifest = json.loads(manifest_path.read_text())
    if (not isinstance(manifest, dict) or type(manifest.get('schema_version')) is not int
            or manifest.get('schema_version') != 1 or manifest.get('cache_key') != cache_key(provenance)
            or manifest.get('provenance') != provenance):
        raise SetupCacheRejected('Native schema cache provenance changed; explicit rebuild is required')
    if manifest.get('pristine_business_state') != {
            'pending_original_migrations': 0, 'selected_fixture_users': 0, 'messaging_records': 0}:
        raise SetupCacheRejected('Native schema cache lacks verified pristine business state')
    with archive.open('rb') as stream:
        if stream.read(5) != b'PGDMP':
            raise SetupCacheRejected('Native schema cache is not a PostgreSQL custom archive')
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    if checksum != manifest.get('archive_sha256'):
        raise SetupCacheRejected('Native schema cache checksum changed; explicit rebuild is required')
    return manifest


def assert_pristine_business_state():
    from django.db import connection
    from django.db.migrations.executor import MigrationExecutor
    from posthog.models import User
    from posthog.models.messaging import MessagingRecord
    executor = MigrationExecutor(connection)
    pending = executor.migration_plan(executor.loader.graph.leaf_nodes())
    if pending:
        raise SetupCacheRejected('Original native migration graph has unapplied migrations')
    users = User.objects.filter(email__endswith='@benchmark.invalid').count()
    messages = MessagingRecord.objects.count()
    if users or messages:
        raise SetupCacheRejected('Native schema cache contains selected business fixtures or messaging data')
    return {'pending_original_migrations': 0, 'selected_fixture_users': users, 'messaging_records': messages}


def prepare_native_schema(*, output_directory, arm_directory, source_root, pg_port, rebuild=False):
    from django.core.management import call_command
    from django.db import connections
    provenance = native_provenance(source_root)
    key = cache_key(provenance)
    root = Path(output_directory).resolve() / '.posthog-schema-cache'
    root.mkdir(exist_ok=True)
    directory = root / key
    directory.mkdir(exist_ok=True)
    archive, manifest_path = directory / 'schema.dump', directory / 'manifest.json'
    arm_directory = Path(arm_directory)
    address = ['--host', '127.0.0.1', '--port', str(pg_port), '--username', 'benchmark']
    with (root / 'cache.lock').open('a+b') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        existing = archive.exists() or manifest_path.exists()
        if existing and not rebuild:
            manifest = validate_cache(directory, provenance)
            connections.close_all()
            with (arm_directory / 'schema-restore.log').open('wb') as log:
                subprocess.run(['pg_restore', *address, '--dbname', 'notification', '--clean', '--if-exists',
                                '--single-transaction', '--exit-on-error', str(archive)],
                               stdout=log, stderr=subprocess.STDOUT, check=True)
            connections.close_all()
            state = assert_pristine_business_state()
            mode = 'native_full_schema_restore'
        else:
            # Migrate the complete unchanged app graph. No app list, table,
            # model or original migration is selected out of this setup.
            with (arm_directory / 'migrations.log').open('w') as log:
                call_command('migrate', interactive=False, stdout=log)
            state = assert_pristine_business_state()
            temporary = directory / f'schema-{uuid.uuid4().hex}.dump.tmp'
            temporary_manifest = directory / f'manifest-{uuid.uuid4().hex}.json.tmp'
            try:
                with (arm_directory / 'schema-dump.log').open('wb') as log:
                    subprocess.run(['pg_dump', *address, '--dbname', 'notification', '--format', 'custom',
                                    '--file', str(temporary)], stdout=log, stderr=subprocess.STDOUT, check=True)
                with temporary.open('rb') as stream:
                    os.fsync(stream.fileno())
                manifest = {'schema_version': 1, 'cache_key': key, 'provenance': provenance,
                            'archive_sha256': hashlib.sha256(temporary.read_bytes()).hexdigest(),
                            'pristine_business_state': state}
                with temporary_manifest.open('w') as stream:
                    json.dump(manifest, stream, sort_keys=True, allow_nan=False)
                    stream.write('\n')
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(archive)
                temporary_manifest.replace(manifest_path)
                validate_cache(directory, provenance)
            finally:
                temporary.unlink(missing_ok=True)
                temporary_manifest.unlink(missing_ok=True)
            mode = 'native_full_migrations_cache_seed' if not existing else 'native_full_migrations_explicit_rebuild'
    return {'mode': mode, 'timed': False, 'cache_scope': 'same_run_output_directory', 'cache_key': key,
            'provenance': provenance, 'archive_sha256': manifest['archive_sha256'], 'validation': state,
            'snapshot_publication': 'private setup cache excluded from publication artifacts'}
