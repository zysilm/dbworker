"""Reject corrupted setup archives and changed provenance before native restore."""
import copy
import hashlib
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.upstream.posthog_setup import (
    SetupCacheRejected, assert_pristine_business_state, cache_key, validate_cache,
)


class PostHogSetupTests(unittest.TestCase):
    def fixture(self, directory):
        # Integrity-only test bytes; this deliberately does not claim a live
        # PostgreSQL archive or any original migration has been executed.
        directory = Path(directory)
        provenance = {'source_commit': 'pinned-source', 'upstream_uv_lock_sha256': 'locked-dependencies',
                      'python_version': '3.14.7', 'django_version': 'native-locked',
                      'postgres_version': 'PostgreSQL native-version', 'installed_apps': ['original.full.app']}
        archive = directory / 'schema.dump'
        archive.write_bytes(b'PGDMP-integrity-fixture-only')
        manifest = {'schema_version': 1, 'cache_key': cache_key(provenance), 'provenance': provenance,
                    'archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
                    'pristine_business_state': {'pending_original_migrations': 0,
                                                'selected_fixture_users': 0, 'messaging_records': 0}}
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        return provenance, manifest

    def test_exact_provenance_and_archive_checksum_are_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            provenance, manifest = self.fixture(temporary)
            self.assertEqual(validate_cache(temporary, provenance), manifest)
            for key in provenance:
                changed = {**provenance, key: ['different'] if key == 'installed_apps' else 'different'}
                with self.subTest(key=key), self.assertRaises(SetupCacheRejected):
                    validate_cache(temporary, changed)
            with (Path(temporary) / 'schema.dump').open('ab') as stream:
                stream.write(b'altered')
            with self.assertRaises(SetupCacheRejected):
                validate_cache(temporary, provenance)

    def test_incomplete_and_nonpristine_cache_never_restores(self):
        with tempfile.TemporaryDirectory() as temporary:
            provenance, manifest = self.fixture(temporary)
            for changed in ({**manifest, 'schema_version': True},
                            {**manifest, 'pristine_business_state': {'selected_fixture_users': 100}},
                            {**manifest, 'cache_key': 'incorrect'}):
                (Path(temporary) / 'manifest.json').write_text(json.dumps(changed))
                with self.subTest(changed=changed), self.assertRaises(SetupCacheRejected):
                    validate_cache(temporary, provenance)
            (Path(temporary) / 'manifest.json').unlink()
            with self.assertRaises(SetupCacheRejected):
                validate_cache(temporary, provenance)

    def test_migration_plan_and_selected_data_gate_are_fail_closed(self):
        # Exercise the gate's decisions with explicit model/executor test doubles;
        # full native migrate/restore admission is performed by the live harness.
        for pending, users, messages in ((['original-migration'], 0, 0), ([], 1, 0), ([], 0, 1), ([], 0, 0)):
            executor = types.SimpleNamespace(
                loader=types.SimpleNamespace(graph=types.SimpleNamespace(leaf_nodes=lambda: ['original-full-graph'])),
                migration_plan=lambda leaves: pending)
            user_model = types.SimpleNamespace(objects=types.SimpleNamespace(
                filter=lambda **kwargs: types.SimpleNamespace(count=lambda: users)))
            message_model = types.SimpleNamespace(objects=types.SimpleNamespace(count=lambda: messages))
            native_models = types.ModuleType('posthog.models')
            native_models.User = user_model
            native_messaging = types.ModuleType('posthog.models.messaging')
            native_messaging.MessagingRecord = message_model
            with self.subTest(pending=pending, users=users, messages=messages), \
                    patch.dict(sys.modules, {'posthog.models': native_models, 'posthog.models.messaging': native_messaging}), \
                    patch('django.db.migrations.executor.MigrationExecutor', return_value=executor):
                if pending or users or messages:
                    with self.assertRaises(SetupCacheRejected):
                        assert_pristine_business_state()
                else:
                    self.assertEqual(assert_pristine_business_state(), {
                        'pending_original_migrations': 0, 'selected_fixture_users': 0, 'messaging_records': 0})


if __name__ == '__main__':
    unittest.main()
