"""Ensure the native result cache retains large batches until native retrieval."""
import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


class SupersetResultCapacityTests(unittest.TestCase):
    def test_generated_configuration_preserves_native_backend_and_default_timeout(self):
        source = Path("benchmarks/upstream/superset_backend.py").read_text()
        tree = ast.parse(source)
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "write_configuration")
        timeout_setting = next(node for node in tree.body if isinstance(node, ast.Assign)
                               and any(isinstance(target, ast.Name) and target.id == "METADATA_SQLITE_BUSY_TIMEOUT_SECONDS"
                                       for target in node.targets))
        namespace = {"ROOT": Path('/isolated/repository'), "os": __import__('os'),
                     "METADATA_SQLITE_BUSY_TIMEOUT_SECONDS": ast.literal_eval(timeout_setting.value)}
        exec(compile(ast.Module(body=[function], type_ignores=[]), '<configuration factory>', 'exec'), namespace)
        class Cache:
            def __init__(self, path, threshold=500, default_timeout=300):
                self.path, self.threshold, self.default_timeout = path, threshold, default_timeout
        upstream_source = 'RESULTS_BACKEND = FileSystemCache("/app/superset_home/sqllab")\n'
        with tempfile.TemporaryDirectory() as temporary, patch.dict('os.environ', {}, clear=True):
            directory = Path(temporary)
            config_path = directory / 'superset_config.py'
            namespace['write_configuration'](config_path, directory, 1234, results_capacity=1010)
            # Evaluate generated settings against the unchanged native cache
            # constructor boundary, without provisioning Superset locally.
            generated = config_path.read_text()
            with patch.object(Path, 'read_text', return_value=upstream_source):
                settings = {'FileSystemCache': Cache}
                exec(compile(generated, str(config_path), 'exec'), settings)
            backend = settings['RESULTS_BACKEND']
            self.assertIsInstance(backend, Cache)
            self.assertEqual(backend.threshold, 1010)
            self.assertEqual(backend.default_timeout, 300)
            self.assertEqual(backend.path, str(directory / 'sql_lab_results'))
            # Both arms and their native workers consume this one generated
            # configuration. Exercise its actual SQLAlchemy connection options.
            from sqlalchemy import create_engine
            engine = create_engine(settings['SQLALCHEMY_DATABASE_URI'])
            try:
                with engine.connect() as connection:
                    self.assertEqual(connection.exec_driver_sql('PRAGMA busy_timeout').scalar(), 30000)
                    self.assertEqual(connection.exec_driver_sql('PRAGMA journal_mode').scalar(), 'delete')
            finally:
                engine.dispose()

    def test_capacity_budget_contains_every_request_and_warmup(self):
        source = Path('benchmarks/upstream/superset_backend.py').read_text()
        self.assertIn('results_capacity = profile["requests"] + 2 + 8', source)
        self.assertIn('"results_cache_capacity": results_capacity', source)
