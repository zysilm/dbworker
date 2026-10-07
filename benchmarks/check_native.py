"""Check actual native Celery worker launch commands before provisioning suites."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from benchmarks.common.native_admission import APPLICATIONS, check_worker_source

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', action='append', choices=sorted(APPLICATIONS))
    args = parser.parse_args()
    results = {}
    for suite in args.suite or APPLICATIONS:
        source = ROOT / ('benchmarks/imagededup_benckmark/src/imagededup_benckmark/runtime.py'
                         if suite == 'imagededup' else f'benchmarks/upstream/{suite}_backend.py')
        results[suite] = check_worker_source(source.read_text(), suite)
    print(json.dumps({'passed': True, 'suites': results}, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
