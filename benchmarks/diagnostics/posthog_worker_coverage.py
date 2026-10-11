"""Observe warmup worker coverage in an already admitted PostHog trace.

This is diagnostic evidence, not formal benchmark admission. Successful native
warmup executions demonstrate initialization in the observed process. Historical
PID coverage does not establish simultaneous liveness after pool recycling.
"""
import argparse
import json
from pathlib import Path


def warmup_readiness(events, expected_workers=8):
    if type(expected_workers) is not int or expected_workers < 1:
        raise ValueError("expected_workers must be a positive integer")
    measured = [row for row in events if str(row.get("operation_id", "")).startswith("notification-")
                and row.get("event") == "submitted"]
    if not measured:
        return {"status": "insufficient", "reason": "No measured publication boundary"}
    boundary = min(row["timestamp_ns"] for row in measured)
    warm = [row for row in events if str(row.get("operation_id", "")).startswith("warmup:notification-")
            and row.get("event") in ("started", "succeeded")]
    if not warm:
        return {"status": "insufficient", "reason": "No successful warmup execution evidence"}
    if any("process_id" not in row for row in warm):
        return {"status": "historicalunavailable", "reason": "Warmup receipts predate process identity observation",
                "measurement_start_ns": boundary}
    measured_started = [row for row in events
                        if str(row.get("operation_id", "")).startswith("notification-")
                        and row.get("event") == "started"]
    measured_identity_complete = bool(measured_started) and all(type(row.get("process_id")) is int and row["process_id"] > 0
                                     for row in measured_started)
    measured_pids = {row["process_id"] for row in measured_started
                     if type(row.get("process_id")) is int and row["process_id"] > 0}
    phases = {}
    for row in warm:
        phases.setdefault(row["node_id"], {}).setdefault(row["event"], []).append(row)
    proven = {}
    for node, node_phases in phases.items():
        if any(len(node_phases.get(phase, [])) != 1 for phase in ("started", "succeeded")):
            continue
        start, success = node_phases["started"][0], node_phases["succeeded"][0]
        pid = start["process_id"]
        if (type(pid) is not int or pid <= 0 or success["process_id"] != pid
                or not start["timestamp_ns"] <= success["timestamp_ns"] < boundary
                or not start.get("details", {}).get("native_worker_origin")):
            continue
        proven.setdefault(pid, []).append({"node_id": node, "initialized_by_ns": start["timestamp_ns"],
                                          "warmup_completed_ns": success["timestamp_ns"]})
    count = len(proven)
    missing_pids = sorted(measured_pids - set(proven))
    verified = count == expected_workers and measured_identity_complete and not missing_pids
    missing_evidence = [{"process_id": pid,
                         "first_measured_start_ns": min(row["timestamp_ns"] for row in measured_started
                                                        if row.get("process_id") == pid),
                         "measured_tasks_started": sum(row.get("process_id") == pid for row in measured_started)}
                        for pid in missing_pids]
    return {"status": "verified" if verified else "insufficient",
            "scope": "Distinct native workers completed warmup before measured publication; assumes no pool recycling",
            "measurement_start_ns": boundary, "expected_workers": expected_workers,
            "verified_workers": count, "worker_evidence": proven,
            "measured_worker_ids": sorted(measured_pids),
            "measured_identity_complete": measured_identity_complete,
            "measured_workers_missing_warm_proof": missing_evidence,
            "reason": "Complete observed pool and measured-worker coverage" if verified else
                      "Observed warmup coverage does not establish the configured live pool"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--workers", type=int, required=True)
    args = parser.parse_args()
    samples = []
    for path in sorted(args.directory.glob("posthog-*/workflow.jsonl")):
        events = [json.loads(line) for line in path.read_text().splitlines()]
        sample_path = path.parent / "sample.json"
        sample = json.loads(sample_path.read_text()) if sample_path.exists() else None
        samples.append({"sample": path.parent.name, "complete_sample": sample is not None,
                        "configured_concurrency": sample.get("configuration", {}).get("concurrency") if sample else None,
                        "warmup_readiness": warmup_readiness(events, args.workers)})
    report = {"schema_version": 1, "expected_workers": args.workers,
              "scope": "Observed process coverage only; native workload admission remains separate",
              "samples": samples}
    (args.directory / "worker-coverage.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
