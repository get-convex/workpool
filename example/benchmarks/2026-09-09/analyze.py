"""Summarize per-run timing and runtime resource counters without double counting nested calls."""

import argparse
import collections
import json
import statistics
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path, help="Directory produced by run.py")
OUT = parser.parse_args().output
metadata = json.loads((OUT / "metadata.json").read_text())
runs = [json.loads(line) for line in (OUT / "runs.jsonl").read_text().splitlines()]
windows = collections.defaultdict(list)
for run in runs:
    run["runtime"] = {}
    component = metadata["variants"][run["variant"]]["component"]
    windows[component].append(run)

errors = []
all_errors = []
seen = set()
for line in (OUT / "runtime.jsonl").open():
    event = json.loads(line)
    if event.get("kind") != "Completion":
        continue
    key = (event.get("executionId"), event.get("timestamp"))
    if key in seen:
        continue
    seen.add(key)
    if event.get("error"):
        all_errors.append(
            {
                key: event.get(key)
                for key in [
                    "componentPath",
                    "identifier",
                    "executionTimestamp",
                    "error",
                    "occInfo",
                    "willRetry",
                ]
            }
        )
    component = event.get("componentPath") or ""
    root_component = component.split("/")[0]
    for run in windows.get(root_component, []):
        start = run["evidence"]["run"]["startTime"]
        end = run["evidence"]["observedAt"]
        if not start <= event.get("executionTimestamp", 0) * 1000 <= end:
            continue
        if event.get("error"):
            errors.append(
                {
                    "label": run["label"],
                    "component": component,
                    "identifier": event["identifier"],
                    "error": event["error"],
                    "occInfo": event.get("occInfo"),
                    "willRetry": event.get("willRetry"),
                }
            )
        group = None
        if (
            component == root_component + "/batchWorker"
            and event["identifier"] == "loop:loop"
        ):
            group = "loop"
        elif component == root_component and event["identifier"] == "lib:enqueueBatch":
            group = "enqueue"
        if group is None:
            break
        stats = run["runtime"].setdefault(
            group,
            {
                "executions": 0,
                "executionMs": [],
                "userExecutionMs": [],
                "readDocuments": 0,
                "writeDocuments": 0,
                "readBytes": 0,
                "writeBytes": 0,
                "ioReadBytes": 0,
                "ioWriteBytes": 0,
                "writeIndexRows": 0,
                "occExecutions": 0,
            },
        )
        stats["executions"] += 1
        stats["executionMs"].append(event["executionTime"] * 1000)
        stats["userExecutionMs"].append(event.get("userExecutionTime", 0) * 1000)
        if event.get("occInfo"):
            stats["occExecutions"] += 1
        usage = event["usageStats"]
        for target, source in [
            ("readDocuments", "databaseReadDocuments"),
            ("writeDocuments", "databaseWriteDocuments"),
            ("readBytes", "databaseReadBytes"),
            ("writeBytes", "databaseWriteBytes"),
            ("ioReadBytes", "databaseIoReadBytes"),
            ("ioWriteBytes", "databaseIoWriteBytes"),
            ("writeIndexRows", "databaseWriteIndexRows"),
        ]:
            stats[target] += usage[source]
        break

summary = {}
for workload in ["mutation", "action"]:
    summary[workload] = {}
    for variant in metadata["variants"]:
        selected = [
            r
            for r in runs
            if r["phase"] == "measured"
            and r["workload"] == workload
            and r["variant"] == variant
        ]
        if not selected:
            continue
        rates = [
            r["args"]["taskCount"] * 1000 / r["result"]["metrics"]["totalDurationMs"]
            for r in selected
        ]
        summary[workload][variant] = {
            "n": len(selected),
            "throughputMedian": statistics.median(rates),
            "throughputRange": [min(rates), max(rates)],
            "enqueueMsMedian": statistics.median(
                r["result"]["enqueueTotal"] for r in selected
            ),
            "p95MsMedian": statistics.median(
                r["result"]["metrics"]["latency"]["p95"] for r in selected
            ),
        }

comparisons = []
for workload in ["mutation", "action"]:
    for rep in [1, 2, 3]:
        group = {
            r["variant"]: r
            for r in runs
            if r["phase"] == "measured"
            and r["workload"] == workload
            and r["rep"] == rep
        }
        if len(group) != 3:
            continue
        for before, after in [("baseline", "ordering"), ("ordering", "packed")]:
            a, b = group[before], group[after]
            comparisons.append(
                {
                    "workload": workload,
                    "rep": rep,
                    "before": before,
                    "after": after,
                    "throughputRatio": a["result"]["metrics"]["totalDurationMs"]
                    / b["result"]["metrics"]["totalDurationMs"],
                    "enqueueRatio": b["result"]["enqueueTotal"]
                    / a["result"]["enqueueTotal"],
                }
            )

result = {
    "metadata": metadata,
    "summary": summary,
    "comparisons": comparisons,
    "executionErrors": errors,
    "allExecutionErrors": all_errors,
    "runs": runs,
}
incident = OUT / "cleanup-incident.json"
if incident.exists():
    result["cleanupIncident"] = json.loads(incident.read_text())
(OUT / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
print(
    json.dumps(
        {
            "summary": summary,
            "comparisons": comparisons,
            "executionErrors": errors,
            "allExecutionErrors": all_errors,
        },
        indent=2,
    )
)
