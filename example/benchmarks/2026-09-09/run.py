"""Serial, balanced measurements of three pinned workpool component mounts."""

import argparse
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--checkout", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument(
    "--metadata", type=Path, default=Path(__file__).with_name("metadata.json")
)
parser.add_argument("--smoke", action="store_true")
ARGS = parser.parse_args()
OUT = ARGS.output.resolve()
OUT.mkdir(parents=True, exist_ok=True)
STAGE = ARGS.checkout.resolve()
META = json.loads(ARGS.metadata.read_text())
NODE = shutil.which("node")
if (
    not NODE
    or int(
        subprocess.check_output([NODE, "--version"], text=True)
        .strip()
        .lstrip("v")
        .split(".")[0]
    )
    < 24
):
    raise RuntimeError("Run with Node 24 or later on PATH")
CLI = STAGE / "node_modules/convex/dist/cli.bundle.cjs"
DEPLOYMENT = META["deployment"]
VARIANTS = META["variants"]
if (
    os.environ.get("CONVEX_DEPLOY_KEY")
    or "CONVEX_DEPLOY_KEY=" in (STAGE / ".env.local").read_text()
):
    raise RuntimeError(
        "Remove the deploy-key override before using the recorded dev target"
    )
for name, info in VARIANTS.items():
    source = STAGE / "benchmark-components" / name / "src/component"
    for path, expected in info["sourceHashes"].items():
        if hashlib.sha256((source / path).read_bytes()).hexdigest() != expected:
            raise RuntimeError(
                f"Component source differs from the recorded revision: {name}/{path}"
            )
for name, expected in META["dependencies"].items():
    if (
        json.loads((STAGE / "node_modules" / name / "package.json").read_text())[
            "version"
        ]
        != expected
    ):
        raise RuntimeError(f"Dependency differs from the recorded version: {name}")
if (OUT / "metadata.json").exists() and json.loads(
    (OUT / "metadata.json").read_text()
) != META:
    raise RuntimeError(
        "Output directory belongs to a different benchmark configuration"
    )
(OUT / "metadata.json").write_text(json.dumps(META, indent=2) + "\n")
COUNT_QUERY = """const counts = {};
for (const table of ["work", "pendingStart", "pendingCompletion", "pendingCancelation", "payload"])
  counts[table] = await ctx.db.query(table).count();
const state = await ctx.db.query("internalState").first();
counts.running = state?.running.length ?? 0;
return counts;"""


def call(
    function=None,
    args=None,
    *,
    component=None,
    query=None,
    label=None,
    allow_empty=False,
):
    cmd = [NODE, str(CLI), "run", "--deployment", DEPLOYMENT]
    if component:
        cmd += ["--component", component]
    cmd += ["--inline-query", query] if query else [function, json.dumps(args or {})]
    result = subprocess.run(
        cmd, cwd=STAGE, text=True, capture_output=True, timeout=660, check=False
    )
    if label:
        (OUT / f"{label}.stdout").write_text(result.stdout)
        (OUT / f"{label}.stderr").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(f"{function or component}: {result.stderr[-3000:]}")
    if not result.stdout.strip():
        if allow_empty:
            return None
        raise RuntimeError(f"Missing response from {function or component}")
    return json.loads(result.stdout)


def drain():
    deadline = time.monotonic() + 180
    while True:
        counts = {
            name: call(component=info["component"], query=COUNT_QUERY)
            for name, info in VARIANTS.items()
        }
        if all(not any(c.values()) for c in counts.values()):
            return counts
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Components did not drain: {counts}")
        time.sleep(1)


def cleanup():
    drain()
    call("test/cleanup:start", {"limit": 1000}, allow_empty=True)
    deadline = time.monotonic() + 180
    while True:
        counts = call("test/cleanup:counts")
        if not any(counts.values()):
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Bookkeeping did not clear: {counts}")
        time.sleep(1)


def measure(kind, variant, phase, rep, task_count):
    label = f"{kind}-{phase}-{rep}-{variant}"
    if (OUT / f"{label}.json").exists():
        print(f"ALREADY RECORDED {label}", flush=True)
        return
    cleanup()
    args = {
        "taskCount": task_count,
        "batchSize": 100,
        "interBatchMs": 50,
        "maxParallelism": 200,
        "taskType": kind,
        "pollTimeoutMs": 180_000,
        "pool": variant,
    }
    if kind == "action":
        args["taskDurationMs"] = 20
    print(f"START {label} target=dev:{DEPLOYMENT}", flush=True)
    began = datetime.datetime.now(datetime.timezone.utc).isoformat()
    result = call("test/scenarios/throughput:default", args, label=label)
    metrics = result.get("metrics") or {}
    if (
        result.get("timedOut")
        or metrics.get("status") != "completed"
        or metrics.get("completedCount") != task_count
    ):
        raise RuntimeError(f"Invalid completion: {label}: {result}")
    evidence = call(
        query="""const run = await ctx.db.query("runs").order("desc").first();
const tasks = await ctx.db.query("tasks").withIndex("runId", q => q.eq("runId", run._id)).take(6001);
const outcomes = {}; for (const t of tasks) outcomes[t.resultKind ?? "unknown"] = (outcomes[t.resultKind ?? "unknown"] ?? 0) + 1;
return {run, observedAt: Date.now(), outcomes, uniqueWorkIds: new Set(tasks.map(t => t.workId)).size};""",
        label=f"{label}-evidence",
    )
    if (
        evidence["run"]["pool"] != variant
        or evidence["uniqueWorkIds"] != task_count
        or evidence["outcomes"] != {"success": task_count}
    ):
        raise RuntimeError(f"Invalid outcomes: {label}: {evidence}")
    drained = drain()
    record = {
        "label": label,
        "phase": phase,
        "rep": rep,
        "variant": variant,
        "workload": kind,
        "began": began,
        "args": args,
        "result": result,
        "evidence": evidence,
        "drained": drained,
    }
    (OUT / f"{label}.json").write_text(json.dumps(record, indent=2) + "\n")
    with (OUT / "runs.jsonl").open("a") as f:
        f.write(json.dumps(record) + "\n")
    print(
        json.dumps(
            {
                "label": label,
                "tps": round(task_count * 1000 / metrics["totalDurationMs"], 2),
                "enqueueMs": result["enqueueTotal"],
                "latency": metrics.get("latency"),
            }
        ),
        flush=True,
    )


def main():
    logs = (OUT / "runtime.jsonl").open("a")
    errors = (OUT / "runtime.stderr").open("a")
    logger = subprocess.Popen(
        [
            NODE,
            str(CLI),
            "logs",
            "--deployment",
            DEPLOYMENT,
            "--success",
            "--jsonl",
            "--history",
            "0",
        ],
        cwd=STAGE,
        stdout=logs,
        stderr=errors,
        start_new_session=True,
    )
    try:
        for component in ["testWorkpool", "oldWorkpool"]:
            counts = call(component=component, query=COUNT_QUERY)
            if any(counts.values()):
                raise RuntimeError(f"Unrelated pool is active: {component}: {counts}")
        for variant, info in VARIANTS.items():
            call(
                "config:update",
                {"maxParallelism": 200, "logLevel": "REPORT"},
                component=info["component"],
                allow_empty=True,
            )
        for variant in VARIANTS:
            measure("mutation", variant, "smoke", 1, 1000)
        if not ARGS.smoke:
            for kind in ["mutation", "action"]:
                for rep, order in enumerate(META["warmupOrders"], 1):
                    for variant in order:
                        measure(kind, variant, "warmup", rep, 5000)
                for rep, order in enumerate(META["measuredOrders"], 1):
                    for variant in order:
                        measure(kind, variant, "measured", rep, 5000)
        cleanup()
        print("COMPLETE", flush=True)
    finally:
        time.sleep(2)
        logger.terminate()
        try:
            logger.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.kill()
            logger.wait()
        logs.close()
        errors.close()


if __name__ == "__main__":
    main()
