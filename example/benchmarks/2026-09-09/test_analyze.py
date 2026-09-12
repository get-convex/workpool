import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


class AnalysisTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name)
        metadata = {
            "variants": {"baseline": {"component": "pool"}},
            "measuredOrders": [],
        }
        run = {
            "label": "smoke",
            "variant": "baseline",
            "phase": "smoke",
            "workload": "mutation",
            "args": {"taskCount": 3, "batchSize": 2},
            "evidence": {"run": {"startTime": 1000}, "observedAt": 2000},
        }
        (self.output / "metadata.json").write_text(json.dumps(metadata))
        (self.output / "runs.jsonl").write_text(json.dumps(run) + "\n")
        self.events = [
            {
                "kind": "Completion",
                "executionId": str(i),
                "timestamp": 1.5,
                "executionTimestamp": 1.5,
                "componentPath": "pool",
                "identifier": "lib:enqueueBatch",
                "executionTime": 0.01,
                "usageStats": {
                    key: 1
                    for key in [
                        "databaseReadDocuments",
                        "databaseWriteDocuments",
                        "databaseReadBytes",
                        "databaseWriteBytes",
                        "databaseIoReadBytes",
                        "databaseIoWriteBytes",
                        "databaseWriteIndexRows",
                    ]
                },
            }
            for i in range(2)
        ]

    def analyze(self, events):
        (self.output / "runtime.jsonl").write_text(
            "".join(json.dumps(e) + "\n" for e in events)
        )
        return subprocess.run(
            [sys.executable, str(HERE / "analyze.py"), str(self.output)],
            text=True,
            capture_output=True,
            check=False,
        )

    def test_accepts_partial_final_batch_and_deduplicates_logs(self):
        result = self.analyze(self.events + self.events)
        self.assertEqual(result.returncode, 0, result.stderr)
        analysis = json.loads((self.output / "analysis.json").read_text())
        self.assertEqual(analysis["runs"][0]["runtime"]["enqueue"]["executions"], 2)

    def test_rejects_missing_logs_and_removes_stale_analysis(self):
        for count in [0, 1]:
            with self.subTest(count=count):
                (self.output / "analysis.json").write_text("{}")
                result = self.analyze(self.events[:count])
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"expected 2 lib:enqueueBatch completions, found {count}",
                    result.stderr,
                )
                self.assertFalse((self.output / "analysis.json").exists())

    def test_rejects_empty_run_list(self):
        (self.output / "runs.jsonl").write_text("")
        result = self.analyze([])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("No completed benchmark runs", result.stderr)


if __name__ == "__main__":
    unittest.main()
