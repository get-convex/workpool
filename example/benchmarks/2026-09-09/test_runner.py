import contextlib
import io
import json
import os
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from environment import check_environment

HERE = Path(__file__).resolve().parent


class EnvironmentTests(unittest.TestCase):
    def test_rejects_deploy_key_assignments(self):
        with patch.dict(os.environ, {}, clear=True):
            for assignment in [
                "CONVEX_DEPLOY_KEY=test-key",
                "  CONVEX_DEPLOY_KEY = 'test-key'",
                "export CONVEX_DEPLOY_KEY = test-key",
                "\texport\tCONVEX_DEPLOY_KEY\t=\ttest-key",
                "UNRELATED=\nCONVEX_DEPLOY_KEY=",
                "CONVEX_DEPLOY_KEY: test-key",
            ]:
                with (
                    self.subTest(assignment=assignment),
                    self.assertRaises(RuntimeError),
                ):
                    check_environment(
                        "CONVEX_DEPLOYMENT=dev:target\n" + assignment, "target"
                    )
            with self.assertRaises(RuntimeError):
                check_environment(
                    "\ufeffCONVEX_DEPLOY_KEY = test-key\nCONVEX_DEPLOYMENT=dev:target",
                    "target",
                )

    def test_accepts_exported_target_and_ignores_comments(self):
        with patch.dict(os.environ, {}, clear=True):
            check_environment(
                '# CONVEX_DEPLOY_KEY = ignored\nexport CONVEX_DEPLOYMENT = "dev:target" # comment',
                "target",
            )

    def test_rejects_process_override(self):
        with (
            patch.dict(os.environ, {"CONVEX_DEPLOY_KEY": "test-key"}, clear=True),
            self.assertRaises(RuntimeError),
        ):
            check_environment("CONVEX_DEPLOYMENT=dev:target", "target")

    def test_checks_last_target_assignment_and_preserves_quoted_hash(self):
        with patch.dict(os.environ, {}, clear=True):
            for text in [
                "CONVEX_DEPLOYMENT=dev:target\nCONVEX_DEPLOYMENT=prod:other",
                'CONVEX_DEPLOYMENT="dev:target#other"',
            ]:
                with self.subTest(text=text), self.assertRaises(RuntimeError):
                    check_environment(text, "target")


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / "output"
        self.metadata = json.loads((HERE / "metadata.json").read_text())
        self.metadata["dependencies"] = {}
        for variant in self.metadata["variants"].values():
            variant["sourceHashes"] = {}
        self.metadata["validation"]["tasksPerVariant"] = 7
        self.metadata["workloads"]["mutation"].update(
            taskCount=23, batchSize=5, interBatchMs=2, maxParallelism=16
        )
        self.metadata["workloads"]["action"].update(
            taskCount=31,
            batchSize=8,
            interBatchMs=3,
            maxParallelism=12,
            taskDurationMs=45,
        )
        (self.root / ".env.local").write_text(
            f"CONVEX_DEPLOYMENT=dev:{self.metadata['deployment']}\n"
        )

    def load_runner(self):
        metadata_file = self.root / "metadata.json"
        metadata_file.write_text(json.dumps(self.metadata))
        argv = [
            str(HERE / "run.py"),
            "--checkout",
            str(self.root),
            "--output",
            str(self.output),
            "--metadata",
            str(metadata_file),
        ]
        with (
            patch.object(sys, "argv", argv),
            patch.dict(os.environ, {}, clear=True),
            patch("shutil.which", return_value="unused-node"),
            patch("subprocess.check_output", return_value="v24.0.0"),
        ):
            module = runpy.run_path(str(HERE / "run.py"))
        namespace = module["measure"].__globals__
        namespace["LOGGER"] = Mock(poll=Mock(return_value=None))
        namespace["cleanup"] = Mock()
        namespace["drain"] = Mock(return_value={"baseline": {"work": 0}})
        return namespace

    def test_metadata_controls_measured_and_smoke_arguments(self):
        runner = self.load_runner()
        for kind, phase, count in [
            ("mutation", "measured", 23),
            ("action", "measured", 31),
            ("mutation", "smoke", 7),
        ]:
            captured = []

            def invoke(cmd, *, calls=captured, task_count=count, **kwargs):
                calls.append(cmd)
                if "--inline-query" in cmd:
                    result = {
                        "run": {"pool": "baseline"},
                        "uniqueWorkIds": task_count,
                        "outcomes": {"success": task_count},
                    }
                else:
                    result = {
                        "metrics": {
                            "status": "completed",
                            "completedCount": task_count,
                            "totalDurationMs": 100,
                        },
                        "enqueueTotal": 10,
                    }
                return subprocess.CompletedProcess(cmd, 0, json.dumps(result), "")

            with (
                self.subTest(kind=kind, phase=phase),
                patch("subprocess.run", side_effect=invoke),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                runner["measure"](kind, "baseline", phase, 1)
                sent = json.loads(captured[0][-1])
                expected = self.metadata["workloads"][kind].copy()
                expected.pop("artificialWork", None)
                expected.update(
                    taskCount=count,
                    taskType=kind,
                    pool="baseline",
                    pollTimeoutMs=180_000,
                )
                self.assertEqual(sent, expected)
                self.assertIn(f".take({count + 1})", captured[1][-1])
                record = json.loads(
                    (self.output / f"{kind}-{phase}-1-baseline.json").read_text()
                )
                self.assertEqual(record["args"], expected)

    def test_rejects_inconsistent_validation_metadata(self):
        for field, value in [
            ("variants", 4),
            ("allSuccessful", False),
            ("executionErrors", 1),
        ]:
            with self.subTest(field=field):
                original = self.metadata["validation"][field]
                self.metadata["validation"][field] = value
                with self.assertRaisesRegex(RuntimeError, "Validation must cover"):
                    self.load_runner()
                self.metadata["validation"][field] = original
                self.assertFalse((self.output / "metadata.json").exists())

    def test_rejects_unsupported_mutation_work(self):
        self.metadata["workloads"]["mutation"]["artificialWork"] = 20
        with self.assertRaisesRegex(RuntimeError, "does not support artificial work"):
            self.load_runner()

    def test_rejects_invalid_counts_and_ignored_settings(self):
        for field, value in [
            ("taskCount", 0),
            ("batchSize", 0),
            ("taskCount", 1.5),
            ("taskDurationMs", 20),
        ]:
            with self.subTest(field=field, value=value):
                original = self.metadata["workloads"]["mutation"].copy()
                self.metadata["workloads"]["mutation"][field] = value
                with self.assertRaises(RuntimeError):
                    self.load_runner()
                self.metadata["workloads"]["mutation"] = original
        self.metadata["validation"]["tasksPerVariant"] = 0
        with self.assertRaisesRegex(RuntimeError, "must be a positive integer"):
            self.load_runner()

    def test_stopped_logger_prevents_calls_even_after_a_clean_exit(self):
        runner = self.load_runner()
        for exit_code in [0, 1]:
            with self.subTest(exit_code=exit_code), patch("subprocess.run") as invoke:
                runner["LOGGER"].poll.return_value = exit_code
                with self.assertRaisesRegex(RuntimeError, "Runtime logging stopped"):
                    runner["call"]("test/cleanup:start")
                invoke.assert_not_called()

    def test_logger_exit_during_a_call_prevents_recording_the_run(self):
        runner = self.load_runner()

        def invoke(cmd, **kwargs):
            runner["LOGGER"].poll.return_value = 0
            result = {
                "metrics": {
                    "status": "completed",
                    "completedCount": 7,
                    "totalDurationMs": 100,
                },
                "enqueueTotal": 10,
            }
            return subprocess.CompletedProcess(cmd, 0, json.dumps(result), "")

        with (
            patch("subprocess.run", side_effect=invoke),
            contextlib.redirect_stdout(io.StringIO()),
            self.assertRaisesRegex(RuntimeError, "Runtime logging stopped"),
        ):
            runner["measure"]("mutation", "baseline", "smoke", 1)
        self.assertTrue((self.output / "mutation-smoke-1-baseline.stdout").exists())
        self.assertFalse((self.output / "mutation-smoke-1-baseline.json").exists())
        self.assertFalse((self.output / "runs.jsonl").exists())

    def test_final_log_failure_prevents_complete(self):
        runner = self.load_runner()
        logger = runner["LOGGER"]
        runner["call"] = Mock(return_value={"work": 0})
        runner["measure"] = Mock()
        output = io.StringIO()
        with (
            patch("subprocess.Popen", return_value=logger),
            patch(
                "time.sleep",
                side_effect=lambda _: setattr(logger.poll, "return_value", 0),
            ),
            contextlib.redirect_stdout(output),
            self.assertRaisesRegex(RuntimeError, "Runtime logging stopped"),
        ):
            runner["main"]()
        self.assertNotIn("COMPLETE", output.getvalue())
        logger.terminate.assert_called_once()

    def test_log_flush_precedes_shutdown_on_success_and_failure(self):
        for failure in [None, RuntimeError("Benchmark failed"), KeyboardInterrupt()]:
            with self.subTest(failure=failure):
                runner = self.load_runner()
                logger = runner["LOGGER"]
                runner["call"] = Mock(return_value={"work": 0})
                runner["measure"] = Mock(side_effect=failure)
                output = io.StringIO()

                def flush(seconds, logger=logger, output=output):
                    self.assertEqual(seconds, 2)
                    logger.terminate.assert_not_called()
                    self.assertNotIn("COMPLETE", output.getvalue())

                with (
                    patch("subprocess.Popen", return_value=logger),
                    patch("time.sleep", side_effect=flush) as sleep,
                    contextlib.redirect_stdout(output),
                ):
                    if failure is None:
                        runner["main"]()
                    else:
                        with self.assertRaises(type(failure)) as raised:
                            runner["main"]()
                        self.assertIs(raised.exception, failure)

                sleep.assert_called_once_with(2)
                logger.terminate.assert_called_once()
                logger.wait.assert_called_once_with(timeout=10)
                self.assertEqual("COMPLETE" in output.getvalue(), failure is None)


if __name__ == "__main__":
    unittest.main()
