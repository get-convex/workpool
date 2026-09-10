"""Prepare the pinned three-component experiment in an isolated checkout."""

import argparse
import io
import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--checkout", type=Path, required=True)
parser.add_argument("--env-file", type=Path, required=True)
args = parser.parse_args()
root = Path(
    subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
)
record = Path(__file__).resolve().parent
metadata = json.loads((record / "metadata.json").read_text())
stage = args.checkout.resolve()
if stage.exists():
    raise RuntimeError(f"Checkout already exists: {stage}")
env = args.env_file.read_text()
deployment = next(
    (
        line.split("=", 1)[1].split("#", 1)[0].strip().strip("\"'")
        for line in env.splitlines()
        if line.startswith("CONVEX_DEPLOYMENT=")
    ),
    None,
)
if (
    deployment != f"dev:{metadata['deployment']}"
    or "CONVEX_DEPLOY_KEY=" in env
    or os.environ.get("CONVEX_DEPLOY_KEY")
):
    raise RuntimeError("Use the recorded dev deployment without a deploy-key override")
for name, version in metadata["dependencies"].items():
    installed = json.loads((root / "node_modules" / name / "package.json").read_text())[
        "version"
    ]
    if installed != version:
        raise RuntimeError(f"{name}: expected {version}, installed {installed}")
stage.mkdir(parents=True)


def extract(revision, destination, *paths):
    contents = subprocess.check_output(["git", "archive", revision, *paths], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(contents)) as archive:
        archive.extractall(destination, filter="data")


extract(metadata["harnessRevision"], stage)
(stage / "node_modules").symlink_to(root / "node_modules", target_is_directory=True)
shutil.copy2(args.env_file, stage / ".env.local")
for name, variant in metadata["variants"].items():
    destination = stage / "benchmark-components" / name
    destination.mkdir(parents=True)
    extract(variant["revision"], destination, "src/component")
subprocess.run(
    ["patch", "-p1", "-i", str(record / "mounts.patch")], cwd=stage, check=True
)
print(
    f"Prepared {stage}; target dev:{metadata['deployment']}. Build and deploy before running."
)
