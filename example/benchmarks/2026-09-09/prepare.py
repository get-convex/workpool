"""Prepare the pinned three-component experiment in an isolated checkout."""

import argparse
import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path

from environment import check_environment

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
check_environment(args.env_file.read_text(), metadata["deployment"])
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
