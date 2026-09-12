"""Validate the environment used by the pinned benchmark checkout."""

import os
import re
import shlex


def check_environment(text, deployment):
    assignments = {}
    for line in text.lstrip("\ufeff").splitlines():
        assignment = re.fullmatch(
            r"\s*(?:export\s+)?([\w.-]+)(?:\s*=\s*|:\s+)(.*)", line
        )
        if assignment:
            assignments[assignment[1]] = assignment[2]
    target = shlex.split(assignments.get("CONVEX_DEPLOYMENT", ""), comments=True)
    if (
        target != [f"dev:{deployment}"]
        or "CONVEX_DEPLOY_KEY" in assignments
        or os.environ.get("CONVEX_DEPLOY_KEY")
    ):
        raise RuntimeError(
            "Use the recorded dev deployment without a deploy-key override"
        )
