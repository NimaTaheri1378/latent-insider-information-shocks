#!/usr/bin/env python3
"""Fail if obvious secrets or proprietary artifacts are staged/tracked."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|password|secret|token)\s*[:=]\s*['\"]?[A-Za-z0-9_\-]{12,}"),
    re.compile(r"wrds-pgdata\.wharton\.upenn\.edu:9737:wrds:"),
]

BLOCKED_SUFFIXES = {".parquet", ".feather", ".h5", ".hdf5", ".pkl", ".pickle", ".sqlite", ".db"}
BLOCKED_DIR_PARTS = {
    "artifacts/raw/",
    "artifacts/processed/",
    "artifacts/models/",
    "data/",
    "logs/",
    "manifests/",
}


def candidate_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [Path(line) for line in result.stdout.splitlines() if line.strip()]


def main() -> int:
    failures: list[str] = []
    for path in candidate_files():
        norm = str(path).replace("\\", "/")
        if path.suffix.lower() in BLOCKED_SUFFIXES:
            failures.append(f"blocked tracked data file: {path}")
        if any(norm.startswith(part) or f"/{part}" in norm for part in BLOCKED_DIR_PARTS):
            failures.append(f"blocked tracked runtime path: {path}")
        if path.exists() and path.is_file() and path.stat().st_size < 2_000_000:
            text = path.read_text(encoding="utf-8", errors="ignore")
            for pattern in SECRET_PATTERNS:
                if pattern.search(text):
                    failures.append(f"secret-like pattern in {path}")
                    break
    if failures:
        print("\n".join(failures))
        return 1
    print("public_safety_scan_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
