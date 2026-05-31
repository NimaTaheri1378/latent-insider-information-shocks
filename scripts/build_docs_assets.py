#!/usr/bin/env python3
"""Copy public artifacts into the MkDocs docs tree for GitHub Pages builds."""

from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC_ARTIFACTS = ROOT / "docs" / "artifacts"


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    if src.exists():
        shutil.copytree(src, dst)


def main() -> int:
    DOC_ARTIFACTS.mkdir(parents=True, exist_ok=True)
    copy_tree(ROOT / "artifacts" / "figures_static", DOC_ARTIFACTS / "figures_static")
    copy_tree(ROOT / "artifacts" / "figures_html", DOC_ARTIFACTS / "figures_html")
    copy_tree(ROOT / "artifacts" / "tables", DOC_ARTIFACTS / "tables")
    print(f"docs_artifacts={DOC_ARTIFACTS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
