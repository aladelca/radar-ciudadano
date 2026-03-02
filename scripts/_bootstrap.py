from __future__ import annotations

from pathlib import Path
import sys


def ensure_src_path() -> None:
    """Ensure local package imports work when running scripts directly."""
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if not src_dir.exists():
        return
    src_path = str(src_dir)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
