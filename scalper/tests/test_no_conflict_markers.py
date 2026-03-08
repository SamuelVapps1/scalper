from __future__ import annotations

from pathlib import Path
import re


def test_no_merge_conflict_markers() -> None:
    root = Path(__file__).resolve().parents[2]
    bad = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"} for part in path.parts):
            continue
        if path.suffix.lower() not in {".py", ".md", ".txt", ".yml", ".yaml", ".env", ".example", ".toml"}:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if re.search(r"^(<<<<<<< |=======|>>>>>>> )", text, flags=re.MULTILINE):
            bad.append(str(path.relative_to(root)))
    assert not bad, f"Merge conflict markers found: {bad}"
