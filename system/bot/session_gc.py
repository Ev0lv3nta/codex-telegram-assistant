from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
import time


SESSION_ID_RE = re.compile(
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GcResult:
    deleted_files: int
    kept_files: int
    skipped_files: int
    errors: int


def _extract_session_id(path: Path) -> str:
    match = None
    for match in SESSION_ID_RE.finditer(path.name):
        pass
    return (match.group(1) if match else "").lower()


def gc_sessions(
    sessions_dir: Path,
    keep_session_ids: set[str],
    older_than_days: int,
) -> GcResult:
    keep = {s.lower() for s in keep_session_ids if s}
    cutoff = time.time() - max(0, older_than_days) * 86400

    deleted = 0
    kept = 0
    skipped = 0
    errors = 0

    if not sessions_dir.exists():
        return GcResult(0, 0, 0, 0)

    for path in sessions_dir.rglob("rollout-*.jsonl"):
        if not path.is_file():
            continue
        try:
            st = path.stat()
        except OSError:
            errors += 1
            continue

        if st.st_mtime >= cutoff:
            skipped += 1
            continue

        session_id = _extract_session_id(path)
        if session_id and session_id in keep:
            kept += 1
            continue

        try:
            path.unlink()
            deleted += 1
        except OSError:
            errors += 1

    # Best-effort: remove empty directories under sessions_dir.
    for dirpath, dirnames, filenames in os.walk(str(sessions_dir), topdown=False):
        if dirpath == str(sessions_dir):
            continue
        if not dirnames and not filenames:
            try:
                Path(dirpath).rmdir()
            except OSError:
                pass

    return GcResult(deleted, kept, skipped, errors)

