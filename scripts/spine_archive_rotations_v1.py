#!/usr/bin/env python3
"""Archive rotated event spine segments with immutable manifest + checkpoint."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SCHEMA_VERSION = "spine_archive_rotations_v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_ts(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iter_jsonl_lines(path: Path) -> Iterable[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                yield line
    else:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                yield line


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _scan_min_max_ts(path: Path) -> Tuple[Optional[str], Optional[str], int]:
    min_ts: Optional[datetime] = None
    max_ts: Optional[datetime] = None
    line_count = 0
    for line in _iter_jsonl_lines(path):
        text = line.strip()
        if not text:
            continue
        line_count += 1
        try:
            record = json.loads(text)
        except Exception:
            continue
        if not isinstance(record, dict):
            continue
        ts: Optional[datetime] = None
        for key in ("timestamp", "ts_utc", "ts", "source_ts"):
            ts = _parse_ts(record.get(key))
            if ts is not None:
                break
        if ts is None:
            continue
        if min_ts is None or ts < min_ts:
            min_ts = ts
        if max_ts is None or ts > max_ts:
            max_ts = ts
    to_iso = lambda dt: dt.isoformat().replace("+00:00", "Z") if dt else None
    return to_iso(min_ts), to_iso(max_ts), line_count


def _rotated_candidates(spine_path: Path) -> List[Path]:
    spine_real = Path(os.path.realpath(spine_path))
    base = spine_real.name
    candidates: List[Path] = []
    for entry in spine_real.parent.iterdir():
        if not entry.is_file():
            continue
        name = entry.name
        if name == base:
            continue
        if name.startswith(base + ".") or name.startswith(base + "-"):
            candidates.append(entry)
    return sorted(candidates, key=lambda item: item.name)


def _load_checkpoint(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "processed_sha256": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": SCHEMA_VERSION, "processed_sha256": {}}
    if not isinstance(data, dict):
        return {"schema_version": SCHEMA_VERSION, "processed_sha256": {}}
    if not isinstance(data.get("processed_sha256"), dict):
        data["processed_sha256"] = {}
    return data


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def run_archive(
    *,
    spine_path: Path,
    manifest_jsonl: Path,
    checkpoint_json: Path,
    archive_dir: Path,
    move: bool,
) -> Dict[str, Any]:
    candidates = _rotated_candidates(spine_path)
    checkpoint = _load_checkpoint(checkpoint_json)
    seen: Dict[str, Any] = checkpoint.get("processed_sha256", {})

    processed = 0
    moved = 0
    skipped = 0
    records: List[Dict[str, Any]] = []

    manifest_jsonl.parent.mkdir(parents=True, exist_ok=True)
    for src in candidates:
        sha = _sha256_file(src)
        if sha in seen:
            skipped += 1
            continue

        min_ts, max_ts, line_count = _scan_min_max_ts(src)
        st = src.stat()
        archived_path: Optional[Path] = None

        if move:
            archive_dir.mkdir(parents=True, exist_ok=True)
            candidate = archive_dir / src.name
            if candidate.exists():
                candidate = archive_dir / f"{src.name}.{sha[:12]}"
            src.replace(candidate)
            archived_path = candidate
            moved += 1

        record = {
            "schema_version": SCHEMA_VERSION,
            "recorded_at_utc": _now_iso(),
            "source_path": str(src),
            "archived_path": str(archived_path) if archived_path else str(src),
            "sha256": sha,
            "size_bytes": int(st.st_size),
            "min_ts_utc": min_ts,
            "max_ts_utc": max_ts,
            "line_count": int(line_count),
        }
        records.append(record)
        seen[sha] = {
            "recorded_at_utc": record["recorded_at_utc"],
            "source_path": record["source_path"],
            "archived_path": record["archived_path"],
        }
        processed += 1

    if records:
        with manifest_jsonl.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + "\n")

    checkpoint["schema_version"] = SCHEMA_VERSION
    checkpoint["processed_sha256"] = seen
    checkpoint["updated_at_utc"] = _now_iso()
    _atomic_write_json(checkpoint_json, checkpoint)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _now_iso(),
        "spine_path": str(spine_path),
        "manifest_jsonl": str(manifest_jsonl),
        "checkpoint_json": str(checkpoint_json),
        "archive_dir": str(archive_dir),
        "move": bool(move),
        "candidates_total": len(candidates),
        "processed_new": processed,
        "moved_new": moved,
        "skipped_known": skipped,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive rotated event spine segments")
    parser.add_argument("--spine-path", default="/root/synthdesk-listener/runs/0.2.0/event_spine.jsonl")
    parser.add_argument("--manifest-jsonl", default="/var/lib/synthdesk/spine_index_v0/archive_manifest_v1.jsonl")
    parser.add_argument("--checkpoint-json", default="/var/lib/synthdesk/spine_index_v0/archive_checkpoint.json")
    parser.add_argument("--archive-dir", default="/var/lib/synthdesk/spine_archive_v1/event_spine_v0")
    parser.add_argument("--move", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = run_archive(
        spine_path=Path(args.spine_path),
        manifest_jsonl=Path(args.manifest_jsonl),
        checkpoint_json=Path(args.checkpoint_json),
        archive_dir=Path(args.archive_dir),
        move=bool(args.move),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
