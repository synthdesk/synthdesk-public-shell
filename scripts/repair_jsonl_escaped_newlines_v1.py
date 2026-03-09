#!/usr/bin/env python3
"""
repair_jsonl_escaped_newlines_v1

Repairs JSONL files containing literal "\\n" separators embedded inside lines.
Useful for recovering from accidental writes that joined multiple JSON objects
into one physical line with escaped newline text.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "repair_jsonl_escaped_newlines_v1"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair JSONL lines containing literal \\\\n separators")
    parser.add_argument("--path", required=True, help="Target JSONL path")
    parser.add_argument("--apply", action="store_true", help="Apply in-place repair (default is dry-run)")
    parser.add_argument(
        "--backup-path",
        default=None,
        help="Optional explicit backup path (default: <path>.bak.<UTCSTAMP>)",
    )
    parser.add_argument(
        "--allow-drop-invalid",
        action="store_true",
        help="Allow dropping invalid fragments; default fail-closed when invalid fragments are present",
    )
    return parser.parse_args()


def _safe_load_json(fragment: str) -> Any:
    return json.loads(fragment)


def build_cleaned_file(src: Path, dst: Path) -> dict[str, Any]:
    line_count = 0
    escaped_split_lines = 0
    fragment_count = 0
    written_rows = 0
    invalid_fragments = 0
    invalid_examples: list[dict[str, Any]] = []

    with src.open("r", encoding="utf-8", errors="replace") as handle, dst.open("w", encoding="utf-8") as out:
        for line_no, line in enumerate(handle, start=1):
            line_count += 1
            parts = line.rstrip("\n").split("\\n")
            if len(parts) > 1:
                escaped_split_lines += 1
            for part_idx, part in enumerate(parts):
                fragment = part.strip()
                if not fragment:
                    continue
                fragment_count += 1
                try:
                    obj = _safe_load_json(fragment)
                except Exception as exc:
                    invalid_fragments += 1
                    if len(invalid_examples) < 5:
                        invalid_examples.append(
                            {
                                "line_no": line_no,
                                "part_idx": part_idx,
                                "error": str(exc),
                                "fragment_preview": fragment[:200],
                            }
                        )
                    continue
                out.write(json.dumps(obj, sort_keys=True) + "\n")
                written_rows += 1

    return {
        "line_count": line_count,
        "escaped_split_lines": escaped_split_lines,
        "fragment_count": fragment_count,
        "written_rows": written_rows,
        "invalid_fragments": invalid_fragments,
        "invalid_examples": invalid_examples,
    }


def main() -> int:
    args = parse_args()
    now = utcnow()

    src = Path(args.path)
    if not src.exists():
        raise RuntimeError(f"path not found: {src}")

    tmp_clean = src.with_suffix(src.suffix + ".cleaned.tmp")
    stats = build_cleaned_file(src, tmp_clean)

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": iso(now),
        "path": str(src),
        "apply": bool(args.apply),
        "stats": stats,
        "status": "ok",
    }

    if stats["invalid_fragments"] > 0 and not args.allow_drop_invalid:
        tmp_clean.unlink(missing_ok=True)
        result["status"] = "blocked_invalid_fragments"
        print(json.dumps(result, indent=2, sort_keys=True))
        return 2

    if not args.apply:
        tmp_clean.unlink(missing_ok=True)
        result["status"] = "dry_run_ok"
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    backup_path = Path(args.backup_path) if args.backup_path else src.with_name(src.name + f".bak.{stamp}")
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, backup_path)

    final_clean = src.with_suffix(src.suffix + ".cleaned")
    os.replace(tmp_clean, final_clean)
    os.replace(final_clean, src)

    result["backup_path"] = str(backup_path)
    result["status"] = "applied"
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
