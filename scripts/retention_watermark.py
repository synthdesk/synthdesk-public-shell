#!/usr/bin/env python3
"""
Retention Watermark

Daily, read-only scan for min/max timestamps across retained datasets.
Writes append-only retention_watermark_YYYY-MM-DD.json and optional warning
artifact if regressions are detected.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _parse_iso_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _epoch_to_utc(value: float) -> datetime:
    if value > 1e12:
        value = value / 1000.0
    return datetime.fromtimestamp(value, tz=timezone.utc)


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                yield json.loads(text)
            except json.JSONDecodeError:
                continue


def _extract_timestamp(record: Dict[str, Any]) -> Optional[datetime]:
    for key in ("ts_utc", "timestamp", "ts", "receipt_ts"):
        if key in record and record[key] is not None:
            value = record[key]
            if isinstance(value, (int, float)):
                return _epoch_to_utc(float(value))
            if isinstance(value, str):
                try:
                    return _parse_iso_utc(value)
                except ValueError:
                    return None
    for key in ("ts_ingest_ms", "ts_exchange_ms", "ts_ms"):
        if key in record and record[key] is not None:
            return _epoch_to_utc(float(record[key]))
    return None


def _utc_date_str(now: Optional[datetime] = None) -> str:
    current = now or datetime.now(timezone.utc)
    return current.strftime("%Y-%m-%d")


def _scan_files(paths: Iterable[Path]) -> Tuple[Optional[str], Optional[str], int, int]:
    min_ts: Optional[datetime] = None
    max_ts: Optional[datetime] = None
    file_count = 0
    byte_count = 0

    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        file_count += 1
        byte_count += path.stat().st_size
        for record in _iter_jsonl(path):
            ts = _extract_timestamp(record)
            if ts is None:
                continue
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

    return (
        min_ts.isoformat() if min_ts else None,
        max_ts.isoformat() if max_ts else None,
        file_count,
        byte_count,
    )


def _latest_prior_watermark(output_dir: Path, today: str) -> Optional[Path]:
    candidates: list[tuple[str, Path]] = []
    for path in output_dir.glob("retention_watermark_*.json"):
        name = path.name.replace("retention_watermark_", "").replace(".json", "")
        if name < today:
            candidates.append((name, path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def build_dataset_records(
    spine_path: Optional[Path],
    ticks_root: Optional[Path],
    soak_root: Optional[Path],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []

    if spine_path:
        min_ts, max_ts, file_count, byte_count = _scan_files([spine_path])
        records.append(
            {
                "dataset_id": "spine",
                "path": str(spine_path),
                "min_ts": min_ts,
                "max_ts": max_ts,
                "file_count": file_count,
                "byte_count": byte_count,
            }
        )

    if ticks_root:
        tick_files = sorted(ticks_root.rglob("tick_observation.jsonl"))
        min_ts, max_ts, file_count, byte_count = _scan_files(tick_files)
        records.append(
            {
                "dataset_id": "ticks",
                "path": str(ticks_root),
                "min_ts": min_ts,
                "max_ts": max_ts,
                "file_count": file_count,
                "byte_count": byte_count,
            }
        )

    if soak_root and soak_root.exists():
        soak_files = sorted(soak_root.rglob("*.jsonl"))
        min_ts, max_ts, file_count, byte_count = _scan_files(soak_files)
        records.append(
            {
                "dataset_id": "soak_artifacts",
                "path": str(soak_root),
                "min_ts": min_ts,
                "max_ts": max_ts,
                "file_count": file_count,
                "byte_count": byte_count,
            }
        )

    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily retention watermark (read-only).")
    parser.add_argument(
        "--spine-path",
        default="/root/synthdesk-listener/runs/0.2.0/event_spine.jsonl",
        help="Path to event_spine.jsonl",
    )
    parser.add_argument(
        "--ticks-root",
        default="/root/synthdesk-listener/runs/0.2.0",
        help="Root directory containing daily tick_observation.jsonl",
    )
    parser.add_argument(
        "--soak-root",
        default="/root/synthdesk-router/soak_artifacts",
        help="Root directory for soak artifacts (optional)",
    )
    parser.add_argument(
        "--output-dir",
        default="retention_watermarks",
        help="Directory to write retention watermarks",
    )
    args = parser.parse_args()

    today = _utc_date_str()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"retention_watermark_{today}.json"

    if out_path.exists():
        print(f"exists: {out_path}", file=sys.stderr)
        return 0

    spine_path = Path(args.spine_path) if args.spine_path else None
    ticks_root = Path(args.ticks_root) if args.ticks_root else None
    soak_root = Path(args.soak_root) if args.soak_root else None

    records = build_dataset_records(spine_path, ticks_root, soak_root)

    payload = {
        "date": today,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": records,
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    regressions: List[Dict[str, Any]] = []
    prior_path = _latest_prior_watermark(output_dir, today)
    if prior_path:
        prior = json.loads(prior_path.read_text(encoding="utf-8"))
        prior_by_id = {item.get("dataset_id"): item for item in prior.get("datasets", [])}
        for record in records:
            dataset_id = record.get("dataset_id")
            prev = prior_by_id.get(dataset_id, {})
            prev_max = _parse_iso(prev.get("max_ts"))
            curr_max = _parse_iso(record.get("max_ts"))
            if prev_max and curr_max and curr_max < prev_max:
                regressions.append(
                    {
                        "dataset_id": dataset_id,
                        "prev_max_ts": prev.get("max_ts"),
                        "curr_max_ts": record.get("max_ts"),
                        "prev_watermark": prior_path.name,
                    }
                )

    if regressions:
        warn_path = output_dir / f"retention_watermark_warning_{today}.json"
        if warn_path.exists():
            return 0
        warn_payload = {
            "date": today,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "regressions": regressions,
        }
        warn_path.write_text(json.dumps(warn_payload, indent=2) + "\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
