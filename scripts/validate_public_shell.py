#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
from collections import Counter
from pathlib import Path
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_REPO_FILES = {
    ".github/workflows/ci.yml",
    "docs/public_shell_export_v1.md",
    "export_audit_report.json",
    "export_audit_report.md",
    "fixtures/jsonl_repair/escaped_newlines_expected.jsonl",
    "fixtures/jsonl_repair/escaped_newlines_input.jsonl",
    "fixtures/jsonl_repair/escaped_newlines_invalid.jsonl",
    "fixtures/schema_examples/courts_index_v1.example.json",
    "fixtures/schema_examples/eval_manifest_v1.example.json",
    "fixtures/schema_examples/narrative_v0.example.json",
    "schemas/courts_index_v1.schema.json",
    "schemas/eval_manifest_v1.schema.json",
    "schemas/narrative_v0.schema.json",
    "scripts/repair_jsonl_escaped_newlines_v1.py",
    "scripts/validate_public_shell.py",
}
EXPORTED_PUBLIC_FILES = {
    "docs/public_shell_export_v1.md",
    "schemas/courts_index_v1.schema.json",
    "schemas/eval_manifest_v1.schema.json",
    "schemas/narrative_v0.schema.json",
}
SCHEMA_EXPECTATIONS = {
    "schemas/courts_index_v1.schema.json": {"title": "courts_index_v1"},
    "schemas/eval_manifest_v1.schema.json": {"$id": "eval_manifest_v1"},
    "schemas/narrative_v0.schema.json": {"$id": "narrative_v0"},
}
AUDIT_COUNT_KEYS = [
    "scanned_files",
    "exported_files",
    "blocked_files",
    "review_required_files",
    "skipped_files",
]
AUDIT_REQUIRED_KEYS = AUDIT_COUNT_KEYS + ["files", "classification_legend", "summary_line"]
DOC_REQUIRED_PHRASES = [
    "Public Shell Export v1",
    "Classification Meanings",
    "Exit Codes",
    "Manual Mirror-Ready Flow",
]
HEX_64 = set("0123456789abcdef")


def _repo_files(root: Path) -> set[str]:
    files = set()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if rel.parts and rel.parts[0] == ".git":
            continue
        files.add(rel.as_posix())
    return files


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def _require_keys(payload: dict, keys: list[str], label: str) -> None:
    missing = [key for key in keys if key not in payload]
    _require(not missing, f"{label} is missing keys: {missing}")


def _is_hex_64(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= HEX_64


def _run_python(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )


def _validate_example_fixtures() -> int:
    fixture_root = ROOT / "fixtures" / "schema_examples"

    courts_index = json.loads((fixture_root / "courts_index_v1.example.json").read_text(encoding="utf-8"))
    _require(isinstance(courts_index, dict), "courts_index example must parse as a JSON object")
    _require_keys(courts_index, ["entries"], "courts_index example")
    _require(isinstance(courts_index["entries"], list) and len(courts_index["entries"]) == 1, "courts_index example must contain one entry")
    _require(isinstance(courts_index.get("generated_utc"), str), "courts_index example generated_utc must be a string")

    eval_manifest = json.loads((fixture_root / "eval_manifest_v1.example.json").read_text(encoding="utf-8"))
    _require(isinstance(eval_manifest, dict), "eval_manifest example must parse as a JSON object")
    _require_keys(
        eval_manifest,
        ["version", "court", "hypothesis_id", "window", "eval_tuple", "eval_id", "inputs"],
        "eval_manifest example",
    )
    _require(_is_hex_64(eval_manifest["eval_id"]), "eval_manifest example eval_id must be a 64-char lowercase hex string")
    _require_keys(eval_manifest["window_bounds"], ["start_ts_utc", "end_ts_utc"], "eval_manifest example window_bounds")
    _require_keys(
        eval_manifest["eval_tuple"],
        ["spine_hash", "feature_version", "regime_model_version", "horizon_spec"],
        "eval_manifest example eval_tuple",
    )
    _require(_is_hex_64(eval_manifest["eval_tuple"]["spine_hash"]), "eval_manifest example spine_hash must be 64-char lowercase hex")
    horizon_spec = eval_manifest["eval_tuple"]["horizon_spec"]
    _require_keys(horizon_spec, ["horizon_minutes"], "eval_manifest example horizon_spec")
    _require(isinstance(horizon_spec["horizon_minutes"], int) and horizon_spec["horizon_minutes"] >= 1, "eval_manifest example horizon_minutes must be >= 1")
    inputs = eval_manifest["inputs"]
    _require_keys(inputs, ["spine_snapshot", "spine_sha256", "spine_bytes", "tick_hashes"], "eval_manifest example inputs")
    _require(_is_hex_64(inputs["spine_sha256"]), "eval_manifest example spine_sha256 must be 64-char lowercase hex")
    _require(isinstance(inputs["spine_bytes"], int) and inputs["spine_bytes"] >= 1, "eval_manifest example spine_bytes must be >= 1")
    _require(isinstance(inputs["tick_hashes"], list) and len(inputs["tick_hashes"]) >= 1, "eval_manifest example tick_hashes must be non-empty")
    tick_hash = inputs["tick_hashes"][0]
    _require_keys(tick_hash, ["path", "sha256"], "eval_manifest example tick_hashes[0]")
    _require(_is_hex_64(tick_hash["sha256"]), "eval_manifest example tick hash must be 64-char lowercase hex")

    narrative = json.loads((fixture_root / "narrative_v0.example.json").read_text(encoding="utf-8"))
    _require(isinstance(narrative, dict), "narrative example must parse as a JSON object")
    _require_keys(
        narrative,
        ["generated_utc", "model_calibration_start", "model_calibration_end", "bocpd_run_id", "bocpd_window", "data_lag_hours", "assets"],
        "narrative example",
    )
    _require(isinstance(narrative["assets"], dict) and "BTCUSDT" in narrative["assets"], "narrative example must contain BTCUSDT asset")
    asset = narrative["assets"]["BTCUSDT"]
    _require_keys(asset, ["current_regime", "dwell", "residual_health", "bocpd_status", "expected_exits"], "narrative example asset")
    current_regime = asset["current_regime"]
    _require_keys(
        current_regime,
        ["state", "state_idx", "confidence", "confidence_window_avg", "log_rv", "vol_of_vol", "emission_mean", "emission_distance_sigma", "window_obs", "last_ts"],
        "narrative example current_regime",
    )
    _require(current_regime["state"] in {"LOW", "MID", "HIGH"}, "narrative example current_regime.state invalid")
    _require(isinstance(current_regime["emission_mean"], list) and len(current_regime["emission_mean"]) == 2, "narrative example emission_mean must have length 2")
    _require(isinstance(current_regime["emission_distance_sigma"], list) and len(current_regime["emission_distance_sigma"]) == 2, "narrative example emission_distance_sigma must have length 2")

    dwell = asset["dwell"]
    _require_keys(dwell, ["current_dwell_obs", "expected_dwell_obs", "dwell_percentile", "interpretation"], "narrative example dwell")
    _require(dwell["interpretation"] in {"early", "normal", "extended"}, "narrative example dwell.interpretation invalid")

    residual = asset["residual_health"]
    _require_keys(
        residual,
        ["mean_d2", "chi2_ref_mean", "d2_ratio", "tail_5pct", "expected_5pct", "tail_inflation", "interpretation"],
        "narrative example residual_health",
    )
    _require(residual["interpretation"] in {"well-fit", "stressed", "under-predicting"}, "narrative example residual_health.interpretation invalid")

    bocpd = asset["bocpd_status"]
    _require_keys(bocpd, ["verdict", "n_cp_episodes", "max_mass_short", "last_reset_ts", "interpretation"], "narrative example bocpd_status")
    _require(bocpd["verdict"] in {"FRESH", "BORDERLINE", "STALE", "UNKNOWN"}, "narrative example bocpd_status.verdict invalid")
    _require(bocpd["interpretation"] in {"none", "stress", "break", "unknown"}, "narrative example bocpd_status.interpretation invalid")

    exits = asset["expected_exits"]
    _require(isinstance(exits, list) and len(exits) == 1, "narrative example expected_exits must contain one entry")
    exit_entry = exits[0]
    _require_keys(
        exit_entry,
        ["to_state", "to_label", "to_state_idx", "probability", "delta_log_rv", "delta_vov", "signature"],
        "narrative example exit_entry",
    )
    _require(exit_entry["to_state"] in {"LOW", "MID", "HIGH"}, "narrative example exit_entry.to_state invalid")

    return 3


def _validate_jsonl_repair_utility() -> int:
    fixture_root = ROOT / "fixtures" / "jsonl_repair"
    script_rel = "scripts/repair_jsonl_escaped_newlines_v1.py"
    expected = (fixture_root / "escaped_newlines_expected.jsonl").read_text(encoding="utf-8")

    with tempfile.TemporaryDirectory(prefix="public-shell-jsonl-repair-") as temp_dir:
        temp_root = Path(temp_dir)

        valid_input = temp_root / "valid.jsonl"
        shutil.copy2(fixture_root / "escaped_newlines_input.jsonl", valid_input)

        dry_run = _run_python([script_rel, "--path", str(valid_input)], cwd=ROOT)
        _require(dry_run.returncode == 0, f"jsonl repair dry-run failed: {dry_run.stderr.strip()}")
        dry_payload = json.loads(dry_run.stdout)
        _require(dry_payload["status"] == "dry_run_ok", "jsonl repair dry-run status mismatch")
        _require(dry_payload["stats"]["escaped_split_lines"] == 1, "jsonl repair dry-run escaped_split_lines mismatch")
        _require(dry_payload["stats"]["written_rows"] == 3, "jsonl repair dry-run written_rows mismatch")
        _require(dry_payload["stats"]["invalid_fragments"] == 0, "jsonl repair dry-run invalid_fragments mismatch")
        _require(valid_input.read_text(encoding="utf-8") == (fixture_root / "escaped_newlines_input.jsonl").read_text(encoding="utf-8"), "jsonl repair dry-run mutated input")

        apply_run = _run_python([script_rel, "--path", str(valid_input), "--apply"], cwd=ROOT)
        _require(apply_run.returncode == 0, f"jsonl repair apply failed: {apply_run.stderr.strip()}")
        apply_payload = json.loads(apply_run.stdout)
        _require(apply_payload["status"] == "applied", "jsonl repair apply status mismatch")
        backup_path = Path(apply_payload["backup_path"])
        _require(backup_path.is_file(), "jsonl repair apply did not create backup")
        _require(valid_input.read_text(encoding="utf-8") == expected, "jsonl repair apply output mismatch")

        invalid_input = temp_root / "invalid.jsonl"
        shutil.copy2(fixture_root / "escaped_newlines_invalid.jsonl", invalid_input)
        invalid_run = _run_python([script_rel, "--path", str(invalid_input)], cwd=ROOT)
        _require(invalid_run.returncode == 2, f"jsonl repair invalid dry-run returncode mismatch: {invalid_run.returncode}")
        invalid_payload = json.loads(invalid_run.stdout)
        _require(invalid_payload["status"] == "blocked_invalid_fragments", "jsonl repair invalid status mismatch")
        _require(invalid_payload["stats"]["invalid_fragments"] >= 1, "jsonl repair invalid count mismatch")

    return 3


def main() -> int:
    repo_files = _repo_files(ROOT)
    _require(repo_files == EXPECTED_REPO_FILES, f"unexpected repo file set: {sorted(repo_files)}")

    docs_path = ROOT / "docs/public_shell_export_v1.md"
    docs_text = docs_path.read_text(encoding="utf-8")
    _require(docs_text.strip() != "", f"{docs_path} is empty")
    for phrase in DOC_REQUIRED_PHRASES:
        _require(phrase in docs_text, f"{docs_path} is missing required phrase: {phrase}")

    schema_payloads: dict[str, dict] = {}
    for rel_path, expectations in SCHEMA_EXPECTATIONS.items():
        path = ROOT / rel_path
        payload = json.loads(path.read_text(encoding="utf-8"))
        _require(isinstance(payload, dict), f"{rel_path} did not parse as a JSON object")
        _require(payload.get("type") == "object", f"{rel_path} must declare top-level type=object")
        _require(payload.get("export") == "public", f"{rel_path} must carry export=public")
        _require("$schema" in payload, f"{rel_path} is missing $schema")
        for key, expected in expectations.items():
            _require(payload.get(key) == expected, f"{rel_path} expected {key}={expected!r}")
        schema_payloads[rel_path] = payload

    audit_json_path = ROOT / "export_audit_report.json"
    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    _require(isinstance(audit, dict), "export_audit_report.json must parse as a JSON object")
    for key in AUDIT_REQUIRED_KEYS:
        _require(key in audit, f"export_audit_report.json is missing key: {key}")
    for key in AUDIT_COUNT_KEYS:
        _require(isinstance(audit[key], int), f"export_audit_report.json key {key} must be an integer")

    files = audit["files"]
    _require(isinstance(files, list), "export_audit_report.json key files must be a list")
    _require(audit["scanned_files"] == len(files), "audit scanned_files does not match files list length")

    counts = Counter(item.get("classification") for item in files)
    blocked_total = counts["blocked_path"] + counts["blocked_metadata"] + counts["blocked_content"]
    _require(audit["exported_files"] == counts["exported"], "audit exported_files count mismatch")
    _require(audit["blocked_files"] == blocked_total, "audit blocked_files count mismatch")
    _require(audit["review_required_files"] == counts["review_required"], "audit review_required_files count mismatch")
    _require(audit["skipped_files"] == counts["skipped_unsupported"], "audit skipped_files count mismatch")

    audit_exported = {
        item["relative_path"]
        for item in files
        if item.get("classification") == "exported"
    }
    _require(audit_exported == EXPORTED_PUBLIC_FILES, f"audit exported file set mismatch: {sorted(audit_exported)}")

    audit_md_path = ROOT / "export_audit_report.md"
    audit_md = audit_md_path.read_text(encoding="utf-8")
    _require(audit_md.strip() != "", f"{audit_md_path} is empty")
    _require("# Public Shell Export Audit" in audit_md, f"{audit_md_path} is missing expected title")

    fixture_count = _validate_example_fixtures()
    jsonl_repair_checks = _validate_jsonl_repair_utility()

    print(
        "validated_public_shell="
        f"repo_files:{len(repo_files)} "
        f"schemas:{len(schema_payloads)} "
        f"schema_examples:{fixture_count} "
        f"jsonl_repair_checks:{jsonl_repair_checks} "
        f"audit_exported:{len(audit_exported)} "
        f"scanned_files:{audit['scanned_files']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
