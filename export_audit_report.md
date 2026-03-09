# Public Shell Export Audit

- generated_at_utc: `2026-03-09T15:48:16Z`
- repo_root: `/Users/lucas/dev/synthdesk`
- policy_path: `/Users/lucas/dev/synthdesk/public_export_policy.yaml`
- dry_run: `False`
- output_dir: `/private/tmp/synthdesk-public-shell-manual-ZUg3MT`
- clean_output_dir: `True`
- output_dir_cleaned: `True`
- exit_status_on_success: `0`
- summary: `summary=exported:4 blocked:1810 review_required:0 skipped:1`

## Counts

- scanned_files: `1815`
- exported_files: `4`
- blocked_files: `1810`
- review_required_files: `0`
- skipped_files: `1`

- warning: `cleaned output_dir entries=4`
- warning: `preserved output_dir entries=1 names=.git`

## Classification Semantics

- `exported`: path, metadata, transform, and content checks all passed; file was exported or would be exported
- `blocked_path`: path policy denied the file or it matched no allow or review path
- `blocked_metadata`: metadata policy blocked the file, including missing export: public on a review path
- `blocked_content`: content scan matched a deny pattern after transforms
- `review_required`: file carried explicit export: review and is reported but not exported in v1
- `skipped_unsupported`: file could not be exported safely in v1, such as binary, symlink, or unsupported transform

## exported

- `docs/public_shell_export_v1.md - export approved`
- `schemas/courts_index_v1.schema.json - export approved`
- `schemas/eval_manifest_v1.schema.json - export approved`
- `schemas/narrative_v0.schema.json - export approved`

## blocked_path

- `.claude/settings.local.json - path did not match allow or review policy`
- `.github/workflows/court-publish-guard.yml - path did not match allow or review policy`
- `.github/workflows/doctrine-gates.yml - path did not match allow or review policy`
- `.github/workflows/golden-gates.yml - path did not match allow or review policy`
- `.github/workflows/remote-deploy-attestation-checks.yml - path did not match allow or review policy`
- `.github/workflows/semantic-slo-info.yml - path did not match allow or review policy`
- `.github/workflows/stage5-determinism-gate.yml - path did not match allow or review policy`
- `.github/workflows/unit-execstart-closure-gate.yml - path did not match allow or review policy`
- `.gitignore - path did not match allow or review policy`
- `.gitmodules - path did not match allow or review policy`
- `0.7 - path did not match allow or review policy`
- `AGENTS.md - path did not match allow or review policy`
- `CONSOLIDATION_GUIDE.md - path did not match allow or review policy`
- `FLOAT_CANONICALIZATION_COMPLETE.md - path did not match allow or review policy`
- `FORENSIC_REPLAY_V1_SUMMARY.md - path did not match allow or review policy`
- `LISTENER_PURIFICATION_CHECKLIST.md - path did not match allow or review policy`
- `Makefile - path did not match allow or review policy`
- `PROMOTION_CERT_v0_2.json - path did not match allow or review policy`
- `README.md - path did not match allow or review policy`
- `RETENTION_POLICY.yaml - path did not match allow or review policy`
- `SDK_MIGRATION_COMPLETE.md - path did not match allow or review policy`
- `SHADOW_ROUTER_COMPLETE.md - path did not match allow or review policy`
- `SOAK_COMPLETE_2026-01-11.md - path did not match allow or review policy`
- `SOAK_COMPLETE_2026-01-14.md - path did not match allow or review policy`
- `SOAK_REPORT_2026-01-05.md - path did not match allow or review policy`
- `1260` more entries in `export_audit_report.json`

## blocked_metadata

- `docs/ACTIVATION_REGIME_HARVEST_v1.md - review path requires explicit export: public tag`
- `docs/ACTIVATION_RISK_DRAWDOWN_v1.md - review path requires explicit export: public tag`
- `docs/ALPHA_COURT_V1.md - review path requires explicit export: public tag`
- `docs/AMENDMENT_REGIME_HARVEST_v1.1.md - review path requires explicit export: public tag`
- `docs/AUTHORITATIVE_SURFACE_RULE.md - review path requires explicit export: public tag`
- `docs/AUTHORITY_DEMOTION_RECOVERY.md - review path requires explicit export: public tag`
- `docs/BITSTAMP_BTCUSD_SHADOW_LANE_V1.md - review path requires explicit export: public tag`
- `docs/CONFIDENCE_SHAPER_SPEC_V1.md - review path requires explicit export: public tag`
- `docs/DEBT_EVENT_TYPE_LITERALS.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_EMPIRICAL_LANGUAGE_ONLY.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_EPISTEMIC_RETIREMENT.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_EVIDENCE_NONACTIONABILITY.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_OBSERVER_DEBUG_ARTIFACTS.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_OBSERVER_IO.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_OUTCOME_LOGGING_V1.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_SPECTRAL_INVARIANTS.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_STORAGE_RETENTION.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_TEMPORAL_NONINFERENCE.md - review path requires explicit export: public tag`
- `docs/DOCTRINE_TIMESTAMP_V1.md - review path requires explicit export: public tag`
- `docs/DRV_CHECKLISTS/DRV_micro_primitives_v1.md - review path requires explicit export: public tag`
- `docs/ENVELOPE_LOSS_SPEC_v0.3.md - review path requires explicit export: public tag`
- `docs/EPOCH_ATLAS_V1.md - review path requires explicit export: public tag`
- `docs/EVIDENCE_DRAWDOWN_v1.md - review path requires explicit export: public tag`
- `docs/EVT0_POSTMORTEM.md - review path requires explicit export: public tag`
- `docs/EVT0_POST_DEPLOY_2026-01-10.md - review path requires explicit export: public tag`
- `500` more entries in `export_audit_report.json`

## skipped_unsupported

- `tests/golden/inputs/spine_epoch_20260103_6h.jsonl.zst - binary file blocked in v1`
