"""Tests for extraction-manifest emission in Targets3 (RGI-950).

target-s3 writes a JSON manifest at end-of-run to the S3 URI in
EXTRACTION_MANIFEST_S3_URI when it's set; the orchestrator (Airflow MDI DAG)
reads that file to learn the record count for the data_extraction_runs POST
to argo. Tests here verify aggregation, the env-var opt-in, and the
non-throwing invariant in the lifecycle hook.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from target_s3.target import (
    EXTRACTION_MANIFEST_S3_URI_ENV,
    Targets3,
)


SAMPLE_CONFIG = {
    "format": {"format_type": "json"},
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_bucket": "test-bucket",
            "aws_region": "us-east-1",
        },
    },
    "prefix": "test",
    "tenant": "3327",
}


def _make_target() -> Targets3:
    return Targets3(config=SAMPLE_CONFIG)


def _fake_sink(records_written: int) -> MagicMock:
    sink = MagicMock()
    sink._total_records_written = records_written
    return sink


def test_emit_manifest_skipped_when_env_unset(monkeypatch):
    """Without EXTRACTION_MANIFEST_S3_URI, manifest emission is a silent no-op."""
    monkeypatch.delenv(EXTRACTION_MANIFEST_S3_URI_ENV, raising=False)
    target = _make_target()
    target._sinks_active = {"events": _fake_sink(42)}

    with patch("target_s3.target.smart_open") as mock_open:
        target._emit_extraction_manifest()
        mock_open.assert_not_called()


def test_emit_manifest_writes_aggregate_to_uri(monkeypatch, tmp_path):
    """With env var set, manifest aggregates per-sink counts and writes JSON."""
    manifest_path = tmp_path / "manifest.json"
    monkeypatch.setenv(EXTRACTION_MANIFEST_S3_URI_ENV, str(manifest_path))

    target = _make_target()
    target._sinks_active = {
        "events": _fake_sink(100),
        "contacts": _fake_sink(50),
        "deals": _fake_sink(0),
    }

    target._emit_extraction_manifest()

    written = json.loads(manifest_path.read_text())
    assert written["tenant"] == "3327"
    assert written["records_extracted"] == 150
    assert written["by_stream"] == {"events": 100, "contacts": 50, "deals": 0}
    assert "started_at" in written
    assert "ended_at" in written


def test_emit_manifest_zero_records_still_emits(monkeypatch, tmp_path):
    """All-zero aggregation still emits the manifest with records_extracted=0.

    Argo's strict semantics will log the run but not bump recency — the
    manifest itself must be present so the orchestrator knows this was a
    no-record completion (vs a missing manifest = extraction crashed).
    """
    manifest_path = tmp_path / "manifest.json"
    monkeypatch.setenv(EXTRACTION_MANIFEST_S3_URI_ENV, str(manifest_path))

    target = _make_target()
    target._sinks_active = {"events": _fake_sink(0)}

    target._emit_extraction_manifest()
    written = json.loads(manifest_path.read_text())
    assert written["records_extracted"] == 0


def test_emit_manifest_treats_missing_count_attr_as_zero(monkeypatch, tmp_path):
    """Defensive: if a sink lacks _total_records_written, treat as 0."""
    manifest_path = tmp_path / "manifest.json"
    monkeypatch.setenv(EXTRACTION_MANIFEST_S3_URI_ENV, str(manifest_path))

    target = _make_target()
    sink_without_count = MagicMock(spec=[])  # spec=[] strips _total_records_written
    target._sinks_active = {"orphan": sink_without_count, "real": _fake_sink(7)}

    target._emit_extraction_manifest()
    written = json.loads(manifest_path.read_text())
    assert written["records_extracted"] == 7
    assert written["by_stream"] == {"orphan": 0, "real": 7}


def test_process_endofpipe_swallows_manifest_errors(monkeypatch):
    """Manifest emission errors must NEVER propagate out of _process_endofpipe.

    The tap+target run is already complete by the time the manifest is
    written; raising here would mark a successful extraction as failed.
    """
    monkeypatch.setenv(EXTRACTION_MANIFEST_S3_URI_ENV, "s3://broken/uri")
    target = _make_target()

    # Stub out the parent drain so we only test the manifest-emission guard.
    with patch("target_s3.target.Target._process_endofpipe"), patch.object(
        target,
        "_emit_extraction_manifest",
        side_effect=RuntimeError("simulated S3 outage"),
    ):
        # Must not raise.
        target._process_endofpipe()


def test_process_endofpipe_calls_emit_after_drain(monkeypatch):
    """Manifest emission runs after the parent drain_all completes."""
    monkeypatch.setenv(EXTRACTION_MANIFEST_S3_URI_ENV, "s3://test/manifest.json")
    target = _make_target()
    call_order: list[str] = []

    def fake_super_endofpipe(_self=None):
        call_order.append("drain")

    def fake_emit():
        call_order.append("emit")

    with patch(
        "target_s3.target.Target._process_endofpipe", side_effect=fake_super_endofpipe
    ), patch.object(target, "_emit_extraction_manifest", side_effect=fake_emit):
        target._process_endofpipe()

    assert call_order == ["drain", "emit"]
