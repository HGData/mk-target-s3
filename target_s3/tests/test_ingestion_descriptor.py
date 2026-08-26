"""Tests for Iceberg ingestion descriptor emission in Targets3.

target-s3 writes a JSON descriptor at end-of-run to the S3 URI in INGESTION_DESCRIPTOR_S3_URI when
it's set; the iceberg ingestion DAG reads it to learn which streams landed, their dt partitions and
locations, and the schema each carried. It carries no service/tenant/connector — the DAG supplies
those. Tests cover the shape, the key parsing, the env-var opt-in, and the non-throwing invariant.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from target_s3.target import (
    INGESTION_DESCRIPTOR_S3_URI_ENV,
    Targets3,
    _parse_dt_partition,
)

SAMPLE_CONFIG = {
    "format": {"format_type": "jsonl"},
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_bucket": "test-bucket",
            "aws_region": "us-east-1",
        },
    },
    "prefix": "data/tenants/objects",
    "tenant": "3327",
}

_ROOT = "test-bucket/data/tenants/objects/3327_Account/source_system=salesforce/tenant=3327"
_SCHEMA = {"type": "object", "properties": {"id": {"type": ["null", "string"]}}}


def _make_target() -> Targets3:
    return Targets3(config=SAMPLE_CONFIG)


def _fake_sink(schema: dict, keys: list[str]) -> MagicMock:
    sink = MagicMock()
    sink.schema = schema
    sink._written_keys = set(keys)
    return sink


class TestParseDtPartition:
    def test_splits_root_dt_and_location(self) -> None:
        """A key with a dt= segment yields table_root, dt value, and the partition location."""
        tr, dt, loc = _parse_dt_partition(f"{_ROOT}/dt=2026-08-26-14-15/f.jsonl.gz")
        assert tr == f"s3://{_ROOT}/"
        assert dt == "2026-08-26-14-15"
        assert loc == f"s3://{_ROOT}/dt=2026-08-26-14-15/"

    def test_key_without_dt_returns_none(self) -> None:
        """A key with no dt= partition cannot be described and is skipped."""
        assert _parse_dt_partition(f"{_ROOT}/f.jsonl.gz") is None


def test_emit_descriptor_skipped_when_env_unset(monkeypatch):
    """Without INGESTION_DESCRIPTOR_S3_URI, descriptor emission is a silent no-op."""
    monkeypatch.delenv(INGESTION_DESCRIPTOR_S3_URI_ENV, raising=False)
    target = _make_target()
    target._sinks_active = {"Account": _fake_sink(_SCHEMA, [f"{_ROOT}/dt=2026-08-26-14-15/f.jsonl.gz"])}

    with patch("target_s3.target.smart_open") as mock_open:
        target._emit_ingestion_descriptor()
        mock_open.assert_not_called()


def test_emit_descriptor_writes_streams_only_shape(monkeypatch, tmp_path):
    """The descriptor lists streams with their schema and dt partitions, and no identity fields."""
    path = tmp_path / "descriptor.json"
    monkeypatch.setenv(INGESTION_DESCRIPTOR_S3_URI_ENV, str(path))
    target = _make_target()
    target._sinks_active = {"Account": _fake_sink(_SCHEMA, [f"{_ROOT}/dt=2026-08-26-14-15/f.jsonl.gz"])}

    target._emit_ingestion_descriptor()

    d = json.loads(path.read_text())
    assert d["version"] == 1
    assert "service" not in d and "tenant" not in d and "connector" not in d
    assert len(d["streams"]) == 1
    stream = d["streams"][0]
    assert stream["stream"] == "Account"
    assert stream["table_root"] == f"s3://{_ROOT}/"
    assert stream["partition_keys"] == [{"name": "dt", "type": "string"}]
    assert len(stream["arrivals"]) == 1
    arrival = stream["arrivals"][0]
    assert arrival["partition"] == {
        "values": {"dt": "2026-08-26-14-15"},
        "location": f"s3://{_ROOT}/dt=2026-08-26-14-15/",
    }
    assert arrival["schema"] == _SCHEMA
    assert "extracted_at" in arrival


def test_multiple_dt_partitions_become_sorted_arrivals(monkeypatch, tmp_path):
    """Several dt partitions for one stream become one arrival each, oldest first."""
    path = tmp_path / "descriptor.json"
    monkeypatch.setenv(INGESTION_DESCRIPTOR_S3_URI_ENV, str(path))
    target = _make_target()
    target._sinks_active = {
        "Account": _fake_sink(
            _SCHEMA,
            [
                f"{_ROOT}/dt=2026-08-26-14-15/f.jsonl.gz",
                f"{_ROOT}/dt=2026-08-26-12-00/f.jsonl.gz",
                f"{_ROOT}/dt=2026-08-26-12-00/second-batch.jsonl.gz",  # same dt -> one arrival
            ],
        )
    }

    target._emit_ingestion_descriptor()

    arrivals = json.loads(path.read_text())["streams"][0]["arrivals"]
    assert [a["partition"]["values"]["dt"] for a in arrivals] == ["2026-08-26-12-00", "2026-08-26-14-15"]


def test_stream_without_dt_partition_is_omitted(monkeypatch, tmp_path):
    """A stream whose keys carry no dt= has nothing to stage and is left out of the descriptor."""
    path = tmp_path / "descriptor.json"
    monkeypatch.setenv(INGESTION_DESCRIPTOR_S3_URI_ENV, str(path))
    target = _make_target()
    target._sinks_active = {"Account": _fake_sink(_SCHEMA, [f"{_ROOT}/f.jsonl.gz"])}

    target._emit_ingestion_descriptor()
    assert json.loads(path.read_text())["streams"] == []


def test_process_endofpipe_swallows_descriptor_errors(monkeypatch):
    """Descriptor emission errors must NEVER propagate out of _process_endofpipe."""
    monkeypatch.setenv(INGESTION_DESCRIPTOR_S3_URI_ENV, "s3://broken/uri")
    target = _make_target()

    with patch("target_s3.target.Target._process_endofpipe"), patch.object(
        target, "_emit_extraction_manifest"
    ), patch.object(target, "_emit_ingestion_descriptor", side_effect=RuntimeError("simulated S3 outage")):
        target._process_endofpipe()  # must not raise
