"""Memory of queued batches (RGI-3169): queued records share field names, and max_buffer_mb writes out the largest
batch early when all batches together hold too much."""

from __future__ import annotations

import io
import json

import pytest

from target_s3.formats.format_jsonl import FormatJsonl
from target_s3.target import Targets3

CONFIG = {
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
    "prefix": "test",
    "max_batch_size": 100000,
}
WIDE_VALUE = "x" * 100_000  # one wide record is about 100 KB of JSON


@pytest.fixture
def written(monkeypatch):
    """Batches written, as (stream, records), with no S3 write."""
    batches = []
    monkeypatch.setattr(FormatJsonl, "run", lambda self: batches.append((self.context["stream_name"], len(self.context["records"]))))
    return batches


def lines(*messages) -> io.StringIO:
    return io.StringIO("".join(json.dumps(m) + "\n" for m in messages))


def schema(stream):
    return {"type": "SCHEMA", "stream": stream, "key_properties": ["id"],
            "schema": {"type": "object", "properties": {"id": {"type": "integer"}, "text": {"type": ["null", "string"]}}}}


def record(stream, i, text="small"):
    return {"type": "RECORD", "stream": stream, "record": {"id": i, "text": text}}


def run(config, *messages) -> Targets3:
    target = Targets3(config={**CONFIG, **config})
    target._process_lines(lines(*messages))
    return target


def test_queued_records_share_field_names(written):
    target = run({}, schema("Lead"), record("Lead", 1), record("Lead", 2))
    first, second = target._sinks_active["Lead"]._pending_batch["records"]
    for a, b in zip(first, second):
        assert a == b and a is b


def test_largest_batch_written_when_all_batches_pass_the_limit(written):
    # 1 MB limit: 10 wide records (about 1 MB) plus a few small ones pass it on the 11th wide record.
    messages = [schema("Lead"), schema("User")] + [record("User", i) for i in range(5)]
    messages += [record("Lead", i, WIDE_VALUE) for i in range(11)]
    target = run({"max_buffer_mb": 1}, *messages)
    assert written == [("Lead", 11)]
    assert target._sinks_active["Lead"].buffered_bytes == 0
    assert target._sinks_active["User"].current_size == 5


def test_nothing_written_early_under_the_limit(written):
    run({"max_buffer_mb": 2}, schema("Lead"), *[record("Lead", i, WIDE_VALUE) for i in range(11)])
    assert written == []


def test_nothing_written_early_when_the_limit_is_not_set(written):
    run({}, schema("Lead"), *[record("Lead", i, WIDE_VALUE) for i in range(30)])
    assert written == []


def test_queued_bytes_counted_per_stream(written):
    target = run({}, schema("Lead"), schema("User"), record("Lead", 1, WIDE_VALUE), record("User", 1))
    lead, user = target._sinks_active["Lead"].buffered_bytes, target._sinks_active["User"].buffered_bytes
    assert lead > 100_000 > user > 0


def test_a_full_batch_still_resets_the_count(written):
    target = run({"max_batch_size": 2}, schema("Lead"), record("Lead", 1), record("Lead", 2), record("Lead", 3))
    assert written == [("Lead", 2)]
    assert 0 < target._sinks_active["Lead"].buffered_bytes < 100
