"""Tests for validate_records: the per-record schema check can be turned off (s3), and stays on by default."""

from __future__ import annotations

import pytest
from jsonschema.exceptions import ValidationError

from target_s3.sinks import s3Sink
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
}
SCHEMA = {"type": "object", "properties": {"event_key": {"type": ["null", "string"]}}}
GOOD = {"event_key": "0.3fd8f6f35573ce4e1a0fe053410"}
BAD = {"event_key": 12345}


def _sink(**config) -> s3Sink:
    target = Targets3(config={**CONFIG, **config})
    return s3Sink(target, "events", SCHEMA, ["event_key"])


def test_checked_by_default():
    assert _sink()._validate_and_parse(dict(GOOD)) == GOOD
    with pytest.raises(ValidationError):
        _sink()._validate_and_parse(dict(BAD))


def test_checked_when_turned_on():
    with pytest.raises(ValidationError):
        _sink(validate_records=True)._validate_and_parse(dict(BAD))


def test_a_good_record_is_unchanged_when_turned_off():
    assert _sink(validate_records=False)._validate_and_parse(dict(GOOD)) == GOOD


def test_a_bad_record_passes_as_it_is_when_turned_off():
    assert _sink(validate_records=False)._validate_and_parse(dict(BAD)) == BAD
