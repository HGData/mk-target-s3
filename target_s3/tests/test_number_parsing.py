"""Decimals are read as Decimal only for parquet (RGI-3169): the JSON writers turn every Decimal back into a float,
so reading floats directly gives the same file and saves memory and time."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from target_s3.formats.format_jsonl import JsonSerialize
from target_s3.target import Targets3

AWS = {"cloud_provider_type": "aws", "aws": {"aws_access_key_id": "test", "aws_secret_access_key": "test",
                                             "aws_bucket": "test-bucket", "aws_region": "us-east-1"}}
NUMBERS = ["0.1", "1.0", "1.50", "-0.0", "3.14159265358979323846", "12345678901234567890.123", "1e5", "1E-7",
           "6.02214076e23", "1e400", "-1e400", "0.30000000000000004", "123456789.987654321", "42", "-7"]


def target(format_config: dict) -> Targets3:
    return Targets3(config={"format": format_config, "cloud_provider": AWS, "prefix": "test"})


def line(value: str) -> str:
    return '{"type": "RECORD", "stream": "s", "record": {"n": ' + value + ', "text": "x"}}'


@pytest.mark.parametrize("value", NUMBERS)
def test_jsonl_output_is_the_same_as_with_decimal(value):
    new = target({"format_type": "jsonl"}).deserialize_json(line(value))["record"]
    old = json.loads(line(value), parse_float=Decimal)["record"]
    assert json.dumps(new, cls=JsonSerialize) == json.dumps(old, cls=JsonSerialize)


def test_jsonl_reads_floats():
    assert type(target({"format_type": "jsonl"}).deserialize_json(line("1.5"))["record"]["n"]) is float


def test_parquet_keeps_decimal():
    assert target({"format_type": "parquet"}).deserialize_json(line("1.5"))["record"]["n"] == Decimal("1.5")


def test_parquet_with_schema_from_tap_reads_floats():
    config = {"format_type": "parquet", "format_parquet": {"get_schema_from_tap": True}}
    assert type(target(config).deserialize_json(line("1.5"))["record"]["n"]) is float
