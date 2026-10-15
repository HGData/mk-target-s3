"""The JSONL writer cleans UTF-8 per record while it writes (RGI-3174): same file as before, no second copy of the
batch, no single big string, and a retry writes the whole file again."""

from __future__ import annotations

import gzip
import io
import json
import logging
from decimal import Decimal

import pytest
from botocore.exceptions import ClientError

import target_s3.formats.format_base as format_base
from target_s3.formats.format_jsonl import FormatJsonl, JsonSerialize

CONFIG = {
    "format": {"format_type": "jsonl"},
    "use_raw_stream_name": True,
    "append_date_to_prefix": False,
    "append_date_to_filename": False,
    "cloud_provider": {"cloud_provider_type": "aws", "aws": {
        "aws_access_key_id": "test", "aws_secret_access_key": "test", "aws_bucket": "test-bucket", "aws_region": "us-east-1"}},
    "prefix": "unit",
}
S = chr(0xD800)  # a lone surrogate, which Redshift rejects
TEXTS = ["plain ascii", "", "café crème", "日本語のテキスト", "emoji 🎉 stays", "a" + S + "b", S, chr(0xDFFF) + "x" + chr(0xDC00),
         "mixé" + S + "🎉" + chr(0xDBFF) + "end"]
RECORDS = [
    {"event_key": "1", "event_text": "Opened " + S + "board", "props": {"title": "Kanban 🎉", "bad": S + "x"}, "tags": ["a", "b" + S]},
    {"event_key": "2", "event_text": "plain", "amount": Decimal("12.50"), "n": 3, "missing": None},
    {"event_key": "3", "event_text": "naïve " + chr(0xDFFF), "props": {}, "tags": []},
]


def old_strip(s: str) -> str:
    """The previous implementation, character by character."""
    return "".join(ch for ch in s if not (0xD800 <= ord(ch) <= 0xDFFF))


def old_sanitize(obj):
    if isinstance(obj, dict):
        return {k: old_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [old_sanitize(v) for v in obj]
    if isinstance(obj, str):
        return old_strip(obj)
    return obj


def old_file(records) -> str:
    return "\n".join(json.dumps(old_sanitize(r), cls=JsonSerialize) for r in records)


class FakeS3:
    """Stands in for smart_open's raw S3 writer: keeps what each attempt wrote, aborts (None) on an error like
    the real writer, and can fail the first attempt part-way."""

    def __init__(self, fail_first_after_lines: int | None = None):
        self.files, self.keys, self.fail_after = [], [], fail_first_after_lines

    def open(self, path, mode="wb", compression=None, transport_params=None):
        fake = self
        assert mode == "wb" and compression == "disable"

        class Handle(io.BytesIO):
            def write(self, data):
                if fake.fail_after is not None and len(fake.files) == 1 and self.getvalue().count(b"\n") >= fake.fail_after:
                    fake.fail_after = None
                    raise ClientError({"Error": {"Code": "SlowDown", "Message": "reduce your request rate"}}, "UploadPart")
                return super().write(data)

            def __exit__(self, exc_type, *exc):
                data = self.getvalue()
                fake.files[-1] = None if exc_type else (gzip.decompress(data) if path.endswith(".gz") else data).decode()
                return False

        self.files.append(None)
        self.keys.append(path)
        return Handle()


def run_writer(monkeypatch, records, fake, config=CONFIG):
    monkeypatch.setattr(format_base, "open", fake.open)
    context = {"stream_name": "events", "batch_start_time": None, "logger": logging.getLogger("t"), "records": records}
    writer = FormatJsonl(config, context)
    writer.run()
    return writer


@pytest.mark.parametrize("text", TEXTS)
def test_cleaning_matches_the_old_implementation(text):
    writer = FormatJsonl(CONFIG, {"stream_name": "events", "batch_start_time": None, "logger": logging.getLogger("t")})
    assert writer.strip_utf8_surrogates(text) == old_strip(text)


def test_the_file_is_byte_for_byte_the_same_as_before(monkeypatch):
    fake = FakeS3()
    run_writer(monkeypatch, RECORDS, fake)
    assert fake.files == [old_file(RECORDS)]


def test_the_batch_is_not_copied(monkeypatch):
    writer = run_writer(monkeypatch, RECORDS, FakeS3())
    assert writer.records is writer.context["records"]


def test_surrogates_in_keys_are_removed_too(monkeypatch):
    fake = FakeS3()
    run_writer(monkeypatch, [{"event_key": "1", "props": {"ti" + S + "tle": "x", "ok": "y"}}], fake)
    assert json.loads(fake.files[0]) == {"event_key": "1", "props": {"title": "x", "ok": "y"}}


def test_one_record_and_no_record(monkeypatch):
    for records in (RECORDS[:1], []):
        fake = FakeS3()
        run_writer(monkeypatch, records, fake)
        assert fake.files == [old_file(records)]


def test_a_retry_writes_the_whole_file_again(monkeypatch):
    many = [{"event_key": str(i), "text": "row " + str(i) + S} for i in range(50)]
    fake = FakeS3(fail_first_after_lines=20)
    monkeypatch.setattr(format_base.backoff, "full_jitter", lambda value: 0)
    monkeypatch.setattr(format_base, "WRITE_BLOCK_CHARS", 1)  # one line per write, so the failure lands part-way
    run_writer(monkeypatch, many, fake)
    assert fake.files == [None, old_file(many)]  # the failed attempt was aborted, not saved part-way


def test_an_error_while_building_a_line_leaves_no_file(monkeypatch):
    fake = FakeS3()
    with pytest.raises(TypeError):
        run_writer(monkeypatch, RECORDS + [{("not", "text"): 1}], fake)  # json.dumps rejects a tuple key
    assert fake.files == [None]


def test_a_gz_key_is_gzipped_and_the_same_as_before(monkeypatch):
    fake = FakeS3()
    config = {**CONFIG, "use_raw_stream_name": False, "append_date_to_prefix": False, "append_date_to_filename": False}
    run_writer(monkeypatch, RECORDS, fake, config)
    assert fake.keys[0].endswith(".jsonl.gz")
    assert fake.files == [old_file(RECORDS)]


@pytest.mark.parametrize("block_chars", [1, 50, 10_000_000])
def test_the_file_is_the_same_for_any_block_size(monkeypatch, block_chars):
    many = [{"event_key": str(i), "text": "row " + str(i) + S} for i in range(200)]
    monkeypatch.setattr(format_base, "WRITE_BLOCK_CHARS", block_chars)
    fake = FakeS3()
    run_writer(monkeypatch, many, fake)
    assert fake.files == [old_file(many)]
