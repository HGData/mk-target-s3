"""Tests for state-message emission guard in Targets3 (RGI-1651).

When the tap crashes before emitting any STATE message (e.g. Argo credential
fetch fails at login), the target reaches end-of-pipe with its default empty
state. singer-sdk 0.33.1 would emit that `{}` to stdout, and Meltano persists
it over the tenant's real bookmarks — downgrading every later run to a full
pull. These tests pin the guard: empty state is never emitted, real state
passes through unchanged.
"""

from __future__ import annotations

import json

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


def test_empty_state_is_not_emitted(capsys):
    """`{}` (the SDK default when no STATE was received) must not reach stdout."""
    target = _make_target()
    target._write_state_message({})
    assert capsys.readouterr().out == ""


def test_non_empty_state_is_emitted_unchanged(capsys):
    """Real bookmark state passes through to stdout exactly as before."""
    target = _make_target()
    state = {"bookmarks": {"Lead": {"SystemModstamp": "2026-07-10T13:14:19.000000Z"}}}
    target._write_state_message(state)
    assert json.loads(capsys.readouterr().out) == state


def test_endofpipe_with_no_input_emits_no_state(monkeypatch, capsys):
    """Reproduces the RGI-1651 incident: tap dies pre-STATE, pipe closes.

    End-of-pipe with zero input lines must produce no state line on stdout,
    so Meltano has nothing to persist and the saved bookmarks survive.
    """
    monkeypatch.delenv(EXTRACTION_MANIFEST_S3_URI_ENV, raising=False)
    target = _make_target()
    target._process_endofpipe()
    assert capsys.readouterr().out == ""


def test_received_empty_state_is_also_suppressed(monkeypatch, capsys):
    """A tap-emitted `STATE {}` is suppressed too, matching singer-sdk >=0.47.0.

    Deliberate, not an accident: upstream introduced a None-sentinel guard in
    0.46.1 (meltano/sdk#3034) and broadened it in 0.47.0 (meltano/sdk#3040)
    because persisting a received empty state still overwrites valid bookmarks
    — the same wipe RGI-1651 fixes. State resets are operational
    (`meltano state clear`), never signalled through the pipe.
    """
    monkeypatch.delenv(EXTRACTION_MANIFEST_S3_URI_ENV, raising=False)
    target = _make_target()
    target._process_state_message({"type": "STATE", "value": {}})
    target._process_endofpipe()
    assert capsys.readouterr().out == ""


def test_endofpipe_after_state_received_still_emits(monkeypatch, capsys):
    """A normally-completing run still emits its final state at end-of-pipe."""
    monkeypatch.delenv(EXTRACTION_MANIFEST_S3_URI_ENV, raising=False)
    target = _make_target()
    state = {"bookmarks": {"Contact": {"SystemModstamp": "2026-07-10T13:13:56.000000Z"}}}
    target._process_state_message({"type": "STATE", "value": state})
    target._process_endofpipe()
    assert json.loads(capsys.readouterr().out) == state
