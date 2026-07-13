"""s3 target class."""

from __future__ import annotations
import decimal
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

from boto3 import Session
from singer_sdk.target_base import Target
from singer_sdk import typing as th
from smart_open import open as smart_open

from target_s3.formats.format_base import DATE_GRAIN

from target_s3.sinks import (
    s3Sink,
)

LOGGER = logging.getLogger("target-s3")

# Env var pointing at the S3 URI where the post-run extraction manifest should
# be written. Set by the orchestrator (Airflow MDI DAG callback uses it to
# locate + read the manifest, then POSTs to argo's data_extraction_runs
# endpoint as part of the RGI-950 skip-gate signal). When unset, manifest
# emission is skipped silently — keeps this target usable as a generic Singer
# target outside the MDI pipeline.
EXTRACTION_MANIFEST_S3_URI_ENV = "EXTRACTION_MANIFEST_S3_URI"


class Targets3(Target):
    """Sample target for s3."""

    name = "target-s3"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "format",
            th.ObjectType(
                th.Property(
                    "format_type",
                    th.StringType,
                    required=True,
                    allowed_values=[
                        "parquet",
                        "json",
                        "jsonl",
                    ],  # TODO: configure this from class
                ),
                th.Property(
                    "format_parquet",
                    th.ObjectType(
                        th.Property(
                            "validate",
                            th.BooleanType,
                            required=False,
                            default=False,
                        ),
                        th.Property(
                            "get_schema_from_tap",
                            th.BooleanType,
                            required=False,
                            default=False,
                            description="Set true if you want to declare schema of the\
                                         resulting parquet file based on taps. Doesn't \
                                         work with 'anyOf' types or when complex data is\
                                         not defined at element level. Doesn't work with \
                                         validate option for now."
                        ),
                    ),
                    required=False,
                ),
                th.Property(
                    "format_json",
                    th.ObjectType(),
                    required=False,
                ),
                th.Property(
                    "format_csv",
                    th.ObjectType(),
                    required=False,
                ),
            ),
        ),
        th.Property(
            "cloud_provider",
            th.ObjectType(
                th.Property(
                    "cloud_provider_type",
                    th.StringType,
                    required=True,
                    allowed_values=["aws"],  # TODO: configure this from class
                ),
                th.Property(
                    "aws",
                    th.ObjectType(
                        th.Property(
                            "aws_access_key_id",
                            th.StringType,
                            required=False,
                            secret=True,
                        ),
                        th.Property(
                            "aws_secret_access_key",
                            th.StringType,
                            required=False,
                            secret=True,
                        ),
                        th.Property(
                            "aws_session_token",
                            th.StringType,
                            required=False,
                            secret=True,
                        ),
                        th.Property(
                            "aws_region",
                            th.StringType,
                            required=True,
                        ),
                        th.Property(
                            "aws_profile_name",
                            th.StringType,
                            required=False,
                        ),
                        th.Property(
                            "aws_bucket",
                            th.StringType,
                            required=True,
                        ),
                        th.Property(
                            "aws_endpoint_override",
                            th.StringType,
                            required=False,
                        ),
                    ),
                    required=False,
                ),
            ),
        ),
        th.Property(
            "prefix",
            th.StringType,
            description="The prefix for the key.",
        ),
        th.Property(
            "stream_name_path_override",
            th.StringType,
            description="The S3 key stream name override.",
        ),
        th.Property(
            "include_process_date",
            th.BooleanType,
            description="A flag indicating whether to append _process_date to record.",
            default=False,
        ),
        th.Property(
            "use_raw_stream_name",
            th.BooleanType,
            description="A flag to force the filename to be identical to the stream name.",
            default=False,
        ),
        th.Property(
            "append_date_to_prefix",
            th.BooleanType,
            description="A flag to append the date to the key prefix.",
            default=True,
        ),
        th.Property(
            "partition_name_enabled",
            th.BooleanType,
            description="A flag (only works if append_date_to_prefix is enabled) to have partitioning name formatted e.g. 'year=2023/month=01/day=01'.",
            default=False,
        ),
        th.Property(
            "append_date_to_prefix_grain",
            th.StringType,
            description="The grain of the date to append to the prefix.",
            allowed_values=DATE_GRAIN.keys(),
            default="day",
        ),
        th.Property(
            "append_date_to_filename",
            th.BooleanType,
            description="A flag to append the date to the key filename.",
            default=True,
        ),
        th.Property(
            "append_date_to_filename_grain",
            th.StringType,
            description="The grain of the date to append to the filename.",
            allowed_values=DATE_GRAIN.keys(),
            default="day",
        ),
        th.Property(
            "max_batch_age",
            th.NumberType,
            description="Maximum time in minutes between state messages when records are streamed in.",
            required=False,
            default=5.0,
        ),
        th.Property(
            "max_batch_size",
            th.IntegerType,
            description="Maximum size of batches when records are streamed in.",
            required=False,
            default=10000,
        ),
        th.Property(
            "partition_by",
            th.ArrayType(th.StringType),
            required=False,
            description="List of key-value strings (e.g., 'tenant=${TENANT}') to prepend as partitions in the S3 key path after the stream name.",
        ),
        th.Property(
            "tenant",
            th.StringType,
            required=False,
            description="Optional tenant string to prefix S3 folder names.",
        ),
        th.Property(
            "dynamic_dt",
            th.BooleanType,
            description="Enable dynamic dt generation for each batch. When enabled, any 'dt=' entries in partition_by will use the current batch timestamp instead of static environment variables.",
            default=False,
        ),
    ).to_dict()

    default_sink_class = s3Sink

    def __init__(self, *args, **kwargs) -> None:  # noqa: D401
        super().__init__(*args, **kwargs)
        # Captured at target instantiation. The Singer SDK runs the target
        # for the duration of one tap+target invocation, so this is a good
        # proxy for the extraction's start time.
        self._extraction_started_at: datetime = datetime.now(tz=timezone.utc)

    @property
    def _MAX_RECORD_AGE_IN_MINUTES(self) -> float:  # type: ignore
        return float(self.config.get("max_batch_age", 5.0))

    def _process_endofpipe(self) -> None:
        """Override the SDK lifecycle hook to emit a manifest after final drain.

        super() finishes draining all sinks, then we aggregate per-sink record
        counts and write a JSON manifest to the URI given by
        EXTRACTION_MANIFEST_S3_URI. Failure to emit the manifest never raises
        — the tap+target run is already complete by the time we get here, and
        the manifest is an observability signal that callers (Airflow → argo)
        fail-open on.
        """
        super()._process_endofpipe()
        try:
            self._emit_extraction_manifest()
        except Exception as exc:  # noqa: BLE001 — must never raise here
            LOGGER.warning(
                "target-s3: failed to emit extraction manifest: %s", exc, exc_info=True
            )

    def _write_state_message(self, state: dict) -> None:
        """Override the SDK hook to never emit an empty state (RGI-1651).

        singer-sdk 0.33.1 initialises `_latest_state` to `{}` and emits it
        unconditionally on every drain, so a tap that dies before sending any
        STATE message (e.g. Argo credential fetch fails at login) makes this
        target emit `{}` at end-of-pipe — which Meltano then persists over the
        tenant's real bookmarks, downgrading every subsequent run to a full
        pull. Newer SDKs (>=0.47.0) skip emission when no state was received;
        this backports that guard. An empty Singer state carries no bookmarks,
        so suppressing it is always safe.
        """
        if not state:
            LOGGER.info(
                "target-s3: no state received from tap; skipping state emission"
            )
            return
        super()._write_state_message(state)

    def _emit_extraction_manifest(self) -> None:
        manifest_uri = os.environ.get(EXTRACTION_MANIFEST_S3_URI_ENV)
        if not manifest_uri:
            LOGGER.debug(
                "target-s3: %s unset, skipping manifest emission",
                EXTRACTION_MANIFEST_S3_URI_ENV,
            )
            return

        ended_at = datetime.now(tz=timezone.utc)

        # Per-stream + total record counts come straight from the SDK sink
        # counters (set via tally_record_written). After _process_endofpipe's
        # drain_all, sinks remain in _sinks_active with finalised totals.
        by_stream: dict[str, int] = {}
        total_records = 0
        for stream_name, sink in self._sinks_active.items():
            written = getattr(sink, "_total_records_written", 0) or 0
            by_stream[stream_name] = written
            total_records += written

        manifest = {
            "tenant": self.config.get("tenant"),
            "started_at": self._extraction_started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "records_extracted": total_records,
            "by_stream": by_stream,
        }

        # Use the same explicit credentials the sinks use for data writes.
        # Without `transport_params={"client": ...}`, smart_open falls through
        # to the default boto3 chain — in ECS that resolves to the task role,
        # which may not have PutObject on the target bucket even when the
        # IAM-user creds in cloud_provider.aws do.
        transport_params: dict = {}
        s3_client = self._build_s3_client()
        if s3_client is not None:
            transport_params["client"] = s3_client

        with smart_open(manifest_uri, "w", transport_params=transport_params) as f:
            f.write(json.dumps(manifest, indent=2))

        LOGGER.info(
            "target-s3: wrote extraction manifest (records=%d, streams=%d) to %s",
            total_records,
            len(by_stream),
            _sanitize_log_uri(manifest_uri),
        )

    def _build_s3_client(self):
        """Build an S3 client from the target's cloud_provider config.

        Mirrors the boto3 session built per-sink in sinks.py. Returns None
        when cloud_provider isn't AWS — caller then lets smart_open fall back
        to its default credential resolution.
        """
        cloud_provider = self.config.get("cloud_provider") or {}
        if cloud_provider.get("cloud_provider_type") != "aws":
            return None
        aws_config = cloud_provider.get("aws") or {}
        session = Session(
            aws_access_key_id=aws_config.get("aws_access_key_id"),
            aws_secret_access_key=aws_config.get("aws_secret_access_key"),
            aws_session_token=aws_config.get("aws_session_token"),
            region_name=aws_config.get("aws_region"),
            profile_name=aws_config.get("aws_profile_name"),
        )
        return session.client(
            "s3",
            endpoint_url=aws_config.get("aws_endpoint_override"),
        )

    def deserialize_json(self, line: str) -> dict:
        """Override base target's method to overcome Decimal cast,
        only applied when generating parquet schema from tap schema.

        :param line: serialized record from stream
        :type line: str
        :return: deserialized record
        :rtype: dict
        """
        try:
            self.format = self.config.get("format", None)
            format_parquet = self.format.get("format_parquet", None)
            if format_parquet and format_parquet.get("get_schema_from_tap", False):
                return json.loads(line)  # type: ignore[no-any-return]
            else:
                return json.loads(  # type: ignore[no-any-return]
                    line, parse_float=decimal.Decimal
                )
        except json.decoder.JSONDecodeError as exc:
            self.logger.error("Unable to parse:\n%s", line, exc_info=exc)
            raise


def _sanitize_log_uri(uri: str) -> str:
    """Strip query string / fragments for safe logging — bucket + key only."""
    try:
        p = urlparse(uri)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:  # noqa: BLE001
        return uri


if __name__ == "__main__":
    Targets3.cli()
