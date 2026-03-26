"""s3 target sink class, which handles writing streams."""

from __future__ import annotations
import logging

from boto3 import Session
from botocore.config import Config
from singer_sdk.sinks import BatchSink

from target_s3.formats.format_base import FormatBase, format_type_factory
from target_s3.formats.format_parquet import FormatParquet
from target_s3.formats.format_csv import FormatCsv
from target_s3.formats.format_json import FormatJson
from target_s3.formats.format_jsonl import FormatJsonl


LOGGER = logging.getLogger("target-s3")
FORMAT_TYPE = {"parquet": FormatParquet, "csv": FormatCsv, "json": FormatJson, "jsonl": FormatJsonl}


class s3Sink(BatchSink):
    """s3 target sink class."""

    def __init__(
        self,
        target: any,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # what type of file are we building?
        self.format_type = self.config.get("format", None).get("format_type", None)
        self.schema = schema
        if self.format_type:
            if self.format_type not in FORMAT_TYPE:
                raise Exception(
                    f"Unknown file type specified. {key_properties['type']}"
                )
        else:
            raise Exception("No file type supplied.")

        # Create boto3 session and S3 client once per sink (reused across batches)
        self._s3_client = None
        self._s3_session = None
        cloud_provider = self.config.get("cloud_provider", None)
        if cloud_provider and cloud_provider.get("cloud_provider_type", None) == "aws":
            aws_config = cloud_provider.get("aws", None)
            if aws_config:
                self._s3_session = Session(
                    aws_access_key_id=aws_config.get("aws_access_key_id", None),
                    aws_secret_access_key=aws_config.get("aws_secret_access_key", None),
                    aws_session_token=aws_config.get("aws_session_token", None),
                    region_name=aws_config.get("aws_region"),
                    profile_name=aws_config.get("aws_profile_name", None),
                )
                s3_retry_config = Config(
                    retries={"mode": "adaptive", "max_attempts": 10}
                )
                self._s3_client = self._s3_session.client(
                    "s3",
                    endpoint_url=aws_config.get("aws_endpoint_override", None),
                    config=s3_retry_config,
                )

    @property
    def max_size(self) -> int:
        """Get maximum batch size.

        Returns:
            Maximum batch size
        """
        return self.config.get("max_batch_size", 10000)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # add stream name to context
        context["stream_name"] = self.stream_name
        context["logger"] = self.logger
        context["stream_schema"] = self.schema
        # pass pre-built S3 client and session to avoid creating new ones per batch
        if self._s3_client:
            context["s3_client"] = self._s3_client
        if self._s3_session:
            context["s3_session"] = self._s3_session
        # creates new object for each batch
        format_type_client = format_type_factory(
            FORMAT_TYPE[self.format_type], self.config, context
        )
        # force base object_type_client to object_type_base class
        assert (
            isinstance(format_type_client, FormatBase) is True
        ), f"format_type_client must be of type Base; Type: {type(self.format_type_client)}."

        format_type_client.run()
