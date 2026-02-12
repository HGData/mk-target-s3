"""Tests for FormatBase methods."""

import logging
import os
from typing import Any

from singer_sdk.testing import get_target_test_class

from target_s3.target import Targets3
from target_s3.formats.format_jsonl import FormatJsonl

LOGGER = logging.getLogger("target-s3")

SAMPLE_CONFIG: dict[str, Any] = {
    "format": {
        "format_type": "json",
    },
    "use_raw_stream_name": True, # avoid attempting to derive folder names which aren't relevent for these tests
    "append_date_to_prefix": False,
    "append_date_to_filename": False,
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "aws_bucket": os.getenv("AWS_BUCKET", "test-bucket"),
            "aws_region": os.getenv("AWS_REGION", "us-east-1"),
            "aws_session_token": os.getenv("AWS_SESSION_TOKEN", None),
        },
    },
    "prefix": "integration-tests",
}

SAMPLE_CONTEXT: dict[str, Any] = {
        "stream_name": "test_stream",
        "batch_start_time": None,
        "logger": LOGGER,
    }

TestTargetS3 = get_target_test_class(Targets3, config=SAMPLE_CONFIG)


def test_strip_utf8_surrogates():
    """Test that strip_utf8_surrogates removes UTF-16 surrogate code points."""

    formatter = FormatJsonl(SAMPLE_CONFIG, SAMPLE_CONTEXT)
    
    # Test with no surrogates
    normal_string = "Hello World"
    assert formatter.strip_utf8_surrogates(normal_string) == normal_string
    
    # Test with surrogate code points (0xD800-0xDFFF)
    # Create a string with surrogate pairs
    surrogate_string = "Hello" + chr(0xD800) + "World" + chr(0xDFFF) + "Test"
    cleaned = formatter.strip_utf8_surrogates(surrogate_string)
    assert cleaned == "HelloWorldTest"
    assert chr(0xD800) not in cleaned
    assert chr(0xDFFF) not in cleaned
    
    # Test with various surrogate ranges
    test_cases = [
        (chr(0xD800), ""),  # Low surrogate start
        (chr(0xDFFF), ""),  # High surrogate end
        (chr(0xDC00), ""),  # Mid-range surrogate
        ("abc" + chr(0xD900) + "def", "abcdef"),  # Surrogate in middle
    ]
    
    for input_str, expected in test_cases:
        result = formatter.strip_utf8_surrogates(input_str)
        assert result == expected


def test_sanitize_utf8_string():
    """Test sanitize_utf8 with string input."""

    formatter = FormatJsonl(SAMPLE_CONFIG, SAMPLE_CONTEXT)
    
    # Test with string containing surrogates
    test_string = "Hello" + chr(0xD800) + "World"
    result = formatter.sanitize_utf8(test_string)
    assert result == "HelloWorld"
    assert isinstance(result, str)
    
    # Test with normal string
    normal_string = "Normal text"
    assert formatter.sanitize_utf8(normal_string) == normal_string


def test_sanitize_utf8_dict():
    """Test sanitize_utf8 with dictionary input."""
    
    formatter = FormatJsonl(SAMPLE_CONFIG, SAMPLE_CONTEXT)
    
    # Test with dict containing surrogates in values
    test_dict = {
        "key1": "value1" + chr(0xD800) + "suffix",
        "key2": "normal_value",
        "key3": chr(0xDFFF) + "prefix",
    }
    result = formatter.sanitize_utf8(test_dict)
    assert result["key1"] == "value1suffix"
    assert result["key2"] == "normal_value"
    assert result["key3"] == "prefix"

def test_sanitize_utf8_list():
    """Test sanitize_utf8 with list input."""
    
    formatter = FormatJsonl(SAMPLE_CONFIG, SAMPLE_CONTEXT)
    
    # Test with list containing surrogates
    test_list = [
        "item1" + chr(0xD800),
        "item2",
        "item3" + chr(0xDFFF),
    ]
    result = formatter.sanitize_utf8(test_list)
    assert result[0] == "item1"
    assert result[1] == "item2"
    assert result[2] == "item3"
    assert len(result) == 3


def test_sanitize_utf8_nested():
    """Test sanitize_utf8 with nested structures."""
    
    formatter = FormatJsonl(SAMPLE_CONFIG, SAMPLE_CONTEXT)
    
    # Test with nested dict and list
    test_data = {
        "level1": {
            "level2": "value" + chr(0xD800),
            "list": ["item1" + chr(0xDFFF), "item2"],
        },
        "simple_list": [{"nested": "data" + chr(0xD800)}],
    }
    result = formatter.sanitize_utf8(test_data)
    assert result["level1"]["level2"] == "value"
    assert result["level1"]["list"][0] == "item1"
    assert result["level1"]["list"][1] == "item2"
    assert result["simple_list"][0]["nested"] == "data"


def test_sanitize_utf8_non_string_types():
    """Test sanitize_utf8 with non-string types (should pass through unchanged)."""
           
    formatter = FormatJsonl(SAMPLE_CONFIG, SAMPLE_CONTEXT)
    
    # Test with int
    assert formatter.sanitize_utf8(42) == 42
    
    # Test with float
    assert formatter.sanitize_utf8(3.14) == 3.14
    
    # Test with bool
    assert formatter.sanitize_utf8(True) is True
    assert formatter.sanitize_utf8(False) is False
    
    # Test with None
    assert formatter.sanitize_utf8(None) is None

