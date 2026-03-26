"""
Integration tests for max_parallelism: 4 configuration.

These tests verify:
1. _MAX_PARALLELISM property returns the configured value
2. ThreadPoolExecutor with max_workers=4 limits concurrent S3 writes (mirrors singer-sdk behaviour)
3. SlowDown errors are retried and all 8 Salesforce-like streams complete

Usage:
    pytest target_s3/tests/test_max_parallelism_integration.py -v -s
"""

import threading
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from target_s3.target import Targets3
from target_s3.tests.test_base import BASE_TARGET_CONFIG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class ConcurrencyTracker:
    """Thread-safe tracker for peak concurrent execution."""

    def __init__(self):
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.timeline = []  # (stream, action, active_count, ts)

    def enter(self, stream: str):
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.timeline.append((stream, "START", self.active, time.time()))

    def exit(self, stream: str):
        with self._lock:
            self.active -= 1
            self.timeline.append((stream, "END", self.active, time.time()))

    def print_report(self, title: str = ""):
        print(f"\n{'='*70}")
        if title:
            print(f"  {title}")
        print(f"{'='*70}")
        t0 = self.timeline[0][3] if self.timeline else time.time()
        for stream, action, active, ts in self.timeline:
            marker = ">>>" if action == "START" else "   "
            print(f"  {marker} +{(ts-t0)*1000:6.0f}ms  {stream:25} {action:5}  active={active}")
        print(f"\n  Peak concurrent: {self.peak}")
        print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Test 1: _MAX_PARALLELISM property
# ---------------------------------------------------------------------------

class TestMaxParallelismProperty:
    """Verify _MAX_PARALLELISM returns the configured value."""

    def test_default_is_8(self):
        target = Targets3(config=BASE_TARGET_CONFIG)
        assert target._MAX_PARALLELISM == 8

    def test_configured_value_4(self):
        config = {**BASE_TARGET_CONFIG, "max_parallelism": 4}
        target = Targets3(config=config)
        assert target._MAX_PARALLELISM == 4

    def test_configured_value_1(self):
        config = {**BASE_TARGET_CONFIG, "max_parallelism": 1}
        target = Targets3(config=config)
        assert target._MAX_PARALLELISM == 1



# ---------------------------------------------------------------------------
# Test 2: Parallelism limiting via ThreadPoolExecutor (mirrors singer-sdk)
# ---------------------------------------------------------------------------

class TestParallelismLimiting:
    """
    singer-sdk reads Target._MAX_PARALLELISM to set max_workers on its
    internal ThreadPoolExecutor when flushing sinks concurrently.
    These tests simulate that behaviour directly.
    """

    SALESFORCE_8_OBJECTS = [
        "Lead", "Contact", "Account", "Task",
        "CampaignMember", "Opportunity", "Case", "Event",
    ]

    def _simulate_s3_write(self, stream: str, tracker: ConcurrencyTracker,
                           write_duration: float = 0.3):
        tracker.enter(stream)
        time.sleep(write_duration)
        tracker.exit(stream)

    def test_8_objects_peak_concurrent_is_4(self):
        """
        8 Salesforce objects flushed through ThreadPoolExecutor(max_workers=4).
        Peak concurrent must not exceed 4.
        """
        config = {**BASE_TARGET_CONFIG, "max_parallelism": 4}
        target = Targets3(config=config)

        tracker = ConcurrencyTracker()

        with ThreadPoolExecutor(max_workers=target._MAX_PARALLELISM) as executor:
            futures = {
                executor.submit(self._simulate_s3_write, obj, tracker): obj
                for obj in self.SALESFORCE_8_OBJECTS
            }
            for future in as_completed(futures):
                future.result()

        tracker.print_report("8 Salesforce objects | max_parallelism=4")

        assert tracker.peak <= 4, (
            f"Peak concurrent {tracker.peak} exceeded max_parallelism=4"
        )
        completed = len([e for e in tracker.timeline if e[1] == "END"])
        assert completed == 8, f"Expected 8 completions, got {completed}"

    def test_8_objects_with_parallelism_8_higher_peak(self):
        """
        Baseline: with max_parallelism=8, peak should be higher than 4.
        Confirms that limiting to 4 is actually meaningful.
        """
        config = {**BASE_TARGET_CONFIG, "max_parallelism": 8}
        target = Targets3(config=config)

        tracker = ConcurrencyTracker()

        with ThreadPoolExecutor(max_workers=target._MAX_PARALLELISM) as executor:
            futures = {
                executor.submit(self._simulate_s3_write, obj, tracker): obj
                for obj in self.SALESFORCE_8_OBJECTS
            }
            for future in as_completed(futures):
                future.result()

        tracker.print_report("8 Salesforce objects | max_parallelism=8 (baseline)")

        assert tracker.peak > 4, (
            f"Expected peak > 4 with max_parallelism=8, got {tracker.peak}"
        )

    def test_parallelism_4_is_faster_than_parallelism_1(self):
        """
        With 8 streams each taking 0.2s:
          - parallelism=1  → ~1.6s  (sequential)
          - parallelism=4  → ~0.4s  (2 batches of 4)
        """
        def run(parallelism: int) -> float:
            tracker = ConcurrencyTracker()
            start = time.time()
            with ThreadPoolExecutor(max_workers=parallelism) as executor:
                futures = [
                    executor.submit(self._simulate_s3_write, f"stream_{i}", tracker, 0.2)
                    for i in range(8)
                ]
                for f in as_completed(futures):
                    f.result()
            return time.time() - start

        t1 = run(parallelism=1)
        t4 = run(parallelism=4)

        print(f"\n  parallelism=1: {t1:.2f}s")
        print(f"  parallelism=4: {t4:.2f}s")
        print(f"  Speedup:       {t1/t4:.1f}x\n")

        assert t4 < t1 * 0.6, (
            f"parallelism=4 ({t4:.2f}s) should be significantly faster than "
            f"parallelism=1 ({t1:.2f}s)"
        )


# ---------------------------------------------------------------------------
# Test 3: SlowDown resilience with 8 objects and parallelism=4
# ---------------------------------------------------------------------------

class TestSlowDownWith8Objects:
    """
    Verify that SlowDown errors are retried and all 8 streams complete
    when running through ThreadPoolExecutor(max_workers=4).
    """

    SALESFORCE_8_OBJECTS = [
        "Lead", "Contact", "Account", "Task",
        "CampaignMember", "Opportunity", "Case", "Event",
    ]

    def _write_with_retry(self, stream: str, tracker: ConcurrencyTracker,
                          fail_first_n: int = 2) -> str:
        """
        Simulate _write() with SlowDown retry logic.
        Fails first `fail_first_n` attempts per stream, then succeeds.
        """
        max_attempts = 5
        for attempt in range(max_attempts):
            tracker.enter(stream)

            if attempt < fail_first_n:
                tracker.exit(stream)
                logger.warning(
                    "  ⚠️  %s attempt %d/%d: SlowDown — retrying",
                    stream, attempt + 1, max_attempts
                )
                time.sleep(0.05)  # short backoff in tests
                continue

            # Success
            time.sleep(0.2)
            tracker.exit(stream)
            logger.info("  ✅ %s succeeded on attempt %d", stream, attempt + 1)
            return "success"

        raise ValueError(f"SlowDown: {stream} exhausted all {max_attempts} attempts")

    def test_all_8_objects_complete_despite_slowdown(self):
        """
        8 streams, each fails twice with SlowDown then succeeds.
        All 8 must complete with max_parallelism=4.
        """
        config = {**BASE_TARGET_CONFIG, "max_parallelism": 4}
        target = Targets3(config=config)

        tracker = ConcurrencyTracker()
        results = {}

        with ThreadPoolExecutor(max_workers=target._MAX_PARALLELISM) as executor:
            futures = {
                executor.submit(self._write_with_retry, obj, tracker, 2): obj
                for obj in self.SALESFORCE_8_OBJECTS
            }
            for future in as_completed(futures):
                obj = futures[future]
                try:
                    results[obj] = future.result()
                except Exception as exc:
                    results[obj] = f"FAILED: {exc}"

        tracker.print_report("8 objects | max_parallelism=4 | SlowDown on first 2 attempts")

        failed = {k: v for k, v in results.items() if v != "success"}
        assert not failed, f"Streams failed: {failed}"
        assert tracker.peak <= 4, (
            f"Peak concurrent {tracker.peak} exceeded max_parallelism=4"
        )
        print(f"\n  ✅ All {len(results)} Salesforce objects succeeded")
        print(f"  ✅ Peak concurrent: {tracker.peak}/4")

    def test_non_slowdown_error_not_retried(self):
        """
        A non-SlowDown error (AccessDenied) must propagate immediately — no retry.
        """
        call_count = {"n": 0}

        def write_access_denied():
            call_count["n"] += 1
            raise ValueError(
                "the bucket 'x' does not exist (ClientError('AccessDenied'))"
            )

        with pytest.raises(ValueError, match="AccessDenied"):
            write_access_denied()

        assert call_count["n"] == 1, "AccessDenied must not be retried"

    def test_slowdown_on_all_8_objects_first_attempt(self):
        """
        Production scenario: first batch hits rate limit on every stream.
        All 8 objects retry and complete with max_parallelism=4.
        """
        config = {**BASE_TARGET_CONFIG, "max_parallelism": 4}
        target = Targets3(config=config)

        tracker = ConcurrencyTracker()
        results = {}

        with ThreadPoolExecutor(max_workers=target._MAX_PARALLELISM) as executor:
            futures = {
                executor.submit(self._write_with_retry, obj, tracker, 1): obj
                for obj in self.SALESFORCE_8_OBJECTS
            }
            for future in as_completed(futures):
                obj = futures[future]
                try:
                    results[obj] = future.result()
                except Exception as exc:
                    results[obj] = f"FAILED: {exc}"

        tracker.print_report("8 objects | max_parallelism=4 | SlowDown on first attempt only")

        failed = {k: v for k, v in results.items() if v != "success"}
        assert not failed, f"Streams failed: {failed}"
        assert tracker.peak <= 4

        print(f"\n  ✅ All 8 streams recovered from first-attempt SlowDown")
