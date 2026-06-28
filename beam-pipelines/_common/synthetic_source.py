"""A controllable, unbounded synthetic event source for the streaming chapters.

Building a true custom unbounded source in Python Beam is advanced (see Ch 14, Splittable DoFn).
For the windowing / watermark / trigger labs we instead use the well-supported ``PeriodicImpulse``,
which emits one impulse per interval and is treated by the runner as an *unbounded* source. We map
each impulse to a synthetic event and (optionally) inject event-time lag so watermarks visibly trail
processing time.

Example:
    events = (
        p
        | "tick" >> SyntheticEvents(events_per_sec=5, duration_sec=120, key_cardinality=3)
    )
    # -> PCollection[dict] with keys: key, value, event_time (unix seconds), seq
"""
from __future__ import annotations

import time
from typing import Any, Dict

import apache_beam as beam
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.utils.timestamp import Timestamp


class _ImpulseToEvent(beam.DoFn):
    """Turn each periodic impulse timestamp into a synthetic, event-timestamped record."""

    def __init__(self, key_cardinality: int, lag_sec: float):
        self._key_cardinality = key_cardinality
        self._lag_sec = lag_sec

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        # ``element`` is the impulse's emission time (seconds, float). Derive a deterministic-ish
        # key and value from it so labs are reproducible without Math.random-style nondeterminism.
        secs = float(element)
        seq = int(secs)
        key = f"sensor-{seq % self._key_cardinality}"
        # Event time deliberately lags processing time by ``lag_sec`` so the watermark trails.
        event_time = secs - self._lag_sec
        record: Dict[str, Any] = {
            "key": key,
            "value": (seq % 100) + 1,
            "event_time": event_time,
            "seq": seq,
        }
        yield beam.window.TimestampedValue(record, Timestamp(event_time))


class SyntheticEvents(beam.PTransform):
    """Unbounded stream of synthetic, event-time-stamped records.

    Args:
        events_per_sec: how many impulses per second (controls throughput).
        duration_sec: total wall-clock duration before the source stops (labs are finite).
        key_cardinality: number of distinct keys (e.g. sensors / users).
        lag_sec: event-time lag vs processing time, to make watermark behaviour visible.
    """

    def __init__(
        self,
        events_per_sec: float = 5.0,
        duration_sec: float = 120.0,
        key_cardinality: int = 3,
        lag_sec: float = 0.0,
    ):
        super().__init__()
        self._interval = 1.0 / events_per_sec
        self._duration = duration_sec
        self._key_cardinality = key_cardinality
        self._lag_sec = lag_sec

    def expand(self, pbegin):
        now = time.time()
        return (
            pbegin
            | "PeriodicImpulse"
            >> PeriodicImpulse(
                start_timestamp=now,
                stop_timestamp=now + self._duration,
                fire_interval=self._interval,
                apply_windowing=False,
            )
            | "ToEvent" >> beam.ParDo(_ImpulseToEvent(self._key_cardinality, self._lag_sec))
        )
