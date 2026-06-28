"""Chapter 9 — Windowing: Fixed, Sliding, Sessions, Global.

A **window** is a per-element *tag*: when you ``beam.WindowInto(...)`` a PCollection, the runner
runs every element's event timestamp through a ``WindowFn`` that assigns it to one *or more*
windows. Nothing is grouped yet — windowing only decorates each element with the window(s) it
belongs to. The grouping happens later, and it is always **per key, per window**: a
``GroupByKey``/``CombinePerKey`` downstream produces one output per (key, window) pair.

This chapter takes ONE replayed, out-of-order, event-time-stamped stream and counts events per key
**three ways**, changing only the ``WindowFn``:

  * ``FixedWindows(60)``        — tumbling: contiguous, non-overlapping 60s buckets. Each element
                                  lands in exactly one window.
  * ``SlidingWindows(60, 30)``  — overlapping: 60s wide, a new one starting every 30s, so each
                                  element lands in TWO windows (size / period = 2).
  * ``Sessions(gap_size=45)``   — data-driven & MERGING: each element seeds a [t, t+gap) window;
                                  windows that touch/overlap are merged, so a burst of activity
                                  with gaps < 45s collapses into one session, and a quiet stretch
                                  ≥ 45s starts a fresh one. Window bounds depend on the DATA.

Beam's GlobalWindows (the default for batch) is mentioned for contrast: a single window spanning
all of time — you only ever get one bucket per key, which is why unbounded sources need a real
window (or a trigger) before you can aggregate.

We attach ``DoFn.WindowParam`` to a logging DoFn so the worker logs exactly which window each
per-key count fell into — that is the whole lesson made observable.

Run it:  ./scripts/submit.sh ch09        (or click "Run on Flink" in the Ch 9 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch09-<runId>.
         Worker logs show lines like  [FIXED] window=[60,120) key=sensor-0 count=3.

Note: worker-side ``logging`` lands in the TaskManager logs (Flink UI / docker logs), not the
submitter's stdout — that is expected for the portable runner.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam.transforms import combiners
from apache_beam.transforms.window import (
    FixedWindows,
    GlobalWindows,
    Sessions,
    SlidingWindows,
)

# Make the shared _common package importable regardless of CWD (same idiom as ch01).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402
from _common.replay import ReplayEvents  # noqa: E402


# A small, hand-built, out-of-order event log. Each record has an ``event_time`` (unix seconds,
# here using a tiny epoch starting at 0 for readability) and a ``key`` (which "sensor" fired).
# Note the deliberate GAP for sensor-0 between t=70 and t=200: that gap is > the session gap_size,
# so Sessions will split it into two sessions while Fixed/Sliding ignore the gap entirely.
RAW_EVENTS = [
    {"event_time": 5, "key": "sensor-0", "value": 1},
    {"event_time": 12, "key": "sensor-1", "value": 1},
    {"event_time": 20, "key": "sensor-0", "value": 1},
    {"event_time": 35, "key": "sensor-0", "value": 1},
    {"event_time": 48, "key": "sensor-1", "value": 1},
    {"event_time": 55, "key": "sensor-0", "value": 1},  # still < 60 -> first fixed window
    {"event_time": 70, "key": "sensor-0", "value": 1},  # crosses into 2nd fixed window
    {"event_time": 82, "key": "sensor-1", "value": 1},
    {"event_time": 95, "key": "sensor-0", "value": 1},
    {"event_time": 118, "key": "sensor-1", "value": 1},
    {"event_time": 200, "key": "sensor-0", "value": 1},  # after a long quiet gap -> new session
    {"event_time": 215, "key": "sensor-0", "value": 1},
    {"event_time": 240, "key": "sensor-1", "value": 1},
]

# Emit them in a deliberately shuffled order so arrival order != event-time order. Windowing keys
# off the *event* timestamp (assigned by ReplayEvents), so the result is identical regardless of
# this arrival order — a key property we want learners to internalise.
EMISSION_ORDER = [3, 0, 7, 1, 10, 2, 5, 11, 4, 8, 6, 12, 9]

SESSION_GAP = 45  # seconds of inactivity that ends a session


class LogPerKeyWindow(beam.DoFn):
    """Log each (key, count) result together with the window it belongs to.

    ``DoFn.WindowParam`` injects the ``BoundedWindow`` for the element currently being processed.
    After a GroupByKey/CombinePerKey, each element is one (key, count) for exactly one window, so
    this prints the per-key-per-window aggregate — the thing windowing actually produces.
    """

    def __init__(self, label: str):
        self._label = label

    def process(self, element, window=beam.DoFn.WindowParam):
        key, count = element
        # window.start / window.end are Beam Timestamps; int() gives the unix-second bound.
        start = int(window.start)
        end = int(window.end)
        logging.info(
            "[%s] window=[%s,%s) key=%s count=%s",
            self._label,
            start,
            end,
            key,
            count,
        )
        # Yield a flat, human-readable string so it could be written to a sink if desired.
        yield f"{self._label} [{start},{end}) {key}={count}"


def count_per_key(events, window_fn, label: str):
    """Apply one WindowFn, then count per key-per-window, then log the window each result fell in.

    The pipeline shape is IDENTICAL for every window type — only ``window_fn`` changes. That is the
    teaching point: windowing is an orthogonal knob you turn, not a different aggregation.
    """
    return (
        events
        # 1) Tag every element with its window(s). For SlidingWindows each element is duplicated
        #    into multiple windows here; for Sessions each element seeds a window that may later
        #    MERGE with neighbours during the GroupByKey.
        | f"Window/{label}" >> beam.WindowInto(window_fn)
        # 2) Pair each event with 1 so we can sum. (key, 1)
        | f"Pair/{label}" >> beam.Map(lambda e: (e["key"], 1))
        # 3) The grouping: one output per (key, window). For Sessions, touching per-key windows
        #    are merged *before* the values are combined.
        | f"Count/{label}" >> combiners.Count.PerKey()
        # 4) Observe the window each per-key count landed in via DoFn.WindowParam.
        | f"Log/{label}" >> beam.ParDo(LogPerKeyWindow(label))
    )


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument(
        "--session_gap",
        type=int,
        default=SESSION_GAP,
        help="Seconds of inactivity that ends a session window.",
    )
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch09-{known.run_id}"
    # streaming=True: windowing is the gateway concept for unbounded data, so we run in streaming
    # mode even though our replayed source is finite. The WindowFns behave identically; what differs
    # in real streaming is *when* results fire (triggers/watermarks — Ch 10-11).
    options = portable_options(job_name, streaming=True)

    logging.info(
        "Submitting windowing demo as job_name=%s (Fixed/Sliding/Sessions, gap=%ss)",
        job_name,
        known.session_gap,
    )

    with beam.Pipeline(options=options) as p:
        # ONE shared, event-time-stamped, deliberately out-of-order stream feeds all three branches.
        events = p | "Replay" >> ReplayEvents(RAW_EVENTS, emission_order=EMISSION_ORDER)

        # --- Branch A: FixedWindows(60) — tumbling 60s buckets. One window per element. ---
        count_per_key(events, FixedWindows(60), "FIXED")

        # --- Branch B: SlidingWindows(60, 30) — 60s wide, new one every 30s. Two windows/element,
        #     so the same event is counted in two overlapping buckets (size/period = 2). ---
        count_per_key(events, SlidingWindows(size=60, period=30), "SLIDING")

        # --- Branch C: Sessions(gap) — data-driven, MERGING. A burst with sub-gap spacing collapses
        #     into one window; a quiet stretch >= gap starts a fresh session. Bounds depend on data. ---
        count_per_key(events, Sessions(gap_size=known.session_gap), "SESSION")

        # For contrast we also show GlobalWindows: a single all-of-time window. Per key you get ONE
        # bucket no matter the timestamps — fine for bounded data, useless for unbounded without a
        # trigger. We add a name suffix so the label reads clearly in the logs.
        count_per_key(events, GlobalWindows(), "GLOBAL")

    logging.info("Pipeline finished. Compare the [FIXED]/[SLIDING]/[SESSION]/[GLOBAL] log lines.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
