"""Chapter 8 — The Streaming Mindset: WHAT / WHERE / WHEN / HOW.

This chapter teaches the single most important shift in thinking that streaming demands: there are
*two* clocks for every event.

  - **Event time**  — when the thing actually happened (stamped at the source: a click, a sensor
    reading, a sale). It is intrinsic to the data and never changes.
  - **Processing time** — when *your pipeline observed* the element. It depends on the network,
    queues, retries, GC pauses, and scheduling — so it drifts.

The gap between them is **skew**. Because of skew, **out-of-order arrival is the default, not the
exception**: an event that happened earlier can be *seen* later than one that happened after it.

Every streaming computation answers four questions (the "Beam model", a.k.a. Dataflow model):

  1. **WHAT** results are computed?         -> transforms (Map / GroupByKey / Combine ...)
  2. **WHERE** in event time?               -> windowing (Ch 9: Fixed / Sliding / Sessions)
  3. **WHEN** in processing time are they emitted? -> watermarks + triggers (Ch 10/11)
  4. **HOW** do refinements relate?         -> accumulation mode (discarding vs accumulating, Ch 11)

This bounded pipeline makes the *first* idea concrete and visible. We replay a handful of events
**deliberately out of order** (arrival order != event-time order) using ``_common.replay.ReplayEvents``,
which assigns each record its event time via ``TimestampedValue``. A logging ``DoFn`` then prints,
for every element, its **event time** (from ``DoFn.TimestampParam``) next to the **processing time**
(``time.time()`` at the moment it is processed) and the resulting **skew** — so you can literally
read the skew off the worker logs and see arrivals jumping backwards in event time.

Nothing here windows or triggers yet; the point is purely to *feel* the two clocks. Chapters 9-12
then answer WHERE / WHEN / HOW in turn.

Run it:  ./scripts/submit.sh ch08        (or click "Run on Flink" in the Ch 8 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch08-<runId>.
Note  :  the per-element skew lines are emitted by a *worker* DoFn via logging; they appear in the
         TaskManager logs (Flink UI), not necessarily in the submitter's stdout.
"""
from __future__ import annotations

import argparse
import logging
import time

import apache_beam as beam

# Make the shared _common package importable regardless of CWD.
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402
from _common.replay import ReplayEvents  # noqa: E402


# A tiny, fully deterministic event set. Each record carries an explicit ``event_time`` (unix
# seconds) — that is *when it happened*. We use round, easy-to-read seconds so the logs are legible.
# Think of these as five sensor readings spanning a 40-second span of event time.
BASE = 1_700_000_000  # an arbitrary fixed epoch so the numbers in the logs are stable across runs.
EVENTS = [
    {"id": "A", "event_time": BASE + 0, "value": 10},
    {"id": "B", "event_time": BASE + 10, "value": 20},
    {"id": "C", "event_time": BASE + 20, "value": 30},
    {"id": "D", "event_time": BASE + 30, "value": 40},
    {"id": "E", "event_time": BASE + 40, "value": 50},
]

# The crux of the chapter: arrival order is NOT event-time order. ReplayEvents emits the records in
# THIS index order, so 'C' (event_time +20) arrives first, then 'A' (+0) arrives *after* it — the
# pipeline sees event time jump backwards. Out-of-order arrival is the default.
#   emission slot:  0    1    2    3    4
#   event index  :  2    0    4    1    3
#   -> arrives    :  C    A    E    B    D
#   -> event time : +20  +0   +40  +10  +30   (clearly not monotonic)
EMISSION_ORDER = [2, 0, 4, 1, 3]


class LogSkew(beam.DoFn):
    """Print each element's two clocks and the skew between them.

    ``DoFn.TimestampParam`` injects the element's **event time** — the timestamp Beam tracks for the
    element (the one ``ReplayEvents`` assigned via ``TimestampedValue``). ``time.time()`` read inside
    ``process`` is the **processing time** — wall-clock at the instant this worker handled the
    element. Their difference is the **skew**; for a live source it is usually positive (we observe
    events after they happen) and varies element to element.
    """

    def process(self, element, event_ts=beam.DoFn.TimestampParam):
        processing_now = time.time()
        event_seconds = float(event_ts)  # Beam Timestamp -> float seconds since the epoch.
        skew = processing_now - event_seconds

        # One human-readable line per element. We log (not print) so it surfaces in the worker logs
        # where Flink collects DoFn output; print() on a worker would not reach the submitter stdout.
        logging.info(
            "id=%s  EVENT_TIME=%.0f  PROCESSING_TIME=%.3f  SKEW=%.3fs  arrival=out-of-order",
            element["id"],
            event_seconds,
            processing_now,
            skew,
        )
        # Pass the enriched element through unchanged so a downstream stage could window it (Ch 9).
        yield {**element, "skew": skew}


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch08-{known.run_id}"

    # BOUNDED chapter: we replay a finite set of events, so no --streaming flag. Even though we are
    # teaching *streaming* concepts, the lesson (two clocks + out-of-order arrival) is fully visible
    # on a bounded PCollection — and bounded keeps the lab fast and free of Kafka.
    options = portable_options(job_name)

    logging.info("Submitting the WHAT/WHERE/WHEN/HOW skew demo as job_name=%s", job_name)
    logging.info("Arrival order is deliberately out-of-order: %s", [EVENTS[i]["id"] for i in EMISSION_ORDER])

    with beam.Pipeline(options=options) as p:
        (
            p
            # WHAT (the transform that produces results). ReplayEvents assigns each record its
            # event time via TimestampedValue and emits them in EMISSION_ORDER so arrival order
            # differs from event-time order — the heart of the demo.
            | "ReplayOutOfOrder" >> ReplayEvents(EVENTS, emission_order=EMISSION_ORDER)
            # Observe both clocks for every element. This is where the skew becomes visible.
            | "LogEventVsProcessingTime" >> beam.ParDo(LogSkew())
        )
        # No WHERE/WHEN/HOW yet: no WindowInto, no trigger, no accumulation. Those are Ch 9-11.
        # The single takeaway here: event time and processing time are different clocks, and the
        # stream does not arrive sorted by either one.

    logging.info("Pipeline finished — read the per-element SKEW lines in the TaskManager logs.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
