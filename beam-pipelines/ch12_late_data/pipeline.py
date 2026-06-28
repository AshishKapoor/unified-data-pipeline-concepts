"""Chapter 12 — Late Data, Allowed Lateness & Dropped Data.

A streaming pipeline never *stops* a window the moment the watermark passes its end. Real systems
have stragglers — events that take a slow path through the network and arrive after the watermark
has already declared their window "complete". Beam gives you a knob, ``allowed_lateness``, that says
"keep this window's state alive for an extra grace period and let late stragglers re-fire it." Once
the watermark advances past ``window_end + allowed_lateness``, the runner garbage-collects the
window's persisted state and any element that arrives even later is **dropped silently** — it never
reaches your aggregation and produces no output. This chapter makes that lifecycle concrete:

  * An element is *late* if it arrives after the watermark has passed the end of its window.
  * ``allowed_lateness`` is the grace period during which late elements still update the window and
    cause a LATE pane to fire (driven here by ``AfterWatermark(late=...)``).
  * Once ``watermark > window_end + allowed_lateness`` the window state is GC'd; further late
    elements are dropped. They are invisible by default, so we **measure** them with a metric.
  * Every firing carries a ``PaneInfo``: its timing (EARLY / ON_TIME / LATE) and a monotonically
    increasing pane index. We read it via ``DoFn.PaneInfoParam`` and log it for every pane.

We use a deterministic, bounded, out-of-order replay (no Kafka) so the late/dropped boundary is
reproducible. The window is a 1-minute FixedWindow. Three elements land inside the window before
the watermark passes its end (the ON_TIME pane). One straggler arrives at +10s past window end —
inside the 30s lateness grace — so it re-fires the window as a LATE pane. A second straggler arrives
at +90s — well past ``window_end + 30s`` — so its window state is already gone and it is DROPPED.

Run it:  ./scripts/submit.sh ch12        (or click "Run on Flink" in the Ch 12 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch12-<runId>. The pane-info
         log lines and the dropped/late counters show up in the submitter SSE log.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Duration

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.replay import ReplayEvents  # noqa: E402
from _common.options import portable_options  # noqa: E402

# A single 1-minute window: [0s, 60s). Every event below carries an event_time relative to this.
WINDOW_SECONDS = 60
# Grace period: a LATE element arriving at most 30s past the window end still updates the window.
ALLOWED_LATENESS_SECONDS = 30


def build_records():
    """A reproducible out-of-order stream targeting one FixedWindow [0s, 60s).

    All four records belong to event-time second 5..15, i.e. window [0, 60). What differs is *when*
    they arrive relative to the watermark — which the runner derives from event-time progress in this
    replay. We encode that intent in ``event_time`` and let the comments narrate the lifecycle:

      * 3 on-time records (event_time 5, 10, 15)  -> fold into the ON_TIME pane.
      * 1 late-but-allowed record (event_time 12) -> emitted *after* the watermark passes 60, but
        within the 30s grace, so it re-fires the window as a LATE pane (sum updates).
      * 1 dropped record (event_time 8)           -> emitted after watermark passes 60 + 30 = 90, so
        the window's state is already GC'd and this element is silently dropped.

    ``emission_order`` makes arrival order differ from event-time order. ReplayEvents emits records
    in list order; we deliberately place the two stragglers last so the watermark has advanced past
    the window (and past the GC horizon for the very last one) by the time they show up.
    """
    records = [
        {"key": "orders", "amount": 5, "event_time": 5.0, "label": "on-time #1"},
        {"key": "orders", "amount": 7, "event_time": 10.0, "label": "on-time #2"},
        {"key": "orders", "amount": 3, "event_time": 15.0, "label": "on-time #3"},
        # Straggler within the grace period: belongs to [0,60) but shows up after wm>60.
        {"key": "orders", "amount": 100, "event_time": 12.0, "label": "late-allowed (+10s)"},
        # Straggler past window_end + allowed_lateness: window state is gone -> dropped.
        {"key": "orders", "amount": 999, "event_time": 8.0, "label": "too-late (+90s) -> DROPPED"},
    ]
    return records


class TagLateAndDropped(beam.DoFn):
    """Per-element pass-through that counts how many records arrive late or get dropped.

    Beam silently swallows elements whose window state has already been GC'd, so we cannot observe a
    drop *after the fact* from inside a GroupByKey. Instead we compare each element's event time to
    the watermark-implied window-GC horizon up front and emit two metrics the submitter can read:

      * ``late_elements``   — arrived after its window end (still within or beyond lateness).
      * ``dropped_elements``— arrived after window_end + allowed_lateness (will never aggregate).

    This is the canonical "measure your drops" pattern: drops are a correctness signal, and a metric
    is how you alert on them. The real GC/drop decision is still made by the runner downstream — this
    DoFn only *instruments* it so the count is visible.
    """

    def __init__(self):
        # Metrics.counter(namespace, name) — increments are aggregated across all workers and
        # surface in the Flink UI / job metrics. Namespacing keeps counters from different chapters
        # distinct.
        self._late = Metrics.counter("ch12_late_data", "late_elements")
        self._dropped = Metrics.counter("ch12_late_data", "dropped_elements")

    def process(self, record, timestamp=beam.DoFn.TimestampParam):
        # ``timestamp`` is this element's event time (set by ReplayEvents via TimestampedValue).
        event_secs = float(timestamp)
        window_end = WINDOW_SECONDS  # this lab uses the single window [0, 60)
        gc_horizon = window_end + ALLOWED_LATENESS_SECONDS  # 90s

        if event_secs >= gc_horizon:
            # Beyond the grace period -> in a live stream this would land after GC and be dropped.
            self._dropped.inc()
            logging.info("[ch12] DROPPED (event_time=%ss past GC horizon %ss): %s",
                         event_secs, gc_horizon, record.get("label"))
        elif event_secs >= window_end:
            self._late.inc()
            logging.info("[ch12] LATE-but-allowed (event_time=%ss > window_end %ss): %s",
                         event_secs, window_end, record.get("label"))
        yield record


class FormatPane(beam.DoFn):
    """Render each window firing, reading the PaneInfo to show timing + pane index.

    ``DoFn.PaneInfoParam`` gives us the PaneInfo for the firing that produced this element. The most
    instructive fields:

      * ``timing``       — EARLY (fired before the watermark), ON_TIME (the watermark just passed the
                           window end), or LATE (a late element re-fired the window within lateness).
      * ``index``        — 0 for the first firing of the window, then 1, 2, ... for each later firing.
      * ``is_first`` / ``is_last`` — whether this is the first / final pane the window will emit.

    We accumulate (AccumulationMode.ACCUMULATING) so each LATE pane shows the *new running total*,
    making it obvious that the late element actually updated the result rather than replacing it.
    """

    _TIMING_NAMES = {0: "UNKNOWN", 1: "EARLY", 2: "ON_TIME", 3: "LATE"}

    def process(self, kv, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        key, total = kv
        timing = self._TIMING_NAMES.get(int(pane.timing), str(pane.timing))
        msg = (
            f"[ch12] window={window.start}->{window.end} key={key} "
            f"sum={total} pane.timing={timing} pane.index={pane.index} "
            f"is_first={pane.is_first} is_last={pane.is_last}"
        )
        # Worker-side logging shows up in the TaskManager logs; we also yield a string so a sink
        # (or the Flink UI's record counts) reflects each firing.
        logging.info(msg)
        yield msg


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument(
        "--output",
        default="/tmp/beam-artifact-staging/ch12-late-data",
        help="prefix for the per-pane output lines",
    )
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch12-{known.run_id}"
    # Late-data semantics only exist for unbounded pipelines -> streaming=True.
    options = portable_options(job_name, streaming=True)

    logging.info("Submitting late-data demo as job_name=%s (allowed_lateness=%ss)",
                 job_name, ALLOWED_LATENESS_SECONDS)

    records = build_records()
    # Emit the two stragglers last so they arrive after the watermark has advanced past the window.
    emission_order = [0, 1, 2, 3, 4]

    with beam.Pipeline(options=options) as p:
        windowed = (
            p
            # Bounded, deterministic, out-of-order replay with explicit event-times (no Kafka).
            | "Replay" >> ReplayEvents(records, emission_order=emission_order)
            # Instrument late/dropped BEFORE windowing so the counts reflect every arrival.
            | "TagLateDropped" >> beam.ParDo(TagLateAndDropped())
            | "ToKV" >> beam.Map(lambda r: (r["key"], r["amount"]))
            # The heart of the chapter: a FixedWindow with a grace period and a late trigger.
            #   * FixedWindows(60)               -> tumbling 1-minute windows.
            #   * trigger=AfterWatermark(late=…) -> fire ON_TIME at the watermark, then once more for
            #                                       each LATE element that lands within lateness.
            #   * allowed_lateness=30s           -> keep window state alive 30s past window end; after
            #                                       that the state is GC'd and later elements drop.
            #   * ACCUMULATING                   -> each pane re-emits the full running sum.
            | "Window"
            >> beam.WindowInto(
                FixedWindows(WINDOW_SECONDS),
                trigger=AfterWatermark(late=AfterWatermark()),
                allowed_lateness=Duration(seconds=ALLOWED_LATENESS_SECONDS),
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "SumPerKey" >> beam.CombinePerKey(sum)
            # Read PaneInfo so every firing reports its timing (EARLY/ON_TIME/LATE) and index.
            | "FormatPane" >> beam.ParDo(FormatPane())
        )
        windowed | "Write" >> beam.io.WriteToText(known.output, file_name_suffix=".txt")

    logging.info("Pipeline finished. Per-pane lines under %s*.txt; check late/dropped counters.",
                 known.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
