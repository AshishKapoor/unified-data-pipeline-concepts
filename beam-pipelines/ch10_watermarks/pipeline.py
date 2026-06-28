"""Chapter 10 — Watermarks: How the System Knows Event Time Advanced.

A **watermark** ``W(t)`` is the runner's promise at processing-time ``t``:

    "I believe I have now seen every element with event time <= W(t)."

It is the system's *estimate of completeness for the past*. Nothing magical produces it — a source
emits a watermark, and every operator downstream advances its own watermark as data flows. Two facts
make watermarks the heartbeat of streaming:

1. **Perfect vs heuristic.** If a source can prove no earlier data exists (a replayed file, a
   monotonic Kafka offset with bounded skew) its watermark is *perfect*. Real sources usually emit a
   *heuristic* watermark — a best guess. A heuristic that runs ahead of reality is what creates
   *late data* (Ch 12); one that lags wastes latency.

2. **Propagation = MIN across input channels.** An operator with several inputs (e.g. the shuffle
   that feeds a GroupByKey from many upstream partitions) can only claim completeness up to the
   *slowest* input. Its output watermark is ``min`` over all input channels. One stalled input drags
   the whole min down — and if that input is simply **idle** (no data, but not finished), the min
   freezes forever unless the source marks it idle (``withIdleness`` in Flink) so it stops voting.

When the watermark passes a window's end, Beam knows that window is complete and **fires** it
(default ``AfterWatermark`` trigger, Ch 11). So watermark advance is literally *what closes windows*.

This pipeline makes the watermark *visibly trail* processing time: ``SyntheticEvents(lag_sec=...)``
stamps each event ``lag_sec`` seconds in the past, so the source watermark is always behind the wall
clock. We put events into fixed windows and a DoFn logs, for each firing, how far the firing pane's
window-end sits behind "now" — i.e. how long the watermark took to cross it.

Watch it: open the Flink Web UI (http://localhost:8081), click this job, and inspect the
*per-operator watermarks* on the operator subtasks. You will see the WindowInto/GroupByKey operators
carrying a "Low Watermark" that lags wall-clock by roughly ``lag_sec`` and only advances as the
source's watermark advances — the min-across-inputs rule in action.

Run it:  ./scripts/submit.sh ch10        (or click "Run on Flink" in the Ch 10 docs)
Watch :  the Flink Web UI at http://localhost:8081 — job ch10-<runId>; open a window operator and
         read its "Low Watermark" metric per subtask.
"""
from __future__ import annotations

import argparse
import logging
import sys
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.transforms import combiners
from apache_beam.transforms.window import FixedWindows

# Make the shared _common package importable regardless of CWD (same idiom as Ch 1).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402
from _common.synthetic_source import SyntheticEvents  # noqa: E402


def _iso(unix_seconds: float) -> str:
    """Format a unix-second timestamp as HH:MM:SS UTC for readable logs."""
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc).strftime("%H:%M:%S")


class LogWindowFiring(beam.DoFn):
    """Log each per-key window result *relative to the watermark*.

    A keyed window only reaches this DoFn once the runner has decided the window is complete —
    which (with the default trigger) means the **watermark has passed the window's end**. So the
    moment we see an element here is, by definition, "watermark > window_end". We log:

      * the window's [start, end) in event time,
      * the count for that (key, window),
      * how far behind processing-time the window end was when it fired — a direct readout of how
        much the watermark trailed the wall clock (driven by ``lag_sec`` in the source).

    Note: this runs *on the worker*, so these lines appear in the TaskManager logs, not the
    submitter stdout. The submitter only sees the lifecycle logging.info calls in ``run``.
    """

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
        pane=beam.DoFn.PaneInfoParam,
    ):
        key, total = element
        # ``window`` exposes the fixed window's bounds as Beam Timestamps (seconds since epoch).
        win_start = float(window.start)
        win_end = float(window.end)
        now = datetime.now(tz=timezone.utc).timestamp()
        # Because the watermark crossing win_end is what triggered this firing, (now - win_end) is
        # roughly the watermark's lateness vs the wall clock at the instant the window closed.
        watermark_trail = now - win_end
        logging.info(
            "WINDOW FIRED key=%s window=[%s, %s) count=%d "
            "pane(is_first=%s, timing=%s) ~watermark_trail=%.1fs",
            key,
            _iso(win_start),
            _iso(win_end),
            total,
            pane.is_first,
            pane.timing,
            watermark_trail,
        )
        yield element


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    # event_time_lag: how far the source stamps events into the past. A bigger lag = a watermark
    # that trails wall-clock further, so windows close later. This is the dial of this chapter.
    parser.add_argument("--event_time_lag", type=float, default=8.0)
    parser.add_argument("--window_size", type=float, default=10.0)
    parser.add_argument("--events_per_sec", type=float, default=4.0)
    parser.add_argument("--duration_sec", type=float, default=120.0)
    parser.add_argument("--key_cardinality", type=int, default=3)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch10-{known.run_id}"
    # streaming=True: an unbounded pipeline. Watermarks only mean something for unbounded data —
    # a bounded source jumps its watermark straight to +infinity when it is exhausted.
    options = portable_options(job_name, streaming=True)

    logging.info(
        "Submitting watermark demo job_name=%s (lag=%.1fs, window=%.1fs). "
        "Open the Flink UI and read per-operator Low Watermark.",
        job_name,
        known.event_time_lag,
        known.window_size,
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            # An unbounded stream whose event-times trail processing time by ``event_time_lag``.
            # That trailing is exactly why the source's watermark sits behind the wall clock.
            | "SyntheticEvents"
            >> SyntheticEvents(
                events_per_sec=known.events_per_sec,
                duration_sec=known.duration_sec,
                key_cardinality=known.key_cardinality,
                lag_sec=known.event_time_lag,  # event_time = processing_time - lag
            )
            # Key by sensor so the downstream GroupByKey/Combine creates a *keyed* operator whose
            # per-key, per-window state is GC'd once the watermark passes the window end.
            | "KeyBySensor" >> beam.Map(lambda e: (e["key"], e["value"]))
            # Chop the unbounded stream into fixed, non-overlapping event-time windows. The window
            # boundaries are pure event-time math; nothing here mentions watermarks. The watermark
            # only decides *when* each of these windows is considered complete.
            | "FixedWindows" >> beam.WindowInto(FixedWindows(known.window_size))
            # Aggregate per (key, window). With the default AfterWatermark trigger, each group is
            # emitted exactly once — the moment the watermark crosses that window's end.
            | "CountPerKeyWindow" >> combiners.Count.PerKey()
            # Log the firing relative to the watermark. This is where you *see* watermark advance:
            # firings appear ~event_time_lag seconds after each window's wall-clock end.
            | "LogFiring" >> beam.ParDo(LogWindowFiring())
        )

    logging.info("Pipeline finished (source duration elapsed). job_name=%s", job_name)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
