"""Chapter 11 — Triggers & Accumulation Modes.

A window decides *which* events belong together (Ch 9). The **watermark** decides when those
events are probably all in (Ch 10). A **trigger** is the third leg: it decides *WHEN* a window
emits a result — and a window may emit more than once, producing a sequence of **panes**.

Two questions this chapter answers:

1. WHEN does the window fire?  ``AfterWatermark`` is the default skeleton:

       trigger = AfterWatermark(
           early=AfterProcessingTime(10),  # speculative early panes while data trickles in
           late=AfterCount(1),             # one extra pane per late element after the watermark
       )

   - ``early=``  fires *before* the watermark passes the end of the window — speculative results.
     Here every 10 seconds of processing time. (Could also be ``AfterCount(n)``.)
   - the **on-time** pane fires exactly once, when the watermark crosses the window's end.
   - ``late=``   fires *after* the on-time pane, for elements that arrive late but within
     ``allowed_lateness``. Here one pane per late element (``AfterCount(1)``).

   Other combinators you can compose: ``Repeatedly`` (fire forever), ``AfterAny`` / ``AfterAll``
   (fire when any / all sub-triggers are ready), ``AfterEach`` (run sub-triggers in sequence).

2. HOW do successive panes relate?  The **accumulation mode**:

   - ``ACCUMULATING`` — each pane re-emits the running total *including all prior data*. Pane
     values grow monotonically: 3, 7, 12, ...  Good when the sink overwrites by key (upsert).
   - ``DISCARDING`` — each pane emits only the *delta* since the previous pane: 3, 4, 5, ...
     Sum the panes downstream to recover the total. Good when the sink appends/adds.

   Same triggers, same data — only the accumulation mode differs, and the pane values differ
   completely. This pipeline runs the *identical* windowed sum twice (one branch per mode) so you
   can read the two streams side by side in the log and see cumulative-vs-delta with your own eyes.

We log ``DoFn.PaneInfoParam`` for every emitted pane so you can see the timing classification
(EARLY / ON_TIME / LATE), whether it's the first/last pane, and the pane index.

streaming=True: triggers only matter for unbounded pipelines, so we drive the window with the
shared ``SyntheticEvents`` source (PeriodicImpulse under the hood) and inject a small event-time
lag so the watermark visibly trails processing time and early panes get a chance to fire.

Run it:  ./scripts/submit.sh ch11        (or click "Run on Flink" in the Ch 11 docs)
         ./scripts/submit.sh ch11 -- --mode accumulating   # restrict to one branch if you like
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch11-<runId>.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterProcessingTime,
    AfterWatermark,
)
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.windowed_value import PaneInfoTiming

# Human-readable names for the integer pane-timing enum (EARLY / ON_TIME / LATE / UNKNOWN).
_TIMING_NAMES = {
    PaneInfoTiming.EARLY: "EARLY",
    PaneInfoTiming.ON_TIME: "ON_TIME",
    PaneInfoTiming.LATE: "LATE",
    PaneInfoTiming.UNKNOWN: "UNKNOWN",
}

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402
from _common.synthetic_source import SyntheticEvents  # noqa: E402

# How long each fixed window is, in seconds. Short so a finite lab fills several windows.
WINDOW_SECONDS = 30
# How long late data is still accepted after the watermark passes the window end.
ALLOWED_LATENESS_SECONDS = 60


def _build_trigger():
    """The reusable trigger skeleton shared by both accumulation branches.

    AfterWatermark gives us the canonical early / on-time / late structure:
      * early = AfterProcessingTime(10): a speculative pane at most every 10s of processing time
        while the window is still open (so dashboards aren't blank waiting for the watermark).
      * the on-time pane fires once, when the watermark crosses the end of the window.
      * late  = AfterCount(1): after the on-time pane, emit one more pane for *every* late element
        that still lands inside allowed_lateness.
    """
    return AfterWatermark(
        early=AfterProcessingTime(10),
        late=AfterCount(1),
    )


class LogPane(beam.DoFn):
    """Emit a human-readable line per pane, exposing PaneInfo so the firing class is visible.

    ``DoFn.PaneInfoParam`` carries the pane's metadata: timing (EARLY / ON_TIME / LATE / UNKNOWN),
    whether it is the first/last pane of the window, the pane index (0,1,2,...), and how many
    on-time panes have fired. This is the single most useful debugging tool for triggers.
    """

    def __init__(self, mode_label: str):
        self._mode = mode_label

    def process(
        self,
        kv,
        window=beam.DoFn.WindowParam,
        pane=beam.DoFn.PaneInfoParam,
    ):
        key, total = kv
        # Map the integer timing enum to a readable tag (EARLY / ON_TIME / LATE / UNKNOWN).
        timing = _TIMING_NAMES.get(pane.timing, str(pane.timing))
        window_end = float(window.end)  # window end as a unix-seconds float
        msg = (
            f"[{self._mode:>11}] key={key} window_end={window_end:.0f} "
            f"pane#{pane.index} timing={timing} "
            f"first={pane.is_first} last={pane.is_last} "
            f"=> value={total}"
        )
        # logging.info is worker-side here; the contrast is easiest to read in the Flink TaskManager
        # logs. We also yield the line so any downstream sink (or the Direct runner) can surface it.
        logging.info(msg)
        yield msg


def windowed_sum(events, *, accumulation_mode, mode_label: str):
    """One windowed sum branch: FixedWindows + the shared trigger + a chosen accumulation mode.

    The ONLY difference between the two branches is ``accumulation_mode`` — everything else
    (window, trigger, allowed_lateness) is identical. That isolation is the whole lesson: the same
    firings produce cumulative totals under ACCUMULATING and per-pane deltas under DISCARDING.
    """
    return (
        events
        # Pair each event with its numeric value so we can sum per key.
        | f"ToKV-{mode_label}" >> beam.Map(lambda e: (e["key"], e["value"]))
        # The window + trigger + accumulation policy. FixedWindows of WINDOW_SECONDS; the shared
        # AfterWatermark trigger; allowed_lateness keeps the window state alive for late panes.
        | f"Window-{mode_label}"
        >> beam.WindowInto(
            FixedWindows(WINDOW_SECONDS),
            trigger=_build_trigger(),
            accumulation_mode=accumulation_mode,
            allowed_lateness=ALLOWED_LATENESS_SECONDS,
        )
        # Sum per key per window. Under ACCUMULATING each firing re-sums all data seen so far for
        # the window; under DISCARDING the runner clears accumulated state after each firing, so a
        # firing only sums what arrived since the previous pane.
        | f"SumPerKey-{mode_label}" >> beam.CombinePerKey(sum)
        # Surface each pane with its PaneInfo so you can read EARLY/ON_TIME/LATE in the logs.
        | f"LogPane-{mode_label}" >> beam.ParDo(LogPane(mode_label))
    )


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    # "both" (default) runs the two branches in one pipeline for a side-by-side contrast; pass
    # "accumulating" or "discarding" to focus on a single branch.
    parser.add_argument(
        "--mode",
        default="both",
        choices=["both", "accumulating", "discarding"],
    )
    parser.add_argument("--events_per_sec", type=float, default=4.0)
    parser.add_argument("--duration_sec", type=float, default=150.0)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch11-{known.run_id}"
    # Triggers are a streaming-only concern: bounded pipelines fire exactly once at end-of-data.
    options = portable_options(job_name, streaming=True)

    logging.info(
        "Submitting trigger/accumulation demo job_name=%s mode=%s window=%ss lateness=%ss",
        job_name,
        known.mode,
        WINDOW_SECONDS,
        ALLOWED_LATENESS_SECONDS,
    )

    with beam.Pipeline(options=options) as p:
        # Unbounded synthetic stream. lag_sec makes event time trail processing time so the
        # watermark lags — which is what lets early (speculative) panes fire before on-time.
        events = p | "Source" >> SyntheticEvents(
            events_per_sec=known.events_per_sec,
            duration_sec=known.duration_sec,
            key_cardinality=2,
            lag_sec=5.0,
        )

        if known.mode in ("both", "accumulating"):
            windowed_sum(
                events,
                accumulation_mode=AccumulationMode.ACCUMULATING,
                mode_label="ACCUMULATING",
            )

        if known.mode in ("both", "discarding"):
            windowed_sum(
                events,
                accumulation_mode=AccumulationMode.DISCARDING,
                mode_label="DISCARDING",
            )

    logging.info("Pipeline finished. Compare ACCUMULATING (cumulative) vs DISCARDING (deltas) panes.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
