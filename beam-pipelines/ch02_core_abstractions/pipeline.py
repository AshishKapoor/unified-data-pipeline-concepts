"""Chapter 2 — Core Abstractions: Pipeline, PCollection, PTransform, PValue.

Beam has only four load-bearing nouns, and once they click the whole API reads cleanly:

  * ``Pipeline``    — the container / directed graph you attach transforms to. Nothing runs
                      when you build it; construction is *deferred* until the ``with`` block exits.
  * ``PValue``      — anything that flows through the graph. The most common kind is a...
  * ``PCollection`` — an immutable, distributed, possibly-unbounded "bag" of elements. It is NOT a
                      list: you cannot index it, its order is not defined, and it carries a *coder*
                      (how to serialize elements between workers) plus a *windowing strategy*.
  * ``PTransform``  — a node that consumes PValues and produces PValues. You compose them with the
                      ``pcoll | "Label" >> transform`` operator. A *composite* PTransform is one you
                      build by subclassing ``beam.PTransform`` and overriding ``expand()`` to wire
                      together smaller inner transforms — that is how reuse and abstraction work.

The second half of the chapter is the **anatomy of an element**. A PCollection element is never a
bare value. Conceptually every element is a 4-tuple:

    (value, event-timestamp, window(s), pane-info)

Even in a plain bounded pipeline these fields exist. We expose them with a ``DoFn`` that asks the
runner to inject the implicit timestamp and window via ``beam.DoFn.TimestampParam`` and
``beam.DoFn.WindowParam``. Records you ``beam.Create`` with no explicit timestamp land at the
"minimum timestamp" sentinel and in the single ``GlobalWindow`` — and seeing that surprising default
is the whole point.

Run it:  ./scripts/submit.sh ch02        (or click "Run on Flink" in the Ch 2 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch02-<runId>.

Note: the per-element logging below runs *on the workers*, so its output shows up in the
TaskManager / SDK-harness logs, not necessarily in the submitter's stdout. The submitter-visible
``logging.info`` lines (graph shape, element count) are emitted from the construction code.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam

# Make the shared _common package importable regardless of CWD (identical to ch01).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402


# ---------------------------------------------------------------------------
# A custom COMPOSITE PTransform.
#
# By subclassing beam.PTransform and overriding expand(), we package two inner
# transforms (a Map then a Filter) behind ONE reusable node. To the caller this
# looks like a single step:  records | "ScoreAndKeep" >> ScoreAndKeep(...).
# Inside, expand() receives the *input PCollection* and must return a PValue
# (here, the output PCollection). This is exactly how Beam's own built-ins
# (e.g. combiners.Count) are constructed — composites all the way down.
# ---------------------------------------------------------------------------
class ScoreAndKeep(beam.PTransform):
    """Enrich each record with a score (Map), then drop low scorers (Filter).

    Demonstrates the two defining features of a composite:
      1. a human-readable label is attached at the call site with ``>>``;
      2. ``expand()`` wires inner transforms, each with its own nested label,
         which Beam renders as a collapsible sub-graph in the Flink/Dataflow UI.
    """

    def __init__(self, threshold: int):
        super().__init__()
        self._threshold = threshold

    def expand(self, pcoll):
        # ``pcoll`` is the input PCollection. We return another PCollection.
        # Note how the inner labels ("AddScore", "DropLowScores") nest UNDER
        # whatever label the caller gave this composite ("ScoreAndKeep").
        return (
            pcoll
            | "AddScore" >> beam.Map(self._add_score)            # 1 -> 1 (enrich)
            | "DropLowScores" >> beam.Filter(                    # 1 -> 0 or 1 (keep)
                lambda rec: rec["score"] >= self._threshold
            )
        )

    @staticmethod
    def _add_score(rec: dict) -> dict:
        # PCollections are IMMUTABLE: never mutate the incoming element in place.
        # Build and return a NEW dict so we don't rely on undefined aliasing
        # behaviour across the distributed runtime.
        score = len(rec["city"]) * rec["qty"]
        return {**rec, "score": score}


# ---------------------------------------------------------------------------
# A DoFn that reveals the implicit ANATOMY of every element.
#
# We never set timestamps or windows ourselves, yet the runner still gives each
# element all four fields. By declaring the special default parameters below,
# Beam injects the per-element metadata at call time:
#   * beam.DoFn.TimestampParam -> the event-timestamp
#   * beam.DoFn.WindowParam    -> the window(s) the element belongs to
#   * beam.DoFn.PaneInfoParam  -> firing/pane metadata (trivial in batch)
# ---------------------------------------------------------------------------
class LogElementAnatomy(beam.DoFn):
    """Log value + event-timestamp + window + pane-info for each element.

    This is a pass-through DoFn: it yields the element unchanged so the pipeline
    can continue, but as a side effect it prints the four anatomy layers. For
    bounded data created via ``beam.Create`` with no explicit timestamp, expect
    the timestamp to be the MIN_TIMESTAMP sentinel and the window to be the
    single ``GlobalWindow`` — the defaults every Beam element starts life with.
    """

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,   # injected event-timestamp
        window=beam.DoFn.WindowParam,         # injected window(s)
        pane=beam.DoFn.PaneInfoParam,         # injected pane-info
    ):
        logging.info(
            "ELEMENT anatomy | value=%r | event_ts=%s | window=%s | pane=%s",
            element,
            timestamp,          # e.g. Timestamp(-9223372036854.775808) == MIN_TIMESTAMP
            window,             # e.g. GlobalWindow
            pane,               # e.g. PaneInfo(first: True, last: True, timing: UNKNOWN, ...)
        )
        # Pass the element through untouched — a DoFn may emit zero, one, or many
        # outputs; here exactly one, preserving the stream.
        yield element


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    # Keep low scorers at/above this threshold (city-length * qty).
    parser.add_argument("--threshold", type=int, default=20)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch02-{known.run_id}"
    # Bounded chapter -> no streaming flag.
    options = portable_options(job_name)

    # A handful of in-memory records. beam.Create turns a plain Python iterable
    # into a bounded PCollection — the simplest possible PValue source.
    records = [
        {"city": "Oslo", "qty": 7},
        {"city": "Lisbon", "qty": 2},
        {"city": "Reykjavik", "qty": 4},
        {"city": "Bern", "qty": 9},
        {"city": "Copenhagen", "qty": 1},
    ]

    logging.info("Submitting ch02 core-abstractions demo as job_name=%s", job_name)
    logging.info(
        "Graph: Create(%d records) -> ScoreAndKeep(threshold=%d) -> LogAnatomy",
        len(records),
        known.threshold,
    )

    # Building the Pipeline only RECORDS the graph; nothing executes until the
    # 'with' block exits (deferred execution). That is why we can reference
    # 'kept' before any data has actually moved.
    with beam.Pipeline(options=options) as p:
        # p is the Pipeline (a PValue 'PBegin' when used as a source).
        created = p | "Create" >> beam.Create(records)          # PCollection[dict]

        # One composite node hides two inner transforms (Map + Filter).
        kept = created | "ScoreAndKeep" >> ScoreAndKeep(known.threshold)

        # Reveal the implicit element anatomy for whatever survived the filter.
        _ = kept | "LogAnatomy" >> beam.ParDo(LogElementAnatomy())

    logging.info(
        "Pipeline finished. Check the TaskManager / SDK-harness logs for the "
        "per-element 'ELEMENT anatomy' lines (value / event_ts / window / pane)."
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
