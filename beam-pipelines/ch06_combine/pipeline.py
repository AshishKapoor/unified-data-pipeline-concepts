"""Chapter 6 — Efficient Aggregation: Combine, CombineFn, Built-ins.

The previous chapter shuffled *every* element to a key with ``GroupByKey`` and then reduced the
grouped iterable. That works, but it ships all the raw data across the network before any reduction
happens. ``Combine`` does better.

The trick is **combiner lifting** (a.k.a. partial / pre-aggregation): if your reduction is
**associative** and **commutative**, the runner can apply it *twice* — once locally per bundle,
*before* the shuffle, and once globally, *after* it. Each worker emits a tiny **accumulator**
(here: a ``(sum, count)`` pair) instead of every raw reading, so far fewer bytes cross the wire.
Flink calls the local half a "combiner" and inserts it for you precisely because the ``CombineFn``
promises associativity/commutativity.

A ``CombineFn`` is defined by **four methods** — memorize this shape, it is the heart of the chapter:

    1. create_accumulator()              -> a fresh, empty accumulator (the identity)
    2. add_input(acc, element)           -> fold one input into an accumulator (runs pre-shuffle)
    3. merge_accumulators([acc, ...])    -> combine partials (runs both pre- AND post-shuffle)
    4. extract_output(acc)               -> turn the final accumulator into the answer

This pipeline computes the **mean reading per sensor** two ways:
  * a hand-written ``AverageFn(beam.CombineFn)`` so you can see all four methods, and
  * the built-in ``beam.combiners.Mean.PerKey()`` — to prove they produce the *same* result.

Run it:  ./scripts/submit.sh ch06        (or click "Run on Flink" in the Ch 6 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch06-<runId>. Note how the
         CombinePerKey operator is split into a "partial" pre-shuffle stage and a "final" stage.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam import combiners

# Make the shared _common package importable regardless of CWD (identical to Ch 1).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402


# A small, deterministic batch of (sensor_id, reading) samples. In a real job these would come
# from a source (Kafka, files, …); inlining them keeps the chapter self-contained and the focus on
# the aggregation itself.
SAMPLE_READINGS = [
    ("sensor-a", 21.0),
    ("sensor-a", 23.0),
    ("sensor-a", 25.0),   # sensor-a mean = 23.0
    ("sensor-b", 10.0),
    ("sensor-b", 20.0),   # sensor-b mean = 15.0
    ("sensor-c", 100.0),
    ("sensor-c", 102.0),
    ("sensor-c", 98.0),
    ("sensor-c", 100.0),  # sensor-c mean = 100.0
]


class AverageFn(beam.CombineFn):
    """Compute an arithmetic mean as a CombineFn.

    The accumulator is a ``(sum, count)`` tuple. Crucially it is *bounded in size* — no matter how
    many readings a sensor has, the accumulator is always two numbers. That is what lets the runner
    "lift" the combiner: it can reduce a million readings on a worker down to one tiny pair before
    the shuffle ever happens.

    Mean is associative+commutative *when carried as (sum, count)*: you cannot average averages, but
    you can add sums and add counts in any order and regroup freely. Picking the right accumulator
    shape is the whole art of writing a CombineFn.
    """

    def create_accumulator(self):
        # (1) The identity/empty accumulator: zero sum, zero count. merge_accumulators with this
        #     must leave the other accumulator unchanged — that is what "identity" means.
        return (0.0, 0)

    def add_input(self, accumulator, element):
        # (2) Fold a single reading into the running accumulator. This is the hot path that the
        #     runner executes *per element, per bundle, before the shuffle* (the "lifting").
        running_sum, count = accumulator
        return (running_sum + element, count + 1)

    def merge_accumulators(self, accumulators):
        # (3) Combine many partial accumulators into one. The runner calls this BOTH to fold the
        #     per-bundle partials on each worker AND to fold the post-shuffle partials per key.
        #     Because we only ever add, the result is independent of grouping/order.
        sums, counts = zip(*accumulators)
        return (sum(sums), sum(counts))

    def extract_output(self, accumulator):
        # (4) Turn the final accumulator into the user-visible answer. Guard against the empty
        #     accumulator (a key with no inputs) to avoid a divide-by-zero.
        running_sum, count = accumulator
        return running_sum / count if count else float("nan")


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument(
        "--output",
        default="/tmp/beam-artifact-staging/ch06-averages",
        help="output path prefix for the formatted per-sensor means",
    )
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch06-{known.run_id}"
    options = portable_options(job_name)  # bounded chapter -> no streaming flag

    logging.info("Submitting Combine demo as job_name=%s to the portable Flink runner", job_name)

    with beam.Pipeline(options=options) as p:
        # The keyed source: a PCollection of (sensor_id, reading) pairs.
        readings = p | "SensorReadings" >> beam.Create(SAMPLE_READINGS)

        # --- Path A: our custom CombineFn ---------------------------------------------------------
        # CombinePerKey runs AverageFn per key. The runner is free to insert a pre-shuffle partial
        # combine because AverageFn is associative/commutative — that is combiner lifting in action.
        custom_means = (
            readings
            | "AverageByHand" >> beam.CombinePerKey(AverageFn())
            | "TagCustom" >> beam.MapTuple(lambda sensor, mean: (sensor, ("custom", round(mean, 4))))
        )

        # --- Path B: the built-in, to prove equivalence -------------------------------------------
        # beam.combiners.Mean.PerKey IS a CombineFn with the very same (sum, count) accumulator under
        # the hood. We run it on the identical input and expect identical numbers.
        builtin_means = (
            readings
            | "AverageBuiltin" >> combiners.Mean.PerKey()
            | "TagBuiltin" >> beam.MapTuple(lambda sensor, mean: (sensor, ("builtin", round(mean, 4))))
        )

        # Union both labelled streams and format them so the log/output shows the two paths agree.
        formatted = (
            (custom_means, builtin_means)
            | "MergeBothPaths" >> beam.Flatten()
            | "Format" >> beam.MapTuple(
                lambda sensor, tagged: f"{sensor} [{tagged[0]:>7}] mean={tagged[1]}"
            )
        )

        formatted | "WriteAverages" >> beam.io.WriteToText(known.output, file_name_suffix=".txt")

        # A couple of built-ins worth knowing, computed globally over all readings:
        #   * Count.Globally  — how many readings did we see in total?
        #   * Top.Of          — the N largest readings (a CombineFn that keeps a bounded heap).
        # These are pure aggregations too, so they lift exactly like the mean above.
        values = readings | "DropKeys" >> beam.Values()
        (
            values
            | "CountAll" >> combiners.Count.Globally()
            | "LogCount" >> beam.Map(lambda n: logging.info("total readings = %d", n))
        )
        (
            values
            | "Top3Readings" >> combiners.Top.Of(3)
            | "LogTop" >> beam.Map(lambda top: logging.info("top-3 readings = %s", top))
        )

    logging.info("Pipeline finished. Per-sensor means written under %s*.txt", known.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
