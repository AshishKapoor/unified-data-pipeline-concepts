"""Chapter 14 — Splittable DoFn (SDF): The Modern IO Primitive.

A plain ``DoFn`` processes one element atomically: ``process()`` runs start-to-finish and the runner
can only parallelise *across* elements. That is fine for cheap, element-wise work — but it is a poor
fit for a *source*, where a single "element" (a file, a Kafka partition, a numeric range) can be huge
or even infinite. You cannot split one fat element across workers, and you cannot checkpoint halfway
through reading it.

A **Splittable DoFn (SDF)** fixes this by making the *unit of work* finer-grained than an element.
Processing one element is now described by a **restriction** — a portion of the work to do for that
element — that is tracked by a **restriction tracker**. The tracker exposes ``try_claim(position)``:
the ``process()`` body claims positions one at a time, and the runner is free to **split** the
restriction at any moment into:

    * a **primary** (the part this worker keeps and finishes), and
    * a **residual** (the leftover, handed back to the runner to schedule elsewhere).

That single mechanism unlocks the three things vanilla DoFn cannot do:

    1. **Dynamic work rebalancing** — a slow/large element's remaining work can be peeled off and
       given to an idle worker mid-flight (no more stragglers).
    2. **Checkpointing mid-element** — the runner can ``try_split`` at fraction 0.0 to checkpoint the
       residual durably, so an unbounded read can persist progress without finishing the element.
    3. **Bounded *and* unbounded restrictions** — a range ``[0, N)`` is bounded; a "tail a Kafka
       partition forever" restriction is unbounded (``[offset, +inf)``), which (with a watermark
       estimator) is exactly how modern IO connectors — KafkaIO, TextIO, PubSubIO — are built.

This chapter ships a deliberately TOY, *bounded* SDF: a source that "reads" the integer range
``[0, N)`` and emits each offset. It is not useful data — it exists purely to make the restriction /
tracker / ``try_claim`` / split mechanics concrete and runnable on Flink. Real connectors do real IO
inside the same skeleton.

Unbounded restrictions and custom watermark estimators (``WatermarkEstimatorProvider``,
``ManualWatermarkEstimator``) are ADVANCED — we name them here but keep the lab bounded.

Run it:  ./scripts/submit.sh ch14        (or click "Run on Flink" in the Ch 14 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch14-<runId>. On a portable
         runner the read may be expressed as an SDF expansion (PairWithRestriction → SplitAndSize →
         ProcessSizedElementsAndRestrictions); you will see those sub-stages in the job graph.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam

# OffsetRange is the canonical *restriction* type for "a half-open interval of positions" and
# OffsetRestrictionTracker is its matching *restriction tracker* (implements try_claim / try_split).
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker

# RestrictionProvider is the contract that tells Beam how to make/split/size a restriction for an
# element. It is what turns an ordinary DoFn into a Splittable DoFn.
from apache_beam.transforms.core import RestrictionProvider

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402


class RangeRestrictionProvider(RestrictionProvider):
    """Teaches Beam how to handle the work of one element as a splittable OffsetRange.

    The "element" here is an integer N. The work for it is "emit every offset in ``[0, N)``", which
    we model as the restriction ``OffsetRange(0, N)``. A ``RestrictionProvider`` must answer four
    questions for the runner; each maps to one method below.
    """

    def initial_restriction(self, element: int) -> OffsetRange:
        """The FULL restriction for the element before any splitting.

        For element N, the whole job is the half-open range ``[0, N)``. The runner starts every SDF
        invocation with this and may immediately split it for initial parallelism.
        """
        return OffsetRange(0, element)

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        """Wrap a restriction in a tracker that the process() body will claim positions against.

        The tracker is the *stateful* object: it remembers the last claimed position and enforces
        that claims only ever move forward and stay inside the (possibly already-split) range.
        """
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element: int, restriction: OffsetRange) -> int:
        """How "big" is this restriction? Used by the runner to weigh splits / size bundles.

        For an offset range the natural size is simply the number of positions it covers. Real IO
        would return an estimate of bytes or records remaining.
        """
        return restriction.size()

    def split(self, element: int, restriction: OffsetRange):
        """OPTIONAL: pre-split the initial restriction so multiple workers start in parallel.

        Without this the runner can still split *dynamically* at runtime via the tracker's
        ``try_split`` (that is the residual hand-off the animation shows). Here we additionally chop
        the full range into chunks of ~``DESIRED_CHUNK`` offsets so the job fans out immediately.
        ``OffsetRange.split`` yields a sequence of sub-ranges that exactly tile the original.
        """
        DESIRED_CHUNK = 8  # offsets per initial sub-restriction (tiny, so the toy job visibly fans out)
        for sub_range in restriction.split(
            desired_num_offsets_per_split=DESIRED_CHUNK,
            min_num_offsets_per_split=1,
        ):
            yield sub_range


class CountingSource(beam.DoFn):
    """A Splittable DoFn that "reads" the range [0, N) and emits each claimed offset.

    What makes this an SDF rather than a plain DoFn is the ``restriction_tracker`` parameter on
    ``process()``. Its default value, ``beam.DoFn.RestrictionParam(...)``, binds our provider to the
    DoFn; at runtime Beam injects a *live tracker* for whatever (possibly split) sub-range this
    invocation owns.
    """

    def process(
        self,
        element: int,
        # The presence of this RestrictionParam is what upgrades the DoFn to splittable. Beam injects
        # an OffsetRestrictionTracker created by RangeRestrictionProvider.create_tracker().
        restriction_tracker=beam.DoFn.RestrictionParam(RangeRestrictionProvider()),
    ):
        # current_restriction() is the sub-range THIS invocation owns (after any pre/dynamic splits).
        current = restriction_tracker.current_restriction()
        logging.info("SDF invocation processing restriction [%d, %d)", current.start, current.stop)

        # The claim loop is the heart of an SDF. We walk positions from the range start and, for each,
        # ASK the tracker for permission via try_claim(pos):
        #   * returns True  -> the position is ours; do the work (here: emit the offset).
        #   * returns False -> the runner has split the range and this position now belongs to the
        #                      residual; we MUST stop immediately and return. The runner will schedule
        #                      the residual (the primary+residual hand-off in the animation).
        position = current.start
        while restriction_tracker.try_claim(position):
            # "Read" position `position`. A real source would fetch a record / line / Kafka message
            # for this offset; our toy source just emits the offset itself.
            yield position
            position += 1

        # Falling out of the loop means either we claimed the whole range or the runner split it away
        # from under us. Either way this invocation is done; checkpointing the residual (if any) and
        # rescheduling it is the runner's job — we never see it again here.


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    # N is the size of the toy range to "read". Kept small so the offsets are easy to eyeball.
    parser.add_argument("--count", type=int, default=64, help="read the range [0, N)")
    parser.add_argument(
        "--output", default="/tmp/beam-artifact-staging/ch14-offsets"
    )
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch14-{known.run_id}"
    # BOUNDED chapter: the restriction [0, N) is finite, so the pipeline terminates. No --streaming.
    options = portable_options(job_name)

    logging.info(
        "Submitting Splittable DoFn 'read [0,%d)' as job_name=%s to the portable Flink runner",
        known.count,
        job_name,
    )

    with beam.Pipeline(options=options) as p:
        offsets = (
            p
            # A single element N. The SDF turns this ONE element into up-to-N units of splittable work.
            | "TheNumberN" >> beam.Create([known.count])
            # ParDo over our SDF: because CountingSource.process() takes a RestrictionParam, Beam runs
            # the full SDF expansion (pair-with-restriction, split-and-size, process-sized-elements).
            | "ReadRangeViaSDF" >> beam.ParDo(CountingSource())
            # Prove the offsets came through. Group so the written output is deterministic regardless
            # of how many sub-restrictions the runner split the range into.
            | "PairWithOne" >> beam.Map(lambda offset: (offset % 10, offset))
            | "GroupByLastDigit" >> beam.GroupByKey()
            | "Format"
            >> beam.MapTuple(
                lambda digit, offs: f"last-digit {digit}: {sorted(offs)}"
            )
        )
        offsets | "WriteOffsets" >> beam.io.WriteToText(
            known.output, file_name_suffix=".txt"
        )

    logging.info(
        "Pipeline finished. Claimed offsets (grouped) written under %s*.txt", known.output
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
