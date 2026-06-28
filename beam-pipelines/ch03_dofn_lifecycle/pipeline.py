"""Chapter 3 — Element-wise Transforms & the DoFn Lifecycle.

``Map``, ``FlatMap`` and ``Filter`` are all *sugar* over a single primitive: ``beam.ParDo`` driven by
a ``DoFn``. Map is "1 in -> 1 out", FlatMap is "1 in -> N out", Filter is "keep or drop". Underneath,
the runner drives every ``DoFn`` through the same **lifecycle**, and understanding that lifecycle is
the key to writing correct, performant element-wise code on Flink:

    setup()            once per DoFn instance        -> open expensive resources (clients, sessions)
      start_bundle()   once per *bundle*             -> begin a fresh batch / buffer
        process(e)     once per element              -> the actual work; may yield 0..N outputs
        process(e)
        ...
      finish_bundle()  once per *bundle*             -> flush the batch; emit any buffered output
    teardown()         once per DoFn instance        -> close resources (BEST-EFFORT, may not run!)

A **bundle** is the runner's unit of commit and retry. The runner slices the input stream into
bundles (sizes are the runner's choice — on Flink they track checkpoints / network buffers, NOT your
``Create`` list). If any element in a bundle fails, the *whole bundle* is retried, which is why
``process`` must be **idempotent**: re-processing the same element must not double-count or corrupt
downstream state. ``teardown`` is best-effort — a crashed worker may never call it — so never rely on
it for correctness (flush in ``finish_bundle`` instead).

This pipeline makes the lifecycle *visible*. A ``LifecycleDoFn`` logs every callback as it fires, so
when you read the worker logs you can literally see bundle boundaries: one ``setup``, then repeating
``start_bundle -> process×N -> finish_bundle`` groups, then (best-effort) ``teardown``.

Run it:  ./scripts/submit.sh ch03        (or click "Run on Flink" in the Ch 3 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch03-<runId>.

NOTE on logs: ``process``/``setup`` etc. run in the **SDK worker harness**, not the submitter, so
their ``logging`` output lands in the *TaskManager* logs (Flink UI -> TaskManager -> Logs), not the
submitter's stdout. The submitter only sees the messages emitted from ``run()`` itself.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam

# Make the shared _common package importable regardless of CWD (same idiom as ch01).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402

_LOG = logging.getLogger("ch03.lifecycle")


class FakeClient:
    """A stand-in for an expensive, *reusable* resource (DB connection, HTTP session, gRPC channel).

    The whole reason ``setup``/``teardown`` exist is so you open one of these **once per worker** and
    reuse it across many elements — never per-element. We log open/close so they show up in the trace.
    """

    def __init__(self, worker_tag: str):
        self.worker_tag = worker_tag
        self.open = True
        _LOG.info("FakeClient OPEN     (worker=%s)  <- setup() acquired a resource", worker_tag)

    def enrich(self, record: dict) -> dict:
        """Pretend to call out to a service to enrich the record."""
        return {**record, "enriched_by": self.worker_tag}

    def close(self) -> None:
        self.open = False
        _LOG.info("FakeClient CLOSE    (worker=%s)  <- teardown() released the resource", self.worker_tag)


class LifecycleDoFn(beam.DoFn):
    """A DoFn that narrates its own lifecycle so bundle boundaries become observable in the logs.

    Read the log stream top-to-bottom and you'll see the exact ordering guaranteed by Beam:

        setup  ->  [ start_bundle  ->  process*  ->  finish_bundle ]*  ->  teardown

    The repeating bracketed group is one **bundle**. The runner decides how many elements land in
    each bundle; you do not. We buffer within a bundle (``start_bundle`` resets the buffer,
    ``finish_bundle`` flushes it) to model a real "batch then commit" pattern.
    """

    def setup(self):
        # Called ONCE per DoFn instance, before any elements. Open expensive, reusable resources here.
        # A unique tag per worker process lets us tell instances apart in the logs.
        self._client = FakeClient(worker_tag=f"pid{os.getpid()}")
        self._bundle_no = 0
        _LOG.info("setup()             instance ready; bundles will be numbered from 1")

    def start_bundle(self):
        # Called ONCE at the START of every bundle. Initialise per-bundle state (here: a flush buffer).
        # Do NOT emit output from start_bundle — there is no element context yet.
        self._bundle_no += 1
        self._buffer: list = []
        _LOG.info("  start_bundle()    --- begin bundle #%d (buffer cleared) ---", self._bundle_no)

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        # Called ONCE PER ELEMENT. This is where Map/FlatMap/Filter logic would live.
        #   - returning/yielding 1 value   == Map
        #   - yielding N values            == FlatMap
        #   - yielding nothing for some    == Filter
        # Here we *batch* the work: buffer the enriched element now, and only flush in finish_bundle.
        # We still yield the element downstream so the count below reflects real output.
        enriched = self._client.enrich(element)
        self._buffer.append(enriched)
        _LOG.info(
            "    process()       bundle #%d  element=%r  ts=%s  (buffered %d so far)",
            self._bundle_no, element.get("id"), timestamp, len(self._buffer),
        )
        # IDEMPOTENCY: if this bundle is retried, every element is replayed from the start, so this
        # code must be safe to re-run. Appending to a fresh per-bundle buffer is — a write keyed by
        # element id would be too; a non-keyed "INSERT" or a "+= 1" global counter would NOT.
        yield enriched

    def finish_bundle(self):
        # Called ONCE at the END of every bundle, AFTER the last process() in the bundle. This is the
        # correct place to FLUSH batched side effects — it runs on the commit path of the bundle, so a
        # successful flush here is durably tied to the bundle's commit. (Flushing in teardown would be
        # wrong: teardown is best-effort and may never run.)
        _LOG.info(
            "  finish_bundle()   *** flush bundle #%d: %d record(s) committed *** ",
            self._bundle_no, len(self._buffer),
        )
        # finish_bundle MAY emit output, but each value must carry an explicit window+timestamp, so we
        # keep emission in process() above and use finish_bundle purely to flush the batch.

    def teardown(self):
        # Called (BEST-EFFORT) ONCE when the DoFn instance is discarded. A hard worker crash can skip
        # this entirely — so it is for resource hygiene only, never for correctness-critical flushes.
        if getattr(self, "_client", None) is not None:
            self._client.close()
        _LOG.info("teardown()          instance discarded (best-effort cleanup done)")


def make_records(n: int = 20):
    """Build ~n tiny records to feed the DoFn. Bounded input -> a finite, observable set of bundles."""
    fruits = ["apple", "banana", "cherry", "date", "elderberry"]
    return [{"id": i, "fruit": fruits[i % len(fruits)], "qty": (i * 7) % 13} for i in range(n)]


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--num_records", type=int, default=20, help="how many records to Create")
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch03-{known.run_id}"
    options = portable_options(job_name)  # BOUNDED chapter -> no streaming flag.

    records = make_records(known.num_records)
    logging.info("Submitting ch03 lifecycle demo as job_name=%s (%d records)", job_name, len(records))

    with beam.Pipeline(options=options) as p:
        out = (
            p
            # A bounded source: an in-memory list becomes a PCollection. The runner is free to split
            # these elements across workers and into one-or-more bundles however it likes.
            | "CreateRecords" >> beam.Create(records)
            # beam.Filter is sugar for a ParDo that yields the element only when the predicate holds.
            # Keep only non-"date" fruits to show Filter in the same graph as our lifecycle DoFn.
            | "DropDates" >> beam.Filter(lambda r: r["fruit"] != "date")
            # beam.ParDo applied to our DoFn is the *general* form. Map/FlatMap/Filter are all special
            # cases of this. Driving a DoFn through ParDo is what surfaces the full lifecycle.
            | "Lifecycle" >> beam.ParDo(LifecycleDoFn())
            # beam.Map is sugar for a 1->1 ParDo: project each enriched dict to a compact log string.
            | "Format" >> beam.Map(lambda r: f"id={r['id']} fruit={r['fruit']} by={r['enriched_by']}")
        )
        # A no-op sink so the graph has a leaf. We log a sample on the worker side; the bundle-by-bundle
        # narration in the TaskManager logs is the real artifact of this chapter.
        out | "LogSample" >> beam.Map(lambda line: _LOG.info("OUT  %s", line))

    logging.info("Pipeline finished. Open the TaskManager logs to read the per-bundle lifecycle trace.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
