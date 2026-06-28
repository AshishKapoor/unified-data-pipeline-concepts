"""Chapter 7 — Routing Data: Partition, Side Inputs, Tagged Outputs.

Most real pipelines do not push every element down one straight path. They *route*:

  * ``beam.Partition`` deterministically splits ONE PCollection into N by a pure function of the
    element (and the partition count). Same element -> same partition, every time. Think of it as a
    fan-out switch with a fixed number of labelled exits.

  * **Side inputs** broadcast a *small* auxiliary dataset to *every* worker so a main ParDo can read
    it as ordinary in-memory data. Beam offers four views over a side PCollection:
    ``beam.pvalue.AsSingleton`` (one value), ``AsList`` (a list), ``AsDict`` (a dict), and
    ``AsIter`` (an iterable). Here we build a tiny "known regions" lookup table once and broadcast it
    as an ``AsDict`` so the validator can check every record against it without a shuffle/join.

  * **Additional (tagged) outputs** let a single ``ParDo`` emit to more than one PCollection. Tag the
    main stream and any number of extra streams (here a ``"dead_letter"`` tag). This is the canonical
    **dead-letter pattern**: clean records flow on to the main output, malformed ones are diverted to
    a side stream you can inspect, alert on, or replay — instead of crashing the whole job.

The pipeline below validates synthetic order records against a broadcast lookup table, splits the
*valid* stream three ways with ``Partition`` (by order amount tier), and routes everything that fails
validation to the ``dead_letter`` tag. We log a summary of every resulting stream.

Run it:  ./scripts/submit.sh ch07        (or click "Run on Flink" in the Ch 7 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch07-<runId>.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam

# Make the shared _common package importable regardless of CWD (same idiom as Ch 1).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402


# --- The reference data we will broadcast as a side input ------------------------------------
# A small lookup table of valid region codes -> human-readable names. In a real job this might be a
# few thousand rows loaded from a file or a tiny DB snapshot — small enough to fit in memory on every
# worker, which is exactly what makes it a good *side input* (broadcast) rather than a join.
KNOWN_REGIONS = {
    "us-east": "US East",
    "us-west": "US West",
    "eu-central": "EU Central",
    "ap-south": "AP South",
}

# Synthetic order records. Some are deliberately malformed so the dead-letter tag has work to do:
#   - "ZZ" is an unknown region code (fails the lookup-table check)
#   - amount = -5 is a negative amount (fails the value check)
#   - the last record is missing the "amount" field entirely (fails the schema check)
RAW_ORDERS = [
    {"id": "o1", "region": "us-east", "amount": 12.50},
    {"id": "o2", "region": "eu-central", "amount": 240.00},
    {"id": "o3", "region": "ap-south", "amount": 75.25},
    {"id": "o4", "region": "ZZ", "amount": 30.00},          # unknown region  -> dead_letter
    {"id": "o5", "region": "us-west", "amount": -5.00},     # negative amount -> dead_letter
    {"id": "o6", "region": "us-east", "amount": 999.99},
    {"id": "o7", "region": "eu-central"},                   # missing amount  -> dead_letter
    {"id": "o8", "region": "us-west", "amount": 49.00},
]

# The tag name for our diverted-bad-records stream. Defining it as a constant avoids typos between
# the DoFn (which produces it) and the pipeline (which consumes it).
DEAD_LETTER = "dead_letter"


class ValidateOrder(beam.DoFn):
    """Validate a record against a broadcast lookup table, fanning out to two tagged outputs.

    The ``regions`` argument is supplied at apply-time as a *side input* (``beam.pvalue.AsDict``).
    Beam materialises the side PCollection and hands this ``process`` method a plain ``dict`` — no
    network shuffle per element, because the table was broadcast to every worker once.

    We emit:
      * a clean, enriched record to the **main** output (via a bare ``yield``), and
      * any record that fails a check to the **dead_letter** output (via
        ``yield beam.pvalue.TaggedOutput(DEAD_LETTER, ...)``), annotated with the reason.
    """

    def process(self, record, regions):
        # 1) Schema check: the fields we depend on must be present.
        if "region" not in record or "amount" not in record:
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER, {**record, "_error": "missing field (region/amount)"}
            )
            return

        # 2) Lookup check: the region must exist in the broadcast table. This is the side input in
        #    action — `regions` is the AsDict view of KNOWN_REGIONS, available in-memory here.
        if record["region"] not in regions:
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER, {**record, "_error": f"unknown region '{record['region']}'"}
            )
            return

        # 3) Value check: amounts must be positive.
        if record["amount"] <= 0:
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER, {**record, "_error": f"non-positive amount {record['amount']}"}
            )
            return

        # Valid: enrich with the human-readable region name from the side input and emit to MAIN.
        yield {**record, "region_name": regions[record["region"]]}


def amount_tier(order, num_partitions):
    """A pure partition function: map a *valid* order to one of ``num_partitions`` tiers by amount.

    ``beam.Partition`` calls this for every element and routes it to the returned index. The function
    MUST be deterministic and return an int in ``[0, num_partitions)``. Here: small / medium / large.
    """
    amount = order["amount"]
    if amount < 50:
        return 0          # small
    if amount < 250:
        return 1          # medium
    return 2              # large


def _summarize(label):
    """Build a DoFn-free logging Map that prints one line per element with a stream label."""
    def _log(element):
        logging.info("[%s] %s", label, element)
        return element
    return _log


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--output", default="/tmp/beam-artifact-staging/ch07-routing")
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch07-{known.run_id}"
    options = portable_options(job_name)  # BOUNDED chapter -> default (non-streaming) options.

    logging.info("Submitting routing demo as job_name=%s to the portable Flink runner", job_name)

    with beam.Pipeline(options=options) as p:
        # --- Build the side input (the broadcast lookup table) -----------------------------------
        # KNOWN_REGIONS is created as a tiny PCollection of (key, value) pairs. Wrapping it in
        # beam.pvalue.AsDict turns it into a *view* the main ParDo can read as an ordinary dict.
        regions_si = (
            p
            | "RegionsTable" >> beam.Create(list(KNOWN_REGIONS.items()))
        )

        # --- Source records --------------------------------------------------------------------
        orders = p | "ReadOrders" >> beam.Create(RAW_ORDERS)

        # --- Validate with the broadcast table, producing TWO tagged outputs --------------------
        # ParDo(...).with_outputs(DEAD_LETTER, main='valid') declares the tag names. The returned
        # object lets us pull each PCollection out by attribute. The broadcast table is passed as a
        # positional side input via beam.pvalue.AsDict(regions_si).
        results = (
            orders
            | "Validate" >> beam.ParDo(
                ValidateOrder(), beam.pvalue.AsDict(regions_si)
            ).with_outputs(DEAD_LETTER, main="valid")
        )
        valid = results.valid
        dead = results[DEAD_LETTER]

        # --- Partition the VALID stream three ways by amount tier -------------------------------
        # beam.Partition splits one PCollection into exactly N, calling amount_tier per element.
        # It returns a list of N PCollections; we name them for clarity.
        small, medium, large = valid | "ByAmountTier" >> beam.Partition(amount_tier, 3)

        # --- Log every resulting stream so the run is observable in the submitter output --------
        small | "LogSmall" >> beam.Map(_summarize("small  (<50)"))
        medium | "LogMedium" >> beam.Map(_summarize("medium (50-250)"))
        large | "LogLarge" >> beam.Map(_summarize("large  (>=250)"))
        dead | "LogDead" >> beam.Map(_summarize("DEAD-LETTER"))

        # --- Persist the dead-letter stream so operators can inspect/replay it later -------------
        # In production the dead-letter sink is usually a durable store (a topic, a table, a file
        # prefix). Here we just write it to text so the pattern is concrete.
        (
            dead
            | "DeadToStr" >> beam.Map(lambda r: str(r))
            | "WriteDead" >> beam.io.WriteToText(
                known.output + "-dead_letter", file_name_suffix=".txt"
            )
        )

    logging.info("Pipeline finished. Dead-letter records written under %s-dead_letter*.txt",
                 known.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
