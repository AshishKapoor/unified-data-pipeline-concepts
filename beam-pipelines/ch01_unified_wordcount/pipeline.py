"""Chapter 1 — The Unified Model: Why Beam Exists.

The canonical WordCount. The whole point of this chapter is that *this same pipeline code* runs
unchanged on any runner. Here we submit it to Flink via the portable runner; the only thing that
selects the engine is ``--runner`` (set for us by ``portable_options``). Swap the runner and the
identical transform graph executes on a different engine — that is the Beam promise.

Run it:  ./scripts/submit.sh ch01        (or click "Run on Flink" in the Ch 1 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch01-<runId>.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys

import apache_beam as beam
from apache_beam.transforms import combiners

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402


def split_words(line: str):
    """1 line -> N lowercased word tokens (a classic FlatMap: one input, many outputs)."""
    for token in re.findall(r"[A-Za-z']+", line.lower()):
        yield token


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--input", default="/pipelines/_common/sample_text.txt")
    parser.add_argument("--output", default="/tmp/beam-artifact-staging/ch01-wordcount")
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch01-{known.run_id}"
    options = portable_options(job_name)

    logging.info("Submitting WordCount as job_name=%s to the portable Flink runner", job_name)

    # The transform graph below is engine-agnostic. Read -> tokenize -> count -> format -> write.
    with beam.Pipeline(options=options) as p:
        counts = (
            p
            | "ReadLines" >> beam.io.ReadFromText(known.input)
            | "Tokenize" >> beam.FlatMap(split_words)
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "CountPerWord" >> combiners.Count.PerKey()
            | "Format" >> beam.MapTuple(lambda word, total: f"{word}: {total}")
        )
        counts | "WriteCounts" >> beam.io.WriteToText(known.output, file_name_suffix=".txt")

    logging.info("Pipeline finished. Counts written under %s*.txt", known.output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
