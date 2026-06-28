"""Chapter 4 — Running on Flink for Real: The Portable Runner Architecture.

This pipeline is deliberately a small WordCount-ish job — the *interesting* part of this
chapter is not the transform graph, it is the journey the graph takes to get onto Flink and
back into your Python code. So the comments and the logged "effective options" focus on the
**submission path**, not the counting.

What actually happens when you click "Run on Flink":

  1. SUBMITTER (this process). Beam constructs your pipeline in memory, then serialises it into a
     language-neutral **pipeline proto** (the Runner API). Nothing Python-specific survives except
     opaque blobs describing each DoFn and the *environment* needed to run it.

  2. JOB SERVER (Beam Flink Job Server, reached via ``--job_endpoint``). It receives the proto,
     stages your artifacts (``--artifact_endpoint``), and translates the proto into a Flink
     **JobGraph** — the runner's job is exactly this translation.

  3. JOBMANAGER. Flink's JobManager schedules the JobGraph across TaskManager slots, just like any
     native Flink job.

  4. TASKMANAGER ⇄ SDK HARNESS. When a TaskManager hits an operator that wraps *your Python code*,
     it cannot run Python itself. Instead it calls back to a **Beam SDK harness** over the
     **Fn API** — four gRPC channels: control (run this bundle), data (elements in/out), state
     (read/write user state & side inputs), and logging. The harness executes your DoFn and streams
     results back. This is the portability boundary.

The ``--environment_type`` flag decides *where that SDK harness lives*:

  * ``LOOPBACK``  — the harness runs **inside this submitting process**. Zero extra infra, perfect
                    for a laptop, but the submitter must stay alive for the whole job. (Ch-4 dev lane.)
  * ``DOCKER``    — the runner starts a per-worker SDK container (Docker-in-Docker). Heavy; avoided here.
  * ``PROCESS``   — the runner forks the harness as a plain OS process from a command you provide.
  * ``EXTERNAL``  — the harness lives in a **pre-started worker pool** sidecar that shares the
                    TaskManager's network namespace (reached at ``--environment_config=localhost:50000``).
                    This is *this project's* production lane.

``portable_options`` (shared by every chapter) already encodes this contract. Below we simply read
the effective values back out and log them so you can *see* the runner / job_endpoint / environment_type
that your run is using.

Run it:  ./scripts/submit.sh ch04        (or click "Run on Flink" in the Ch 4 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch04-<runId>.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PortableOptions, SetupOptions, StandardOptions
from apache_beam.transforms import combiners

# Make the shared _common package importable regardless of CWD (same idiom as Ch 1).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402


def split_words(line: str):
    """1 line -> N lowercased word tokens. A trivial FlatMap; the point of the chapter is *how*
    this DoFn reaches a TaskManager, not what it computes."""
    for token in re.findall(r"[A-Za-z']+", line.lower()):
        yield token


def log_submission_contract(options) -> None:
    """Print the *effective* portability contract so the reader can see, in the submitter log, the
    exact wiring that ``portable_options`` produced. These are the flags that route Python → Flink.

    NOTE: this runs in the SUBMITTER process, so ``logging.info`` here DOES reach the stdout the
    Run panel streams over SSE. Worker-side ``print``/logging (inside your DoFn) goes to the SDK
    harness logging channel instead and shows up in the Flink/TaskManager logs, not here.
    """
    std = options.view_as(StandardOptions)
    portable = options.view_as(PortableOptions)
    setup = options.view_as(SetupOptions)

    logging.info("---- effective portable-runner submission contract ----")
    logging.info("  runner            = %s", std.runner)            # PortableRunner — the portability shim
    logging.info("  streaming         = %s", std.streaming)         # False here: a bounded batch job
    logging.info("  job_endpoint      = %s", portable.job_endpoint)         # → Beam Flink Job Server
    logging.info("  artifact_endpoint = %s", portable.artifact_endpoint)   # → where artifacts are staged
    logging.info("  environment_type  = %s", portable.environment_type)    # LOOPBACK / EXTERNAL / PROCESS / DOCKER
    logging.info("  environment_config= %s", portable.environment_config)  # e.g. localhost:50000 for EXTERNAL pool
    logging.info("  save_main_session = %s", setup.save_main_session)      # ship __main__ globals to the harness
    logging.info("-------------------------------------------------------")


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--input", default="/pipelines/_common/sample_text.txt")
    parser.add_argument("--output", default="/tmp/beam-artifact-staging/ch04-wordcount")
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch04-{known.run_id}"

    # BOUNDED chapter → no streaming flag. ``portable_options`` builds the PortableRunner wiring
    # (job_endpoint, artifact_endpoint, environment_type/config) from the environment.
    options = portable_options(job_name)

    logging.info("Submitting WordCount as job_name=%s via the portable Flink runner", job_name)
    # Surface the contract BEFORE we build the graph, so the reader sees the routing first.
    log_submission_contract(options)

    # The transform graph itself is ordinary (and engine-agnostic). What is special is that when a
    # TaskManager executes "Tokenize"/"CountPerWord", it does NOT run Python natively — it calls back
    # to the SDK harness selected by --environment_type over the Fn API control+data channels.
    with beam.Pipeline(options=options) as p:
        counts = (
            p
            | "ReadLines" >> beam.io.ReadFromText(known.input)       # bounded source → finite pipeline
            | "Tokenize" >> beam.FlatMap(split_words)                # runs in the SDK harness, not Flink
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "CountPerWord" >> combiners.Count.PerKey()             # the shuffle/aggregation lands on Flink
            | "Format" >> beam.MapTuple(lambda word, total: f"{word}: {total}")
        )
        counts | "WriteCounts" >> beam.io.WriteToText(known.output, file_name_suffix=".txt")

    logging.info(
        "Pipeline finished. The proto → JobGraph → JobManager → TaskManager ⇄ harness round trip "
        "completed; counts written under %s*.txt", known.output
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
