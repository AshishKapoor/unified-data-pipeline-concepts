"""Chapter 16 — Exactly-Once, Fault Tolerance & the Flink Runtime.

This chapter is the same shape as a Kafka streaming pipeline (Ch 15): read clicks from Kafka,
count per user in a fixed window, write the counts back to Kafka. What is *new* here is not the
transform graph — it is what happens underneath it when a machine dies.

How Flink gives you exactly-once STATE
--------------------------------------
Flink uses **Asynchronous Barrier Snapshotting** (ABS), a variant of the Chandy–Lamport
distributed-snapshot algorithm:

  1. The JobManager's checkpoint coordinator periodically injects a numbered *barrier* into every
     source. The barrier flows *inline* with the records — it is just another item in the stream.
  2. When an operator with multiple inputs sees a barrier on one input, it **aligns**: it buffers
     that input and waits until the same barrier arrives on every other input. This guarantees the
     snapshot reflects a consistent cut of the dataflow.
  3. Once aligned, the operator snapshots its state (asynchronously, to the state backend) and
     forwards the barrier downstream. When *all* operators + sinks acknowledge barrier *n*, the
     checkpoint is **complete** and durable.

Beam's role: a Beam *bundle* is the unit of commit. The portable Flink runner finalizes a bundle
together with the surrounding checkpoint, so re-processing after a restart replays only the records
since the last completed checkpoint — counted state is restored, not double-counted.

  ``--checkpointing_interval`` (set inside ``portable_options``) controls how often barriers are
  injected. Smaller = less replay on failure, more overhead.

Kill a TaskManager — what happens
---------------------------------
  docker compose kill taskmanager      # simulate a crash

Flink detects the lost slot, **rolls every operator back to the last completed checkpoint**, and
**replays** the source from the offsets recorded in that checkpoint. Because Kafka is replayable and
the counts were snapshotted, the per-window counts come out the same — no double count. Exactly-once
*state* survives the crash.

Exactly-once STATE != exactly-once OUTPUT
-----------------------------------------
Replay means a record may be *re-emitted* to the sink. To get end-to-end exactly-once you also need
a **transactional or idempotent sink**:
  - Kafka EOS: ``WriteToKafka`` can use a transactional producer (two-phase commit) so the output
    records of an aborted attempt are never read by downstream consumers (read_committed).
  - Idempotent upsert sinks (keyed writes) achieve the same effect without transactions.
Without one of these, a checkpoint-and-replay system is only *at-least-once* at the boundary.

Savepoints vs checkpoints / rescaling
-------------------------------------
  - **Checkpoints** are automatic, periodic, owned by the runtime, and (by default) cleaned up.
  - **Savepoints** are manual, durable, portable images you trigger for upgrades / rescaling:
        flink savepoint <jobId> s3://.../savepoints
        flink run -s s3://.../savepoints/savepoint-xxxx -p 4 <jar>   # restore at parallelism 4
    Bumping ``-p`` (or ``--parallelism`` here) and restoring from a savepoint **rescales** the job;
    keyed state is redistributed across the new task slots via key-group reassignment.

Run it:  ./scripts/up-kafka.sh && ./scripts/seed-kafka.sh && ./scripts/submit.sh ch16
         (or click "Run on Flink" in the Ch 16 docs)
Watch :  Flink UI http://localhost:8081 — open the job, then the *Checkpoints* tab to see barriers
         complete every ~10s, and *Back Pressure* to see credit-based flow control.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options, kafka_bootstrap  # noqa: E402

# Reuse the job server's Java expansion service for KafkaIO (same as Ch 15). In-network this is
# beam-job-server:8097 (set by docker-compose); falls back to localhost:8097 for host-run.
EXPANSION_SERVICE = os.environ.get("BEAM_EXPANSION_ENDPOINT", "localhost:8097")


def to_count_record(user: str, total: int):
    """Format a per-user window count as Kafka (key, value) bytes for the output topic."""
    # WriteToKafka expects a (key_bytes, value_bytes) tuple.
    return (user.encode("utf-8"), str(total).encode("utf-8"))


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--input_topic", default="clicks-in")
    parser.add_argument("--output_topic", default="counts-out")
    parser.add_argument("--window_sec", type=int, default=10)
    # Pedagogical knob: a higher parallelism is what you'd pass when *rescaling* from a savepoint.
    parser.add_argument("--parallelism", type=int, default=2)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch16-{known.run_id}"

    # streaming=True makes this an unbounded job, so Flink's checkpointing is active and
    # ``--checkpointing_interval`` (default 10_000 ms in portable_options) injects barriers.
    # parallelism maps 1:1 to task slots; raising it from a savepoint is how you rescale the job.
    options = portable_options(
        job_name,
        streaming=True,
        parallelism=known.parallelism,
    )

    bootstrap = kafka_bootstrap()
    logging.info(
        "Submitting ch16 exactly-once job_name=%s reading %s -> %s on bootstrap=%s (parallelism=%d)",
        job_name,
        known.input_topic,
        known.output_topic,
        bootstrap,
        known.parallelism,
    )

    with beam.Pipeline(options=options) as p:
        counts = (
            p
            # ---- SOURCE: KafkaIO (cross-language Java transform driven from Python) ----------
            # Kafka is *replayable*: on recovery Flink rewinds to the offsets stored in the last
            # completed checkpoint and re-reads from there. Consumer offsets are committed *with*
            # the checkpoint, not eagerly, which is what keeps state exactly-once.
            | "ReadClicks"
            >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": bootstrap,
                    "auto.offset.reset": "earliest",
                    # Offsets are owned by the checkpoint, so disable Kafka's own auto-commit.
                    "enable.auto.commit": "false",
                    "group.id": f"ch16-{known.run_id}",
                },
                topics=[known.input_topic],
                # Each Kafka record arrives as (key_bytes, value_bytes).
                with_metadata=False,
                expansion_service=EXPANSION_SERVICE,
            )
            # ---- key by user (the Kafka message key) -----------------------------------------
            | "KeyByUser"
            >> beam.Map(lambda kv: (kv[0].decode("utf-8") if kv[0] else "anon", 1))
            # ---- WINDOW: fixed windows so the count is bounded per interval -------------------
            # Per-key counts inside each window are the *state* that ABS snapshots at each barrier.
            | "Window"
            >> beam.WindowInto(beam.window.FixedWindows(known.window_sec))
            | "CountPerUser" >> beam.CombinePerKey(sum)
            | "ToKafkaRecord" >> beam.MapTuple(to_count_record)
        )

        # ---- SINK: write counts back to Kafka -----------------------------------------------
        # For end-to-end exactly-once OUTPUT (not just state) you would configure a transactional
        # producer here (Kafka EOS / two-phase commit) so replayed records never surface twice to
        # a read_committed consumer. This basic writer is at-least-once at the boundary.
        _ = counts | "WriteCounts" >> WriteToKafka(
            producer_config={"bootstrap.servers": bootstrap},
            topic=known.output_topic,
            expansion_service=EXPANSION_SERVICE,
        )

    logging.info(
        "Pipeline submitted. In the Flink UI, watch the Checkpoints tab complete every ~10s; "
        "try `docker compose kill taskmanager` to see rollback-and-replay with no double count."
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
