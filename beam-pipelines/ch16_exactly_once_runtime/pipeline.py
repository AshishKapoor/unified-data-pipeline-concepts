"""Chapter 16 — Exactly-Once, Fault Tolerance & the Flink Runtime.

A streaming windowed aggregation, used as a vehicle to study what happens underneath it when a
machine dies. The transform graph is ordinary; the lesson is the runtime.

How Flink gives you exactly-once STATE
--------------------------------------
Flink uses **Asynchronous Barrier Snapshotting** (ABS), a variant of the Chandy–Lamport
distributed-snapshot algorithm:

  1. The JobManager's checkpoint coordinator periodically injects a numbered *barrier* into every
     source. The barrier flows *inline* with the records — it is just another item in the stream.
  2. An operator with multiple inputs **aligns**: on seeing a barrier on one input it buffers that
     input until the same barrier arrives on every other input, so the snapshot is a consistent cut.
  3. Once aligned, the operator snapshots its state (asynchronously) and forwards the barrier. When
     *all* operators + sinks acknowledge barrier *n*, checkpoint *n* is **complete** and durable.

Beam's role: a Beam *bundle* is the unit of commit. The portable Flink runner finalizes a bundle with
the surrounding checkpoint, so after a restart only records since the last completed checkpoint are
replayed — counted state is restored, not double-counted.

  The checkpoint interval is configured at the **cluster** level via Flink's
  ``execution.checkpointing.interval: 10000`` (see ``FLINK_PROPERTIES`` in docker-compose) — barriers
  every ~10s. (The portable runner takes no ``--checkpointing_interval`` flag.)

Kill a TaskManager — what happens
---------------------------------
  docker compose kill taskmanager      # simulate a crash
Flink detects the lost slot, **rolls every operator back to the last completed checkpoint**, and
**replays** from there. With a replayable source the windowed counts come out the same — no double
count. Exactly-once *state* survives the crash.

Exactly-once STATE != exactly-once OUTPUT
-----------------------------------------
Replay may *re-emit* a record to the sink. End-to-end exactly-once additionally needs a
**transactional/idempotent sink** (Kafka EOS / two-phase commit, or idempotent upserts). Without one,
a checkpoint-and-replay system is only *at-least-once* at the boundary.

Savepoints vs checkpoints / rescaling
-------------------------------------
  - **Checkpoints**: automatic, periodic, runtime-owned, cleaned up by default.
  - **Savepoints**: manual, durable images for upgrades/rescaling. Restoring at a different
    ``--parallelism`` rescales the job; keyed state is redistributed via key-group reassignment.

────────────────────────────────────────────────────────────────────────────────────────────────
What runs by default
────────────────────────────────────────────────────────────────────────────────────────────────
By default this runs a **pure-Python streaming demo** (an unbounded synthetic stream, windowed and
counted) so checkpointing/recovery is observable out of the box — open the Flink UI ▸ *Checkpoints*
tab to watch them complete every ~10s, then ``docker compose kill taskmanager`` to see recovery.
A truly replayable end-to-end-exactly-once source is **Kafka**; that real KafkaIO path is in
``build_kafka_pipeline`` and (like Ch 15) needs a Java SDK worker pool — enable with
``ENABLE_XLANG_KAFKA=1`` after ``./scripts/up-kafka.sh && ./scripts/seed-kafka.sh``.

Run it:  ./scripts/submit.sh ch16
Watch :  Flink UI ▸ Checkpoints (complete every ~10s) and Back Pressure (credit-based flow control).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options, kafka_bootstrap, kafka_expansion_service  # noqa: E402
from _common.synthetic_source import SyntheticEvents  # noqa: E402


def to_count_record(user, total):
    """Format a per-user window count as Kafka (key, value) bytes for the output topic."""
    return (user.encode("utf-8"), str(total).encode("utf-8"))


def build_kafka_pipeline(p, known):
    """The REAL replayable Kafka path (ENABLE_XLANG_KAFKA=1) — needed for true end-to-end EOS.

    Kafka is replayable: on recovery Flink rewinds to the offsets stored in the last completed
    checkpoint (offsets are committed *with* the checkpoint, not eagerly), which is what makes the
    counted state exactly-once. For exactly-once OUTPUT you would additionally configure a
    transactional producer here (Kafka EOS / two-phase commit).
    """
    from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

    bootstrap = kafka_bootstrap()
    expansion = kafka_expansion_service()
    logging.info("Kafka bootstrap (in-network) = %s", bootstrap)

    counts = (
        p
        | "ReadClicks"
        >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": bootstrap,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",  # offsets owned by the checkpoint
                "group.id": f"ch16-{known.run_id}",
            },
            topics=[known.input_topic],
            with_metadata=False,
            expansion_service=expansion,
        )
        | "KeyByUser" >> beam.Map(lambda kv: (kv[0].decode("utf-8") if kv[0] else "anon", 1))
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(known.window_sec))
        | "CountPerUser" >> beam.CombinePerKey(sum)
        | "ToKafkaRecord" >> beam.MapTuple(to_count_record)
    )
    _ = counts | "WriteCounts" >> WriteToKafka(
        producer_config={"bootstrap.servers": bootstrap},
        topic=known.output_topic,
        expansion_service=expansion,
    )


def build_synthetic_pipeline(p, known):
    """Out-of-the-box demo: unbounded synthetic stream, windowed counts. Checkpoints every ~10s.

    The per-key-per-window counts are exactly the *state* that ABS snapshots at each barrier — so
    killing the TaskManager and watching the counts recover demonstrates exactly-once state directly.
    """
    (
        p
        | "SyntheticClicks"
        >> SyntheticEvents(
            events_per_sec=known.events_per_sec,
            duration_sec=known.duration_sec,
            key_cardinality=known.users,
        )
        | "KeyByUser" >> beam.Map(lambda e: (e["key"], 1))
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(known.window_sec))
        | "CountPerUser" >> beam.CombinePerKey(sum)
        | "Log"
        >> beam.MapTuple(lambda user, n: logging.info("window count %s=%d", user, n) or (user, n))
    )


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--input_topic", default="clicks-in")
    parser.add_argument("--output_topic", default="counts-out")
    parser.add_argument("--window_sec", type=int, default=10)
    parser.add_argument("--events_per_sec", type=float, default=5.0)
    parser.add_argument("--duration_sec", type=float, default=180.0)
    parser.add_argument("--users", type=int, default=3)
    # A higher parallelism is what you'd pass when *rescaling* from a savepoint.
    parser.add_argument("--parallelism", type=int, default=2)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch16-{known.run_id}"
    # streaming=True -> unbounded, so checkpointing is active (cluster interval = 10s).
    options = portable_options(job_name, streaming=True, parallelism=known.parallelism)

    use_kafka = os.environ.get("ENABLE_XLANG_KAFKA") == "1"
    logging.info(
        "Submitting ch16 job_name=%s parallelism=%d (mode=%s)",
        job_name,
        known.parallelism,
        "KAFKA-XLANG" if use_kafka else "synthetic-demo",
    )

    with beam.Pipeline(options=options) as p:
        if use_kafka:
            build_kafka_pipeline(p, known)
        else:
            logging.info(
                "Running the SYNTHETIC-STREAM demo so checkpointing/recovery is observable out of the "
                "box. For a truly replayable end-to-end-exactly-once source, enable the Kafka path with "
                "ENABLE_XLANG_KAFKA=1 + a Java SDK worker pool (see README / chapter docs)."
            )
            build_synthetic_pipeline(p, known)

    logging.info(
        "ch16 pipeline finished. In the Flink UI, watch the Checkpoints tab; try "
        "`docker compose kill taskmanager` mid-run to see rollback-and-replay."
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
