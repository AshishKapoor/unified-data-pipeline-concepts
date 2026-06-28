"""Chapter 15 — IO Connectors & Cross-Language: Files and KafkaIO.

Beam splits the world into two kinds of IO:

  * **Bounded IO** — sources with a known end. ``beam.io.ReadFromText`` (Ch 1) reads every line of
    a file set and then signals "done". The watermark jumps to +infinity and the job terminates.
  * **Unbounded IO** — sources that never declare themselves complete. ``KafkaIO`` is the canonical
    one: a topic is an endless log, so the read is a *streaming* source that advances a watermark
    forever (the job runs until you cancel it).

The twist for Python users: **there is no native Python KafkaIO**. The production-grade KafkaIO is a
*Java* transform. Python reaches it through Beam's **cross-language (xlang) framework**:

  1. At *construction* time, ``ReadFromKafka`` / ``WriteToKafka`` (thin Python stubs) connect to a
     Java **expansion service** — by default the one bundled in the Beam Flink **job server** on
     port **8097**. The Python SDK sends the transform's URN + parameters; the expansion service
     *expands* it into a real Java sub-graph and hands the proto back. Python splices that Java
     sub-graph into the pipeline it is building.
  2. At *runtime*, the runner starts **two** SDK harnesses side by side: a **Java** harness that
     executes the Kafka read/write, and a **Python** harness that executes your ``CombinePerKey``
     and DoFns. Records cross the language boundary over the Fn API (a gRPC data plane).

So a single Python file produces a pipeline whose Kafka edges run as Java. That is the whole point
of this chapter — you write Python, but you reuse the battle-tested Java connector ecosystem.

Topology:
    ReadFromKafka('clicks-in')           # JAVA transform, via expansion service :8097
      -> decode key bytes -> (user, 1)   # Python DoFn
      -> FixedWindows(10s)               # streaming windowing (Ch 9)
      -> CombinePerKey(sum)              # Python combine — per-user click count per window
      -> encode -> (user_bytes, count_bytes)
      -> WriteToKafka('counts-out')      # JAVA transform, via expansion service :8097

REQUIRES KAFKA. Brokers must be reachable at the *in-network* listener ``kafka:29092`` from the SDK
harnesses (they run inside the cluster network) — NOT ``localhost:9092``, which only works from your
laptop. ``_common.options.kafka_bootstrap()`` returns the correct in-network address.

Run it:
    ./scripts/up-kafka.sh        # start the Kafka broker overlay
    ./scripts/seed-kafka.sh      # create topics clicks-in / counts-out and produce sample clicks
    ./scripts/submit.sh ch15     # (or click "Run on Flink" in the Ch 15 docs)
Watch :
    the Flink Web UI at http://localhost:8081 — the job appears as ch15-<runId> and runs until
    cancelled. Consume results with:
        docker compose ... exec kafka kafka-console-consumer \
            --bootstrap-server localhost:9092 --topic counts-out --from-beginning \
            --property print.key=true
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam

# KafkaIO ships as Python stubs that drive the Java expansion service under the hood.
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

# Make the shared _common package importable regardless of CWD.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options, kafka_bootstrap  # noqa: E402

# The expansion service that knows how to expand the Java KafkaIO URNs. The Beam Flink job server
# exposes it on 8097; override with BEAM_EXPANSION_ENDPOINT if your topology differs.
EXPANSION_SERVICE = os.environ.get("BEAM_EXPANSION_ENDPOINT", "localhost:8097")


def to_user_one(kv):
    """KafkaIO yields (key_bytes, value_bytes). Decode the key (the user id) and pair with 1.

    The seeded records look like ``user-1:click`` — key is the user, value is the literal ``click``.
    We count *clicks per user*, so only the key matters here.
    """
    key_bytes, _value_bytes = kv
    user = (key_bytes or b"").decode("utf-8") or "unknown"
    return (user, 1)


def encode_result(kv):
    """CombinePerKey gives (user, count). Re-encode to (key_bytes, value_bytes) for WriteToKafka.

    WriteToKafka requires both halves to be ``bytes`` — it does not serialize Python objects for you.
    """
    user, count = kv
    return (user.encode("utf-8"), str(count).encode("utf-8"))


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    parser.add_argument("--input_topic", default="clicks-in")
    parser.add_argument("--output_topic", default="counts-out")
    parser.add_argument("--window_secs", type=int, default=10)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch15-{known.run_id}"
    # KafkaIO is unbounded -> this is a STREAMING job. portable_options(..., streaming=True) flips
    # the --streaming flag so the runner builds an unbounded execution graph that runs until cancel.
    options = portable_options(job_name, streaming=True)

    bootstrap = kafka_bootstrap()  # "kafka:29092" — the in-network listener the harnesses can reach.
    logging.info("Submitting ch15 xlang Kafka pipeline job_name=%s", job_name)
    logging.info("Kafka bootstrap (in-network) = %s", bootstrap)
    logging.info("Expansion service (Java KafkaIO) = %s", EXPANSION_SERVICE)

    with beam.Pipeline(options=options) as p:
        counts = (
            p
            # --- JAVA transform, expanded via the cross-language expansion service on :8097 ---
            # ReadFromKafka returns a PCollection of (key_bytes, value_bytes). It is UNBOUNDED.
            | "ReadFromKafka"
            >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": bootstrap,  # MUST be kafka:29092 (in-network), not 9092.
                    "auto.offset.reset": "earliest",  # replay seeded clicks from the start.
                    "group.id": f"{job_name}-consumer",
                },
                topics=[known.input_topic],
                expansion_service=EXPANSION_SERVICE,
                # Commit offsets back so a restarted job resumes where it left off.
                commit_offset_in_finalize=True,
            )
            # --- everything below runs in the PYTHON harness ---
            | "ToUserOne" >> beam.Map(to_user_one)  # (key_bytes, value_bytes) -> (user, 1)
            # Streaming aggregation needs a window, otherwise the global window never closes and the
            # CombinePerKey would buffer forever. Fixed 10s tumbling windows of click counts.
            | "Window"
            >> beam.WindowInto(beam.window.FixedWindows(known.window_secs))
            | "CountPerUser" >> beam.CombinePerKey(sum)  # per-user clicks within each window.
            | "Encode" >> beam.Map(encode_result)  # (user, count) -> (bytes, bytes) for Kafka.
        )

        # --- JAVA transform again: write the per-window counts back to Kafka ---
        _ = counts | "WriteToKafka" >> WriteToKafka(
            producer_config={"bootstrap.servers": bootstrap},
            topic=known.output_topic,
            expansion_service=EXPANSION_SERVICE,
        )

    logging.info("Pipeline submitted. It runs until cancelled (unbounded Kafka source).")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
