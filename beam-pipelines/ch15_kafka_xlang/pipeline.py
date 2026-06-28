"""Chapter 15 — IO Connectors & Cross-Language: Files and KafkaIO.

Beam splits the world into two kinds of IO:

  * **Bounded IO** — sources with a known end. ``beam.io.ReadFromText`` (Ch 1) reads every line of
    a file set and then signals "done"; the watermark jumps to +infinity and the job terminates.
  * **Unbounded IO** — sources that never declare themselves complete. ``KafkaIO`` is the canonical
    one: a topic is an endless log, so the read is a *streaming* source that advances a watermark
    forever (the job runs until you cancel it).

The twist for Python users: **there is no native Python KafkaIO**. The production-grade KafkaIO is a
*Java* transform. Python reaches it through Beam's **cross-language (xlang) framework**:

  1. At *construction* time, ``ReadFromKafka`` / ``WriteToKafka`` (thin Python stubs) connect to a
     Java **expansion service**. The Python SDK sends the transform's URN + parameters; the expansion
     service *expands* it into a real Java sub-graph and returns the proto, which Python splices in.
  2. At *runtime*, the runner starts **two** SDK harnesses side by side: a **Java** harness that runs
     the Kafka read/write, and a **Python** harness that runs your ``CombinePerKey`` and DoFns.
     Records cross the language boundary over the Fn API.

────────────────────────────────────────────────────────────────────────────────────────────────
IMPORTANT — what actually runs when you click "Run on Flink"
────────────────────────────────────────────────────────────────────────────────────────────────
Running the *Java* KafkaIO harness on a portable Flink cluster needs a **Java SDK worker pool**
(the Java analogue of this project's Python worker pool). On a Flink cluster using EXTERNAL workers
that is genuinely involved infra (the stock ``apache/beam_javaXX_sdk`` image does not expose the
simple ``--worker_pool`` mode the Python image does). So by default this chapter runs a **pure-Python
streaming demo** — an unbounded synthetic click stream, windowed and counted per user — which
exercises the *same streaming + windowed-aggregation shape* as the Kafka pipeline and runs flawlessly
out of the box.

The **real cross-language KafkaIO code** is right below in ``build_kafka_pipeline`` (this is the code
the chapter teaches). To actually run it you must:
  * ``./scripts/up-kafka.sh && ./scripts/seed-kafka.sh``   (Kafka broker + topics)
  * provide a Java SDK worker pool reachable at ``$BEAM_JAVA_WORKER_POOL`` (advanced — see README), and
  * submit with ``ENABLE_XLANG_KAFKA=1``.
Brokers are reached at the in-network listener ``kafka:29092`` (NOT ``localhost:9092``).

Run it:  ./scripts/submit.sh ch15          # pure-Python streaming demo (default)
Watch :  the Flink Web UI — the job appears as ch15-<runId>.
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


def to_user_one(kv):
    """KafkaIO yields (key_bytes, value_bytes). Decode the key (the user id) and pair with 1."""
    key_bytes, _value_bytes = kv
    user = (key_bytes or b"").decode("utf-8") or "unknown"
    return (user, 1)


def encode_result(kv):
    """CombinePerKey gives (user, count). Re-encode to (key_bytes, value_bytes) for WriteToKafka."""
    user, count = kv
    return (user.encode("utf-8"), str(count).encode("utf-8"))


def build_kafka_pipeline(p, known):
    """The REAL cross-language KafkaIO pipeline (enabled with ENABLE_XLANG_KAFKA=1).

    ReadFromKafka and WriteToKafka are Java transforms expanded via a local (loopback) expansion
    service whose jar is baked into the submitter image; the expansion stamps them with an EXTERNAL
    environment pointing at a Java SDK worker pool (``$BEAM_JAVA_WORKER_POOL``).
    """
    from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

    bootstrap = kafka_bootstrap()
    expansion = kafka_expansion_service()  # one local expansion service, shared by read + write
    logging.info("Kafka bootstrap (in-network) = %s", bootstrap)

    counts = (
        p
        | "ReadFromKafka"
        >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": bootstrap,
                "auto.offset.reset": "earliest",
                "group.id": f"{known.job_name or 'ch15'}-consumer",
            },
            topics=[known.input_topic],
            expansion_service=expansion,
            commit_offset_in_finalize=True,
        )
        | "ToUserOne" >> beam.Map(to_user_one)
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(known.window_secs))
        | "CountPerUser" >> beam.CombinePerKey(sum)
        | "Encode" >> beam.Map(encode_result)
    )
    _ = counts | "WriteToKafka" >> WriteToKafka(
        producer_config={"bootstrap.servers": bootstrap},
        topic=known.output_topic,
        expansion_service=expansion,
    )


def build_synthetic_pipeline(p, known):
    """The out-of-the-box demo: an unbounded synthetic click stream, windowed and counted per user.

    Same streaming + windowed-aggregation *shape* as the Kafka pipeline, but pure Python so it runs on
    the default stack with no Java worker pool. This is what teaches that windowed aggregation over an
    unbounded source behaves identically regardless of where the bytes come from.
    """
    (
        p
        | "SyntheticClicks"
        >> SyntheticEvents(
            events_per_sec=known.events_per_sec,
            duration_sec=known.duration_sec,
            key_cardinality=known.users,
        )
        | "ToUserOne" >> beam.Map(lambda e: (e["key"], 1))
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(known.window_secs))
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
    parser.add_argument("--window_secs", type=int, default=10)
    parser.add_argument("--events_per_sec", type=float, default=5.0)
    parser.add_argument("--duration_sec", type=float, default=120.0)
    parser.add_argument("--users", type=int, default=3)
    known, _ = parser.parse_known_args(argv)

    known.job_name = known.job_name or f"ch15-{known.run_id}"
    options = portable_options(known.job_name, streaming=True)

    use_kafka = os.environ.get("ENABLE_XLANG_KAFKA") == "1"
    logging.info("Submitting ch15 job_name=%s (mode=%s)", known.job_name, "KAFKA-XLANG" if use_kafka else "synthetic-demo")

    with beam.Pipeline(options=options) as p:
        if use_kafka:
            build_kafka_pipeline(p, known)
        else:
            logging.info(
                "Running the out-of-the-box SYNTHETIC-STREAM demo. The real cross-language KafkaIO "
                "code is in build_kafka_pipeline(); enable it with ENABLE_XLANG_KAFKA=1 plus a Java "
                "SDK worker pool (see README / chapter docs)."
            )
            build_synthetic_pipeline(p, known)

    logging.info("ch15 pipeline finished.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
