"""Chapter 5 — Keyed Aggregation: GroupByKey, CoGroupByKey, Flatten.

This chapter is about the three keyed/collection primitives that everything else in Beam is
built on top of:

* ``GroupByKey`` (GBK) — the fundamental keyed primitive. Given a ``PCollection`` of ``(K, V)``
  pairs it produces ``(K, Iterable[V])`` by bringing every value for a key to the *same* worker.
  That movement of data across the cluster is the **shuffle**, and it is the most important — and
  most expensive — thing a runner does. On Flink this becomes a keyed network exchange.
* ``CoGroupByKey`` (CoGBK) — a *relational join*. It co-groups several keyed ``PCollection``s by
  their common key so you can enrich one stream with another (here: tag every click with the
  user's profile). It is GBK generalised to multiple inputs.
* ``Flatten`` — the union operator. It merges N ``PCollection``s **of the same element type** into
  one, without grouping or shuffling. Think ``UNION ALL``, not ``JOIN``.

A ``KV`` here is just a Python 2-tuple ``(key, value)``; Beam's keyed transforms key off element[0].

One thing to remember for later chapters: on **unbounded** (streaming) data, GBK/CoGBK require a
**window** — you cannot group "all values for a key" if the key never stops receiving values, so a
window bounds the grouping. This pipeline is *bounded*, so the default global window is fine.

Run it:  ./scripts/submit.sh ch05        (or click "Run on Flink" in the Ch 5 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the GroupByKey/CoGroupByKey stages appear as
         keyed exchanges (the shuffle) in the job graph.
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


# --- Inline sample data -----------------------------------------------------------------------
# Two "tables" keyed by user_id. In a real pipeline these would be two ReadFrom* sources (e.g. a
# click stream and a profile dimension table); inlining keeps the lab self-contained and bounded.
CLICKS = [
    ("u1", "/home"),
    ("u1", "/pricing"),
    ("u2", "/home"),
    ("u3", "/docs/ch05"),
    ("u2", "/checkout"),
    ("u1", "/checkout"),
]

# A second click stream (e.g. from a different source/region) to demonstrate Flatten (union).
MORE_CLICKS = [
    ("u3", "/blog"),
    ("u4", "/home"),       # u4 has clicks but NO profile -> shows an unmatched left side in the join
]

PROFILES = [
    ("u1", {"name": "Ada", "plan": "pro"}),
    ("u2", {"name": "Linus", "plan": "free"}),
    ("u3", {"name": "Grace", "plan": "pro"}),
    # Note: there is no profile for "u4" on purpose (join keeps the click, profile side is empty).
]


def format_grouped(kv):
    """Render a GroupByKey result (key, Iterable[value]) as a readable log line."""
    user_id, paths = kv
    # ``paths`` is an iterable; materialise it so we can list every value gathered for this key.
    return f"[GBK] {user_id} -> {sorted(paths)}"


def enrich_with_profile(kv):
    """Render a CoGroupByKey result into one enriched record per click.

    ``kv`` is ``(user_id, {'clicks': [...], 'profile': [...]})``. CoGBK gives each tagged input as a
    *list* (there may be 0..N matches per side). Profiles are a dimension table so there is at most
    one; if it is missing (e.g. user u4) we fall back to a placeholder — that is a LEFT-join shape.
    """
    user_id, grouped = kv
    clicks = grouped["clicks"]
    profiles = grouped["profile"]
    profile = profiles[0] if profiles else {"name": "<unknown>", "plan": "n/a"}
    out = []
    for path in clicks:
        out.append(
            f"[CoGBK] {user_id} ({profile['name']}, {profile['plan']}) clicked {path}"
        )
    return out


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch05-{known.run_id}"
    # BOUNDED chapter -> default (batch) options; no --streaming, so the default global window
    # lets GBK/CoGBK group "all" values per key without us declaring an explicit window.
    options = portable_options(job_name)

    logging.info("Submitting GroupByKey/CoGroupByKey/Flatten demo as job_name=%s", job_name)

    with beam.Pipeline(options=options) as p:
        # Two keyed PCollections of KV (user_id, payload). beam.Create just materialises our inline
        # lists; the (key, value) tuples are what make these "keyed" collections.
        clicks = p | "Clicks" >> beam.Create(CLICKS)
        more_clicks = p | "MoreClicks" >> beam.Create(MORE_CLICKS)
        profiles = p | "Profiles" >> beam.Create(PROFILES)

        # --- Flatten: UNION the two click streams into one PCollection (same element type) -------
        # No shuffle, no grouping — just a merge of the two streams of (user_id, path) tuples.
        all_clicks = (clicks, more_clicks) | "UnionClicks" >> beam.Flatten()

        # --- GroupByKey: the fundamental shuffle. (user_id, path) -> (user_id, [paths...]) -------
        # Every value for a given user_id is brought to the same worker. Watch this become a keyed
        # network exchange in the Flink job graph.
        per_user_paths = all_clicks | "GroupClicksByUser" >> beam.GroupByKey()
        per_user_paths | "LogGrouped" >> beam.Map(
            lambda kv: logging.info(format_grouped(kv))
        )

        # --- CoGroupByKey: a relational join across the two keyed PCollections -------------------
        # We tag each input so the co-grouped result is a dict keyed by tag. CoGBK shuffles BOTH
        # inputs by user_id and hands us, per key, the matching values from each side.
        enriched = (
            {"clicks": all_clicks, "profile": profiles}
            | "JoinClicksToProfiles" >> beam.CoGroupByKey()
            | "Enrich" >> beam.FlatMap(enrich_with_profile)  # 1 join row -> N enriched click lines
        )
        enriched | "LogEnriched" >> beam.Map(lambda line: logging.info(line))

    logging.info("Pipeline finished. See the worker logs / Flink UI for the grouped + joined rows.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
