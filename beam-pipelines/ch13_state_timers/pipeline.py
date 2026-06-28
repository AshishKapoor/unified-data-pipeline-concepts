"""Chapter 13 — Stateful Processing: State & Timers in DoFn.

Up to now every transform has been *stateless from your code's perspective*: GroupByKey, Combine,
and windowing carry state for you, but you never touched a state cell directly. This chapter opens
the lid. A **stateful DoFn** gets its own private, durable storage that the runner keys and scopes
**per key, per window** — and a set of **timers** it can arm to wake itself up at a future moment in
event time or processing time.

The mental model
----------------
For each (key, window) the runner hands your DoFn a tiny set of *state cells*:

* ``ReadModifyWriteStateSpec`` → a **ValueState**: one mutable slot (read / write / clear).
* ``BagStateSpec``            → a **BagState**: append-only multiset you can iterate then clear.
* ``CombiningValueStateSpec`` → a **CombiningState**: a running aggregate folded through a CombineFn
  (here ``sum``), so you never materialise the whole bag just to count it.

Plus ``TimerSpec`` declares a **timer** in a time domain:

* ``TimeDomain.WATERMARK``  → fires when the **watermark** passes the set event-time instant
  (use this for "the session has been idle for N seconds of *event* time").
* ``TimeDomain.REAL_TIME``  → fires at a wall-clock (**processing-time**) instant.

A method decorated with ``@on_timer(SPEC)`` is the callback the runner invokes when that timer fires;
inside it you may read/clear the same state cells and ``yield`` output. State + timers together let you
build sessionization, deduplication, fraud heuristics, and join-with-timeout logic by hand — the
primitives every higher-level streaming feature is built from.

Beam state → Flink state
------------------------
On the portable Flink runner these cells become **Flink keyed, checkpointed state** (ValueState /
ListState / AggregatingState) living in the configured state backend, and timers become Flink event-
time / processing-time timers. So everything here participates in checkpointing and is restored
exactly-once after failure (Ch 16). Note: the *exotic* Beam state types (``OrderedListState``,
``MultimapState``, ``SetState``) are still **less mature in Python-on-Flink** — so this course stays on
the rock-solid trio: **Value, Bag, Combining**.

What this pipeline does
-----------------------
A per-user "session / fraud-ish" detector. Events are keyed by user. For each user we:
  1. buffer the raw events in a **BagState**,
  2. count them in a **CombiningState** (sum of 1s),
  3. (re)arm an **event-time WATERMARK timer** for ``now + GAP`` on every event — an idle-gap timer.
When ``GAP`` seconds of event time pass with no new event, the watermark crosses the timer, the
``@on_timer`` callback fires, drains the bag, and emits one session summary (flagging it if the burst
looks suspicious). This is sessionization implemented *by hand* with the raw state/timer API.

Run it:  ./scripts/submit.sh ch13        (or click "Run on Flink" in the Ch 13 docs)
Watch :  the Flink Web UI at http://localhost:8081 — the job appears as ch13-<runId>.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.transforms.userstate import (
    BagStateSpec,
    CombiningValueStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.utils.timestamp import Duration
from apache_beam.coders import FloatCoder, VarIntCoder

# Make the shared _common package importable regardless of CWD (same idiom as ch01).
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common.options import portable_options  # noqa: E402
from _common.synthetic_source import SyntheticEvents  # noqa: E402

# How long (in EVENT-TIME seconds) a user can be idle before we close their session.
SESSION_GAP_SEC = 8.0
# If a user produces at least this many events before going idle, flag the session as a burst.
BURST_THRESHOLD = 4


class SumInts(beam.CombineFn):
    """A tiny integer-accumulator CombineFn for the counting state.

    IMPORTANT: do NOT pass the builtin ``sum`` to CombiningValueStateSpec. A bare callable is wrapped
    as a CallableWrapperCombineFn whose accumulator is a *list*, which then fails to encode against
    the VarIntCoder ("an integer is required"). A real CombineFn whose accumulator is an int matches
    the coder. (CombineFn internals are taught in Ch 6.)
    """

    def create_accumulator(self):
        return 0

    def add_input(self, acc, value):
        return acc + value

    def merge_accumulators(self, accs):
        return sum(accs)

    def extract_output(self, acc):
        return acc


class SessionizingDoFn(beam.DoFn):
    """Hand-rolled, per-user sessionization using raw State + Timers.

    Every state/timer below is automatically scoped by the runner to the *current key* (the user) and
    *current window* (here the GlobalWindow, since we never WindowInto a fixed window). That per-key
    isolation is the whole point: user-A's bag never sees user-B's events.
    """

    # --- State cell declarations -------------------------------------------------------------
    # BagState: an append-only buffer of this user's raw events. We drain + clear it on flush.
    BUFFER = BagStateSpec("buffer", beam.coders.PickleCoder())
    # CombiningState: a running count folded through an int-accumulator CombineFn. Cheaper than
    # len(list(bag)) because the runner keeps only the accumulator, not every element, in hot state.
    COUNT = CombiningValueStateSpec("count", VarIntCoder(), SumInts())
    # ValueState (ReadModifyWrite): remembers the latest event-time we've seen for this user, so the
    # flushed summary can report the session's span.
    LAST_TS = ReadModifyWriteStateSpec("last_ts", FloatCoder())

    # --- Timer declaration -------------------------------------------------------------------
    # An EVENT-TIME (watermark) timer. We re-arm it to `latest_event_time + GAP` on every event; it
    # only actually fires once the watermark proves no earlier data can still arrive.
    GAP_TIMER = TimerSpec("gap_timer", TimeDomain.WATERMARK)

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        buffer=beam.DoFn.StateParam(BUFFER),
        count=beam.DoFn.StateParam(COUNT),
        last_ts=beam.DoFn.StateParam(LAST_TS),
        gap_timer=beam.DoFn.TimerParam(GAP_TIMER),
    ):
        """Handle one event for one user.

        ``element`` is a KV: (user_key, event_dict). State/timer params are injected by the runner and
        already bound to this user+window. We never look up a key ourselves — that's the magic.
        """
        _user, event = element

        # 1) Buffer the raw event so the flush callback can summarise the whole burst.
        buffer.add(event)
        # 2) Bump the running count (CombiningState folds this 1 into the accumulator via `sum`).
        count.add(1)
        # 3) Remember the newest event time we've observed for this user.
        last_ts.write(float(timestamp))

        # 4) (Re)arm the idle-gap timer GAP seconds (event time) after THIS event. Setting the same
        #    timer again just overwrites the previous target — so a steady stream keeps pushing the
        #    deadline out, and the timer only fires after a real lull.
        deadline = timestamp + Duration(seconds=SESSION_GAP_SEC)
        gap_timer.set(deadline)

    @on_timer(GAP_TIMER)
    def on_gap(
        self,
        buffer=beam.DoFn.StateParam(BUFFER),
        count=beam.DoFn.StateParam(COUNT),
        last_ts=beam.DoFn.StateParam(LAST_TS),
        window=beam.DoFn.WindowParam,
    ):
        """Fires when GAP seconds of EVENT TIME elapse with no new event — close the session.

        Drains the bag, reads the count, emits one summary, then CLEARS every cell so the next burst
        for this user starts fresh (otherwise state would grow without bound — a classic stateful bug).
        """
        events = list(buffer.read())
        total = count.read() or 0
        ended_at = last_ts.read() or 0.0

        # A toy "fraud-ish" heuristic: a tight burst of many events is suspicious.
        suspicious = total >= BURST_THRESHOLD
        summary = {
            "events": total,
            "ended_at": ended_at,
            "sample_values": [e.get("value") for e in events[:5]],
            "flag": "BURST" if suspicious else "ok",
        }

        # IMPORTANT: clear the cells, or per-key state leaks forever. Bag, count, and value all reset.
        buffer.clear()
        count.clear()
        last_ts.clear()

        logging.info("session flushed: %s events flag=%s", total, summary["flag"])
        # Output is timestamped at the session end inside the current window.
        yield (summary["flag"], summary)


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", default="local")
    parser.add_argument("--job_name", default=None)
    # Keep the lab finite and reproducible: a handful of users, a couple of minutes of stream.
    parser.add_argument("--events_per_sec", type=float, default=4.0)
    parser.add_argument("--duration_sec", type=float, default=120.0)
    parser.add_argument("--users", type=int, default=3)
    known, _ = parser.parse_known_args(argv)

    job_name = known.job_name or f"ch13-{known.run_id}"
    # streaming=True: state + watermark timers only make sense on an unbounded pipeline.
    options = portable_options(job_name, streaming=True)

    logging.info("Submitting stateful sessionizer as job_name=%s (gap=%ss)", job_name, SESSION_GAP_SEC)

    with beam.Pipeline(options=options) as p:
        (
            p
            # An unbounded, event-time-stamped synthetic stream (see _common/synthetic_source.py).
            | "Stream"
            >> SyntheticEvents(
                events_per_sec=known.events_per_sec,
                duration_sec=known.duration_sec,
                key_cardinality=known.users,
            )
            # Stateful DoFns REQUIRE keyed input: the runner shuffles by key so each key's state lives
            # on exactly one worker. We re-key the dict into a KV(user, event).
            | "KeyByUser" >> beam.Map(lambda e: (e["key"], e))
            # The global window is fine here — sessions are delimited by our timer, not by windowing.
            | "Global" >> beam.WindowInto(window.GlobalWindows())
            # The hand-rolled stateful sessionizer. Emits (flag, summary) when each session closes.
            | "Sessionize" >> beam.ParDo(SessionizingDoFn())
            # Worker-side prints don't reach the submitter; logging.info above is what surfaces. We
            # also log each emitted session here for visibility in the Flink TaskManager logs.
            | "Log" >> beam.Map(lambda kv: logging.info("EMIT %s -> %s", kv[0], kv[1]) or kv)
        )

    logging.info("Pipeline finished (stream duration elapsed).")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
