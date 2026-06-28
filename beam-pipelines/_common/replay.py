"""Out-of-order event replay with explicit event timestamps.

Many chapters (8-12) need a *bounded* but deliberately out-of-order stream so we can show event time
vs processing time, late data, and triggers without standing up Kafka. ``ReplayEvents`` takes inline
records (or a CSV string), assigns each one its event-time via ``TimestampedValue``, and optionally
shuffles emission order so arrival order != event order.

Record format: dict with at least ``event_time`` (unix seconds) plus arbitrary payload fields.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp


def parse_csv(csv_text: str, columns: Sequence[str]) -> List[Dict[str, Any]]:
    """Parse a small inline CSV (header-less) into dict records.

    ``event_time`` and numeric-looking fields are coerced to float where possible.
    """
    rows: List[Dict[str, Any]] = []
    for line in csv_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        cells = [c.strip() for c in line.split(",")]
        record: Dict[str, Any] = {}
        for col, cell in zip(columns, cells):
            try:
                record[col] = float(cell) if ("." in cell or col == "event_time") else int(cell)
            except ValueError:
                record[col] = cell
        rows.append(record)
    return rows


class _AssignEventTime(beam.DoFn):
    def process(self, record):
        yield beam.window.TimestampedValue(record, Timestamp(float(record["event_time"])))


class ReplayEvents(beam.PTransform):
    """Emit inline records as a timestamped PCollection.

    Args:
        records: list of dict records, each with an ``event_time`` field (unix seconds).
        emission_order: optional list of indices controlling the order records are produced, so a
            lab can make arrival order differ from event-time order deterministically.
    """

    def __init__(
        self,
        records: List[Dict[str, Any]],
        emission_order: Optional[List[int]] = None,
    ):
        super().__init__()
        if emission_order is not None:
            self._records = [records[i] for i in emission_order]
        else:
            self._records = list(records)

    def expand(self, pbegin):
        return (
            pbegin
            | "Create" >> beam.Create(self._records)
            | "AssignEventTime" >> beam.ParDo(_AssignEventTime())
        )
