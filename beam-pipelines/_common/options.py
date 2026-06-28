"""The single submission contract every chapter pipeline uses.

This builds the ``PipelineOptions`` that target the Beam Flink Job Server over the portability
framework. See BUILD_BRIEF.md sections 2-3 for the rationale behind each flag.

Environment variables (set by docker-compose / scripts):
    BEAM_JOB_ENDPOINT        e.g. "beam-job-server:8099" (in-network) or "localhost:8099" (host)
    BEAM_ARTIFACT_ENDPOINT   e.g. "beam-job-server:8098"
    BEAM_ENVIRONMENT_TYPE    "EXTERNAL" (cluster lane) or "LOOPBACK" (laptop-dev lane, Ch 4)
    BEAM_ENVIRONMENT_CONFIG  "localhost:50000" for EXTERNAL (worker pool shares the TM netns)
"""
from __future__ import annotations

import os
from typing import Iterable, Optional

from apache_beam.options.pipeline_options import PipelineOptions


def portable_options(
    job_name: str,
    *,
    streaming: bool = False,
    parallelism: int = 2,
    checkpointing_interval_ms: int = 10_000,
    extra: Optional[Iterable[str]] = None,
) -> PipelineOptions:
    """Return PipelineOptions wired to the portable Flink runner.

    Args:
        job_name: shown in the Flink UI; the NestJS submitter passes ``<chapter>-<runId>`` so a
            run can be correlated back to its Flink jobId.
        streaming: set True for unbounded pipelines (windowing/watermark/trigger chapters).
        parallelism: keep == taskmanager.numberOfTaskSlots (2 in this project).
        checkpointing_interval_ms: surfaces checkpoints in the Flink UI (Ch 16 labs).
        extra: any additional ``--flag=value`` strings the chapter needs.
    """
    env_type = os.environ.get("BEAM_ENVIRONMENT_TYPE", "EXTERNAL")

    args = [
        "--runner=PortableRunner",
        f"--job_endpoint={os.environ.get('BEAM_JOB_ENDPOINT', 'localhost:8099')}",
        f"--artifact_endpoint={os.environ.get('BEAM_ARTIFACT_ENDPOINT', 'localhost:8098')}",
        f"--environment_type={env_type}",
        f"--job_name={job_name}",
        f"--parallelism={parallelism}",
        "--save_main_session",
        f"--checkpointing_interval={checkpointing_interval_ms}",
    ]

    # LOOPBACK runs the SDK harness inside the submitting process itself and needs no config; the
    # EXTERNAL worker pool is reached at localhost:50000 because it shares the TaskManager netns.
    if env_type == "EXTERNAL":
        args.append(
            f"--environment_config={os.environ.get('BEAM_ENVIRONMENT_CONFIG', 'localhost:50000')}"
        )

    if streaming:
        args.append("--streaming")

    if extra:
        args.extend(extra)

    return PipelineOptions(args)


def kafka_bootstrap() -> str:
    """In-network Kafka listener the SDK harnesses must use (NOT localhost:9092)."""
    return os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
