# unified-data-pipeline-concepts

A **hands-on teaching course** for the unified batch + stream processing model, taught with
**Apache Beam** (Python pipelines) running on **Apache Flink**, orchestrated by a **NestJS**
(TypeScript) control plane, with **self-contained interactive HTML docs**.

> One programming model. Two execution modes (batch = a bounded special case of streaming).
> Many runners. This course shows *how it actually works* — every chapter ships a pipeline you
> run on a real Flink cluster and watch in the Flink UI.

```
        ┌──────────────────────────────────────────────────────────────────┐
        │  Browser → interactive docs (/docs)   ·   Swagger (/docs/api)      │
        └───────────────┬──────────────────────────────────────────────────┘
                        │  POST /api/pipelines/:concept/run   (SSE live logs)
                ┌───────▼────────┐   spawn      ┌──────────────┐
                │  NestJS  (api) │ ───────────▶ │  submitter   │  python pipeline.py
                │  control plane │              │ (Beam 2.74)  │  --runner=PortableRunner
                └───────┬────────┘              └──────┬───────┘
            GET /api/flink/* (typed proxy)             │  gRPC :8099
                        │                       ┌──────▼────────────┐
                        │                       │  beam-job-server  │ Beam proto → Flink JobGraph
                        ▼                       └──────┬────────────┘
                ┌───────────────┐  REST :8081         │ submit
                │   Flink UI    │◀──────────── ┌───────▼──────┐   Fn API   ┌──────────────────┐
                │  JobManager   │              │ TaskManager  │ ◀────────▶ │ beam-worker-pool │
                └───────────────┘              └──────────────┘ :50000     │ (EXTERNAL harness)│
                                                                           └──────────────────┘
```

## What you get

- **16 chapters**, fundamentals → advanced (see [the curriculum](#curriculum)).
- A **runnable Python Beam pipeline** per chapter, submitted to Flink via the **portable runner**.
- A **NestJS API** that launches pipelines, streams their logs over **SSE**, and proxies the Flink REST API.
- **Interactive HTML docs** (Mermaid diagrams + D3 animations for windowing, watermarks, triggers,
  checkpoint barriers, …) with a live **"Run on Flink"** button on every chapter.

## Prerequisites

- Docker + Docker Compose v2/v5 (the whole cluster runs in containers).
- **~8 GB free RAM** for Docker (the TaskManager alone is sized at 5 GB so streaming jobs don't OOM).
- That's it — you do **not** need Python, Node, or a JDK installed locally; everything is containerized.

### Port conflicts? (optional)

The stack publishes `8081` (Flink UI), `3000` (docs/API), and `9092` (Kafka). If any are taken on
your machine, copy [`docker/.env.example`](docker/.env.example) to `docker/.env` and set
`FLINK_UI_PORT` / `API_PORT` / `KAFKA_PORT` to free ports — compose and the `scripts/` auto-load it.

## Quick start

```bash
# 1. Bring up Flink + Beam job server + worker pool + the NestJS API
./scripts/up.sh

# 2. Open the course
open http://localhost:3000/docs          # interactive chapters
open http://localhost:3000/docs/api      # Swagger API explorer
open http://localhost:8081               # Flink Web UI

# 3. Smoke-test the portability path with stock WordCount (no custom code)
./scripts/submit.sh wordcount

# 4. Run a chapter pipeline from the CLI (or click "Run on Flink" in the docs)
./scripts/submit.sh ch01

# Chapters 15 (Kafka) and 16 (exactly-once) need the Kafka overlay:
./scripts/up-kafka.sh
./scripts/seed-kafka.sh

# Tear down
./scripts/down.sh
```

## Curriculum

| # | Chapter | You learn |
|---|---------|-----------|
| 1 | The Unified Model | Beam = Batch + strEAM; one pipeline → many runners |
| 2 | Core Abstractions | Pipeline, PCollection, PTransform, PValue; element anatomy |
| 3 | Element-wise Transforms & DoFn Lifecycle | Map/FlatMap/Filter/ParDo; bundle lifecycle |
| 4 | Running on Flink for Real | Portable runner, Job Server, Fn API, environment types |
| 5 | Keyed Aggregation | GroupByKey, CoGroupByKey, Flatten; the shuffle |
| 6 | Efficient Aggregation | Combine, CombineFn, combiner lifting |
| 7 | Routing Data | Partition, side inputs, tagged outputs (dead-letter) |
| 8 | The Streaming Mindset | Event vs processing time; WHAT/WHERE/WHEN/HOW |
| 9 | Windowing | Fixed, sliding, sessions, global |
| 10 | Watermarks | Min-across-inputs propagation, idleness |
| 11 | Triggers & Accumulation | Early/on-time/late panes; accumulating vs discarding |
| 12 | Late Data | Allowed lateness, PaneInfo, dropped-data metrics |
| 13 | Stateful Processing | State (Value/Bag/Combining) + event/processing-time timers |
| 14 | Splittable DoFn | Restriction trackers, residuals, dynamic splitting |
| 15 | IO & Cross-Language | TextIO + KafkaIO via the Expansion Service |
| 16 | Exactly-Once & the Flink Runtime | Checkpointing/ABS, savepoints, backpressure, recovery |

## Repository layout

```
docker/           docker-compose core + Kafka overlay
nestjs-app/       TypeScript control plane (API + serves the docs)
beam-pipelines/   one Python Beam pipeline per chapter + shared _common/ helpers
docs/             self-contained interactive HTML site (the course itself)
scripts/          up/down/submit/seed helpers
BUILD_BRIEF.md    the locked architecture & version pins (source of truth)
```

## The golden rule (read before you change versions)

The **submitting Python, the SDK worker pool, and the job server must all be Beam `2.74.0` on
Python `3.11`**, and the **Flink cluster minor (`1.19`) must equal the job-server's Flink minor**.
Version skew here is the #1 cause of cryptic coder/proto/gRPC errors. All image tags are pinned in
[`docker/docker-compose.yml`](docker/docker-compose.yml); see [`BUILD_BRIEF.md`](BUILD_BRIEF.md) §1.

## Verified

All 16 chapter pipelines have been run end-to-end on the real Dockerized Flink cluster
(`STOPPED → RUNNING → DONE`), and the full control-plane flow was exercised live:
`POST /api/pipelines/:concept/run` → SSE log stream → Flink job correlation → `SUCCEEDED`. The
`up.sh` → `submit.sh` → `down.sh` lifecycle starts, configures, and tears down with **zero leaked
containers, volumes, or networks**.

## Status / honesty notes

This is a **learning environment**, not production infrastructure:
- The run registry is in-memory (`ReplaySubject`) — runs are lost on API restart.
- **Chapters 15 & 16 (cross-language KafkaIO):** the *Java* KafkaIO harness needs a Java SDK worker
  pool, which is genuinely involved on a Flink EXTERNAL-worker cluster (the stock Java SDK image does
  not expose the simple `--worker_pool` mode the Python image does). So by default these chapters run
  a **pure-Python streaming demo** with the same windowed-aggregation shape (and, for Ch 16, live
  checkpointing/recovery you can watch). The **real KafkaIO cross-language code is present and taught**;
  enable it with `ENABLE_XLANG_KAFKA=1` once you provide a Java worker pool (see the chapter pages).
- Some advanced Beam features are less mature in Python-on-Flink; chapters flag these explicitly
  (Ch 13 sticks to Value/Bag/Combining state; Ch 14 teaches a bounded toy SDF; Ch 16 separates
  exactly-once *state* from end-to-end sink exactly-once).

## License

MIT — teaching material, use freely.
