# BUILD BRIEF — unified-data-pipeline-concepts

> Single source of truth for the project. Synthesized from four research deep-dives
> (Beam↔Flink portability, curriculum, Flink runtime internals, NestJS orchestration).
> If code and this brief disagree, the brief wins until deliberately revised.

**Teaching project:** Apache Beam (Python) + Apache Flink portability, orchestrated by a
NestJS (TypeScript) control plane, with self-contained interactive HTML docs.

### Locked lead-architect decisions
- **Flink line = 1.19** (per-minor prebuilt job-server image, widest example base). Flink 2.x is a documented upgrade path, not the default.
- **SDK harness environment = EXTERNAL worker pool** — avoids Docker-in-Docker (`DOCKER`) and is the cluster-grade path. `LOOPBACK` is kept only for the fast laptop-dev lane (Ch 4).
- **NestJS spawns a thin Python submitter**, which talks to a long-running **Beam Flink Job Server**. Node never bundles Python/JDK.

---

## 1. Locked technology versions

| Component | Locked version / tag | Notes |
|---|---|---|
| Apache Beam (Python SDK) | `apache-beam==2.74.0` | Pin everywhere identically. |
| Apache Flink | `1.19` (`flink:1.19`) | Cluster minor **must** equal job-server minor. |
| Python | `3.11` | Harness `apache/beam_python3.11_sdk:2.74.0`; submitter venv must be CPython 3.11. |
| Node.js | `22` (`node:22-bookworm-slim`) | Multi-stage Docker build. |
| NestJS | `^11.1.27` | 11.1.27 fixed SSE teardown/empty-response bugs we rely on. |
| @nestjs/swagger | `11.2.0` | |
| Kafka image | `confluentinc/cp-kafka:7.7.0` (KRaft) | Ch 15+ only. |
| Beam Flink Job Server | `apache/beam_flink1.19_job_server:2.74.0` | Never `:latest`. |
| Beam Python SDK harness | `apache/beam_python3.11_sdk:2.74.0` | Run as `--worker_pool`. |

**Golden rule:** the submitting Python, the SDK worker pool, and the job server must **all** be
Beam `2.74.0` on Python `3.11`, and the Flink cluster minor (`1.19`) must equal the job-server's
Flink minor. Version skew produces cryptic coder/proto/gRPC errors.

```
flink:1.19
apache/beam_flink1.19_job_server:2.74.0
apache/beam_python3.11_sdk:2.74.0
confluentinc/cp-kafka:7.7.0          # Ch 15+
node:22-bookworm-slim                # NestJS api build + runtime
python:3.11-slim                     # submitter image (apache-beam==2.74.0 baked in)
```

---

## 2. Portable-runner submission contract

Every pipeline submits with these flags (see `beam-pipelines/_common/options.py`):

| Flag | Locked value | Why |
|---|---|---|
| `--runner` | `PortableRunner` | Mandatory for Python portability. Never `FlinkRunner` (JVM-only). |
| `--job_endpoint` | `beam-job-server:8099` (in-net) / `localhost:8099` (host) | JobService gRPC. |
| `--artifact_endpoint` | `…:8098` | ArtifactStaging; set explicitly so wiring is visible. |
| `--environment_type` | `EXTERNAL` | Avoids Docker-in-Docker and custom images. |
| `--environment_config` | `localhost:50000` | Worker pool shares the TaskManager netns → reached at `localhost`. |
| `--parallelism` | `2` | Matches `taskmanager.numberOfTaskSlots: 2`. |
| `--job_name` | `<chapter>-<runId>` | Findable in Flink UI; set by the NestJS submitter. |
| `--save_main_session` | flag | Pickles main-module globals (top-level DoFns). |
| `--checkpointing_interval` | `10000` | Surfaces checkpoints in Ch 16 labs. |

Local-dev lane (Ch 4): drop the worker pool, use `--environment_type=LOOPBACK` (no `--environment_config`).

---

## 3. docker-compose service map

`docker/docker-compose.yml` (core, Ch 1–14, 16):

| # | Service | Image | Ports | Role |
|---|---|---|---|---|
| 1 | `jobmanager` | `flink:1.19` | 8081 | Flink Web UI + REST API |
| 2 | `taskmanager` | `flink:1.19` | — | Runs operators; must reach worker pool at `localhost:50000` |
| 3 | `beam-job-server` | `apache/beam_flink1.19_job_server:2.74.0` | 8099/8098/8097 | Beam proto → Flink JobGraph |
| 4 | `beam-worker-pool` | `apache/beam_python3.11_sdk:2.74.0` | — (`network_mode: service:taskmanager`) | EXTERNAL SDK harness |
| 5 | `api` | local build (`node:22`) | 3000 | NestJS control plane |
| 6 | `submitter` | local build (`python:3.11-slim`) | — | Idle container the API `exec`s `python pipeline.py` into |

`docker/docker-compose.kafka.yml` (overlay, Ch 15): adds `kafka` (`confluentinc/cp-kafka:7.7.0`, KRaft).
**Reachability rule:** SDK harnesses reach the broker at `kafka:29092`; the host uses `localhost:9092`.

**Make-or-break detail:** `beam-worker-pool` runs with `network_mode: "service:taskmanager"` and **no
`ports:`**, so the SDK control endpoint resolves at `localhost:50000` *inside* the TaskManager.

**Shared artifact volume:** one named volume `beam-artifacts` mounted at the identical path
`/tmp/beam-artifact-staging` on jobmanager, taskmanager, job server, worker pool, and submitter.

---

## 4. Chapter list (16 chapters, 4 parts)

Each chapter ships `beam-pipelines/chNN_*/pipeline.py` + `docs/chapters/chNN.html` with a named diagram
(Mermaid for static DAGs, D3 for time-driven animations).

**Part I — Unified model & core abstractions**
1. The Unified Model: Why Beam Exists — Beam = Batch + strEAM; one pipeline → many runners. *(Mermaid fan-out)*
2. Core Abstractions: Pipeline / PCollection / PTransform / PValue — element anatomy. *(D3 element card + Mermaid DAG)*
3. Element-wise Transforms & the DoFn Lifecycle — setup→start_bundle→process→finish_bundle→teardown. *(D3 lifecycle timeline)*
4. Running on Flink for Real: the Portable Runner Architecture — Job Server, Fn API, environment_type. *(D3 submission journey)*

**Part II — Transforms & aggregation**
5. Keyed Aggregation: GroupByKey, CoGroupByKey, Flatten — the shuffle, relational joins. *(D3 shuffle)*
6. Efficient Aggregation: Combine, CombineFn, built-ins — combiner lifting. *(D3 lifting before/after)*
7. Routing Data: Partition, Side Inputs, Tagged Outputs — dead-letter pattern. *(D3 routing/broadcast)*

**Part III — Time, windowing & the streaming heart**
8. The Streaming Mindset: WHAT/WHERE/WHEN/HOW — event vs processing time. *(D3 dual-axis skew)*
9. Windowing: Fixed, Sliding, Sessions, Global. *(D3 windowing visualizer)*
10. Watermarks: how the system knows event time advanced — min-across-inputs, idleness. *(D3 watermark gauges)*
11. Triggers & Accumulation Modes — early/on-time/late panes, accumulating vs discarding. *(D3 pane-firing timeline)*
12. Late Data, Allowed Lateness & Dropped Data — PaneInfo, drop counters. *(D3 late-data gauntlet)*

**Part IV — Stateful processing, IO & the Flink runtime**
13. Stateful Processing: State & Timers in DoFn — ValueState/BagState/CombiningState, @on_timer. *(D3 state cells)*
14. Splittable DoFn (SDF): the modern IO primitive — restriction trackers, residuals. *(D3 restriction split)*
15. IO Connectors & Cross-Language: Files and KafkaIO via the Expansion Service. *(Mermaid + D3 xlang sequence)*
16. Exactly-Once, Fault Tolerance & the Flink Runtime — ABS/Chandy-Lamport, savepoints, backpressure. *(D3 barrier snapshot)*

---

## 5. NestJS API surface

Global prefix `api`. `ServeStaticModule` serves hand-crafted docs at `/docs`; Swagger UI at `/docs/api`.

| Method & path | Purpose |
|---|---|
| `GET /api/concepts` | List the 16 concepts (catalog) |
| `GET /api/concepts/:concept` | One concept + linked doc chapter |
| `POST /api/pipelines/:concept/run` | Submit the chapter's Beam pipeline (async) → `{ runId }` |
| `GET /api/runs` / `GET /api/runs/:id` | Run list / run state + Flink jobId |
| `GET /api/runs/:id/events` | **SSE** live stdout/stderr + status |
| `POST /api/runs/:id/cancel` | SIGTERM submitter + cancel Flink job |
| `GET /api/flink/jobs` / `:jid` / `:jid/checkpoints` / `:jid/metrics` | Typed proxy to Flink REST |
| `GET /api/health` | Terminus liveness/readiness |

**Modules:** ConfigModule (global, validated), ConceptsModule (catalog/registry), PipelinesModule
(spawns submitter), RunsModule (RxJS ReplaySubject registry + SSE), FlinkModule (typed REST client),
DocsModule (ServeStatic), HealthModule (terminus), CommonModule (DTOs/filters/interceptors).

---

## 6. Docs design system

- Self-contained, offline-capable, served at `/docs`. Vendored (pinned) Mermaid + D3 — no CDN.
- `assets/css/site.css` — design tokens, 2-column chapter layout (prose + sticky diagram pane), run-status badges, pane-state legend mirroring Flink (blue=idle / red=busy / black=backpressure).
- `assets/js/site.js` — chapter nav, the **Run on Flink** button (`POST /api/pipelines/:concept/run` → `EventSource('/api/runs/:id/events')`), SSE log viewer, Flink deep-link.
- `assets/js/d3-anim/*.js` — one ES module per animation, each exporting `mount(el, config)`.
- `_template/chapter.html` — canonical chapter template (prose column + diagram aside + Run panel).

---

## 7. Build order

- **Phase 0 — Infra spine:** scaffold, core compose, scripts, submitter image, `_common/`. Smoke-test stock WordCount via PortableRunner/EXTERNAL **before** any custom code.
- **Phase 1 — Control plane:** `nest new`, ConfigModule, FlinkModule (verify against live cluster), RunsModule+SSE, PipelinesModule, ConceptsModule, HealthModule.
- **Phase 2 — Docs design system:** css/js, vendored mermaid/d3, template, index grid, wire DocsModule.
- **Phase 3 — Per-chapter loop (1→16):** pipeline.py → registry entry → D3/Mermaid diagram → chapter HTML → verify Run button.
- **Phase 4 — Streaming/runtime hardening:** Kafka overlay + seed (before Ch 15); checkpointing + kill/recover + savepoint/rescale (before Ch 16).

---

## 8. Known risks & mitigations (top items)

1. `environment_type=DOCKER` in a Dockerized cluster → DinD failure. **Mitigation:** locked to EXTERNAL worker pool.
2. EXTERNAL worker pool unreachable (control host pinned to `localhost`). **Mitigation:** `network_mode: "service:taskmanager"`, no `ports:` on the pool.
3. Version skew (submitter/job-server/worker-pool/Python). **Mitigation:** everything Beam 2.74.0 + Python 3.11; never `:latest`.
4. Flink minor ≠ job-server minor. **Mitigation:** both locked to 1.19.
5. TaskManager binds loopback / can't reach pool. **Mitigation:** `taskmanager.bind-host: 0.0.0.0`, `taskmanager.host: taskmanager`, `rest.bind-address: 0.0.0.0`.
6. Artifact staging path mismatch. **Mitigation:** one `beam-artifacts` volume at identical path everywhere.
7. Cross-language KafkaIO weight. **Mitigation:** isolated to Ch 15 overlay; harnesses use `kafka:29092`; `seed-kafka.sh` pre-creates topics.
8. Stateful DoFns/timers less mature in Python-on-Flink. **Mitigation:** Ch 13 uses only ValueState/BagState/CombiningState.
9. Custom unbounded SDF uncommon in Python. **Mitigation:** Ch 14 teaches a bounded toy range; unbounded marked advanced.
10. End-to-end exactly-once ≠ checkpointing alone. **Mitigation:** Ch 16 separates EOS state from sink EOS.
11. `exec` buffers/truncates + injection. **Mitigation:** NestJS `spawn` with array args, `shell:false`, `PYTHONUNBUFFERED=1`.
12. Express 5 wildcard shadowing API routes. **Mitigation:** API under `/api`; ServeStatic `exclude:['/api/{*splat}']`; Swagger at `/docs/api`.
13. In-memory run registry lost on restart. **Mitigation:** acceptable for a learning app; `ReplaySubject(500)` caps memory; documented.
14. Hung JobManager stalls API. **Mitigation:** axios `timeout: 5000` → `ServiceUnavailableException`; health pings `/overview`.
15. Flink 2.x temptation. **Mitigation:** stay on 1.19; documented upgrade path only.
16. No `--job_name` → unfindable jobs. **Mitigation:** submitter always sets `--job_name=<chapter>-<runId>` and parses it back.
