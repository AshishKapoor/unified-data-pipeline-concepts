import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { spawn } from 'node:child_process';
import { SubmitterConfig } from '../config/configuration';
import { ConceptsService } from '../concepts/concepts.service';
import { FlinkService } from '../flink/flink.service';
import { RunRegistryService } from '../runs/run-registry.service';
import { RunDto } from '../runs/run.model';

/**
 * Launches a chapter's Beam pipeline by `docker exec`-ing `python pipeline.py` inside the
 * long-running submitter container, then streams its output to the run registry and correlates the
 * run with its Flink job (by the `--job_name` we set).
 *
 * Safety: we use `spawn` with an explicit argv array and `shell: false` — never string
 * interpolation into a shell — so a concept id can't inject a command.
 */
@Injectable()
export class PipelinesService {
  private readonly logger = new Logger(PipelinesService.name);
  private readonly cfg: SubmitterConfig;

  constructor(
    config: ConfigService,
    private readonly concepts: ConceptsService,
    private readonly registry: RunRegistryService,
    private readonly flink: FlinkService,
  ) {
    this.cfg = config.getOrThrow<SubmitterConfig>('submitter');
  }

  run(conceptId: string): RunDto {
    const concept = this.concepts.getConceptOrThrow(conceptId);
    const dto = this.registry.create(conceptId);
    const pipelinePath = `${this.cfg.pipelinesDir}/${concept.pipelineDir}/pipeline.py`;

    // docker exec <container> python <pipeline> --run_id=<id> --job_name=<concept>-<id>
    const argv = [
      'exec',
      this.cfg.container,
      'python',
      pipelinePath,
      `--run_id=${dto.runId}`,
      `--job_name=${dto.jobName}`,
    ];

    this.logger.log(`Launching ${conceptId}: docker ${argv.join(' ')}`);
    const proc = spawn('docker', argv, { shell: false, env: process.env });
    this.registry.attachProcess(dto.runId, proc);
    this.registry.setStatus(dto.runId, 'RUNNING');

    this.pipeStream(dto.runId, proc.stdout, 'stdout');
    this.pipeStream(dto.runId, proc.stderr, 'stderr');
    this.correlateFlinkJob(dto.runId, dto.jobName);

    proc.on('error', (err) => {
      // e.g. ENOENT if `docker` is missing in the API container.
      this.registry.appendLog(dto.runId, 'stderr', `launch error: ${err.message}`);
      this.registry.complete(dto.runId, 'FAILED', null);
    });

    proc.on('close', (code, signal) => {
      const cur = this.registry.get(dto.runId);
      if (cur.status === 'CANCELLED' || signal === 'SIGTERM' || signal === 'SIGKILL') {
        this.registry.complete(dto.runId, 'CANCELLED', code);
      } else {
        this.registry.complete(dto.runId, code === 0 ? 'SUCCEEDED' : 'FAILED', code);
      }
    });

    return dto;
  }

  cancel(runId: string): RunDto {
    const dto = this.registry.requestCancel(runId);
    this.registry.setStatus(runId, 'CANCELLED');
    return dto;
  }

  /** Split a stream into newline-delimited log events without losing partial trailing lines. */
  private pipeStream(
    runId: string,
    stream: NodeJS.ReadableStream | null,
    which: 'stdout' | 'stderr',
  ): void {
    if (!stream) return;
    let buffer = '';
    stream.setEncoding('utf8');
    stream.on('data', (chunk: string) => {
      buffer += chunk;
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        this.registry.appendLog(runId, which, line);
      }
    });
    stream.on('end', () => {
      if (buffer.length) this.registry.appendLog(runId, which, buffer);
    });
  }

  /**
   * Poll the Flink REST API until a job named `<concept>-<runId>` appears, then link it to the run
   * so the docs can deep-link into the Flink UI. Gives up after ~60s or when the run ends.
   */
  private correlateFlinkJob(runId: string, jobName: string): void {
    let tries = 0;
    const maxTries = 30;
    const interval = setInterval(() => {
      void (async () => {
        tries += 1;
        const run = this.registry.get(runId);
        const terminal = run.status === 'SUCCEEDED' || run.status === 'FAILED' || run.status === 'CANCELLED';
        if (run.flinkJobId || terminal || tries > maxTries) {
          clearInterval(interval);
          return;
        }
        const job = await this.flink.findJobByName(jobName);
        if (job) {
          this.registry.linkFlinkJob(runId, job.jid);
          clearInterval(interval);
        }
      })();
    }, 2000);
    interval.unref?.();
  }
}
