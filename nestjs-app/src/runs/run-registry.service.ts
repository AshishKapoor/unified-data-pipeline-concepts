import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { Observable, ReplaySubject } from 'rxjs';
import { RunDto, RunEvent, RunStatus } from './run.model';

interface RunHandle {
  dto: RunDto;
  /** ReplaySubject(500) so a browser that connects mid-run still gets recent history. */
  subject: ReplaySubject<RunEvent>;
  seq: number;
  /** The submitter child process, kept for cancellation. */
  proc?: ChildProcess;
}

/**
 * In-memory registry of pipeline runs. This is a *learning* app: runs live in process memory and
 * are lost on restart, and it is not safe across multiple replicas. ReplaySubject(500) caps the
 * buffered history per run. Swap for Redis/BullMQ only if this is ever scaled out.
 */
@Injectable()
export class RunRegistryService {
  private readonly logger = new Logger(RunRegistryService.name);
  private readonly runs = new Map<string, RunHandle>();
  /** Keep the most recent N runs visible in the list endpoint. */
  private readonly order: string[] = [];
  private static readonly MAX_RUNS = 100;

  create(conceptId: string): RunDto {
    const runId = randomUUID().slice(0, 8);
    const dto: RunDto = {
      runId,
      conceptId,
      jobName: `${conceptId}-${runId}`,
      status: 'PENDING',
      flinkJobId: null,
      exitCode: null,
      startedAt: Date.now(),
      endedAt: null,
    };
    this.runs.set(runId, { dto, subject: new ReplaySubject<RunEvent>(500), seq: 0 });
    this.order.push(runId);
    this.evictIfNeeded();
    return dto;
  }

  private evictIfNeeded(): void {
    while (this.order.length > RunRegistryService.MAX_RUNS) {
      const oldest = this.order.shift();
      if (oldest) {
        this.runs.get(oldest)?.subject.complete();
        this.runs.delete(oldest);
      }
    }
  }

  private handle(runId: string): RunHandle {
    const h = this.runs.get(runId);
    if (!h) throw new NotFoundException(`Unknown run '${runId}'.`);
    return h;
  }

  get(runId: string): RunDto {
    return this.handle(runId).dto;
  }

  list(): RunDto[] {
    return this.order
      .slice()
      .reverse()
      .map((id) => this.runs.get(id)?.dto)
      .filter((d): d is RunDto => !!d);
  }

  attachProcess(runId: string, proc: ChildProcess): void {
    this.handle(runId).proc = proc;
  }

  private emit(h: RunHandle, event: Omit<RunEvent, 'seq' | 'ts'>): void {
    h.subject.next({ ...event, seq: h.seq++, ts: Date.now() });
  }

  appendLog(runId: string, stream: 'stdout' | 'stderr', line: string): void {
    this.emit(this.handle(runId), { type: 'log', stream, line });
  }

  setStatus(runId: string, status: RunStatus): void {
    const h = this.handle(runId);
    h.dto.status = status;
    this.emit(h, { type: 'status', status });
  }

  linkFlinkJob(runId: string, flinkJobId: string): void {
    const h = this.handle(runId);
    if (h.dto.flinkJobId === flinkJobId) return;
    h.dto.flinkJobId = flinkJobId;
    this.emit(h, { type: 'flink', flinkJobId });
  }

  complete(runId: string, status: RunStatus, exitCode: number | null): void {
    const h = this.handle(runId);
    h.dto.status = status;
    h.dto.exitCode = exitCode;
    h.dto.endedAt = Date.now();
    this.emit(h, { type: 'end', status });
    h.subject.complete();
  }

  /** Best-effort cancel: signal the submitter process; the controller also cancels the Flink job. */
  requestCancel(runId: string): RunDto {
    const h = this.handle(runId);
    if (h.proc && !h.proc.killed) {
      h.proc.kill('SIGTERM');
      // Escalate if it ignores SIGTERM.
      setTimeout(() => {
        if (h.proc && !h.proc.killed) h.proc.kill('SIGKILL');
      }, 5000).unref();
    }
    return h.dto;
  }

  events$(runId: string): Observable<RunEvent> {
    return this.handle(runId).subject.asObservable();
  }
}
