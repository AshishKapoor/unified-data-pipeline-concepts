import { HttpService } from '@nestjs/axios';
import { Injectable, Logger, ServiceUnavailableException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AxiosError } from 'axios';
import { catchError, firstValueFrom, timeout } from 'rxjs';
import { FlinkConfig } from '../config/configuration';
import {
  FlinkCheckpointStatsDto,
  FlinkJobDetailDto,
  FlinkJobSummaryDto,
  FlinkJobsOverviewDto,
  FlinkMetricDto,
} from './dto/flink.dto';

/**
 * Typed client for the Flink JobManager REST API. The browser never talks to Flink directly — it
 * goes through this proxy so we get DTOs, timeouts, and clean error mapping.
 */
@Injectable()
export class FlinkService {
  private readonly logger = new Logger(FlinkService.name);
  private readonly cfg: FlinkConfig;

  constructor(
    private readonly http: HttpService,
    config: ConfigService,
  ) {
    this.cfg = config.getOrThrow<FlinkConfig>('flink');
  }

  private async get<T>(path: string): Promise<T> {
    const url = `${this.cfg.restUrl}${path}`;
    const response = await firstValueFrom(
      this.http.get<T>(url).pipe(
        timeout(this.cfg.timeoutMs),
        catchError((err: AxiosError) => {
          this.logger.warn(`Flink REST ${path} failed: ${err.message}`);
          throw new ServiceUnavailableException(
            `Flink JobManager unreachable or slow at ${this.cfg.restUrl} (${err.message}).`,
          );
        }),
      ),
    );
    return response.data;
  }

  async getJobsOverview(): Promise<FlinkJobsOverviewDto> {
    return this.get<FlinkJobsOverviewDto>('/jobs/overview');
  }

  async getJob(jid: string): Promise<FlinkJobDetailDto> {
    return this.get<FlinkJobDetailDto>(`/jobs/${jid}`);
  }

  async getCheckpoints(jid: string): Promise<FlinkCheckpointStatsDto> {
    return this.get<FlinkCheckpointStatsDto>(`/jobs/${jid}/checkpoints`);
  }

  async getMetrics(jid: string): Promise<FlinkMetricDto[]> {
    return this.get<FlinkMetricDto[]>(`/jobs/${jid}/metrics`);
  }

  /**
   * Correlate a run with its Flink job by the `--job_name` the submitter set
   * (`<concept>-<runId>`). Returns the most recent match, or undefined if not yet visible.
   */
  async findJobByName(jobName: string): Promise<FlinkJobSummaryDto | undefined> {
    try {
      const overview = await this.getJobsOverview();
      const matches = overview.jobs.filter((j) => j.name === jobName);
      matches.sort((a, b) => b['start-time'] - a['start-time']);
      return matches[0];
    } catch {
      return undefined;
    }
  }

  /** Request cancellation of a running Flink job. Best-effort. */
  async cancelJob(jid: string): Promise<void> {
    const url = `${this.cfg.restUrl}/jobs/${jid}?mode=cancel`;
    await firstValueFrom(
      this.http.patch(url).pipe(
        timeout(this.cfg.timeoutMs),
        catchError((err: AxiosError) => {
          this.logger.warn(`Cancel job ${jid} failed: ${err.message}`);
          throw new ServiceUnavailableException(`Could not cancel Flink job ${jid}.`);
        }),
      ),
    );
  }
}
