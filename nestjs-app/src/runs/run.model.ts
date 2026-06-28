import { ApiProperty } from '@nestjs/swagger';

export type RunStatus = 'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED';

/** A single event in a run's lifecycle, streamed to the browser over SSE. */
export interface RunEvent {
  /** 'log' = a stdout/stderr line, 'status' = state change, 'flink' = jobId linked, 'end' = terminal. */
  type: 'log' | 'status' | 'flink' | 'end';
  /** Monotonic sequence number within the run. */
  seq: number;
  /** ms since epoch. */
  ts: number;
  /** For 'log': the text line. */
  line?: string;
  /** For 'log': which stream. */
  stream?: 'stdout' | 'stderr';
  /** For 'status'/'end': the run status. */
  status?: RunStatus;
  /** For 'flink': the correlated Flink job id. */
  flinkJobId?: string;
}

/** Public, serializable view of a run. */
export class RunDto {
  @ApiProperty({ example: 'a1b2c3d4' }) runId!: string;
  @ApiProperty({ example: 'ch09' }) conceptId!: string;
  @ApiProperty({ example: 'ch09-a1b2c3d4', description: 'Flink --job_name set by the submitter' })
  jobName!: string;
  @ApiProperty({ enum: ['PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'] })
  status!: RunStatus;
  @ApiProperty({ required: false, nullable: true }) flinkJobId!: string | null;
  @ApiProperty({ required: false, nullable: true }) exitCode!: number | null;
  @ApiProperty() startedAt!: number;
  @ApiProperty({ required: false, nullable: true }) endedAt!: number | null;
}
