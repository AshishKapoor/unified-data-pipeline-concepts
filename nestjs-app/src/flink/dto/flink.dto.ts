import { ApiProperty } from '@nestjs/swagger';

/** One job as returned by Flink's `/jobs/overview`. */
export class FlinkJobSummaryDto {
  @ApiProperty({ example: '3b4c…', description: 'Flink job id (jid)' }) jid!: string;
  @ApiProperty({ example: 'ch09-cli-1730000000' }) name!: string;
  @ApiProperty({ example: 'RUNNING' }) state!: string;
  @ApiProperty({ example: 1730000000000 }) 'start-time'!: number;
  @ApiProperty({ example: -1 }) 'end-time'!: number;
  @ApiProperty({ example: 12000 }) duration!: number;
}

export class FlinkJobsOverviewDto {
  @ApiProperty({ type: [FlinkJobSummaryDto] }) jobs!: FlinkJobSummaryDto[];
}

/** A vertex (operator) inside a job, from `/jobs/:jid`. */
export class FlinkVertexDto {
  @ApiProperty() id!: string;
  @ApiProperty() name!: string;
  @ApiProperty({ example: 2 }) parallelism!: number;
  @ApiProperty({ example: 'RUNNING' }) status!: string;
}

export class FlinkJobDetailDto {
  @ApiProperty() jid!: string;
  @ApiProperty() name!: string;
  @ApiProperty({ example: 'RUNNING' }) state!: string;
  @ApiProperty({ type: [FlinkVertexDto] }) vertices!: FlinkVertexDto[];
}

export class FlinkCheckpointStatsDto {
  @ApiProperty({ description: 'Raw checkpoint-stats payload from Flink', type: Object })
  counts!: Record<string, number>;
  @ApiProperty({ type: Object, required: false }) latest?: Record<string, unknown>;
}

export class FlinkMetricDto {
  @ApiProperty() id!: string;
  @ApiProperty({ required: false }) value?: string;
}
