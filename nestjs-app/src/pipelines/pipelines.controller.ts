import { Controller, HttpCode, Param, Post } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { RunDto } from '../runs/run.model';
import { PipelinesService } from './pipelines.service';

@ApiTags('pipelines')
@Controller('pipelines')
export class PipelinesController {
  constructor(private readonly pipelines: PipelinesService) {}

  /**
   * Launch the Beam pipeline for a concept. Returns immediately with a runId; the client then opens
   * `GET /api/runs/:runId/events` (SSE) to stream logs and watch the run reach the Flink cluster.
   */
  @Post(':concept/run')
  @HttpCode(201)
  @ApiOperation({ summary: 'Submit a chapter pipeline to Flink (async); returns a runId' })
  run(@Param('concept') concept: string): RunDto {
    return this.pipelines.run(concept);
  }
}
