import {
  Controller,
  Get,
  HttpCode,
  MessageEvent,
  Param,
  Post,
  Sse,
} from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { map, Observable } from 'rxjs';
import { FlinkService } from '../flink/flink.service';
import { RunRegistryService } from './run-registry.service';
import { RunDto } from './run.model';

@ApiTags('runs')
@Controller('runs')
export class RunsController {
  constructor(
    private readonly registry: RunRegistryService,
    private readonly flink: FlinkService,
  ) {}

  @Get()
  @ApiOperation({ summary: 'List recent pipeline runs' })
  list(): RunDto[] {
    return this.registry.list();
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get a run’s current state, exit code, and Flink job id' })
  get(@Param('id') id: string): RunDto {
    return this.registry.get(id);
  }

  /**
   * Server-Sent Events: live stdout/stderr lines, status changes, and the linked Flink job id.
   * The browser opens `new EventSource('/api/runs/:id/events')` and renders each event.
   */
  @Sse(':id/events')
  @ApiOperation({ summary: 'Live run events (SSE): logs, status, Flink job link' })
  events(@Param('id') id: string): Observable<MessageEvent> {
    return this.registry.events$(id).pipe(
      map((event) => ({ data: event, type: event.type } as MessageEvent)),
    );
  }

  @Post(':id/cancel')
  @HttpCode(202)
  @ApiOperation({ summary: 'Cancel a run (SIGTERM the submitter + cancel the Flink job)' })
  async cancel(@Param('id') id: string): Promise<RunDto> {
    const dto = this.registry.requestCancel(id);
    if (dto.flinkJobId) {
      await this.flink.cancelJob(dto.flinkJobId).catch(() => undefined);
    }
    return dto;
  }
}
