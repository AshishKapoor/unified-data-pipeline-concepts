import { Controller, Get, Param } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { FlinkService } from './flink.service';
import {
  FlinkCheckpointStatsDto,
  FlinkJobDetailDto,
  FlinkJobsOverviewDto,
  FlinkMetricDto,
} from './dto/flink.dto';

@ApiTags('flink')
@Controller('flink')
export class FlinkController {
  constructor(private readonly flink: FlinkService) {}

  @Get('jobs')
  @ApiOperation({ summary: 'Proxy Flink /jobs/overview' })
  jobs(): Promise<FlinkJobsOverviewDto> {
    return this.flink.getJobsOverview();
  }

  @Get('jobs/:jid')
  @ApiOperation({ summary: 'Proxy Flink /jobs/:jid (job graph + vertices)' })
  job(@Param('jid') jid: string): Promise<FlinkJobDetailDto> {
    return this.flink.getJob(jid);
  }

  @Get('jobs/:jid/checkpoints')
  @ApiOperation({ summary: 'Proxy Flink checkpoint statistics' })
  checkpoints(@Param('jid') jid: string): Promise<FlinkCheckpointStatsDto> {
    return this.flink.getCheckpoints(jid);
  }

  @Get('jobs/:jid/metrics')
  @ApiOperation({ summary: 'Proxy selected Flink job metrics' })
  metrics(@Param('jid') jid: string): Promise<FlinkMetricDto[]> {
    return this.flink.getMetrics(jid);
  }
}
