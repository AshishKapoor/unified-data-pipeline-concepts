import { Controller, Get } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import {
  HealthCheck,
  HealthCheckResult,
  HealthCheckService,
  HttpHealthIndicator,
} from '@nestjs/terminus';
import { FlinkConfig } from '../config/configuration';

@ApiTags('health')
@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly http: HttpHealthIndicator,
    private readonly config: ConfigService,
  ) {}

  /** Liveness/readiness — confirms the Flink JobManager REST API is reachable. */
  @Get()
  @HealthCheck()
  @ApiOperation({ summary: 'Health check (pings the Flink JobManager)' })
  check(): Promise<HealthCheckResult> {
    const flink = this.config.getOrThrow<FlinkConfig>('flink');
    return this.health.check([
      () => this.http.pingCheck('flink-jobmanager', `${flink.restUrl}/overview`),
    ]);
  }
}
