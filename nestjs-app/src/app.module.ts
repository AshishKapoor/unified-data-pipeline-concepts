import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import configuration from './config/configuration';
import { validateEnv } from './config/env.validation';
import { ConceptsModule } from './concepts/concepts.module';
import { DocsModule } from './docs/docs.module';
import { FlinkModule } from './flink/flink.module';
import { HealthModule } from './health/health.module';
import { PipelinesModule } from './pipelines/pipelines.module';
import { RunsModule } from './runs/runs.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [configuration],
      validate: validateEnv,
      cache: true,
    }),
    // Static interactive docs at /docs (declared first so it is registered early).
    DocsModule,
    // REST + SSE API under /api.
    ConceptsModule,
    FlinkModule,
    RunsModule,
    PipelinesModule,
    HealthModule,
  ],
})
export class AppModule {}
