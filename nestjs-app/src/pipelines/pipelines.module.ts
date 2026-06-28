import { Module } from '@nestjs/common';
import { ConceptsModule } from '../concepts/concepts.module';
import { FlinkModule } from '../flink/flink.module';
import { RunsModule } from '../runs/runs.module';
import { PipelinesController } from './pipelines.controller';
import { PipelinesService } from './pipelines.service';

@Module({
  imports: [ConceptsModule, RunsModule, FlinkModule],
  controllers: [PipelinesController],
  providers: [PipelinesService],
})
export class PipelinesModule {}
