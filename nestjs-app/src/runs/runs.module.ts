import { Module } from '@nestjs/common';
import { FlinkModule } from '../flink/flink.module';
import { RunRegistryService } from './run-registry.service';
import { RunsController } from './runs.controller';

@Module({
  imports: [FlinkModule],
  controllers: [RunsController],
  providers: [RunRegistryService],
  exports: [RunRegistryService],
})
export class RunsModule {}
