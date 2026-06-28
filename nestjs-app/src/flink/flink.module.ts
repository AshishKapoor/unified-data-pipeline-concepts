import { HttpModule } from '@nestjs/axios';
import { Module } from '@nestjs/common';
import { FlinkController } from './flink.controller';
import { FlinkService } from './flink.service';

@Module({
  imports: [HttpModule.register({ timeout: 5000, maxRedirects: 2 })],
  controllers: [FlinkController],
  providers: [FlinkService],
  exports: [FlinkService],
})
export class FlinkModule {}
