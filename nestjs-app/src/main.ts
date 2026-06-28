import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import type { Request, Response } from 'express';
import { AppModule } from './app.module';
import { AppConfig } from './config/configuration';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  const logger = new Logger('Bootstrap');

  // All REST/SSE endpoints live under /api so the static docs site can own /docs/*.
  app.setGlobalPrefix('api');
  app.enableCors();
  app.useGlobalPipes(
    new ValidationPipe({ transform: true, whitelist: true, forbidNonWhitelisted: false }),
  );
  app.enableShutdownHooks();

  // Swagger UI at /docs/api (separate from the hand-crafted /docs site). Built AFTER the global
  // prefix so documented paths include /api.
  const swaggerConfig = new DocumentBuilder()
    .setTitle('unified-data-pipeline-concepts API')
    .setDescription('Launch Apache Beam pipelines on Flink, stream run logs (SSE), proxy Flink REST.')
    .setVersion('1.0')
    .addTag('concepts')
    .addTag('pipelines')
    .addTag('runs')
    .addTag('flink')
    .addTag('health')
    .build();
  const document = SwaggerModule.createDocument(app, swaggerConfig);
  SwaggerModule.setup('docs/api', app, document, {
    jsonDocumentUrl: 'docs/api-json',
  });

  // Friendly landing: send the bare root to the course.
  const http = app.getHttpAdapter().getInstance();
  http.get('/', (_req: Request, res: Response) => res.redirect('/docs'));

  const { port } = app.get(ConfigService).getOrThrow<AppConfig>('app');
  await app.listen(port, '0.0.0.0');
  logger.log(`API listening on :${port}  ·  docs http://localhost:${port}/docs  ·  swagger /docs/api`);
}

void bootstrap();
