import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ServeStaticModule } from '@nestjs/serve-static';
import { AppConfig } from '../config/configuration';

/**
 * Serves the hand-crafted, self-contained HTML course at `/docs` (and its assets at
 * `/docs/assets/...`). The site is plain HTML/CSS/JS with vendored Mermaid + D3 — no build step.
 *
 * `exclude: ['/api/{*splat}']` keeps the static handler from shadowing the REST/SSE API (Express 5
 * wildcard syntax). `index.html` is served at the `/docs` root.
 */
@Module({
  imports: [
    ServeStaticModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const app = config.getOrThrow<AppConfig>('app');
        return [
          {
            rootPath: app.docsRoot,
            serveRoot: '/docs',
            exclude: ['/api/{*splat}'],
            serveStaticOptions: { index: 'index.html', fallthrough: true },
          },
        ];
      },
    }),
  ],
})
export class DocsModule {}
