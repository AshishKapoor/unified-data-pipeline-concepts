import { resolve } from 'node:path';

/**
 * Typed, namespaced configuration assembled from environment variables.
 *
 * Defaults target the local-dev lane (running `npm run start:dev` from `nestjs-app/` with the
 * cluster's ports published to localhost). docker-compose overrides these for the in-network lane.
 */
export interface AppConfig {
  port: number;
  /** Root of the hand-crafted HTML course served at /docs. */
  docsRoot: string;
}

export interface FlinkConfig {
  /** Base URL of the Flink JobManager REST API. */
  restUrl: string;
  /** Per-request timeout (ms) so a hung JobManager can't stall the API. */
  timeoutMs: number;
}

export interface BeamConfig {
  jobEndpoint: string;
  artifactEndpoint: string;
  environmentType: 'EXTERNAL' | 'LOOPBACK';
  environmentConfig: string;
}

export interface SubmitterConfig {
  /** docker-compose service name of the long-running Python submitter container. */
  service: string;
  /** docker-compose project name. */
  composeProject: string;
  /** Concrete container name the API `docker exec`s into (defaults to <project>-<service>-1). */
  container: string;
  /** Absolute path of the mounted beam-pipelines dir inside the submitter container. */
  pipelinesDir: string;
}

export interface RootConfig {
  app: AppConfig;
  flink: FlinkConfig;
  beam: BeamConfig;
  submitter: SubmitterConfig;
}

export default (): RootConfig => ({
  app: {
    port: parseInt(process.env.PORT ?? '3000', 10),
    docsRoot: process.env.DOCS_ROOT ?? resolve(process.cwd(), '..', 'docs'),
  },
  flink: {
    restUrl: process.env.FLINK_REST_URL ?? 'http://localhost:8081',
    timeoutMs: parseInt(process.env.FLINK_TIMEOUT_MS ?? '5000', 10),
  },
  beam: {
    jobEndpoint: process.env.BEAM_JOB_ENDPOINT ?? 'localhost:8099',
    artifactEndpoint: process.env.BEAM_ARTIFACT_ENDPOINT ?? 'localhost:8098',
    environmentType:
      (process.env.BEAM_ENVIRONMENT_TYPE as 'EXTERNAL' | 'LOOPBACK') ?? 'EXTERNAL',
    environmentConfig: process.env.BEAM_ENVIRONMENT_CONFIG ?? 'localhost:50000',
  },
  submitter: {
    service: process.env.SUBMITTER_SERVICE ?? 'submitter',
    composeProject: process.env.COMPOSE_PROJECT ?? 'unified-data-pipeline-concepts',
    container:
      process.env.SUBMITTER_CONTAINER ??
      `${process.env.COMPOSE_PROJECT ?? 'unified-data-pipeline-concepts'}-${
        process.env.SUBMITTER_SERVICE ?? 'submitter'
      }-1`,
    pipelinesDir: process.env.PIPELINES_DIR ?? '/pipelines',
  },
});
