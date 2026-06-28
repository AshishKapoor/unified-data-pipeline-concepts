import { plainToInstance } from 'class-transformer';
import { IsInt, IsOptional, IsString, Max, Min, validateSync } from 'class-validator';

/**
 * Boot-time environment validation. Required keys must be present and well-formed before the app
 * starts, so misconfiguration fails fast with a clear message instead of cryptic runtime errors.
 */
class EnvVars {
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(65535)
  PORT?: number;

  @IsOptional()
  @IsString()
  FLINK_REST_URL?: string;

  @IsOptional()
  @IsString()
  BEAM_JOB_ENDPOINT?: string;

  @IsOptional()
  @IsString()
  BEAM_ARTIFACT_ENDPOINT?: string;

  @IsOptional()
  @IsString()
  BEAM_ENVIRONMENT_TYPE?: string;

  @IsOptional()
  @IsString()
  DOCS_ROOT?: string;
}

export function validateEnv(config: Record<string, unknown>): Record<string, unknown> {
  const validated = plainToInstance(EnvVars, config, { enableImplicitConversion: true });
  const errors = validateSync(validated, { skipMissingProperties: true });
  if (errors.length > 0) {
    throw new Error(`Invalid environment configuration:\n${errors.toString()}`);
  }
  return config;
}
