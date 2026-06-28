import { ApiProperty } from '@nestjs/swagger';
import { Concept } from './concepts.registry';

/** A single course chapter / concept in the catalog. */
export class ConceptDto implements Concept {
  @ApiProperty({ example: 'ch09' }) id!: string;
  @ApiProperty({ example: 9 }) number!: number;
  @ApiProperty({ example: 3 }) part!: number;
  @ApiProperty({ example: 'Time, Windowing & the Streaming Heart' }) partTitle!: string;
  @ApiProperty() title!: string;
  @ApiProperty() summary!: string;
  @ApiProperty({ type: [String] }) beamApis!: string[];
  @ApiProperty({ example: 'ch09_windowing' }) pipelineDir!: string;
  @ApiProperty({ example: '/docs/chapters/ch09.html' }) docHref!: string;
  @ApiProperty() requiresKafka!: boolean;
  @ApiProperty() streaming!: boolean;
}
