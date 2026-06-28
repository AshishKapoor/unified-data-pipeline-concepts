import { Controller, Get, Param } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { ConceptsService } from './concepts.service';
import { ConceptDto } from './concepts.dto';

@ApiTags('concepts')
@Controller('concepts')
export class ConceptsController {
  constructor(private readonly concepts: ConceptsService) {}

  /** List the full 16-chapter curriculum catalog. */
  @Get()
  @ApiOperation({ summary: 'List all course concepts/chapters' })
  findAll(): ConceptDto[] {
    return this.concepts.findAll();
  }

  /** Fetch a single concept's metadata and the link to its doc chapter. */
  @Get(':id')
  @ApiOperation({ summary: 'Get one concept by id (e.g. ch09)' })
  findOne(@Param('id') id: string): ConceptDto {
    return this.concepts.findOne(id);
  }
}
