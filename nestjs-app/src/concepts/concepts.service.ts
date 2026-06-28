import { Injectable, NotFoundException } from '@nestjs/common';
import { CONCEPTS, CONCEPTS_BY_ID, Concept, PARTS } from './concepts.registry';
import { ConceptDto } from './concepts.dto';

@Injectable()
export class ConceptsService {
  private toDto(c: Concept): ConceptDto {
    return { ...c, partTitle: PARTS[c.part] ?? `Part ${c.part}` };
  }

  findAll(): ConceptDto[] {
    return CONCEPTS.map((c) => this.toDto(c));
  }

  findOne(id: string): ConceptDto {
    const concept = CONCEPTS_BY_ID.get(id);
    if (!concept) {
      throw new NotFoundException(`Unknown concept '${id}'. Try GET /api/concepts for the catalog.`);
    }
    return this.toDto(concept);
  }

  /** Used by other modules that need the raw registry entry (e.g. PipelinesService). */
  getConceptOrThrow(id: string): Concept {
    const concept = CONCEPTS_BY_ID.get(id);
    if (!concept) {
      throw new NotFoundException(`Unknown concept '${id}'.`);
    }
    return concept;
  }
}
