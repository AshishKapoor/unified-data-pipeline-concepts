/**
 * The catalog of the 16 course chapters — the single source of truth shared by the Concepts API
 * (browse the curriculum) and the Pipelines API (which `pipeline.py` to launch for a concept).
 */
export interface Concept {
  /** Stable id used in URLs and as the run target, e.g. "ch09". */
  id: string;
  /** 1-based chapter number. */
  number: number;
  /** Course part (1-4). */
  part: number;
  title: string;
  /** One-line summary of what the chapter teaches. */
  summary: string;
  /** Key Beam (or Flink) APIs introduced. */
  beamApis: string[];
  /** Directory under beam-pipelines/ holding pipeline.py. */
  pipelineDir: string;
  /** Relative href of the rendered HTML chapter under /docs. */
  docHref: string;
  /** Whether running this chapter needs the Kafka overlay (Ch 15-16). */
  requiresKafka: boolean;
  /** True for unbounded/streaming pipelines (affects how a run is considered "done"). */
  streaming: boolean;
}

export const PARTS: Record<number, string> = {
  1: 'The Unified Model & Core Abstractions',
  2: 'Transforms & Aggregation',
  3: 'Time, Windowing & the Streaming Heart',
  4: 'Stateful Processing, IO & the Flink Runtime',
};

export const CONCEPTS: Concept[] = [
  {
    id: 'ch01',
    number: 1,
    part: 1,
    title: 'The Unified Model: Why Beam Exists',
    summary:
      'One pipeline definition, many runners. Batch is a bounded special case of streaming; Beam = Batch + strEAM.',
    beamApis: ['apache_beam.Pipeline', 'beam.Create', 'beam.Map', 'beam.io.WriteToText', '--runner'],
    pipelineDir: 'ch01_unified_wordcount',
    docHref: '/docs/chapters/ch01.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch02',
    number: 2,
    part: 1,
    title: 'Core Abstractions: Pipeline, PCollection, PTransform, PValue',
    summary:
      'The four primitives and the anatomy of an element: value + event-timestamp + window(s) + pane-info.',
    beamApis: ['PCollection', 'PTransform.expand', 'composite transforms', 'coders'],
    pipelineDir: 'ch02_core_abstractions',
    docHref: '/docs/chapters/ch02.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch03',
    number: 3,
    part: 1,
    title: 'Element-wise Transforms & the DoFn Lifecycle',
    summary:
      'Map/FlatMap/Filter/ParDo and the bundle lifecycle: setup → start_bundle → process → finish_bundle → teardown.',
    beamApis: ['beam.ParDo', 'beam.DoFn', 'start_bundle/finish_bundle', 'DoFn.TimestampParam'],
    pipelineDir: 'ch03_dofn_lifecycle',
    docHref: '/docs/chapters/ch03.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch04',
    number: 4,
    part: 1,
    title: 'Running on Flink for Real: The Portable Runner Architecture',
    summary:
      'What actually happens when Python runs on Flink: Job Server, the Fn API, SDK harness, environment types.',
    beamApis: ['PortableRunner', '--job_endpoint', '--environment_type', 'Fn API'],
    pipelineDir: 'ch04_portable_runner',
    docHref: '/docs/chapters/ch04.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch05',
    number: 5,
    part: 2,
    title: 'Keyed Aggregation: GroupByKey, CoGroupByKey, Flatten',
    summary: 'The shuffle, relational joins via CoGroupByKey, and unioning PCollections with Flatten.',
    beamApis: ['beam.GroupByKey', 'beam.CoGroupByKey', 'beam.Flatten', 'KV'],
    pipelineDir: 'ch05_groupbykey_cogbk',
    docHref: '/docs/chapters/ch05.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch06',
    number: 6,
    part: 2,
    title: 'Efficient Aggregation: Combine, CombineFn, Built-ins',
    summary:
      'Why Combine beats GroupByKey+reduce: partial aggregation (combiner lifting) via associative/commutative CombineFns.',
    beamApis: ['beam.CombinePerKey', 'beam.CombineFn', 'beam.combiners.Mean/Count/Top'],
    pipelineDir: 'ch06_combine',
    docHref: '/docs/chapters/ch06.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch07',
    number: 7,
    part: 2,
    title: 'Routing Data: Partition, Side Inputs, Tagged Outputs',
    summary: 'Deterministic N-way splits, broadcast side inputs, and the dead-letter pattern via tagged outputs.',
    beamApis: ['beam.Partition', 'beam.pvalue.AsDict/AsList', 'ParDo.with_outputs', 'TaggedOutput'],
    pipelineDir: 'ch07_partition_sideinputs_tagged',
    docHref: '/docs/chapters/ch07.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch08',
    number: 8,
    part: 3,
    title: 'The Streaming Mindset: WHAT / WHERE / WHEN / HOW',
    summary: 'Event time vs processing time and the four questions that structure every streaming computation.',
    beamApis: ['beam.window.TimestampedValue', 'DoFn.TimestampParam', 'event vs processing time'],
    pipelineDir: 'ch08_streaming_mindset',
    docHref: '/docs/chapters/ch08.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch09',
    number: 9,
    part: 3,
    title: 'Windowing: Fixed, Sliding, Sessions, Global',
    summary: 'Window assignment as per-element tagging; fixed, sliding, merging sessions, and the global window.',
    beamApis: ['FixedWindows', 'SlidingWindows', 'Sessions', 'GlobalWindows', 'beam.WindowInto'],
    pipelineDir: 'ch09_windowing',
    docHref: '/docs/chapters/ch09.html',
    requiresKafka: false,
    streaming: true,
  },
  {
    id: 'ch10',
    number: 10,
    part: 3,
    title: 'Watermarks: How the System Knows Event Time Advanced',
    summary: 'Watermark = "no more data with event time ≤ T"; min-across-inputs propagation and idleness.',
    beamApis: ['watermarks', 'WatermarkEstimatorProvider (conceptual)', 'Flink per-operator watermarks'],
    pipelineDir: 'ch10_watermarks',
    docHref: '/docs/chapters/ch10.html',
    requiresKafka: false,
    streaming: true,
  },
  {
    id: 'ch11',
    number: 11,
    part: 3,
    title: 'Triggers & Accumulation Modes',
    summary: 'WHEN to emit and HOW panes relate: early/on-time/late firings, accumulating vs discarding.',
    beamApis: ['AfterWatermark', 'AfterProcessingTime', 'AfterCount', 'Repeatedly', 'accumulation_mode'],
    pipelineDir: 'ch11_triggers_accumulation',
    docHref: '/docs/chapters/ch11.html',
    requiresKafka: false,
    streaming: true,
  },
  {
    id: 'ch12',
    number: 12,
    part: 3,
    title: 'Late Data, Allowed Lateness & Dropped Data',
    summary: 'The late-element lifecycle, allowed-lateness grace, window state GC, and measuring dropped data.',
    beamApis: ['allowed_lateness', 'DoFn.PaneInfoParam', 'beam.metrics.Metrics.counter'],
    pipelineDir: 'ch12_late_data',
    docHref: '/docs/chapters/ch12.html',
    requiresKafka: false,
    streaming: true,
  },
  {
    id: 'ch13',
    number: 13,
    part: 4,
    title: 'Stateful Processing: State & Timers in DoFn',
    summary: 'Per-key-per-window state cells (Value/Bag/Combining) and event/processing-time timers with @on_timer.',
    beamApis: ['ReadModifyWriteStateSpec', 'BagStateSpec', 'CombiningValueStateSpec', 'TimerSpec', '@on_timer'],
    pipelineDir: 'ch13_state_timers',
    docHref: '/docs/chapters/ch13.html',
    requiresKafka: false,
    streaming: true,
  },
  {
    id: 'ch14',
    number: 14,
    part: 4,
    title: 'Splittable DoFn (SDF): The Modern IO Primitive',
    summary: 'One element splits dynamically and checkpoints mid-processing via a restriction + restriction tracker.',
    beamApis: ['RestrictionProvider', 'OffsetRangeTracker', 'try_claim', 'RestrictionParam'],
    pipelineDir: 'ch14_splittable_dofn',
    docHref: '/docs/chapters/ch14.html',
    requiresKafka: false,
    streaming: false,
  },
  {
    id: 'ch15',
    number: 15,
    part: 4,
    title: 'IO Connectors & Cross-Language: Files and KafkaIO',
    summary:
      'TextIO vs KafkaIO; the cross-language expansion service that lets Python use the Java KafkaIO transform.',
    beamApis: ['beam.io.ReadFromText/WriteToText', 'ReadFromKafka/WriteToKafka', 'expansion service'],
    pipelineDir: 'ch15_kafka_xlang',
    docHref: '/docs/chapters/ch15.html',
    requiresKafka: true,
    streaming: true,
  },
  {
    id: 'ch16',
    number: 16,
    part: 4,
    title: 'Exactly-Once, Fault Tolerance & the Flink Runtime',
    summary:
      'Checkpointing (ABS / Chandy-Lamport) + Beam bundles → exactly-once state; savepoints, rescaling, backpressure.',
    beamApis: ['--checkpointing_interval', 'state.backend', 'Flink REST /checkpoints', 'savepoints'],
    pipelineDir: 'ch16_exactly_once_runtime',
    docHref: '/docs/chapters/ch16.html',
    requiresKafka: true,
    streaming: true,
  },
];

export const CONCEPTS_BY_ID: Map<string, Concept> = new Map(CONCEPTS.map((c) => [c.id, c]));
