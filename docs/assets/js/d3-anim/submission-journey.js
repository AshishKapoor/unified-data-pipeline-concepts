/* Ch 04 — "the submission journey".
   An animated horizontal flow of what happens when Python runs on Flink:
     Python submitter → Job Server → JobManager → TaskManager ⇄ EXTERNAL worker pool.
   A "packet" travels left→right through the four stages (proto → JobGraph → schedule → execute);
   when it reaches the TaskManager, the four Fn-API channels (control / data / state / logging)
   pulse between the TaskManager and the SDK worker pool — that is the portability boundary. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// The four pipeline stages, left → right. `payload` is what the packet is carrying as it arrives.
const STAGES = [
  { key: 'sub', label: 'Python\nsubmitter', sub: 'builds pipeline proto', payload: 'pipeline proto' },
  { key: 'js', label: 'Beam Flink\nJob Server', sub: 'proto → JobGraph', payload: 'JobGraph' },
  { key: 'jm', label: 'JobManager', sub: 'schedules tasks', payload: 'task deploy' },
  { key: 'tm', label: 'TaskManager', sub: 'runs operators', payload: 'run user code' },
];

// The Fn-API channels between TaskManager and the SDK worker pool.
const FN_CHANNELS = [
  { name: 'control', tint: () => COLORS.accent() },
  { name: 'data', tint: () => COLORS.beam() },
  { name: 'state', tint: () => COLORS.accent2() },
  { name: 'logging', tint: () => COLORS.soft() },
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 480, H = 340;
  const svg = createSvg(el, W, H);

  // Layout: four stage boxes along a baseline, worker pool sits below the TaskManager.
  const boxW = 96, boxH = 52, rowY = 70;
  const xs = [18, 142, 266, 390];                 // left x of each stage box
  const cx = (i) => xs[i] + boxW / 2;             // centre x of stage i
  const cy = rowY + boxH / 2;                     // centre y of the stage row
  const poolX = xs[3], poolY = 232, poolW = boxW, poolH = 58; // EXTERNAL worker pool under the TM

  // `phase` 0..3 = packet has arrived at STAGES[phase].  At phase 3 the Fn-API channels are live.
  let phase = 0;
  // `progress` 0..1 = how far the packet has travelled from STAGES[phase-1] toward STAGES[phase].
  let progress = 1;
  let pulse = 0; // monotonic counter to animate the Fn-API dashes while at the TaskManager.

  function stageColor(i) {
    if (i === 0) return COLORS.beam();   // Python / Beam
    if (i === 3) return COLORS.flink();  // TaskManager (Flink runtime)
    return COLORS.accent();              // Job Server / JobManager (control plane)
  }

  function draw() {
    svg.selectAll('*').remove();

    svg.append('text').attr('x', W / 2).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('how Python reaches Flink — the submission journey');

    // --- connectors between consecutive stages ---
    for (let i = 0; i < STAGES.length - 1; i++) {
      const reached = phase > i || (phase === i + 1);
      svg.append('line')
        .attr('x1', xs[i] + boxW).attr('y1', cy)
        .attr('x2', xs[i + 1]).attr('y2', cy)
        .attr('stroke', reached ? stageColor(i + 1) : COLORS.border())
        .attr('stroke-width', 2.5)
        .attr('marker-end', '');
      // little arrowhead
      svg.append('path')
        .attr('d', `M ${xs[i + 1] - 8} ${cy - 4} L ${xs[i + 1]} ${cy} L ${xs[i + 1] - 8} ${cy + 4} Z`)
        .attr('fill', reached ? stageColor(i + 1) : COLORS.border());
    }

    // --- stage boxes ---
    STAGES.forEach((s, i) => {
      const active = i === phase;
      const done = i < phase;
      const col = stageColor(i);
      const g = svg.append('g');
      g.append('rect').attr('x', xs[i]).attr('y', rowY).attr('width', boxW).attr('height', boxH)
        .attr('rx', 9)
        .attr('fill', active
          ? 'color-mix(in srgb,' + col + ' 16%, transparent)'
          : (done ? COLORS.surface2() : COLORS.surface()))
        .attr('stroke', active || done ? col : COLORS.border())
        .attr('stroke-width', active ? 2.6 : 1.6);
      // two-line label
      const lines = s.label.split('\n');
      lines.forEach((ln, k) => {
        g.append('text').attr('x', cx(i)).attr('y', rowY + 19 + k * 13)
          .attr('text-anchor', 'middle').attr('font-size', 11).attr('font-weight', 700)
          .attr('fill', active || done ? col : COLORS.soft()).text(ln);
      });
      g.append('text').attr('x', cx(i)).attr('y', rowY + boxH - 6)
        .attr('text-anchor', 'middle').attr('font-size', 8.5)
        .attr('fill', COLORS.soft()).text(s.sub);
      // checkmark on completed stages
      if (done) {
        g.append('circle').attr('cx', xs[i] + boxW - 11).attr('cy', rowY + 11).attr('r', 5)
          .attr('fill', COLORS.ok());
        g.append('text').attr('x', xs[i] + boxW - 11).attr('y', rowY + 14.5)
          .attr('text-anchor', 'middle').attr('font-size', 8).attr('fill', '#fff')
          .attr('font-weight', 700).text('✓');
      }
    });

    // --- the travelling packet ---
    // While progress<1 the packet is mid-flight between phase-1 and phase; otherwise it rests on `phase`.
    let px, py, carrying;
    if (phase === 0) {
      px = cx(0); py = cy; carrying = STAGES[0].payload;
    } else if (progress < 1) {
      px = cx(phase - 1) + (cx(phase) - cx(phase - 1)) * progress;
      py = cy;
      carrying = STAGES[phase].payload; // it is delivering this stage's input
    } else {
      px = cx(phase); py = cy; carrying = STAGES[phase].payload;
    }
    svg.append('circle').attr('cx', px).attr('cy', py).attr('r', 8)
      .attr('fill', COLORS.beam()).attr('stroke', COLORS.surface()).attr('stroke-width', 2);
    // payload caption floating above the packet
    svg.append('text').attr('x', px).attr('y', py - 16).attr('text-anchor', 'middle')
      .attr('font-size', 9.5).attr('font-weight', 700).attr('fill', COLORS.beam())
      .text(carrying);

    // --- EXTERNAL worker pool (SDK harness) under the TaskManager ---
    const fnLive = phase === 3;
    const poolG = svg.append('g');
    poolG.append('rect').attr('x', poolX).attr('y', poolY).attr('width', poolW).attr('height', poolH)
      .attr('rx', 9)
      .attr('fill', fnLive ? 'color-mix(in srgb,' + COLORS.beam() + ' 14%, transparent)' : COLORS.surface())
      .attr('stroke', fnLive ? COLORS.beam() : COLORS.border())
      .attr('stroke-width', fnLive ? 2.6 : 1.6).attr('stroke-dasharray', '4 3');
    poolG.append('text').attr('x', poolX + poolW / 2).attr('y', poolY + 22)
      .attr('text-anchor', 'middle').attr('font-size', 10.5).attr('font-weight', 700)
      .attr('fill', fnLive ? COLORS.beam() : COLORS.soft()).text('EXTERNAL');
    poolG.append('text').attr('x', poolX + poolW / 2).attr('y', poolY + 35)
      .attr('text-anchor', 'middle').attr('font-size', 9).attr('fill', COLORS.soft()).text('worker pool');
    poolG.append('text').attr('x', poolX + poolW / 2).attr('y', poolY + 49)
      .attr('text-anchor', 'middle').attr('font-size', 8).attr('fill', COLORS.soft())
      .text('SDK harness');

    // --- the four Fn-API channels between TaskManager and worker pool ---
    // Drawn as four short vertical lanes; when fnLive they animate dashes (alternating direction).
    const laneGap = 18;
    const laneTop = rowY + boxH;       // bottom of the TaskManager box
    const laneBot = poolY;             // top of the worker-pool box
    const laneBaseX = poolX + 10;
    FN_CHANNELS.forEach((ch, i) => {
      const lx = laneBaseX + i * laneGap;
      const dash = fnLive ? ((pulse + i * 3) % 12) : 0;
      svg.append('line').attr('x1', lx).attr('y1', laneTop).attr('x2', lx).attr('y2', laneBot)
        .attr('stroke', fnLive ? ch.tint() : COLORS.border())
        .attr('stroke-width', fnLive ? 2.4 : 1.4)
        .attr('stroke-dasharray', '4 4')
        .attr('stroke-dashoffset', dash);
    });
    // channel legend (vertical text labels to the right of the lanes)
    svg.append('text').attr('x', poolX - 6).attr('y', (laneTop + laneBot) / 2 + 3)
      .attr('text-anchor', 'end').attr('font-size', 9).attr('font-weight', 700)
      .attr('fill', fnLive ? COLORS.flink() : COLORS.soft()).text('Fn API');
    FN_CHANNELS.forEach((ch, i) => {
      const lx = laneBaseX + i * laneGap;
      svg.append('text').attr('x', lx).attr('y', laneTop - 4)
        .attr('text-anchor', 'middle').attr('font-size', 7)
        .attr('fill', fnLive ? ch.tint() : COLORS.soft())
        .attr('transform', `rotate(-90 ${lx} ${laneTop - 4})`).text(ch.name);
    });

    // --- caption ---
    const captions = [
      'Beam serialises your pipeline into a language-neutral proto.',
      'The Job Server translates the proto into a Flink JobGraph.',
      'The JobManager schedules the JobGraph across TaskManager slots.',
      'TaskManager runs user code by calling the SDK harness over the Fn API.',
    ];
    svg.append('text').attr('x', W / 2).attr('y', H - 14).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10.5).text(captions[phase]);
  }

  // advance one logical step (used by Step button and the play loop)
  function step() {
    if (progress < 1) { progress = 1; draw(); return; }
    phase = (phase + 1) % STAGES.length;
    progress = phase === 0 ? 1 : 0; // restart the travel animation toward the new stage
    draw();
  }

  // the play loop: smoothly slide the packet, and once at the TaskManager keep the Fn channels pulsing
  const ticker = makeTicker(() => {
    if (phase === 3) {
      // dwell at the TaskManager and animate the Fn-API channels for a few frames, then loop.
      pulse += 1;
      draw();
      if (pulse % 14 === 0) { phase = 0; progress = 1; }
      return;
    }
    if (progress < 1) {
      progress = Math.min(1, progress + 0.12);
      draw();
    } else {
      phase += 1;
      progress = 0;
      draw();
    }
  }, 90);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); step(); },
    reset: () => { ticker.stop(); phase = 0; progress = 1; pulse = 0; draw(); },
  });

  draw();
}
