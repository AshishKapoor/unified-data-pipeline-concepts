/* Ch 16 — Asynchronous Barrier Snapshotting (Chandy–Lamport) on Flink.

   A small dataflow runs left→right: two sources feed a 2-input keyed operator (Count), which feeds a
   sink. A numbered checkpoint BARRIER is injected at both sources and flows *inline* with the
   records. At the 2-input operator the barriers ALIGN (the early one waits, shown as a buffer pip);
   once both arrive the operator snapshots its state and forwards the barrier. When the barrier
   reaches the sink, the checkpoint is COMPLETE — every operator flashes "snapshotted".

   Buttons / controls:
     • Play / Step — advance one barrier wave.
     • Reset       — clear to last completed checkpoint.
     • "Kill TM"   — simulate a TaskManager crash: state rolls back to the last completed checkpoint
                     and the sources REPLAY from there (operators flash rollback, then replay).
     • backpressure heat — each operator carries an idle/busy/backpressure tint (credit-based flow
                     control) so you can see where the stream stalls. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Operator layout. Two sources (top/bottom-left) converge on a 2-input Count operator, then Sink.
const NODES = [
  { id: 'srcA', label: 'Source A', x: 70, y: 70, inputs: 0 },
  { id: 'srcB', label: 'Source B', x: 70, y: 190, inputs: 0 },
  { id: 'count', label: 'Count\n(keyed)', x: 250, y: 130, inputs: 2 },
  { id: 'sink', label: 'Sink', x: 420, y: 130, inputs: 1 },
];
const EDGES = [
  ['srcA', 'count'],
  ['srcB', 'count'],
  ['count', 'sink'],
];
// Backpressure heat per node (purely illustrative of credit-based flow control).
const HEAT = { srcA: 'idle', srcB: 'busy', count: 'back', sink: 'idle' };

// Phases of one barrier wave. The 2-input operator aligns before snapshotting.
const PHASES = [
  'inject',   // barriers injected at both sources
  'flow',     // barriers travel along the edges toward Count
  'align',    // one barrier reached Count first; it buffers and waits for the other
  'snapshot', // both arrived → Count snapshots state, forwards barrier
  'complete', // barrier reached Sink → checkpoint n complete (all flash snapshotted)
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 520, H = 300;
  const svg = createSvg(el, W, H);
  const byId = Object.fromEntries(NODES.map((n) => [n.id, n]));

  let cp = 0;          // last COMPLETED checkpoint number
  let phaseIdx = -1;   // -1 = settled (showing last completed checkpoint), else index into PHASES
  let recovering = 0;  // >0 while a kill→rollback→replay animation plays out (counts down)
  let showHeat = false;

  function heatColor(state) {
    if (state === 'busy') return COLORS.busy();
    if (state === 'back') return COLORS.back();
    return COLORS.idle();
  }

  function phase() { return phaseIdx >= 0 ? PHASES[phaseIdx] : null; }

  // Where is the barrier on a given edge in the current phase? Returns 0..1 fraction, or null.
  function barrierFrac(from, to) {
    const ph = phase();
    if (ph === 'flow') return 0.55;
    if (ph === 'align') {
      // srcA's barrier has arrived at Count and is buffered; srcB's is still in flight.
      if (to === 'count') return from === 'srcA' ? 1 : 0.6;
    }
    if (ph === 'snapshot') { if (to === 'count') return 1; }
    if (ph === 'complete') { if (to === 'sink') return 1; }
    return null;
  }

  function draw() {
    svg.selectAll('*').remove();
    const ph = phase();

    // Title / status line.
    svg.append('text').attr('x', W / 2).attr('y', 22).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 12).attr('font-weight', 700)
      .text(
        recovering
          ? 'TaskManager killed → rollback to checkpoint ' + cp + ' + replay'
          : ph
            ? 'checkpoint ' + (cp + 1) + ' — phase: ' + ph + (ph === 'align' ? ' (buffering input)' : '')
            : 'last completed checkpoint: ' + cp,
      );

    // --- edges ---
    EDGES.forEach(([f, t]) => {
      const a = byId[f], b = byId[t];
      svg.append('line').attr('x1', a.x + 34).attr('y1', a.y).attr('x2', b.x - 38).attr('y2', b.y)
        .attr('stroke', COLORS.border()).attr('stroke-width', 2);
    });

    // --- record dots drifting along edges (the stream itself) ---
    EDGES.forEach(([f, t], ei) => {
      const a = byId[f], b = byId[t];
      for (let k = 0; k < 3; k++) {
        const frac = ((ei * 0.17 + k * 0.31 + (recovering ? 0.0 : 0.0)) % 1);
        const x = (a.x + 34) + (b.x - 38 - (a.x + 34)) * frac;
        const y = a.y + (b.y - a.y) * frac;
        svg.append('circle').attr('cx', x).attr('cy', y).attr('r', 3)
          .attr('fill', recovering ? COLORS.late() : COLORS.soft()).attr('opacity', 0.5);
      }
    });

    // --- barriers in flight (the inline checkpoint marker) ---
    EDGES.forEach(([f, t]) => {
      const fr = barrierFrac(f, t);
      if (fr == null) return;
      const a = byId[f], b = byId[t];
      const x = (a.x + 34) + (b.x - 38 - (a.x + 34)) * fr;
      const y = a.y + (b.y - a.y) * fr;
      // A barrier is drawn as a bold vertical bar crossing the stream.
      svg.append('line').attr('x1', x).attr('y1', y - 16).attr('x2', x).attr('y2', y + 16)
        .attr('stroke', COLORS.flink()).attr('stroke-width', 4).attr('stroke-linecap', 'round');
      svg.append('text').attr('x', x).attr('y', y - 22).attr('text-anchor', 'middle')
        .attr('fill', COLORS.flink()).attr('font-size', 10).attr('font-weight', 700)
        .text('║' + (cp + 1));
    });

    // --- buffered-barrier pip at the aligning operator ---
    if (ph === 'align') {
      const c = byId.count;
      svg.append('rect').attr('x', c.x - 50).attr('y', c.y + 30).attr('width', 100).attr('height', 16)
        .attr('rx', 4).attr('fill', 'none').attr('stroke', COLORS.flink()).attr('stroke-dasharray', '3 3');
      svg.append('text').attr('x', c.x).attr('y', c.y + 42).attr('text-anchor', 'middle')
        .attr('fill', COLORS.flink()).attr('font-size', 9).text('aligning: buffering input A');
    }

    // --- nodes ---
    NODES.forEach((n) => {
      const snapped = (ph === 'snapshot' && n.id === 'count')
        || (ph === 'complete')                                   // all flash on completion
        || (phaseIdx === -1 && cp > 0 && !recovering && false);
      const rolledBack = recovering > 0;

      let stroke = COLORS.beam();
      if (snapped) stroke = COLORS.ok();
      if (rolledBack) stroke = COLORS.late();

      const g = svg.append('g');
      g.append('circle').attr('cx', n.x).attr('cy', n.y).attr('r', 34)
        .attr('fill', showHeat
          ? 'color-mix(in srgb,' + heatColor(HEAT[n.id]) + ' 18%, ' + COLORS.surface() + ')'
          : COLORS.surface2())
        .attr('stroke', stroke).attr('stroke-width', snapped || rolledBack ? 3 : 2);

      n.label.split('\n').forEach((ln, i, arr) => {
        g.append('text').attr('x', n.x).attr('y', n.y + 4 - (arr.length - 1) * 6 + i * 12)
          .attr('text-anchor', 'middle').attr('fill', COLORS.text())
          .attr('font-size', 11).attr('font-weight', 600).text(ln);
      });

      // snapshot badge
      if (snapped) {
        g.append('text').attr('x', n.x).attr('y', n.y - 42).attr('text-anchor', 'middle')
          .attr('fill', COLORS.ok()).attr('font-size', 9).attr('font-weight', 700).text('✓ snapshotted');
      }
      if (rolledBack) {
        g.append('text').attr('x', n.x).attr('y', n.y - 42).attr('text-anchor', 'middle')
          .attr('fill', COLORS.late()).attr('font-size', 9).attr('font-weight', 700).text('↺ restored');
      }
      // heat label
      if (showHeat) {
        g.append('text').attr('x', n.x).attr('y', n.y + 48).attr('text-anchor', 'middle')
          .attr('fill', heatColor(HEAT[n.id])).attr('font-size', 9).attr('font-weight', 700)
          .text(HEAT[n.id] === 'back' ? 'backpressure' : HEAT[n.id]);
      }
    });

    // --- JobManager / coordinator chip ---
    svg.append('rect').attr('x', W - 150).attr('y', 250).attr('width', 138).attr('height', 36)
      .attr('rx', 8).attr('fill', COLORS.surface()).attr('stroke', COLORS.accent()).attr('stroke-width', 1.5);
    svg.append('text').attr('x', W - 81).attr('y', 266).attr('text-anchor', 'middle')
      .attr('fill', COLORS.accent()).attr('font-size', 10).attr('font-weight', 700)
      .text('JobManager');
    svg.append('text').attr('x', W - 81).attr('y', 279).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 9)
      .text('checkpoint coordinator');

    // legend
    svg.append('text').attr('x', 14).attr('y', 278).attr('fill', COLORS.flink())
      .attr('font-size', 10).attr('font-weight', 700).text('║ barrier (inline)');
    svg.append('text').attr('x', 14).attr('y', 292).attr('fill', COLORS.soft())
      .attr('font-size', 9).text('aligns at 2-input op → snapshot → complete');
  }

  // Advance one phase of the barrier wave; wrap → completed checkpoint increments.
  function stepPhase() {
    if (recovering > 0) return; // ignore while a recovery animation is running
    phaseIdx += 1;
    if (phaseIdx >= PHASES.length) {
      cp += 1;          // checkpoint complete
      phaseIdx = -1;    // settle on the new completed checkpoint
    }
    draw();
  }

  // Kill a TaskManager: roll back to last completed checkpoint, then replay.
  function killTM() {
    ticker.stop();
    phaseIdx = -1;       // any in-flight checkpoint is aborted
    recovering = 3;
    draw();
    const iv = setInterval(() => {
      recovering -= 1;
      if (recovering <= 0) { recovering = 0; clearInterval(iv); }
      draw();
    }, 700);
  }

  const ticker = makeTicker(() => { stepPhase(); }, 1100);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); stepPhase(); },
    reset: () => { ticker.stop(); recovering = 0; phaseIdx = -1; cp = 0; draw(); },
  });

  // Augment the control bar with chapter-specific buttons (Kill TM + heat toggle), if present.
  const card = el.closest('.diagram-card') || el.parentElement;
  const bar = card && card.querySelector('.anim-controls');
  if (bar) {
    const killBtn = bar.querySelector('[data-act="kill"]');
    if (killBtn) killBtn.addEventListener('click', killTM);
    const heatBtn = bar.querySelector('[data-act="heat"]');
    if (heatBtn) heatBtn.addEventListener('click', () => { showHeat = !showHeat; draw(); });
  }

  draw();
}
