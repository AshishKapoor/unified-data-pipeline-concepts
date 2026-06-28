/* Ch 10 — "the output watermark is the MIN of its input watermarks".
   A tiny DAG: two input channels (A, B) each carry a watermark, feeding one operator. The operator's
   output watermark = min(A, B). Play advances both inputs; one input goes IDLE and stalls the min,
   freezing the downstream watermark — then resumes. This is the propagation rule that makes a single
   slow/idle input drag a whole streaming graph.  See watermark-min.js mounted from ch10.html. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// A scripted timeline of input watermarks (in arbitrary "event-time" units, 0..100).
// Step 4..6: channel B goes IDLE (null) — without idleness handling the MIN cannot advance.
// Step 7+: B resumes; the min jumps forward again.
const TIMELINE = [
  { a: 8,  b: 5,  idleB: false, note: 'both inputs advancing' },
  { a: 20, b: 14, idleB: false, note: 'min tracks the slower input (B)' },
  { a: 34, b: 22, idleB: false, note: 'output watermark = min(A, B) = B' },
  { a: 48, b: 30, idleB: false, note: 'min still pinned to channel B' },
  { a: 62, b: 30, idleB: true,  note: 'B goes IDLE → min STALLS at 30' },
  { a: 78, b: 30, idleB: true,  note: 'A races ahead, but output is frozen' },
  { a: 90, b: 30, idleB: true,  note: 'one idle input freezes the whole min' },
  { a: 90, b: 72, idleB: false, note: 'withIdleness: B resumes, min jumps to 72' },
  { a: 95, b: 88, idleB: false, note: 'caught up — min = 88 (B) again' },
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);
  let step = 0;

  // Gauge geometry: a horizontal event-time track from x0..x1, value 0..100.
  const x0 = 132, x1 = 300;
  const scale = (v) => x0 + (Math.max(0, Math.min(100, v)) / 100) * (x1 - x0);

  function gauge(g, y, label, value, color, idle) {
    // track
    g.append('line').attr('x1', x0).attr('y1', y).attr('x2', x1).attr('y2', y)
      .attr('stroke', COLORS.border()).attr('stroke-width', 6).attr('stroke-linecap', 'round');
    // filled portion = how far the watermark has advanced
    if (value != null) {
      g.append('line').attr('x1', x0).attr('y1', y).attr('x2', scale(value)).attr('y2', y)
        .attr('stroke', color).attr('stroke-width', 6).attr('stroke-linecap', 'round');
      // the watermark "front" marker
      g.append('circle').attr('cx', scale(value)).attr('cy', y).attr('r', 6)
        .attr('fill', color).attr('stroke', COLORS.surface()).attr('stroke-width', 1.5);
      g.append('text').attr('x', scale(value)).attr('y', y - 12).attr('text-anchor', 'middle')
        .attr('font-size', 10).attr('font-weight', 700).attr('fill', color).text('W=' + value);
    }
    // channel label
    g.append('text').attr('x', x0 - 12).attr('y', y + 4).attr('text-anchor', 'end')
      .attr('font-size', 11).attr('font-weight', 700).attr('fill', COLORS.text()).text(label);
    // idle badge
    if (idle) {
      const bx = x1 + 8;
      g.append('rect').attr('x', bx).attr('y', y - 9).attr('width', 42).attr('height', 18).attr('rx', 5)
        .attr('fill', 'color-mix(in srgb,' + COLORS.late() + ' 18%, transparent)')
        .attr('stroke', COLORS.late()).attr('stroke-width', 1.2);
      g.append('text').attr('x', bx + 21).attr('y', y + 4).attr('text-anchor', 'middle')
        .attr('font-size', 9).attr('font-weight', 700).attr('fill', COLORS.late()).text('IDLE');
    }
  }

  function draw() {
    svg.selectAll('*').remove();
    const s = TIMELINE[step];
    // The propagation rule: an idle input STOPS voting, so it is excluded from the min.
    const candidates = [s.a];
    if (!s.idleB) candidates.push(s.b);
    const outW = Math.min(...candidates);

    svg.append('text').attr('x', W / 2).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('output watermark = MIN(input channels)');

    // --- two input channels (left) ---
    const yA = 64, yB = 116;
    gauge(svg.append('g'), yA, 'A', s.a, COLORS.accent(), false);
    gauge(svg.append('g'), yB, 'B', s.b, s.idleB ? COLORS.idle() : COLORS.accent2(), s.idleB);

    // --- edges into the operator ---
    const opX = 360, opY = 168, opW = 78, opH = 56;
    const opCx = opX, opCy = opY;
    [yA, yB].forEach((yi, i) => {
      const idleEdge = i === 1 && s.idleB;
      svg.append('path')
        .attr('d', `M ${x1 + (i === 1 && s.idleB ? 54 : 6)} ${yi} C ${opCx - 60} ${yi}, ${opCx - 60} ${opCy}, ${opCx - opW / 2} ${opCy - 8 + i * 16}`)
        .attr('fill', 'none')
        .attr('stroke', idleEdge ? COLORS.late() : COLORS.border())
        .attr('stroke-width', 2)
        .attr('stroke-dasharray', idleEdge ? '4 4' : null);
    });

    // --- the operator box ---
    svg.append('rect').attr('x', opCx - opW / 2).attr('y', opCy - opH / 2).attr('width', opW).attr('height', opH)
      .attr('rx', 10).attr('fill', COLORS.surface2()).attr('stroke', COLORS.flink()).attr('stroke-width', 2.2);
    svg.append('text').attr('x', opCx).attr('y', opCy - 4).attr('text-anchor', 'middle')
      .attr('font-size', 11).attr('font-weight', 700).attr('fill', COLORS.text()).text('operator');
    svg.append('text').attr('x', opCx).attr('y', opCy + 11).attr('text-anchor', 'middle')
      .attr('font-size', 9).attr('fill', COLORS.soft()).text('GroupByKey');

    // --- output watermark gauge (below operator) ---
    const yOut = 250;
    const frozen = s.idleB; // when an input is idle, the min is stuck at that input's last value
    const outColor = frozen ? COLORS.late() : COLORS.ok();
    gauge(svg.append('g'), yOut, 'out', outW, outColor, false);
    // edge operator -> output
    svg.append('path')
      .attr('d', `M ${opCx} ${opCy + opH / 2} C ${opCx} 220, ${scale(outW)} 224, ${scale(outW)} ${yOut - 9}`)
      .attr('fill', 'none').attr('stroke', outColor).attr('stroke-width', 2.2);

    // freeze annotation
    if (frozen) {
      svg.append('text').attr('x', x0).attr('y', yOut + 26).attr('font-size', 10).attr('font-weight', 700)
        .attr('fill', COLORS.late()).text('FROZEN — min cannot pass the idle channel');
    } else {
      svg.append('text').attr('x', x0).attr('y', yOut + 26).attr('font-size', 10)
        .attr('fill', COLORS.soft()).text('min(A' + (s.idleB ? '' : ', B') + ') = ' + outW);
    }

    // caption + progress
    svg.append('text').attr('x', W / 2).attr('y', H - 6).attr('text-anchor', 'middle')
      .attr('font-size', 11).attr('font-weight', 600)
      .attr('fill', frozen ? COLORS.late() : COLORS.soft()).text(s.note);
    svg.append('text').attr('x', W - 6).attr('y', 20).attr('text-anchor', 'end')
      .attr('font-size', 10).attr('fill', COLORS.soft()).text((step + 1) + '/' + TIMELINE.length);
  }

  const ticker = makeTicker(() => {
    step = (step + 1) % TIMELINE.length;
    draw();
    return step !== 0; // pause the loop after one full pass
  }, 1500);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); step = (step + 1) % TIMELINE.length; draw(); },
    reset: () => { ticker.stop(); step = 0; draw(); },
  });
  draw();
}
