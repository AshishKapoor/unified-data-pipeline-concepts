/* Ch 08 — "two clocks: event time vs processing time".
   A dual-axis scatter: x = processing time (when observed), y = event time (when it happened).
   The dashed diagonal is the no-skew line (event time == processing time). Play "advances" the
   stream: each step increases the skew (and a little jitter / out-of-order wobble), pushing the
   points BELOW the diagonal — event time lags the wall clock. Step nudges skew one notch; Reset
   snaps every point back onto the diagonal (a perfect, impossible "no skew" world). */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Five events, each with a base position along the diagonal (processing & event time start equal).
// `jitter` makes arrival order wobble so the demo also *looks* out-of-order, not just lagged.
const POINTS = [
  { id: 'A', base: 0.10, jitter: 0.02 },
  { id: 'B', base: 0.28, jitter: -0.05 },
  { id: 'C', base: 0.46, jitter: 0.06 },
  { id: 'D', base: 0.64, jitter: -0.03 },
  { id: 'E', base: 0.82, jitter: 0.04 },
];

const STEPS = 6;          // how many notches of skew Play walks through.
const MAX_LAG = 0.42;     // maximum normalised skew at the last step.

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);

  // Plot area (square so the diagonal reads as a true 45° line).
  const M = { l: 52, r: 18, t: 30, b: 46 };
  const pw = W - M.l - M.r;
  const ph = H - M.t - M.b;
  const x = d3.scaleLinear().domain([0, 1]).range([M.l, M.l + pw]);
  // y is event time; SVG y grows downward, so invert so "later event time" is higher on screen.
  const y = d3.scaleLinear().domain([0, 1]).range([M.t + ph, M.t]);

  let step = 0; // 0 = no skew (on diagonal), STEPS = max lag.

  // Current normalised skew for this step (0..MAX_LAG).
  const lagAt = (s) => (s / STEPS) * MAX_LAG;

  function draw() {
    svg.selectAll('*').remove();
    const lag = lagAt(step);

    // ---- title ----
    svg.append('text').attr('x', W / 2).attr('y', 18).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text(step === 0 ? 'event time = processing time (no skew)' : 'event time lags processing time');

    // ---- plot frame ----
    svg.append('rect').attr('x', M.l).attr('y', M.t).attr('width', pw).attr('height', ph)
      .attr('fill', COLORS.surface()).attr('stroke', COLORS.border()).attr('stroke-width', 1);

    // ---- axes ----
    // x axis = processing time (the wall clock that always moves forward).
    svg.append('line').attr('x1', M.l).attr('y1', M.t + ph).attr('x2', M.l + pw).attr('y2', M.t + ph)
      .attr('stroke', COLORS.border()).attr('stroke-width', 1.5);
    svg.append('text').attr('x', M.l + pw / 2).attr('y', H - 12).attr('text-anchor', 'middle')
      .attr('fill', COLORS.text()).attr('font-size', 11).attr('font-weight', 700)
      .text('processing time  →  (when observed)');
    // arrow on x
    svg.append('path').attr('d', `M ${M.l + pw} ${M.t + ph} l -8 -4 l 0 8 z`).attr('fill', COLORS.border());

    // y axis = event time.
    svg.append('line').attr('x1', M.l).attr('y1', M.t + ph).attr('x2', M.l).attr('y2', M.t)
      .attr('stroke', COLORS.border()).attr('stroke-width', 1.5);
    svg.append('text').attr('transform', `translate(16 ${M.t + ph / 2}) rotate(-90)`)
      .attr('text-anchor', 'middle').attr('fill', COLORS.text()).attr('font-size', 11).attr('font-weight', 700)
      .text('event time  →  (when it happened)');
    svg.append('path').attr('d', `M ${M.l} ${M.t} l -4 8 l 8 0 z`).attr('fill', COLORS.border());

    // ---- the no-skew diagonal (event time == processing time) ----
    svg.append('line')
      .attr('x1', x(0)).attr('y1', y(0)).attr('x2', x(1)).attr('y2', y(1))
      .attr('stroke', COLORS.ok()).attr('stroke-width', 2).attr('stroke-dasharray', '6 5')
      .attr('opacity', 0.8);
    svg.append('text').attr('x', x(0.74)).attr('y', y(0.86)).attr('fill', COLORS.ok())
      .attr('font-size', 10).attr('font-weight', 700).text('no-skew line');

    // ---- region below the diagonal where skew lives ----
    if (lag > 0.001) {
      svg.append('text').attr('x', M.l + pw - 6).attr('y', M.t + ph - 8)
        .attr('text-anchor', 'end').attr('fill', COLORS.late()).attr('font-size', 10)
        .attr('font-weight', 700).text('skew region (event time < processing time)');
    }

    // ---- points ----
    POINTS.forEach((pt) => {
      const px = pt.base;                                    // processing-time position (x)
      // event time = processing time pushed DOWN by the lag, plus per-point jitter scaled by lag so
      // the cloud both drops and fans out (out-of-order wobble) as skew grows.
      const ev = Math.max(0.02, pt.base - lag + pt.jitter * (lag / MAX_LAG));
      const cx = x(px);
      const cy = y(ev);
      const onDiag = lag <= 0.001;

      // vertical "skew" connector from the diagonal down to the actual event-time position.
      if (!onDiag) {
        svg.append('line')
          .attr('x1', cx).attr('y1', y(px)).attr('x2', cx).attr('y2', cy)
          .attr('stroke', COLORS.late()).attr('stroke-width', 1.5).attr('opacity', 0.55)
          .attr('stroke-dasharray', '2 3');
      }

      svg.append('circle').attr('cx', cx).attr('cy', cy).attr('r', 9)
        .attr('fill', onDiag ? COLORS.ok() : COLORS.beam())
        .attr('stroke', COLORS.surface()).attr('stroke-width', 2);
      svg.append('text').attr('x', cx).attr('y', cy + 3.5).attr('text-anchor', 'middle')
        .attr('fill', '#fff').attr('font-size', 9).attr('font-weight', 800).text(pt.id);
    });

    // ---- skew readout ----
    svg.append('text').attr('x', M.l + 6).attr('y', M.t + 16)
      .attr('fill', lag > 0.001 ? COLORS.late() : COLORS.ok())
      .attr('font-size', 11).attr('font-weight', 700)
      .text(`skew = ${(lag * 100).toFixed(0)}%  ·  step ${step}/${STEPS}`);
  }

  const ticker = makeTicker(() => {
    step = (step + 1) % (STEPS + 1);
    draw();
    return step !== STEPS ? true : false; // pause once we hit max lag.
  }, 700);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); step = (step + 1) % (STEPS + 1); draw(); },
    reset: () => { ticker.stop(); step = 0; draw(); },
  });

  draw();
}
