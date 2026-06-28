/* Ch 11 — "triggers decide WHEN a window emits; accumulation decides HOW panes relate".
   A single fixed window sits on an event-time axis. A watermark line sweeps left→right across
   processing time. As it sweeps:
     • EARLY panes fire on processing-time ticks while the window is still open (watermark < end).
     • the ON_TIME pane fires exactly when the watermark crosses the window end.
     • LATE panes fire for elements that land after the watermark, one pane per late element.
   A toggle flips ACCUMULATING (each pane = running total) vs DISCARDING (each pane = delta since
   the last pane) and re-renders every pane's printed value. Same firings, different numbers — the
   core lesson of the chapter. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// The window spans event-time [WIN_START, WIN_END]. Each element is (eventTime, value). The order
// in the array is ARRIVAL order (processing-time order) — note the last element is LATE: its event
// time (22) is inside the window, but it arrives after the watermark has already passed the end.
const WIN_START = 10;
const WIN_END = 40;
// Each step adds one element. `pane` marks which firing this element triggers AFTER it lands.
//   wm: where the watermark sits (in event-time units) at the moment this element arrives.
//   fires: the kind of pane emitted right after this element ('early' | 'ontime' | 'late' | null).
const STEPS = [
  { et: 14, v: 3, wm: 8, fires: null },            // first element lands, window still filling
  { et: 18, v: 4, wm: 16, fires: 'early' },        // processing-time tick → speculative EARLY pane
  { et: 26, v: 5, wm: 24, fires: 'early' },        // another EARLY pane (watermark still < end)
  { et: 33, v: 2, wm: 38, fires: null },           // lands, watermark nearly at end
  { et: 0, v: 0, wm: 41, fires: 'ontime' },        // watermark crosses WIN_END → the ON_TIME pane
  { et: 22, v: 6, wm: 41, fires: 'late' },         // a LATE element (et inside window, arrives late)
];

const PANE_COLOR = {
  early: () => COLORS.accent(),
  ontime: () => COLORS.ok(),
  late: () => COLORS.late(),
};
const PANE_LABEL = { early: 'EARLY', ontime: 'ON-TIME', late: 'LATE' };

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 340;
  const svg = createSvg(el, W, H);

  // event-time axis geometry
  const axisY = 96;
  const x0 = 40, x1 = W - 20;
  const tMin = 0, tMax = 50;
  const xOf = (t) => x0 + ((t - tMin) / (tMax - tMin)) * (x1 - x0);

  let step = -1;          // -1 = nothing happened yet
  let accumulating = true; // accumulation-mode toggle

  // Recompute the printed value for each fired pane up to the current step, honoring the mode.
  // ACCUMULATING: running sum of every element that has landed in the window so far.
  // DISCARDING: only the elements that landed since the previous pane fired.
  function computePanes() {
    const panes = [];
    let runningTotal = 0;     // all-time total of window elements (for ACCUMULATING)
    let sinceLast = 0;        // delta since the previous firing (for DISCARDING)
    for (let i = 0; i <= step && i < STEPS.length; i++) {
      const s = STEPS[i];
      // The "watermark crosses end" pseudo-step (et=0,v=0) contributes no element value.
      if (s.v > 0) { runningTotal += s.v; sinceLast += s.v; }
      if (s.fires) {
        panes.push({
          kind: s.fires,
          value: accumulating ? runningTotal : sinceLast,
          idx: panes.length,
        });
        sinceLast = 0; // DISCARDING clears window state after every firing
      }
    }
    return panes;
  }

  function draw() {
    svg.selectAll('*').remove();

    // --- title ---
    svg.append('text').attr('x', W / 2).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 12).attr('font-weight', 700)
      .text('one FixedWindow · AfterWatermark(early, late)');

    // --- the window band on the event-time axis ---
    svg.append('rect')
      .attr('x', xOf(WIN_START)).attr('y', axisY - 30)
      .attr('width', xOf(WIN_END) - xOf(WIN_START)).attr('height', 60)
      .attr('rx', 6)
      .attr('fill', 'color-mix(in srgb,' + COLORS.beam() + ' 10%, transparent)')
      .attr('stroke', COLORS.beam()).attr('stroke-width', 1.5).attr('stroke-dasharray', '4 3');
    svg.append('text').attr('x', (xOf(WIN_START) + xOf(WIN_END)) / 2).attr('y', axisY - 38)
      .attr('text-anchor', 'middle').attr('fill', COLORS.beam())
      .attr('font-size', 10).attr('font-weight', 700).text('window [10,40)');

    // --- the event-time axis line ---
    svg.append('line').attr('x1', x0).attr('y1', axisY + 38).attr('x2', x1).attr('y2', axisY + 38)
      .attr('stroke', COLORS.border()).attr('stroke-width', 1.5);
    svg.append('text').attr('x', x1).attr('y', axisY + 54).attr('text-anchor', 'end')
      .attr('fill', COLORS.soft()).attr('font-size', 10).text('event time →');

    // --- elements that have landed so far (skip the et=0 watermark-cross pseudo-step) ---
    for (let i = 0; i <= step && i < STEPS.length; i++) {
      const s = STEPS[i];
      if (s.v <= 0) continue;
      const isLate = s.fires === 'late';
      svg.append('circle').attr('cx', xOf(s.et)).attr('cy', axisY).attr('r', 12)
        .attr('fill', isLate ? COLORS.late() : COLORS.surface2())
        .attr('stroke', isLate ? COLORS.late() : COLORS.beam()).attr('stroke-width', 2);
      svg.append('text').attr('x', xOf(s.et)).attr('y', axisY + 4).attr('text-anchor', 'middle')
        .attr('fill', COLORS.text()).attr('font-size', 11).attr('font-weight', 700).text(s.v);
    }

    // --- the sweeping watermark line ---
    if (step >= 0) {
      const wmT = Math.min(STEPS[Math.min(step, STEPS.length - 1)].wm, tMax);
      const wx = xOf(wmT);
      const crossed = wmT >= WIN_END;
      svg.append('line').attr('x1', wx).attr('y1', axisY - 46).attr('x2', wx).attr('y2', axisY + 40)
        .attr('stroke', crossed ? COLORS.ok() : COLORS.flink()).attr('stroke-width', 2.5);
      svg.append('path')
        .attr('d', `M ${wx} ${axisY - 46} l -5 -7 l 10 0 z`)
        .attr('fill', crossed ? COLORS.ok() : COLORS.flink());
      svg.append('text').attr('x', wx).attr('y', axisY - 50).attr('text-anchor', 'middle')
        .attr('fill', crossed ? COLORS.ok() : COLORS.flink())
        .attr('font-size', 9).attr('font-weight', 700).text('watermark');
    }

    // --- the mode toggle ---
    const toggleY = axisY + 70;
    const tg = svg.append('g').style('cursor', 'pointer')
      .on('click', () => { accumulating = !accumulating; draw(); });
    tg.append('rect').attr('x', x0).attr('y', toggleY).attr('width', 220).attr('height', 26)
      .attr('rx', 13).attr('fill', COLORS.surface()).attr('stroke', COLORS.border());
    const knobLeft = accumulating;
    tg.append('rect')
      .attr('x', knobLeft ? x0 + 2 : x0 + 110).attr('y', toggleY + 2)
      .attr('width', 108).attr('height', 22).attr('rx', 11)
      .attr('fill', 'color-mix(in srgb,' + COLORS.accent() + ' 16%, transparent)')
      .attr('stroke', COLORS.accent()).attr('stroke-width', 1.5);
    tg.append('text').attr('x', x0 + 56).attr('y', toggleY + 17).attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-weight', 700)
      .attr('fill', accumulating ? COLORS.accent() : COLORS.soft()).text('ACCUMULATING');
    tg.append('text').attr('x', x0 + 165).attr('y', toggleY + 17).attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-weight', 700)
      .attr('fill', accumulating ? COLORS.soft() : COLORS.accent()).text('DISCARDING');
    svg.append('text').attr('x', x1).attr('y', toggleY + 17).attr('text-anchor', 'end')
      .attr('fill', COLORS.soft()).attr('font-size', 9)
      .text(accumulating ? 'pane = running total' : 'pane = delta since last');

    // --- the emitted panes (the sequence of firings) ---
    const panes = computePanes();
    const paneY = toggleY + 52;
    svg.append('text').attr('x', x0).attr('y', paneY - 8)
      .attr('fill', COLORS.soft()).attr('font-size', 10).attr('font-weight', 700)
      .text('panes emitted →');

    const pw = 64, gap = 10;
    panes.forEach((pane, i) => {
      const px = x0 + i * (pw + gap);
      if (px + pw > x1) return; // keep within the canvas
      const c = PANE_COLOR[pane.kind]();
      const g = svg.append('g');
      g.append('rect').attr('x', px).attr('y', paneY).attr('width', pw).attr('height', 50)
        .attr('rx', 8).attr('fill', 'color-mix(in srgb,' + c + ' 12%, transparent)')
        .attr('stroke', c).attr('stroke-width', 2);
      g.append('text').attr('x', px + pw / 2).attr('y', paneY + 16).attr('text-anchor', 'middle')
        .attr('fill', c).attr('font-size', 9).attr('font-weight', 700).text(PANE_LABEL[pane.kind]);
      g.append('text').attr('x', px + pw / 2).attr('y', paneY + 38).attr('text-anchor', 'middle')
        .attr('fill', COLORS.text()).attr('font-size', 16).attr('font-weight', 800).text(pane.value);
    });

    if (panes.length === 0) {
      svg.append('text').attr('x', x0).attr('y', paneY + 28)
        .attr('fill', COLORS.soft()).attr('font-size', 11)
        .text('press ▶ Play or ⏭ Step — watch the watermark sweep and panes fire');
    }
  }

  const ticker = makeTicker(() => {
    step += 1;
    if (step >= STEPS.length) { step = STEPS.length - 1; draw(); return false; }
    draw();
  }, 1300);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => {
      ticker.stop();
      step = Math.min(step + 1, STEPS.length - 1);
      draw();
    },
    reset: () => { ticker.stop(); step = -1; draw(); },
  });

  draw();
}
