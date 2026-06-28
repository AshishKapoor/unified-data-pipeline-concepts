/* Ch 09 — "windowing assigns each element to one or more windows".
   A single timeline of event ticks (the same events for every mode). A toggle / Play cycles the
   WindowFn — Fixed, Sliding, Sessions, Global — and the ticks get bracketed into the corresponding
   windows live. Sessions are computed from the data and visibly MERGE wherever the inter-event gap
   is smaller than gap_size; a quiet stretch >= gap starts a fresh session. Step advances the mode;
   Play auto-cycles. Click a mode label to pin it. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// The shared event log (event times in "seconds" on a small epoch, matching the pipeline's spirit).
// Note the long quiet gap between t=95 and t=200 — that is what splits the Sessions into two.
const EVENTS = [5, 12, 20, 35, 48, 55, 70, 82, 95, 118, 200, 215, 240];

// Timeline domain a touch past the last event so the final bracket has room.
const T_MIN = 0;
const T_MAX = 260;

const MODES = ['fixed', 'sliding', 'sessions', 'global'];
const MODE_LABEL = {
  fixed: 'FixedWindows(60)',
  sliding: 'SlidingWindows(60, 30)',
  sessions: 'Sessions(gap=45)',
  global: 'GlobalWindows()',
};
const MODE_NOTE = {
  fixed: 'tumbling · 1 window / element',
  sliding: 'overlapping · 2 windows / element',
  sessions: 'data-driven · merges across gaps < 45s',
  global: 'one window spanning all of time',
};

const FIXED_SIZE = 60;
const SLIDE_SIZE = 60;
const SLIDE_PERIOD = 30;
const GAP = 45;

/** Return [start,end) window intervals for the current mode. */
function windowsFor(mode) {
  if (mode === 'global') {
    return [{ start: T_MIN, end: T_MAX, merged: false }];
  }
  if (mode === 'fixed') {
    const out = [];
    for (let s = 0; s <= T_MAX - 1; s += FIXED_SIZE) out.push({ start: s, end: s + FIXED_SIZE, merged: false });
    return out;
  }
  if (mode === 'sliding') {
    const out = [];
    // A sliding window starts every PERIOD and is SIZE wide; we keep those that cover any event.
    for (let s = -SLIDE_SIZE + SLIDE_PERIOD; s <= T_MAX - 1; s += SLIDE_PERIOD) {
      const w = { start: s, end: s + SLIDE_SIZE, merged: false };
      if (EVENTS.some((t) => t >= w.start && t < w.end)) out.push(w);
    }
    return out;
  }
  // sessions: each event seeds [t, t+GAP); merge intervals that touch/overlap.
  const seeds = EVENTS.map((t) => ({ start: t, end: t + GAP })).sort((a, b) => a.start - b.start);
  const out = [];
  for (const sd of seeds) {
    const last = out[out.length - 1];
    if (last && sd.start <= last.end) {
      last.end = Math.max(last.end, sd.end);
      last.merged = true; // this window absorbed more than one seed -> it MERGED
    } else {
      out.push({ start: sd.start, end: sd.end, merged: false });
    }
  }
  return out;
}

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 300;
  const svg = createSvg(el, W, H);

  const padL = 24, padR = 24;
  const axisY = 150;          // the event timeline
  const x = d3.scaleLinear().domain([T_MIN, T_MAX]).range([padL, W - padR]);

  let modeIdx = 0; // start on Fixed

  function draw() {
    svg.selectAll('*').remove();
    const mode = MODES[modeIdx];
    const windows = windowsFor(mode);

    // --- title: current WindowFn ---
    svg.append('text').attr('x', W / 2).attr('y', 22).attr('text-anchor', 'middle')
      .attr('fill', COLORS.beam()).attr('font-size', 14).attr('font-weight', 800)
      .text(MODE_LABEL[mode]);
    svg.append('text').attr('x', W / 2).attr('y', 39).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10.5)
      .text(MODE_NOTE[mode]);

    // --- window brackets (drawn above the timeline) ---
    // Sliding windows overlap, so stagger them on two rows to keep the overlap legible.
    windows.forEach((w, i) => {
      const x0 = x(Math.max(w.start, T_MIN));
      const x1 = x(Math.min(w.end, T_MAX));
      const row = mode === 'sliding' ? i % 2 : 0;
      const topY = 58 + row * 20;
      const botY = axisY - 16;
      const merged = w.merged;
      const stroke = mode === 'global'
        ? COLORS.idle()
        : merged ? COLORS.flink() : COLORS.accent();
      const fill = mode === 'global'
        ? 'color-mix(in srgb,' + COLORS.idle() + ' 8%, transparent)'
        : merged
          ? 'color-mix(in srgb,' + COLORS.flink() + ' 12%, transparent)'
          : 'color-mix(in srgb,' + COLORS.accent() + ' 9%, transparent)';

      // shaded window band down to the axis
      svg.append('rect')
        .attr('x', x0).attr('y', topY).attr('width', Math.max(2, x1 - x0)).attr('height', botY - topY)
        .attr('rx', 5).attr('fill', fill).attr('stroke', stroke).attr('stroke-width', 1.4)
        .attr('stroke-dasharray', mode === 'sliding' ? '4 3' : null);

      // bracket label: [start,end)
      svg.append('text')
        .attr('x', (x0 + x1) / 2).attr('y', topY + 12).attr('text-anchor', 'middle')
        .attr('fill', stroke).attr('font-size', 9).attr('font-weight', 700)
        .text(`[${w.start < T_MIN ? T_MIN : w.start},${w.end > T_MAX ? '∞' : w.end})`);

      // a "merged" badge so the merge is unmistakable
      if (merged) {
        svg.append('text')
          .attr('x', (x0 + x1) / 2).attr('y', topY + 24).attr('text-anchor', 'middle')
          .attr('fill', COLORS.flink()).attr('font-size', 8).attr('font-weight', 800)
          .text('⤿ MERGED');
      }
    });

    // --- the event timeline axis ---
    svg.append('line').attr('x1', padL).attr('y1', axisY).attr('x2', W - padR).attr('y2', axisY)
      .attr('stroke', COLORS.border()).attr('stroke-width', 2);
    // a few time gridmarks every 60s
    for (let t = 0; t <= T_MAX; t += 60) {
      svg.append('line').attr('x1', x(t)).attr('y1', axisY - 4).attr('x2', x(t)).attr('y2', axisY + 4)
        .attr('stroke', COLORS.soft()).attr('stroke-width', 1);
      svg.append('text').attr('x', x(t)).attr('y', axisY + 18).attr('text-anchor', 'middle')
        .attr('fill', COLORS.soft()).attr('font-size', 8.5).text(`${t}s`);
    }
    svg.append('text').attr('x', W - padR).attr('y', axisY - 10).attr('text-anchor', 'end')
      .attr('fill', COLORS.soft()).attr('font-size', 9).text('event time →');

    // --- event ticks: each event sits on the axis; colour by how many windows contain it ---
    EVENTS.forEach((t) => {
      const inN = windows.filter((w) => t >= w.start && t < w.end).length;
      const fill = inN >= 2 ? COLORS.flink() : COLORS.beam();
      svg.append('circle').attr('cx', x(t)).attr('cy', axisY).attr('r', 5.5)
        .attr('fill', fill).attr('stroke', COLORS.surface()).attr('stroke-width', 1.5);
      // little "×N windows" hint when an element is duplicated (sliding) or in a session
      if (inN >= 2) {
        svg.append('text').attr('x', x(t)).attr('y', axisY - 12).attr('text-anchor', 'middle')
          .attr('fill', COLORS.flink()).attr('font-size', 8).attr('font-weight', 700)
          .text(`×${inN}`);
      }
    });

    // --- bottom: clickable mode chips (also driven by Step/Play) ---
    const chipY = 232, chipH = 26, gap = 8;
    const chipW = (W - padL - padR - gap * (MODES.length - 1)) / MODES.length;
    MODES.forEach((m, i) => {
      const cx = padL + i * (chipW + gap);
      const on = i === modeIdx;
      const g = svg.append('g').style('cursor', 'pointer')
        .on('click', () => { modeIdx = i; draw(); });
      g.append('rect').attr('x', cx).attr('y', chipY).attr('width', chipW).attr('height', chipH)
        .attr('rx', 7)
        .attr('fill', on ? 'color-mix(in srgb,' + COLORS.beam() + ' 16%, transparent)' : COLORS.surface())
        .attr('stroke', on ? COLORS.beam() : COLORS.border()).attr('stroke-width', on ? 2 : 1.2);
      g.append('text').attr('x', cx + chipW / 2).attr('y', chipY + chipH / 2 + 3.5)
        .attr('text-anchor', 'middle').attr('font-size', 10).attr('font-weight', 700)
        .attr('fill', on ? COLORS.beam() : COLORS.soft())
        .text(m.charAt(0).toUpperCase() + m.slice(1));
    });

    // --- count of windows produced, the "grouping is per-key-per-window" reminder ---
    svg.append('text').attr('x', W / 2).attr('y', 280).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 9.5)
      .text(`${windows.length} window${windows.length === 1 ? '' : 's'} → one (key,count) per window after GroupByKey`);
  }

  const ticker = makeTicker(() => { modeIdx = (modeIdx + 1) % MODES.length; draw(); }, 1800);
  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); modeIdx = (modeIdx + 1) % MODES.length; draw(); },
    reset: () => { ticker.stop(); modeIdx = 0; draw(); },
  });
  draw();
}
