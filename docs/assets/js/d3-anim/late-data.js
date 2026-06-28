/* Ch 12 — "the late-data gauntlet".
   A single FixedWindow [0,60) sits on a timeline. To its right is the allowed-lateness band
   (60 -> 90s) and then the GC horizon. Stepping advances arriving events:
     - on-time events land inside the window and fold into the sum (the ON_TIME pane).
     - a straggler that arrives within the lateness band re-fires the window (green LATE pane, sum
       grows) — it "made it through the gauntlet".
     - a straggler that crosses the GC horizon turns red (DROPPED) and ticks the drop counter; the
       window's sum does NOT change.
   The watermark marker sweeps right as the play loop runs, shrinking the surviving grace span. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Timeline domain in "seconds". The window is [0,60); lateness extends the live span to 90; the
// strip runs to 110 so the dropped straggler has somewhere to land past the GC horizon.
const T_WIN_START = 0;
const T_WIN_END = 60;
const T_GC = 90; // window_end + allowed_lateness
const T_MAX = 110;

// The scripted arrivals, in arrival order (this is what Step walks through). `at` is where the
// event lands on the timeline (its event-time / drawn x); the watermark at the time of arrival
// decides its fate, but for teaching we hard-classify by where it lands relative to the window.
const EVENTS = [
  { id: 'e1', at: 5, amount: 5, kind: 'ontime', label: 'on-time' },
  { id: 'e2', at: 10, amount: 7, kind: 'ontime', label: 'on-time' },
  { id: 'e3', at: 15, amount: 3, kind: 'ontime', label: 'on-time' },
  { id: 'e4', at: 70, amount: 100, kind: 'late', label: 'late +10s' }, // inside [60,90) band
  { id: 'e5', at: 100, amount: 999, kind: 'dropped', label: 'too-late +90s' }, // past GC horizon
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 300;
  const svg = createSvg(el, W, H);

  const padL = 40, padR = 24;
  const axisY = 150;
  const x = d3.scaleLinear().domain([T_WIN_START, T_MAX]).range([padL, W - padR]);

  let cursor = 0;      // how many events have "arrived" (0..EVENTS.length)
  let sum = 0;         // running window sum (ON_TIME + accepted LATE)
  let drops = 0;       // dropped-element counter

  function recompute() {
    sum = 0; drops = 0;
    for (let i = 0; i < cursor; i++) {
      const e = EVENTS[i];
      if (e.kind === 'dropped') drops += 1;
      else sum += e.amount; // on-time and accepted-late both fold into the window
    }
  }

  function draw() {
    recompute();
    svg.selectAll('*').remove();

    // The furthest-arrived event drives the watermark position: it has advanced to at least the
    // newest arrival's event-time (clamped to the strip). Before anything arrives it sits at 0.
    const wmAt = cursor === 0 ? 0 : Math.min(T_MAX, EVENTS[cursor - 1].at);

    // --- title ---
    svg.append('text').attr('x', W / 2).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('the late-data gauntlet · FixedWindow [0,60)');

    // --- the live window band [0,60): elements here always count ---
    svg.append('rect')
      .attr('x', x(T_WIN_START)).attr('y', axisY - 54)
      .attr('width', x(T_WIN_END) - x(T_WIN_START)).attr('height', 54)
      .attr('rx', 6)
      .attr('fill', 'color-mix(in srgb,' + COLORS.beam() + ' 12%, transparent)')
      .attr('stroke', COLORS.beam()).attr('stroke-width', 1.5);
    svg.append('text').attr('x', (x(T_WIN_START) + x(T_WIN_END)) / 2).attr('y', axisY - 38)
      .attr('text-anchor', 'middle').attr('fill', COLORS.beam())
      .attr('font-size', 10).attr('font-weight', 700).text('window');

    // --- the allowed-lateness grace band [60,90): late-but-allowed survives here ---
    svg.append('rect')
      .attr('x', x(T_WIN_END)).attr('y', axisY - 54)
      .attr('width', x(T_GC) - x(T_WIN_END)).attr('height', 54)
      .attr('rx', 6)
      .attr('fill', 'color-mix(in srgb,' + COLORS.ok() + ' 12%, transparent)')
      .attr('stroke', COLORS.ok()).attr('stroke-width', 1.5).attr('stroke-dasharray', '4 3');
    svg.append('text').attr('x', (x(T_WIN_END) + x(T_GC)) / 2).attr('y', axisY - 38)
      .attr('text-anchor', 'middle').attr('fill', COLORS.ok())
      .attr('font-size', 9).attr('font-weight', 700).text('allowed_lateness 30s');

    // --- the dropped zone past the GC horizon ---
    svg.append('rect')
      .attr('x', x(T_GC)).attr('y', axisY - 54)
      .attr('width', x(T_MAX) - x(T_GC)).attr('height', 54)
      .attr('rx', 6)
      .attr('fill', 'color-mix(in srgb,' + COLORS.late() + ' 10%, transparent)')
      .attr('stroke', COLORS.late()).attr('stroke-width', 1).attr('stroke-dasharray', '2 3');
    svg.append('text').attr('x', (x(T_GC) + x(T_MAX)) / 2).attr('y', axisY - 38)
      .attr('text-anchor', 'middle').attr('fill', COLORS.late())
      .attr('font-size', 9).attr('font-weight', 700).text('GC · dropped');

    // --- the timeline axis with tick marks at the key boundaries ---
    svg.append('line').attr('x1', x(T_WIN_START)).attr('y1', axisY)
      .attr('x2', x(T_MAX)).attr('y2', axisY)
      .attr('stroke', COLORS.border()).attr('stroke-width', 2);
    [[T_WIN_END, 'window_end 60s'], [T_GC, 'GC horizon 90s']].forEach(([t, lbl]) => {
      svg.append('line').attr('x1', x(t)).attr('y1', axisY - 54).attr('x2', x(t)).attr('y2', axisY + 8)
        .attr('stroke', COLORS.soft()).attr('stroke-width', 1).attr('stroke-dasharray', '3 3');
      svg.append('text').attr('x', x(t)).attr('y', axisY + 22).attr('text-anchor', 'middle')
        .attr('fill', COLORS.soft()).attr('font-size', 9).text(lbl);
    });

    // --- the watermark marker, sweeping right as events arrive ---
    const wmX = x(wmAt);
    svg.append('path')
      .attr('d', `M ${wmX} ${axisY - 60} L ${wmX - 6} ${axisY - 70} L ${wmX + 6} ${axisY - 70} Z`)
      .attr('fill', COLORS.flink());
    svg.append('line').attr('x1', wmX).attr('y1', axisY - 60).attr('x2', wmX).attr('y2', axisY + 8)
      .attr('stroke', COLORS.flink()).attr('stroke-width', 2);
    svg.append('text').attr('x', wmX).attr('y', axisY - 74).attr('text-anchor', 'middle')
      .attr('fill', COLORS.flink()).attr('font-size', 9).attr('font-weight', 700).text('watermark');

    // --- arrived events as dots on the axis ---
    for (let i = 0; i < cursor; i++) {
      const e = EVENTS[i];
      const cx = x(e.at);
      let fill, ring;
      if (e.kind === 'dropped') { fill = COLORS.late(); ring = COLORS.late(); }
      else if (e.kind === 'late') { fill = COLORS.ok(); ring = COLORS.ok(); }
      else { fill = COLORS.beam(); ring = COLORS.beam(); }

      svg.append('circle').attr('cx', cx).attr('cy', axisY).attr('r', 9)
        .attr('fill', fill).attr('stroke', COLORS.surface()).attr('stroke-width', 1.5);
      svg.append('text').attr('x', cx).attr('y', axisY + 4).attr('text-anchor', 'middle')
        .attr('fill', COLORS.surface()).attr('font-size', 8).attr('font-weight', 700)
        .text(e.amount);

      // label + an "×" for the dropped one
      svg.append('text').attr('x', cx).attr('y', axisY + 40).attr('text-anchor', 'middle')
        .attr('fill', ring).attr('font-size', 8).attr('font-weight', 600).text(e.label);
      if (e.kind === 'dropped') {
        svg.append('text').attr('x', cx).attr('y', axisY - 14).attr('text-anchor', 'middle')
          .attr('fill', COLORS.late()).attr('font-size', 14).attr('font-weight', 800).text('✕');
      }
    }

    // --- the readout panel: running sum + which pane just fired + drop counter ---
    const panelY = axisY + 64;
    const lastKind = cursor > 0 ? EVENTS[cursor - 1].kind : null;
    const paneLabel = lastKind === 'late' ? 'LATE pane' :
      lastKind === 'ontime' ? 'ON_TIME pane' :
      lastKind === 'dropped' ? '(no pane — dropped)' : '—';
    const paneColor = lastKind === 'dropped' ? COLORS.late() :
      lastKind === 'late' ? COLORS.ok() : COLORS.beam();

    svg.append('rect').attr('x', padL).attr('y', panelY).attr('width', W - padL - padR).attr('height', 44)
      .attr('rx', 8).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border());

    svg.append('text').attr('x', padL + 14).attr('y', panelY + 19)
      .attr('fill', COLORS.soft()).attr('font-size', 10).text('window sum');
    svg.append('text').attr('x', padL + 14).attr('y', panelY + 36)
      .attr('fill', COLORS.text()).attr('font-size', 16).attr('font-weight', 800).text(sum);

    svg.append('text').attr('x', W / 2).attr('y', panelY + 19).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10).text('last firing');
    svg.append('text').attr('x', W / 2).attr('y', panelY + 36).attr('text-anchor', 'middle')
      .attr('fill', paneColor).attr('font-size', 12).attr('font-weight', 800).text(paneLabel);

    svg.append('text').attr('x', W - padR - 14).attr('y', panelY + 19).attr('text-anchor', 'end')
      .attr('fill', COLORS.soft()).attr('font-size', 10).text('dropped');
    svg.append('text').attr('x', W - padR - 14).attr('y', panelY + 36).attr('text-anchor', 'end')
      .attr('fill', drops > 0 ? COLORS.late() : COLORS.text())
      .attr('font-size', 16).attr('font-weight', 800).text(drops);
  }

  function advance() {
    if (cursor >= EVENTS.length) { cursor = 0; return false; } // loop completed -> stop the ticker
    cursor += 1;
    draw();
    return cursor < EVENTS.length;
  }

  const ticker = makeTicker(() => advance(), 1300);
  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); if (cursor >= EVENTS.length) cursor = 0; advance(); },
    reset: () => { ticker.stop(); cursor = 0; draw(); },
  });
  draw();
}
