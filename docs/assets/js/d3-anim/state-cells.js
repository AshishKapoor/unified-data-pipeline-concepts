/* Ch 13 — "keyed state cells & an idle-gap timer".
   Two users, each with their own private state box holding three cells: a Bag (buffered events),
   a Combining count, and an event-time gap-timer countdown. Incoming elements stream into a user's
   box, append to the bag, bump the count, and RESET that user's timer. When a user's timer reaches
   zero (no new event in time), the @on_timer callback fires: it drains the bag, emits a session
   summary, and clears the cells — driving home per-key isolation + timer-driven flush. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

const GAP = 5;            // timer starts at this many "ticks" and counts down to 0
const BURST = 4;          // count >= BURST flags the emitted session as a BURST
const USERS = ['user-A', 'user-B'];

// A scripted event timeline: which user receives an event on each step (null = a quiet tick where
// nothing arrives, so timers can drain). Crafted so user-A bursts (flagged) and user-B trickles.
const SCRIPT = [
  'user-A', 'user-A', 'user-B', 'user-A', 'user-A',
  null, null, null, null, null,        // quiet — user-A's gap timer expires -> flush (BURST)
  'user-B', null, null, null, null, null, // user-B's lone event then idle -> flush (ok)
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);

  // Per-user state. timer === null means "disarmed" (no events buffered).
  let cursor;          // index into SCRIPT
  let flash;           // {user, ...summary} for the most recent flush, briefly highlighted
  let arriving;        // which user just received an event this step (for the inflow pulse)
  const state = {};

  function resetState() {
    cursor = 0; flash = null; arriving = null;
    USERS.forEach((u) => { state[u] = { bag: 0, count: 0, timer: null }; });
  }
  resetState();

  // Advance the simulation by one step: deliver the scripted event (if any), then tick every armed
  // timer down by one. A timer hitting zero fires the flush callback for that user.
  function advance() {
    arriving = null;
    flash = null;

    const who = SCRIPT[cursor % SCRIPT.length];
    if (who) {
      const s = state[who];
      s.bag += 1;            // BagState.add(event)
      s.count += 1;          // CombiningState.add(1)
      s.timer = GAP;         // (re)arm the event-time gap timer: gap_timer.set(now + GAP)
      arriving = who;
    }

    // Watermark advances: decrement each armed timer; fire @on_timer when it reaches 0.
    USERS.forEach((u) => {
      const s = state[u];
      if (s.timer !== null && who !== u) {
        s.timer -= 1;
        if (s.timer <= 0) {
          // @on_timer(GAP_TIMER): drain bag, read count, emit, then clear all cells.
          flash = { user: u, count: s.count, flag: s.count >= BURST ? 'BURST' : 'ok' };
          s.bag = 0; s.count = 0; s.timer = null;
        }
      }
    });

    cursor += 1;
    draw();
  }

  function cell(g, x, y, w, h, label, body, accent) {
    g.append('rect').attr('x', x).attr('y', y).attr('width', w).attr('height', h)
      .attr('rx', 7).attr('fill', COLORS.surface2())
      .attr('stroke', accent || COLORS.border()).attr('stroke-width', accent ? 2 : 1.2);
    g.append('text').attr('x', x + 8).attr('y', y + 15).attr('font-size', 9.5)
      .attr('font-weight', 700).attr('fill', COLORS.soft()).text(label);
    g.append('text').attr('x', x + w / 2).attr('y', y + h - 11).attr('text-anchor', 'middle')
      .attr('font-size', 13).attr('font-weight', 700).attr('fill', COLORS.text()).text(body);
  }

  function draw() {
    svg.selectAll('*').remove();

    svg.append('text').attr('x', W / 2).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 12).attr('font-weight', 700)
      .text('each key gets its own state cells + gap timer');

    const boxW = 200, boxH = 116, gap = 24;
    const startX = (W - (USERS.length * boxW + (USERS.length - 1) * gap)) / 2;
    const boxY = 44;

    USERS.forEach((u, i) => {
      const x = startX + i * (boxW + gap);
      const s = state[u];
      const incoming = arriving === u;
      const fired = flash && flash.user === u;
      const armed = s.timer !== null;
      const boxColor = incoming ? COLORS.beam() : fired ? COLORS.flink() : COLORS.border();

      const g = svg.append('g');
      // The per-key state box.
      g.append('rect').attr('x', x).attr('y', boxY).attr('width', boxW).attr('height', boxH)
        .attr('rx', 12).attr('fill', COLORS.surface())
        .attr('stroke', boxColor).attr('stroke-width', incoming || fired ? 2.6 : 1.4);
      g.append('text').attr('x', x + 12).attr('y', boxY + 18).attr('font-size', 12)
        .attr('font-weight', 700).attr('fill', COLORS.text()).text(u);

      // Inflow pulse: a beam-coloured dot dropping into the box.
      if (incoming) {
        g.append('circle').attr('cx', x + boxW - 16).attr('cy', boxY + 12).attr('r', 5)
          .attr('fill', COLORS.beam());
        g.append('text').attr('x', x + boxW - 28).attr('y', boxY + 16).attr('text-anchor', 'end')
          .attr('font-size', 9).attr('font-weight', 700).attr('fill', COLORS.beam()).text('event ▸');
      }

      // Three cells: BagState, CombiningState(count), the gap-timer.
      const cy = boxY + 28, ch = 50, pad = 10;
      const cw = (boxW - pad * 4) / 3;
      cell(g, x + pad, cy, cw, ch, 'Bag', '▤'.repeat(Math.min(s.bag, 4)) || '∅',
        s.bag ? COLORS.accent() : null);
      cell(g, x + pad * 2 + cw, cy, cw, ch, 'Count', String(s.count),
        s.count >= BURST ? COLORS.flink() : (s.count ? COLORS.accent() : null));
      cell(g, x + pad * 3 + cw * 2, cy, cw, ch, 'Timer',
        armed ? `${s.timer}s` : '–', armed ? COLORS.late() : null);

      // Timer countdown bar (event-time watermark approaching the deadline).
      const barY = boxY + boxH - 8;
      g.append('rect').attr('x', x + pad).attr('y', barY).attr('width', boxW - pad * 2).attr('height', 4)
        .attr('rx', 2).attr('fill', COLORS.surface2());
      if (armed) {
        g.append('rect').attr('x', x + pad).attr('y', barY)
          .attr('width', (boxW - pad * 2) * (s.timer / GAP)).attr('height', 4)
          .attr('rx', 2).attr('fill', COLORS.late());
      }
    });

    // The flush / @on_timer emission banner.
    const banner = svg.append('g');
    banner.append('rect').attr('x', 40).attr('y', 178).attr('width', W - 80).attr('height', 40)
      .attr('rx', 9).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border()).attr('stroke-width', 1.2);
    if (flash) {
      const isBurst = flash.flag === 'BURST';
      banner.select('rect').attr('stroke', isBurst ? COLORS.busy() : COLORS.ok()).attr('stroke-width', 2.4);
      banner.append('text').attr('x', W / 2).attr('y', 195).attr('text-anchor', 'middle')
        .attr('font-size', 11.5).attr('font-weight', 700).attr('fill', COLORS.text())
        .text(`@on_timer fires → flush ${flash.user}`);
      banner.append('text').attr('x', W / 2).attr('y', 211).attr('text-anchor', 'middle')
        .attr('font-size', 11).attr('font-weight', 700)
        .attr('fill', isBurst ? COLORS.busy() : COLORS.ok())
        .text(`emit session: count=${flash.count}  flag=${flash.flag}  (cells cleared)`);
    } else {
      banner.append('text').attr('x', W / 2).attr('y', 202).attr('text-anchor', 'middle')
        .attr('font-size', 11).attr('fill', COLORS.soft())
        .text('idle gap timer → fires the flush callback when it hits 0');
    }

    // Legend / time-domain reminder.
    svg.append('text').attr('x', W / 2).attr('y', 244).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10)
      .text('timer domain = WATERMARK (event time) · every event re-arms the gap');
    svg.append('text').attr('x', W / 2).attr('y', 262).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10)
      .text(`Bag buffers events · Count combines (sum) · BURST when count ≥ ${BURST}`);
    const next = SCRIPT[cursor % SCRIPT.length];
    svg.append('text').attr('x', W / 2).attr('y', 286).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10.5).attr('font-weight', 600)
      .text(next ? `next step → event for ${next}` : 'next step → quiet tick (watermark advances)');
  }

  const ticker = makeTicker(advance, 1100);
  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); advance(); },
    reset: () => { ticker.stop(); resetState(); draw(); },
  });
  draw();
}
