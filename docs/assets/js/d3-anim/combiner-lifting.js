/* Ch 06 — "combiner lifting": GroupByKey+reduce vs Combine, side by side.
   LEFT  (GroupByKey): every raw element is shipped across the shuffle, then reduced. The byte
                       counter equals the full payload — all N elements cross the wire.
   RIGHT (Combine):    each worker folds its elements into a tiny (sum,count) accumulator BEFORE
                       the shuffle, so only the small partials cross. The byte counter is far lower.
   Play animates the elements flowing into the shuffle line and the two byte counters filling up,
   driving home that associative/commutative CombineFns move *less* data. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Each worker holds a few raw readings; both sides start from the same data so the comparison is fair.
const WORKERS = [
  { key: 'A', vals: [21, 23, 25] },
  { key: 'B', vals: [10, 20] },
  { key: 'C', vals: [100, 102, 98, 100] },
];
const RAW_BYTES = 8;   // pretend each raw element is 8 bytes on the wire
const ACC_BYTES = 16;  // a (sum,count) accumulator ~ 16 bytes, but only ONE per worker per key
const TOTAL_ELEMS = WORKERS.reduce((n, w) => n + w.vals.length, 0);

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);

  // progress = how many of the TOTAL_ELEMS have "flowed" through. Both panels advance together so
  // the viewer compares the two byte counters at the same moment.
  let progress = 0;

  // panel geometry --------------------------------------------------------------------------------
  const colW = W / 2;          // left half / right half
  const srcY = 92;             // y of the worker rows
  const shuffleY = 196;        // the shuffle "wire"
  const sinkY = 260;           // the reducer/output

  function panel(x0, title, lifted) {
    // x0 = left edge of this panel. `lifted` toggles Combine (pre-aggregation) behaviour.
    const cx = x0 + colW / 2;

    // panel heading
    svg.append('text').attr('x', cx).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', lifted ? COLORS.beam() : COLORS.soft()).attr('font-size', 13).attr('font-weight', 800)
      .text(title);
    svg.append('text').attr('x', cx).attr('y', 36).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10)
      .text(lifted ? 'pre-aggregate, then shuffle' : 'shuffle everything, then reduce');

    // the shuffle wire
    svg.append('line').attr('x1', x0 + 24).attr('y1', shuffleY).attr('x2', x0 + colW - 24).attr('y2', shuffleY)
      .attr('stroke', COLORS.border()).attr('stroke-width', 2).attr('stroke-dasharray', '4 4');
    svg.append('text').attr('x', cx).attr('y', shuffleY - 8).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 9).attr('font-weight', 700).text('— shuffle —');

    // worker boxes (sources) and the elements / accumulators that cross the wire
    const wgap = (colW - 40) / WORKERS.length;
    let bytesShipped = 0;
    let flowedSoFar = 0;

    WORKERS.forEach((w, wi) => {
      const wx = x0 + 24 + wgap * wi + wgap / 2;

      // worker container
      svg.append('rect').attr('x', wx - 26).attr('y', srcY - 22).attr('width', 52).attr('height', 44)
        .attr('rx', 7).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border());
      svg.append('text').attr('x', wx).attr('y', srcY - 26).attr('text-anchor', 'middle')
        .attr('fill', COLORS.soft()).attr('font-size', 9).text(`key ${w.key}`);

      // how many of this worker's elements have flowed given global `progress`
      const localFlow = Math.max(0, Math.min(w.vals.length, progress - flowedSoFar));
      flowedSoFar += w.vals.length;

      if (lifted) {
        // COMBINE: render ONE small accumulator chip; it lights up once this worker's elems flowed.
        const ready = localFlow >= w.vals.length;
        const partialDone = localFlow > 0;
        svg.append('rect').attr('x', wx - 22).attr('y', srcY - 8).attr('width', 44).attr('height', 18)
          .attr('rx', 4)
          .attr('fill', partialDone ? 'color-mix(in srgb,' + COLORS.beam() + ' 22%, transparent)' : COLORS.surface())
          .attr('stroke', partialDone ? COLORS.beam() : COLORS.border()).attr('stroke-width', 1.5);
        svg.append('text').attr('x', wx).attr('y', srcY + 5).attr('text-anchor', 'middle')
          .attr('fill', partialDone ? COLORS.beam() : COLORS.soft()).attr('font-size', 9).attr('font-weight', 700)
          .text('(Σ,n)');
        // a single accumulator crosses the wire once ready
        if (ready) {
          bytesShipped += ACC_BYTES;
          svg.append('circle').attr('cx', wx).attr('cy', shuffleY).attr('r', 5)
            .attr('fill', COLORS.beam());
          svg.append('line').attr('x1', wx).attr('y1', srcY + 12).attr('x2', wx).attr('y2', shuffleY - 5)
            .attr('stroke', COLORS.beam()).attr('stroke-width', 1.5);
        }
      } else {
        // GROUPBYKEY: render every raw element; each that has flowed crosses the wire individually.
        w.vals.forEach((v, vi) => {
          const ex = wx - 18 + (vi % 3) * 18;
          const ey = srcY - 4 + Math.floor(vi / 3) * 14;
          const crossed = vi < localFlow;
          svg.append('circle').attr('cx', ex).attr('cy', ey).attr('r', 5)
            .attr('fill', crossed ? COLORS.flink() : COLORS.surface())
            .attr('stroke', crossed ? COLORS.flink() : COLORS.border()).attr('stroke-width', 1.2);
          if (crossed) {
            bytesShipped += RAW_BYTES;
            // a dot sitting on the shuffle wire = it has been shipped
            svg.append('circle').attr('cx', ex).attr('cy', shuffleY).attr('r', 3.5)
              .attr('fill', COLORS.flink()).attr('opacity', 0.85);
          }
        });
      }
    });

    // sink (reducer / final combine)
    svg.append('rect').attr('x', cx - 50).attr('y', sinkY - 16).attr('width', 100).attr('height', 30)
      .attr('rx', 7).attr('fill', COLORS.surface())
      .attr('stroke', lifted ? COLORS.beam() : COLORS.flink()).attr('stroke-width', 1.8);
    svg.append('text').attr('x', cx).attr('y', sinkY + 4).attr('text-anchor', 'middle')
      .attr('fill', COLORS.text()).attr('font-size', 10).attr('font-weight', 700)
      .text(lifted ? 'final merge → mean' : 'reduce iterable → mean');

    // byte counter — the punchline
    svg.append('rect').attr('x', cx - 56).attr('y', sinkY + 22).attr('width', 112).attr('height', 22)
      .attr('rx', 6).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border());
    svg.append('text').attr('x', cx).attr('y', sinkY + 37).attr('text-anchor', 'middle')
      .attr('fill', lifted ? COLORS.ok() : COLORS.busy()).attr('font-size', 11).attr('font-weight', 800)
      .text(`${bytesShipped} bytes shuffled`);

    return bytesShipped;
  }

  function draw() {
    svg.selectAll('*').remove();

    const gbkBytes = panel(0, 'GroupByKey + reduce', false);
    const combineBytes = panel(colW, 'Combine (lifted)', true);

    // dividing line between the two panels
    svg.append('line').attr('x1', colW).attr('y1', 8).attr('x2', colW).attr('y2', H - 6)
      .attr('stroke', COLORS.border()).attr('stroke-width', 1).attr('stroke-dasharray', '2 5');

    // savings caption once everything has flowed
    if (progress >= TOTAL_ELEMS && gbkBytes > 0) {
      const saved = Math.round((1 - combineBytes / gbkBytes) * 100);
      svg.append('text').attr('x', W / 2).attr('y', H - 4).attr('text-anchor', 'middle')
        .attr('fill', COLORS.ok()).attr('font-size', 10).attr('font-weight', 700)
        .text(`combiner lifting moved ${saved}% fewer bytes across the shuffle`);
    }
  }

  // play: advance one element per tick; stop when everything has crossed.
  const ticker = makeTicker(() => {
    progress += 1;
    draw();
    if (progress >= TOTAL_ELEMS) return false;
  }, 650);

  wireControls(el, {
    play: () => {
      if (progress >= TOTAL_ELEMS) { progress = 0; draw(); }
      ticker.running ? ticker.stop() : ticker.start();
    },
    step: () => { ticker.stop(); progress = Math.min(TOTAL_ELEMS, progress + 1); draw(); },
    reset: () => { ticker.stop(); progress = 0; draw(); },
  });

  draw();
}
