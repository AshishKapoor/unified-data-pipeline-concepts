/* Ch 05 — "the shuffle" that GroupByKey / CoGroupByKey perform.
   Keyed elements (coloured by key) start scattered across two worker lanes on the LEFT. Play/Step
   migrates them across the network to per-key partitions on the RIGHT, where same-key elements
   co-locate on the same downstream worker — exactly what GBK does so it can hand you (K, [V...]).
   This data movement is the most important (and most expensive) thing a runner does. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Four keys (think user_id). Each gets a stable colour drawn from the theme tokens.
const KEYS = ['u1', 'u2', 'u3', 'u4'];
function keyColor(k) {
  return {
    u1: COLORS.beam(),
    u2: COLORS.accent(),
    u3: COLORS.ok(),
    u4: COLORS.late(),
  }[k];
}

// The unsorted input as it sits across two source workers (lane = which worker it arrived on).
// Order here is intentionally mixed so the shuffle visibly re-sorts by key.
const INPUT = [
  { key: 'u1', lane: 0 },
  { key: 'u3', lane: 0 },
  { key: 'u2', lane: 0 },
  { key: 'u1', lane: 0 },
  { key: 'u2', lane: 1 },
  { key: 'u4', lane: 1 },
  { key: 'u3', lane: 1 },
  { key: 'u1', lane: 1 },
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);

  const leftX = 96;          // x of the source-worker column
  const rightX = 364;        // x of the per-key partition column
  const topY = 64;           // first row baseline
  const rowH = 28;           // vertical spacing of elements

  // Pre-compute layout: each element gets a source slot (left) and a target slot (right).
  // Right side is grouped by key (one partition per key, stacked), so same-key elements co-locate.
  const items = INPUT.map((d, i) => ({ ...d, id: i }));

  // Left slots: stack within each lane in arrival order.
  const laneCounters = [0, 0];
  items.forEach((d) => {
    d.x0 = d.lane === 0 ? leftX : leftX;
    d.y0 = topY + (d.lane === 0 ? 0 : (laneCounters[0]) * 0) ; // placeholder, set below
  });
  // Place left items: two vertical stacks side-by-side-ish (lane 0 upper block, lane 1 lower block).
  const lane0 = items.filter((d) => d.lane === 0);
  const lane1 = items.filter((d) => d.lane === 1);
  lane0.forEach((d, i) => { d.x0 = leftX; d.y0 = topY + i * rowH; });
  lane1.forEach((d, i) => { d.x0 = leftX; d.y0 = topY + (lane0.length + 1) * rowH + i * rowH; });

  // Right slots: one partition (vertical stack) per key, ordered by KEYS.
  const perKeyCount = {};
  // Compute the starting Y for each key's partition so partitions are visually separated.
  const keyStartY = {};
  let cursor = topY;
  KEYS.forEach((k) => {
    keyStartY[k] = cursor;
    const n = items.filter((d) => d.key === k).length;
    cursor += n * rowH + 14; // gap between partitions
    perKeyCount[k] = 0;
  });
  items.forEach((d) => {
    d.x1 = rightX;
    d.y1 = keyStartY[d.key] + perKeyCount[d.key] * rowH;
    perKeyCount[d.key] += 1;
  });

  // progress in [0,1]: 0 = all on left (pre-shuffle), 1 = all on right (post-shuffle, grouped).
  let progress = 0;

  function lerp(a, b, t) { return a + (b - a) * t; }

  function draw() {
    svg.selectAll('*').remove();

    // Column headers.
    svg.append('text').attr('x', leftX).attr('y', 30).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 12).attr('font-weight', 700)
      .text('source workers');
    svg.append('text').attr('x', leftX).attr('y', 45).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10)
      .text('(K,V) arrive unsorted');
    svg.append('text').attr('x', rightX).attr('y', 30).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 12).attr('font-weight', 700)
      .text('per-key partitions');
    svg.append('text').attr('x', rightX).attr('y', 45).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10)
      .text('(K,[V…]) co-located');

    // The "network" band in the middle — the shuffle happens here.
    svg.append('rect').attr('x', leftX + 36).attr('y', topY - 14).attr('width', rightX - leftX - 72)
      .attr('height', H - topY - 28).attr('rx', 12)
      .attr('fill', 'color-mix(in srgb,' + COLORS.flink() + ' 6%, transparent)')
      .attr('stroke', COLORS.border()).attr('stroke-dasharray', '4 4');
    svg.append('text').attr('x', W / 2).attr('y', H - 18).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 11).attr('font-weight', 600)
      .text('the shuffle — keyed network exchange');

    // Right-side partition frames (one box per key) appear as items land.
    KEYS.forEach((k) => {
      const n = items.filter((d) => d.key === k).length;
      const fy = keyStartY[k] - 9;
      const fh = n * rowH + 4;
      const landed = progress > 0.55;
      svg.append('rect').attr('x', rightX - 26).attr('y', fy).attr('width', 52).attr('height', fh)
        .attr('rx', 8).attr('fill', 'none')
        .attr('stroke', landed ? keyColor(k) : COLORS.border())
        .attr('stroke-width', landed ? 2 : 1)
        .attr('opacity', 0.9);
      svg.append('text').attr('x', rightX + 34).attr('y', fy + fh / 2 + 4)
        .attr('text-anchor', 'start').attr('font-size', 11).attr('font-weight', 700)
        .attr('fill', landed ? keyColor(k) : COLORS.soft()).text(k);
    });

    // Source-worker frames (two lanes) on the left.
    const lane0H = lane0.length * rowH;
    const lane1H = lane1.length * rowH;
    svg.append('rect').attr('x', leftX - 26).attr('y', topY - 9).attr('width', 52)
      .attr('height', lane0H + 4).attr('rx', 8).attr('fill', 'none')
      .attr('stroke', COLORS.border()).attr('stroke-width', 1);
    svg.append('rect').attr('x', leftX - 26)
      .attr('y', topY + (lane0.length + 1) * rowH - 9).attr('width', 52)
      .attr('height', lane1H + 4).attr('rx', 8).attr('fill', 'none')
      .attr('stroke', COLORS.border()).attr('stroke-width', 1);

    // The flying elements. Each is a rounded chip coloured by its key; it interpolates from its
    // left slot to its grouped right slot as progress advances.
    items.forEach((d) => {
      const x = lerp(d.x0, d.x1, progress);
      const y = lerp(d.y0, d.y1, progress);
      const g = svg.append('g').attr('transform', `translate(${x},${y})`);
      g.append('rect').attr('x', -20).attr('y', -10).attr('width', 40).attr('height', 20)
        .attr('rx', 6).attr('fill', keyColor(d.key)).attr('opacity', 0.92)
        .attr('stroke', COLORS.surface()).attr('stroke-width', 1.5);
      g.append('text').attr('x', 0).attr('y', 4).attr('text-anchor', 'middle')
        .attr('font-size', 10).attr('font-weight', 700).attr('fill', '#fff').text(d.key);
    });

    // Caption that flips when the shuffle completes.
    svg.append('text').attr('x', W / 2).attr('y', topY - 30).attr('text-anchor', 'middle')
      .attr('fill', progress >= 1 ? COLORS.ok() : COLORS.soft())
      .attr('font-size', 11).attr('font-weight', 700)
      .text(progress >= 1
        ? 'same-key elements now co-located → GBK can emit (K,[V…])'
        : 'GroupByKey moves every value for a key to one worker');
  }

  // Play: animate progress smoothly 0 -> 1. Step: jump in thirds. Reset: back to scattered input.
  const ticker = makeTicker(() => {
    progress = Math.min(1, progress + 0.05);
    draw();
    if (progress >= 1) return false; // stop the loop once fully shuffled
  }, 60);

  wireControls(el, {
    play: () => {
      if (ticker.running) { ticker.stop(); return; }
      if (progress >= 1) progress = 0; // replay from the start
      ticker.start();
    },
    step: () => {
      ticker.stop();
      progress = progress >= 1 ? 0 : Math.min(1, progress + 1 / 3);
      draw();
    },
    reset: () => { ticker.stop(); progress = 0; draw(); },
  });

  draw();
}
