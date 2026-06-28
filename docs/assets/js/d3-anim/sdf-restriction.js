/* Ch 14 — Splittable DoFn: restriction, try_claim, and primary/residual split.

   A worker owns a restriction [start, stop) over the offset range [0..N]. "Play" walks the claim
   pointer forward one offset at a time (try_claim succeeds -> the cell fills). "Step" claims a single
   offset. "Reset" restores the full restriction to worker A.

   The headline move is the SPLIT: click the bar (or hit play far enough) and the *remaining* range is
   cleaved at the current claim point — everything already claimed plus a slice stays as worker A's
   PRIMARY, and the tail is peeled off as the RESIDUAL and handed to worker B's lane. That is exactly
   how the runner does dynamic work rebalancing / mid-element checkpointing in the pipeline. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

const N = 24; // offsets in the range [0, N)

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 300;
  const svg = createSvg(el, W, H);

  // --- model -------------------------------------------------------------
  // worker A owns [aStart, aStop); claimPos is the next offset try_claim will attempt.
  // Once a split happens, worker B owns the residual [bStart, bStop) and is `hasResidual`.
  let claimPos, aStart, aStop, bStart, bStop, hasResidual, didSplit;

  function reset() {
    aStart = 0; aStop = N;          // worker A initially owns the whole restriction [0, N)
    claimPos = 0;                   // nothing claimed yet
    bStart = bStop = N;             // worker B owns nothing
    hasResidual = false;
    didSplit = false;
  }
  reset();

  // claim the next offset, if any remain in A's current restriction
  function claimOne() {
    if (claimPos < aStop) { claimPos += 1; return true; }
    return false; // try_claim() returned False: A's range is exhausted
  }

  // try_split at the current remainder: keep claimed work + a small primary slice on A; peel the
  // rest off to B as the residual. Splits at the midpoint of the *unclaimed* tail.
  function split() {
    if (didSplit) return false;                 // one residual hand-off is enough for the demo
    const unclaimed = aStop - claimPos;
    if (unclaimed <= 1) return false;           // nothing meaningful to peel off
    const cut = claimPos + Math.ceil(unclaimed / 2); // primary keeps [.., cut), residual is [cut, aStop)
    bStart = cut; bStop = aStop;                // B gets the residual
    aStop = cut;                                // A's restriction shrinks to its primary
    hasResidual = true;
    didSplit = true;
    return true;
  }

  // --- geometry ----------------------------------------------------------
  const padX = 28;
  const barW = W - padX * 2;
  const cellW = barW / N;
  const laneAY = 96;   // worker A lane
  const laneBY = 196;  // worker B (residual) lane
  const barH = 30;
  const xOf = (i) => padX + i * cellW;

  function lane(yTop, lo, hi, who, color, active) {
    // lane label chip
    svg.append('rect').attr('x', padX - 4).attr('y', yTop - 30).attr('rx', 6)
      .attr('width', 92).attr('height', 20)
      .attr('fill', active ? 'color-mix(in srgb,' + color + ' 16%, transparent)' : COLORS.surface())
      .attr('stroke', active ? color : COLORS.border()).attr('stroke-width', active ? 2 : 1);
    svg.append('text').attr('x', padX + 42).attr('y', yTop - 16).attr('text-anchor', 'middle')
      .attr('font-size', 11).attr('font-weight', 700)
      .attr('fill', active ? color : COLORS.soft()).text(who);

    // baseline track (full [0,N) extent, faint) so both lanes share the same coordinate frame
    svg.append('rect').attr('x', xOf(0)).attr('y', yTop).attr('width', barW).attr('height', barH)
      .attr('rx', 4).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border());

    if (hi <= lo) return; // lane owns nothing

    // this lane's owned restriction [lo, hi)
    svg.append('rect').attr('x', xOf(lo)).attr('y', yTop)
      .attr('width', cellW * (hi - lo)).attr('height', barH).attr('rx', 4)
      .attr('fill', 'color-mix(in srgb,' + color + ' 10%, transparent)')
      .attr('stroke', color).attr('stroke-width', 1.5);

    // per-offset cells: claimed (filled) only meaningful on worker A
    for (let i = lo; i < hi; i++) {
      const claimed = who === 'worker A' && i < claimPos;
      svg.append('rect').attr('x', xOf(i) + 1).attr('y', yTop + 1)
        .attr('width', Math.max(1, cellW - 2)).attr('height', barH - 2).attr('rx', 2)
        .attr('fill', claimed ? color : 'transparent')
        .attr('opacity', claimed ? 0.85 : 1);
    }

    // start/stop labels for the restriction
    svg.append('text').attr('x', xOf(lo)).attr('y', yTop + barH + 14).attr('text-anchor', 'middle')
      .attr('font-size', 9).attr('fill', COLORS.soft()).text(lo);
    svg.append('text').attr('x', xOf(hi)).attr('y', yTop + barH + 14).attr('text-anchor', 'middle')
      .attr('font-size', 9).attr('fill', COLORS.soft()).text(hi);
  }

  function draw() {
    svg.selectAll('*').remove();

    // title
    svg.append('text').attr('x', W / 2).attr('y', 22).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('restriction over offsets [0, ' + N + ')');

    // ---- worker A lane (the original owner) ----
    lane(laneAY, aStart, aStop, 'worker A', COLORS.beam(), true);

    // try_claim pointer on A
    if (claimPos <= aStop) {
      const px = xOf(claimPos);
      svg.append('path')
        .attr('d', `M ${px} ${laneAY - 6} l -5 -8 l 10 0 z`)
        .attr('fill', COLORS.flink());
      svg.append('text').attr('x', px).attr('y', laneAY - 18).attr('text-anchor', 'middle')
        .attr('font-size', 9).attr('font-weight', 700).attr('fill', COLORS.flink())
        .text(claimPos < aStop ? 'try_claim(' + claimPos + ')' : 'done');
    }

    // ---- worker B (residual) lane ----
    lane(laneBY, bStart, bStop, 'worker B', COLORS.accent(), hasResidual);

    // residual hand-off arrow A -> B, drawn at the split point
    if (hasResidual) {
      const sx = xOf(bStart);
      svg.append('path')
        .attr('d', `M ${sx} ${laneAY + barH} C ${sx} 150, ${sx} 150, ${sx} ${laneBY}`)
        .attr('fill', 'none').attr('stroke', COLORS.accent()).attr('stroke-width', 2.5)
        .attr('stroke-dasharray', '5 4')
        .append('animate').attr('attributeName', 'stroke-dashoffset')
        .attr('from', 18).attr('to', 0).attr('dur', '0.6s').attr('repeatCount', 'indefinite');
      svg.append('text').attr('x', sx + 6).attr('y', 150).attr('font-size', 9)
        .attr('font-weight', 700).attr('fill', COLORS.accent())
        .text('residual [' + bStart + ', ' + bStop + ')');
    }

    // split divider on A at the cut point (only after a split)
    if (didSplit) {
      const cx = xOf(aStop);
      svg.append('line').attr('x1', cx).attr('y1', laneAY - 2).attr('x2', cx).attr('y2', laneAY + barH + 2)
        .attr('stroke', COLORS.flink()).attr('stroke-width', 2).attr('stroke-dasharray', '3 2');
    }

    // status / hint line
    const done = claimPos >= aStop;
    const msg = !didSplit
      ? (done ? 'A claimed its whole range — try Split before finishing for the residual demo'
              : 'Play claims offsets one-by-one. Click the bar (or Split) to peel off a residual.')
      : (done ? 'A finished its primary; the residual runs on worker B (dynamic rebalancing)'
              : 'A keeps claiming its primary; B already owns the residual');
    svg.append('text').attr('x', W / 2).attr('y', H - 12).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10.5).text(msg);

    // make worker A's bar clickable to trigger a split at the current claim point
    svg.append('rect').attr('x', xOf(0)).attr('y', laneAY).attr('width', barW).attr('height', barH)
      .attr('fill', 'transparent').style('cursor', didSplit ? 'default' : 'pointer')
      .on('click', () => { if (split()) draw(); });
  }

  // play loop: claim offsets; auto-split once at the midpoint so the residual is demonstrated;
  // stop when worker A has fully claimed its (possibly shrunken) primary.
  const ticker = makeTicker(() => {
    // demonstrate a dynamic split mid-stream the first time we reach the original midpoint
    if (!didSplit && claimPos >= Math.floor(N / 2)) { split(); draw(); return true; }
    const more = claimOne();
    draw();
    return more; // false stops the ticker
  }, 380);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); claimOne(); draw(); },
    reset: () => { ticker.stop(); reset(); draw(); },
  });

  draw();
}
