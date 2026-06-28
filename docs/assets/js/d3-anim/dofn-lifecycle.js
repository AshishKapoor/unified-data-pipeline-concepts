/* Ch 03 — the DoFn lifecycle as a timeline.
   A single DoFn instance processes a stream of elements that the runner has sliced into BUNDLES.
   Across the bottom we lay out the exact callback sequence the runner drives:

     setup -> [ start_bundle -> process×N -> finish_bundle ]× bundles -> teardown

   Play walks a token left-to-right, lighting each callback in order. setup/teardown are drawn as
   instance-scoped (they fire once, outside the bundle loop); start_bundle/finish_bundle are the
   bundle boundaries (vertical markers); process steps are the per-element work inside a bundle.
   The point: bundles are the unit of commit/retry, and the runner — not your code — picks them. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Two bundles of 3 and 2 elements => mirrors a runner that committed in two chunks.
const BUNDLES = [3, 2];

// Build the flat ordered list of lifecycle steps the animation walks through.
function buildSteps() {
  const steps = [];
  steps.push({ kind: 'setup', label: 'setup', sub: 'open client', scope: 'instance' });
  BUNDLES.forEach((count, b) => {
    steps.push({ kind: 'start', label: 'start_bundle', sub: `bundle #${b + 1}`, scope: 'bundle', bundle: b });
    for (let i = 0; i < count; i++) {
      steps.push({ kind: 'process', label: 'process', sub: `elem ${i + 1}`, scope: 'element', bundle: b });
    }
    steps.push({ kind: 'finish', label: 'finish_bundle', sub: 'flush', scope: 'bundle', bundle: b });
  });
  steps.push({ kind: 'teardown', label: 'teardown', sub: 'close (best-effort)', scope: 'instance' });
  return steps;
}

const STEPS = buildSteps();

function colorFor(kind) {
  switch (kind) {
    case 'setup': return COLORS.beam();
    case 'teardown': return COLORS.idle();
    case 'start':
    case 'finish': return COLORS.flink();
    case 'process': return COLORS.accent();
    default: return COLORS.border();
  }
}

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);

  // -1 = nothing lit yet; STEPS.length-1 = fully done.
  let cur = -1;

  // Horizontal layout: evenly distribute the steps across the track.
  const padL = 26, padR = 26;
  const trackY = 168;
  const slotW = (W - padL - padR) / STEPS.length;
  const nodeW = Math.min(slotW - 8, 46);
  const nodeH = 40;
  const cx = (i) => padL + slotW * i + slotW / 2;

  function draw() {
    svg.selectAll('*').remove();

    // --- title + instance label -------------------------------------------------
    svg.append('text').attr('x', W / 2).attr('y', 20).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('one DoFn instance · runner-chosen bundles');

    // --- the instance "lane" spanning setup..teardown ---------------------------
    svg.append('rect')
      .attr('x', cx(0) - nodeW / 2 - 6).attr('y', trackY - 58)
      .attr('width', cx(STEPS.length - 1) - cx(0) + nodeW + 12).attr('height', 116)
      .attr('rx', 12).attr('fill', 'none')
      .attr('stroke', COLORS.border()).attr('stroke-width', 1.5).attr('stroke-dasharray', '4 4');
    svg.append('text').attr('x', cx(0) - nodeW / 2 - 6 + 8).attr('y', trackY - 58 - 7)
      .attr('fill', COLORS.soft()).attr('font-size', 10).attr('font-weight', 600)
      .text('instance lifetime (setup → teardown)');

    // --- bundle bands (commit/retry units) --------------------------------------
    // Find the index range of each bundle (start_bundle .. finish_bundle inclusive).
    BUNDLES.forEach((_, b) => {
      const idxs = STEPS.map((s, i) => (s.bundle === b ? i : -1)).filter((i) => i >= 0);
      if (!idxs.length) return;
      const lo = idxs[0], hi = idxs[idxs.length - 1];
      const x0 = cx(lo) - nodeW / 2 - 4;
      const x1 = cx(hi) + nodeW / 2 + 4;
      svg.append('rect')
        .attr('x', x0).attr('y', trackY - 30).attr('width', x1 - x0).attr('height', 60)
        .attr('rx', 9)
        .attr('fill', 'color-mix(in srgb,' + COLORS.flink() + ' 8%, transparent)')
        .attr('stroke', 'color-mix(in srgb,' + COLORS.flink() + ' 40%, transparent)')
        .attr('stroke-width', 1.5);
      svg.append('text').attr('x', (x0 + x1) / 2).attr('y', trackY - 36)
        .attr('text-anchor', 'middle').attr('fill', COLORS.flink())
        .attr('font-size', 10).attr('font-weight', 800)
        .text(`bundle #${b + 1} — commit / retry unit`);
    });

    // --- connector line through all steps ---------------------------------------
    svg.append('line')
      .attr('x1', cx(0)).attr('y1', trackY).attr('x2', cx(STEPS.length - 1)).attr('y2', trackY)
      .attr('stroke', COLORS.border()).attr('stroke-width', 2);

    // --- each lifecycle node -----------------------------------------------------
    STEPS.forEach((s, i) => {
      const x = cx(i);
      const done = i < cur;
      const active = i === cur;
      const base = colorFor(s.kind);
      const fill = active
        ? base
        : done
          ? 'color-mix(in srgb,' + base + ' 22%, ' + COLORS.surface() + ')'
          : COLORS.surface();
      const stroke = active || done ? base : COLORS.border();

      const g = svg.append('g').style('cursor', 'pointer')
        .on('click', () => { cur = i; draw(); });

      g.append('rect')
        .attr('x', x - nodeW / 2).attr('y', trackY - nodeH / 2)
        .attr('width', nodeW).attr('height', nodeH).attr('rx', 8)
        .attr('fill', fill).attr('stroke', stroke)
        .attr('stroke-width', active ? 3 : 1.6);

      // a glow ring on the active node
      if (active) {
        g.append('rect')
          .attr('x', x - nodeW / 2 - 4).attr('y', trackY - nodeH / 2 - 4)
          .attr('width', nodeW + 8).attr('height', nodeH + 8).attr('rx', 11)
          .attr('fill', 'none').attr('stroke', base).attr('stroke-width', 1.5).attr('opacity', 0.5);
      }

      // label inside the node (abbreviated to fit the narrow node)
      const short = { setup: 'setup', start: 'start', process: 'proc', finish: 'finish', teardown: 'tear' }[s.kind];
      g.append('text').attr('x', x).attr('y', trackY + 3).attr('text-anchor', 'middle')
        .attr('font-size', 9).attr('font-weight', 800)
        .attr('fill', active ? COLORS.surface() : (done ? base : COLORS.soft()))
        .text(short);

      // sub-label under each node
      g.append('text').attr('x', x).attr('y', trackY + nodeH / 2 + 13).attr('text-anchor', 'middle')
        .attr('font-size', 8).attr('fill', COLORS.soft()).text(s.sub);
    });

    // --- the moving "execution cursor" token ------------------------------------
    if (cur >= 0 && cur < STEPS.length) {
      const x = cx(cur);
      svg.append('path')
        .attr('d', `M ${x} ${trackY - nodeH / 2 - 16} l -7 -11 l 14 0 z`)
        .attr('fill', colorFor(STEPS[cur].kind));
    }

    // --- current-step readout banner --------------------------------------------
    const y0 = 250;
    const cardW = W - 40;
    svg.append('rect').attr('x', 20).attr('y', y0).attr('width', cardW).attr('height', 50)
      .attr('rx', 9).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border()).attr('stroke-width', 1);

    if (cur < 0) {
      svg.append('text').attr('x', W / 2).attr('y', y0 + 30).attr('text-anchor', 'middle')
        .attr('fill', COLORS.soft()).attr('font-size', 12)
        .text('press ▶ Play — watch the callbacks fire in order');
    } else {
      const s = STEPS[cur];
      const notes = {
        setup: 'runs ONCE per instance — open reusable clients here',
        start: 'begins a bundle — reset per-bundle buffers',
        process: 'per element — must be IDEMPOTENT (bundles retry)',
        finish: 'flush the batch — commit-tied, not best-effort',
        teardown: 'BEST-EFFORT — a crash may skip it; never flush here',
      };
      svg.append('circle').attr('cx', 38).attr('cy', y0 + 25).attr('r', 7).attr('fill', colorFor(s.kind));
      svg.append('text').attr('x', 54).attr('y', y0 + 21).attr('fill', COLORS.text())
        .attr('font-size', 12).attr('font-weight', 800).text(s.label + '()');
      svg.append('text').attr('x', 54).attr('y', y0 + 38).attr('fill', COLORS.soft())
        .attr('font-size', 10).text(notes[s.kind]);
    }
  }

  function next() {
    if (cur >= STEPS.length - 1) return false; // stop the ticker at the end
    cur += 1;
    draw();
    return true;
  }

  const ticker = makeTicker(() => next(), 850);
  wireControls(el, {
    play: () => {
      if (ticker.running) { ticker.stop(); return; }
      if (cur >= STEPS.length - 1) { cur = -1; draw(); }
      ticker.start();
    },
    step: () => { ticker.stop(); if (!next()) { cur = -1; draw(); } },
    reset: () => { ticker.stop(); cur = -1; draw(); },
  });

  draw();
}
