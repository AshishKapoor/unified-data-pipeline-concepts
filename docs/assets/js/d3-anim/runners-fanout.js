/* Ch 01 — "one pipeline, many runners".
   Draws the same 3-stage pipeline once, with four runner chips below it. Play cycles which runner
   is "executing" the identical graph, driving home that the engine is a swappable detail. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

const STAGES = ['Read', 'Count', 'Write'];
const RUNNERS = ['DirectRunner', 'FlinkRunner', 'DataflowRunner', 'SparkRunner'];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 300;
  const svg = createSvg(el, W, H);
  let active = 1; // start on Flink (this course's engine)

  function draw() {
    svg.selectAll('*').remove();

    // --- the single pipeline graph (top) ---
    const y = 56;
    const xs = [80, 230, 380];
    svg.append('text').attr('x', W / 2).attr('y', 22).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('one pipeline definition');

    xs.forEach((x, i) => {
      if (i < xs.length - 1) {
        svg.append('line').attr('x1', x + 34).attr('y1', y).attr('x2', xs[i + 1] - 34).attr('y2', y)
          .attr('stroke', COLORS.border()).attr('stroke-width', 2);
      }
    });
    xs.forEach((x, i) => {
      svg.append('circle').attr('cx', x).attr('cy', y).attr('r', 30)
        .attr('fill', COLORS.surface2()).attr('stroke', COLORS.beam()).attr('stroke-width', 2);
      svg.append('text').attr('x', x).attr('y', y + 4).attr('text-anchor', 'middle')
        .attr('fill', COLORS.text()).attr('font-size', 12).attr('font-weight', 600).text(STAGES[i]);
    });

    // --- runner chips (bottom) ---
    const chipY = 168, chipW = 96, chipH = 64, gap = 14;
    const totalW = RUNNERS.length * chipW + (RUNNERS.length - 1) * gap;
    const startX = (W - totalW) / 2;

    RUNNERS.forEach((name, i) => {
      const cx = startX + i * (chipW + gap);
      const isActive = i === active;
      const color = isActive ? COLORS.flink() : COLORS.border();

      // connector from pipeline to the active runner
      if (isActive) {
        svg.append('path')
          .attr('d', `M ${W / 2} ${y + 30} C ${W / 2} 120, ${cx + chipW / 2} 130, ${cx + chipW / 2} ${chipY}`)
          .attr('fill', 'none').attr('stroke', COLORS.flink()).attr('stroke-width', 2.5)
          .attr('stroke-dasharray', '5 4')
          .append('animate').attr('attributeName', 'stroke-dashoffset')
          .attr('from', 18).attr('to', 0).attr('dur', '0.6s').attr('repeatCount', 'indefinite');
      }

      const g = svg.append('g');
      g.append('rect').attr('x', cx).attr('y', chipY).attr('width', chipW).attr('height', chipH)
        .attr('rx', 10).attr('fill', isActive ? 'color-mix(in srgb,' + COLORS.flink() + ' 14%, transparent)' : COLORS.surface())
        .attr('stroke', color).attr('stroke-width', isActive ? 2.5 : 1.5)
        .style('cursor', 'pointer')
        .on('click', () => { active = i; draw(); });
      g.append('text').attr('x', cx + chipW / 2).attr('y', chipY + chipH / 2 - 4)
        .attr('text-anchor', 'middle').attr('font-size', 11).attr('font-weight', 700)
        .attr('fill', isActive ? COLORS.flink() : COLORS.soft()).text(name.replace('Runner', ''));
      g.append('text').attr('x', cx + chipW / 2).attr('y', chipY + chipH / 2 + 12)
        .attr('text-anchor', 'middle').attr('font-size', 9)
        .attr('fill', COLORS.soft()).text('Runner');
      if (isActive) {
        g.append('circle').attr('cx', cx + chipW - 12).attr('cy', chipY + 12).attr('r', 4)
          .attr('fill', COLORS.ok());
      }
    });

    svg.append('text').attr('x', W / 2).attr('y', 270).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 11)
      .text('same graph → executes on the highlighted engine (click a chip)');
  }

  const ticker = makeTicker(() => { active = (active + 1) % RUNNERS.length; draw(); }, 1400);
  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); active = (active + 1) % RUNNERS.length; draw(); },
    reset: () => { ticker.stop(); active = 1; draw(); },
  });
  draw();
}
