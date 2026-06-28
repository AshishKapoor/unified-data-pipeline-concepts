/* Ch 07 — "routing data": one stream branches at a validator.
   A side-input "broadcast cloud" feeds a validator node. Records flow in from the left; the validator
   checks each one against the broadcast table and routes it: valid records (green/orange) continue to
   the MAIN sink, malformed ones (red) are diverted down to the DEAD-LETTER sink. Play streams a fixed,
   deterministic sequence of records so the dead-letter pattern is visible, not random. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// A deterministic batch of records: `bad` ones get diverted to the dead-letter sink.
const RECORDS = [
  { id: 'o1', bad: false },
  { id: 'o2', bad: false },
  { id: 'o4', bad: true },   // unknown region
  { id: 'o3', bad: false },
  { id: 'o5', bad: true },   // negative amount
  { id: 'o6', bad: false },
  { id: 'o7', bad: true },   // missing field
  { id: 'o8', bad: false },
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 300;
  const svg = createSvg(el, W, H);

  // Layout anchors.
  const inX = 40;            // where records enter
  const valX = 200, valY = 150, valR = 34;   // validator node
  const mainX = 420, mainY = 96;             // main sink
  const deadX = 420, deadY = 232;            // dead-letter sink
  const cloudX = 200, cloudY = 48;           // broadcast side-input cloud

  let i = 0;          // index of the record currently being routed
  let routed = [];    // [{id, bad}] history, to show counts at the sinks

  function staticScene() {
    svg.selectAll('*').remove();

    // --- broadcast side-input cloud (top) ---
    const cloud = svg.append('g');
    cloud.append('ellipse').attr('cx', cloudX).attr('cy', cloudY).attr('rx', 66).attr('ry', 22)
      .attr('fill', 'color-mix(in srgb,' + COLORS.accent() + ' 14%, transparent)')
      .attr('stroke', COLORS.accent()).attr('stroke-width', 2);
    cloud.append('text').attr('x', cloudX).attr('y', cloudY - 2).attr('text-anchor', 'middle')
      .attr('fill', COLORS.accent()).attr('font-size', 11).attr('font-weight', 700)
      .text('side input');
    cloud.append('text').attr('x', cloudX).attr('y', cloudY + 11).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 9).text('AsDict(regions) — broadcast');

    // dashed broadcast feed from cloud down to the validator
    svg.append('line').attr('x1', cloudX).attr('y1', cloudY + 22).attr('x2', valX).attr('y2', valY - valR)
      .attr('stroke', COLORS.accent()).attr('stroke-width', 1.6).attr('stroke-dasharray', '4 4')
      .attr('opacity', 0.8);

    // --- input rail (left -> validator) ---
    svg.append('line').attr('x1', inX).attr('y1', valY).attr('x2', valX - valR).attr('y2', valY)
      .attr('stroke', COLORS.border()).attr('stroke-width', 2);
    svg.append('text').attr('x', inX).attr('y', valY - 12).attr('text-anchor', 'start')
      .attr('fill', COLORS.soft()).attr('font-size', 10).text('records');

    // --- validator node ---
    svg.append('circle').attr('cx', valX).attr('cy', valY).attr('r', valR)
      .attr('fill', COLORS.surface2()).attr('stroke', COLORS.beam()).attr('stroke-width', 2.5);
    svg.append('text').attr('x', valX).attr('y', valY - 3).attr('text-anchor', 'middle')
      .attr('fill', COLORS.text()).attr('font-size', 11).attr('font-weight', 700).text('Validate');
    svg.append('text').attr('x', valX).attr('y', valY + 11).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 9).text('ParDo');

    // --- branch paths: validator -> main (up), validator -> dead-letter (down) ---
    svg.append('path')
      .attr('d', `M ${valX + valR} ${valY} C ${valX + 80} ${valY}, ${mainX - 90} ${mainY}, ${mainX - 46} ${mainY}`)
      .attr('fill', 'none').attr('stroke', COLORS.ok()).attr('stroke-width', 2);
    svg.append('path')
      .attr('d', `M ${valX + valR} ${valY} C ${valX + 80} ${valY}, ${deadX - 90} ${deadY}, ${deadX - 46} ${deadY}`)
      .attr('fill', 'none').attr('stroke', COLORS.busy()).attr('stroke-width', 2);

    // --- sinks ---
    drawSink(mainX, mainY, 'main', COLORS.ok(), routed.filter((r) => !r.bad).length);
    drawSink(deadX, deadY, "dead_letter", COLORS.busy(), routed.filter((r) => r.bad).length);

    // tags on the branches
    svg.append('text').attr('x', valX + 96).attr('y', mainY - 8).attr('text-anchor', 'middle')
      .attr('fill', COLORS.ok()).attr('font-size', 9).attr('font-weight', 700).text("main='valid'");
    svg.append('text').attr('x', valX + 96).attr('y', deadY + 18).attr('text-anchor', 'middle')
      .attr('fill', COLORS.busy()).attr('font-size', 9).attr('font-weight', 700).text("tag='dead_letter'");

    // caption
    svg.append('text').attr('x', W / 2).attr('y', H - 6).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 10)
      .text('valid → main · malformed → dead-letter (with_outputs)');
  }

  function drawSink(x, y, label, color, count) {
    const g = svg.append('g');
    g.append('rect').attr('x', x - 46).attr('y', y - 20).attr('width', 56).attr('height', 40)
      .attr('rx', 8).attr('fill', 'color-mix(in srgb,' + color + ' 12%, transparent)')
      .attr('stroke', color).attr('stroke-width', 2);
    g.append('text').attr('x', x - 18).attr('y', y - 4).attr('text-anchor', 'middle')
      .attr('fill', color).attr('font-size', 9).attr('font-weight', 700).text(label);
    g.append('text').attr('x', x - 18).attr('y', y + 12).attr('text-anchor', 'middle')
      .attr('fill', COLORS.text()).attr('font-size', 12).attr('font-weight', 800).text(String(count));
  }

  // Animate a single record: travel the input rail, pulse the validator, then branch to a sink.
  function routeNext() {
    if (i >= RECORDS.length) return false;     // done -> ticker stops
    const rec = RECORDS[i];
    i += 1;

    staticScene();

    const fill = rec.bad ? COLORS.busy() : COLORS.beam();
    const dot = svg.append('g');
    dot.append('circle').attr('r', 9).attr('fill', fill)
      .attr('stroke', COLORS.surface()).attr('stroke-width', 1.5);
    dot.append('text').attr('y', 3).attr('text-anchor', 'middle').attr('font-size', 8)
      .attr('font-weight', 700).attr('fill', COLORS.surface()).text(rec.id);
    dot.attr('transform', `translate(${inX},${valY})`);

    // phase 1: glide along the input rail to the validator
    dot.transition().duration(420).ease(d3.easeLinear)
      .attr('transform', `translate(${valX},${valY})`)
      .on('end', () => {
        // pulse the validator ring to show the broadcast-table check happening
        svg.append('circle').attr('cx', valX).attr('cy', valY).attr('r', valR)
          .attr('fill', 'none').attr('stroke', fill).attr('stroke-width', 3)
          .transition().duration(360).attr('r', valR + 12).attr('stroke-opacity', 0).remove();

        // phase 2: branch to the correct sink
        const tx = rec.bad ? deadX - 18 : mainX - 18;
        const ty = rec.bad ? deadY : mainY;
        dot.transition().delay(120).duration(480).ease(d3.easeCubicInOut)
          .attr('transform', `translate(${tx},${ty})`)
          .on('end', () => {
            routed.push(rec);
            dot.transition().duration(180).attr('opacity', 0).remove();
            staticScene();    // refresh the sink counters
          });
      });
    return true;
  }

  const ticker = makeTicker(() => routeNext(), 1100);

  wireControls(el, {
    play: () => {
      if (ticker.running) { ticker.stop(); return; }
      if (i >= RECORDS.length) { i = 0; routed = []; }   // replay from the start
      ticker.start();
    },
    step: () => { ticker.stop(); routeNext(); },
    reset: () => { ticker.stop(); i = 0; routed = []; staticScene(); },
  });

  staticScene();
}
