/* Ch 15 — Cross-language (xlang) KafkaIO.
   Two acts, stepped through by play/step:
     Act A (construction): the Python SDK calls the Expansion Service (:8097), which returns an
       expanded JAVA transform that Python splices into the pipeline graph.
     Act B (runtime): a Java harness and a Python harness run side by side. Records flow from the
       'clicks-in' topic through the Java KafkaIO read, across the Fn API to the Python combine,
       back to the Java KafkaIO write, and out to 'counts-out'.
   The point: you write Python, but the Kafka edges execute as Java. */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// Construction handshake steps (Act A) then runtime packet positions (Act B).
// phase 0..2 = construction handshake, phase 3..6 = runtime flow.
const PHASES = 7;

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);
  let phase = 0;

  // ---- node geometry (shared by both acts) --------------------------------
  function box(x, y, w, h, fill, stroke, sw) {
    return svg.append('rect').attr('x', x).attr('y', y).attr('width', w).attr('height', h)
      .attr('rx', 9).attr('fill', fill).attr('stroke', stroke).attr('stroke-width', sw || 1.5);
  }
  function label(x, y, txt, color, size, weight) {
    return svg.append('text').attr('x', x).attr('y', y).attr('text-anchor', 'middle')
      .attr('fill', color).attr('font-size', size || 11).attr('font-weight', weight || 600).text(txt);
  }
  function softFill(c, pct) { return `color-mix(in srgb, ${c} ${pct}%, transparent)`; }

  // ===========================================================================
  // ACT A — construction handshake (phases 0..2)
  // ===========================================================================
  function drawConstruction() {
    label(W / 2, 22, 'Construction time — pipeline being built', COLORS.soft(), 13, 700);

    // Python SDK (left) and Expansion Service (right)
    const pyX = 40, pyY = 70, esX = 280, esY = 70, bw = 140, bh = 58;
    box(pyX, pyY, bw, bh, softFill(COLORS.accent(), 12), COLORS.accent(), 2);
    label(pyX + bw / 2, pyY + 24, 'Python SDK', COLORS.accent(), 12, 700);
    label(pyX + bw / 2, pyY + 42, 'ReadFromKafka(...)', COLORS.soft(), 10, 500);

    box(esX, esY, bw, bh, softFill(COLORS.flink(), 12), COLORS.flink(), 2);
    label(esX + bw / 2, esY + 22, 'Expansion Service', COLORS.flink(), 12, 700);
    label(esX + bw / 2, esY + 40, ':8097 (Java)', COLORS.soft(), 10, 500);

    // three handshake arrows, highlighting up to the current phase
    const steps = [
      { y: 150, from: pyX + bw, to: esX, text: '1. expansion request (URN + params)', dir: 1 },
      { y: 188, from: esX, to: pyX + bw, text: '2. expanded JAVA transform (proto)', dir: -1 },
      { y: 226, from: pyX + bw, to: esX, text: '3. splice Java sub-graph into pipeline', dir: 1 },
    ];
    steps.forEach((s, i) => {
      const on = i <= phase;
      const c = on ? (s.dir > 0 ? COLORS.accent() : COLORS.flink()) : COLORS.border();
      const x1 = s.dir > 0 ? s.from + 4 : s.from - 4;
      const x2 = s.dir > 0 ? s.to - 6 : s.to + 6;
      const ax = s.dir > 0 ? x2 : x2;
      svg.append('line').attr('x1', x1).attr('y1', s.y).attr('x2', x2).attr('y2', s.y)
        .attr('stroke', c).attr('stroke-width', on ? 2.5 : 1.5)
        .attr('stroke-dasharray', on ? null : '4 4');
      // arrow head
      const dx = s.dir > 0 ? -7 : 7;
      svg.append('path')
        .attr('d', `M ${ax} ${s.y} l ${dx} -4 l 0 8 z`)
        .attr('fill', c);
      label(W / 2, s.y - 8, s.text, on ? COLORS.text() : COLORS.soft(), 10, on ? 700 : 500);
      // moving dot on the active step
      if (i === phase) {
        const t = 0.5;
        const px = x1 + (x2 - x1) * t;
        svg.append('circle').attr('cx', px).attr('cy', s.y).attr('r', 5).attr('fill', c)
          .append('animate').attr('attributeName', 'cx')
          .attr('from', x1).attr('to', x2).attr('dur', '1s').attr('repeatCount', 'indefinite');
      }
    });

    label(W / 2, 285, 'Python uses the Java connector without writing Java.',
      COLORS.soft(), 11, 500);
    label(W / 2, 302, 'press ▶ / Step to reach runtime →', COLORS.soft(), 10, 500);
  }

  // ===========================================================================
  // ACT B — runtime: Java + Python harnesses over Kafka (phases 3..6)
  // ===========================================================================
  function drawRuntime() {
    label(W / 2, 20, 'Runtime — two harnesses run side by side', COLORS.soft(), 13, 700);

    // Kafka topics (far left in, far right out)
    const inX = 16, outX = W - 16 - 70, topY = 70, tw = 70, th = 44;
    box(inX, topY, tw, th, COLORS.surface2(), COLORS.border(), 1.5);
    label(inX + tw / 2, topY + 19, 'clicks-in', COLORS.text(), 10, 700);
    label(inX + tw / 2, topY + 34, 'topic', COLORS.soft(), 9, 500);

    box(outX, topY, tw, th, COLORS.surface2(), COLORS.border(), 1.5);
    label(outX + tw / 2, topY + 19, 'counts-out', COLORS.text(), 10, 700);
    label(outX + tw / 2, topY + 34, 'topic', COLORS.soft(), 9, 500);

    // Java harness (read) — left-center
    const jrX = 110, jY = 56, jw = 96, jh = 72;
    box(jrX, jY, jw, jh, softFill(COLORS.flink(), 12), COLORS.flink(), 2);
    label(jrX + jw / 2, jY + 22, 'Java harness', COLORS.flink(), 10.5, 700);
    label(jrX + jw / 2, jY + 40, 'KafkaIO', COLORS.soft(), 10, 600);
    label(jrX + jw / 2, jY + 56, 'read', COLORS.soft(), 9, 500);

    // Python harness (combine) — center
    const pX = 248, pw = 100;
    box(pX, jY, pw, jh, softFill(COLORS.accent(), 12), COLORS.accent(), 2);
    label(pX + pw / 2, jY + 22, 'Python harness', COLORS.accent(), 10.5, 700);
    label(pX + pw / 2, jY + 40, 'window +', COLORS.soft(), 10, 600);
    label(pX + pw / 2, jY + 56, 'CombinePerKey', COLORS.soft(), 9, 500);

    // Java harness (write) — right of python, small, sits under counts-out
    const jwX = outX - 6, jwY = 150, jww = 84, jwh = 50;
    box(jwX, jwY, jww, jwh, softFill(COLORS.flink(), 12), COLORS.flink(), 2);
    label(jwX + jww / 2, jwY + 20, 'Java harness', COLORS.flink(), 9.5, 700);
    label(jwX + jww / 2, jwY + 36, 'KafkaIO write', COLORS.soft(), 9, 500);

    // edges
    // clicks-in -> java read
    edge(inX + tw, topY + th / 2, jrX, jY + jh / 2, COLORS.border());
    // java read -> python (Fn API data plane)
    edge(jrX + jw, jY + jh / 2, pX, jY + jh / 2, COLORS.soft());
    label((jrX + jw + pX) / 2, jY - 4, 'Fn API', COLORS.soft(), 9, 600);
    // python -> java write (down-right)
    edge(pX + pw, jY + jh / 2, jwX + jww / 2, jwY, COLORS.soft());
    // java write -> counts-out
    edge(jwX + jww / 2, jwY, outX + tw / 2, topY + th, COLORS.border());

    // Animated record packet whose position depends on phase (3..6)
    const stops = [
      { x: inX + tw + 8, y: topY + th / 2, c: COLORS.accent2(), txt: 'click' },                  // p3: leaving topic
      { x: jrX + jw / 2, y: jY + jh / 2, c: COLORS.flink(), txt: '(k,v) bytes' },                // p4: in java read
      { x: pX + pw / 2, y: jY + jh / 2, c: COLORS.accent(), txt: '(user,1)' },                   // p5: in python combine
      { x: jwX + jww / 2, y: jwY + jwh / 2, c: COLORS.flink(), txt: '(user,n)' },                // p6: in java write
    ];
    const idx = Math.min(phase - 3, stops.length - 1);
    for (let i = 0; i <= idx; i++) {
      const s = stops[i];
      const isHead = i === idx;
      svg.append('circle').attr('cx', s.x).attr('cy', s.y).attr('r', isHead ? 8 : 5)
        .attr('fill', s.c).attr('opacity', isHead ? 1 : 0.35);
      if (isHead) {
        svg.append('circle').attr('cx', s.x).attr('cy', s.y).attr('r', 8).attr('fill', 'none')
          .attr('stroke', s.c).attr('stroke-width', 2)
          .append('animate').attr('attributeName', 'r').attr('from', 8).attr('to', 16)
          .attr('dur', '1s').attr('repeatCount', 'indefinite');
        label(s.x, s.y - 14, s.txt, COLORS.text(), 10, 700);
      }
    }

    // legend
    legendDot(40, 296, COLORS.flink(), 'Java harness (KafkaIO)');
    legendDot(240, 296, COLORS.accent(), 'Python harness (your code)');
  }

  function edge(x1, y1, x2, y2, c) {
    svg.append('line').attr('x1', x1).attr('y1', y1).attr('x2', x2).attr('y2', y2)
      .attr('stroke', c).attr('stroke-width', 2);
  }
  function legendDot(x, y, c, txt) {
    svg.append('circle').attr('cx', x).attr('cy', y).attr('r', 5).attr('fill', c);
    svg.append('text').attr('x', x + 10).attr('y', y + 4).attr('text-anchor', 'start')
      .attr('fill', COLORS.soft()).attr('font-size', 10).attr('font-weight', 600).text(txt);
  }

  function draw() {
    svg.selectAll('*').remove();
    if (phase <= 2) drawConstruction();
    else drawRuntime();
  }

  const ticker = makeTicker(() => { phase = (phase + 1) % PHASES; draw(); }, 1400);
  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); phase = (phase + 1) % PHASES; draw(); },
    reset: () => { ticker.stop(); phase = 0; draw(); },
  });
  draw();
}
