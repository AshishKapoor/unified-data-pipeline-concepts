/* Ch 02 — "the anatomy of an element".
   A PCollection element is never a bare value. It is a 4-layer card:
     value / event-timestamp / window(s) / pane-info.
   Play/step reveals each layer in turn with a short explanation, driving home that even a record
   you `beam.Create` with no timestamp still carries all four fields (MIN_TIMESTAMP + GlobalWindow). */
import { d3, COLORS, createSvg, wireControls, makeTicker } from './_base.js';

// The four layers, top -> bottom, with the example value a bounded `beam.Create` record gets.
const LAYERS = [
  {
    key: 'value',
    title: 'value',
    sample: '{ city: "Bern", qty: 9, score: 36 }',
    note: 'The payload your transforms read & write. Everything else is metadata the runner attaches.',
    color: () => COLORS.beam(),
  },
  {
    key: 'ts',
    title: 'event-timestamp',
    sample: 'MIN_TIMESTAMP  (no explicit ts set)',
    note: 'When the event happened. Create() with no timestamp ⇒ the minimum-timestamp sentinel.',
    color: () => COLORS.accent(),
  },
  {
    key: 'window',
    title: 'window(s)',
    sample: 'GlobalWindow',
    note: 'Which window(s) the element lives in. With no windowing applied, every element is in one GlobalWindow.',
    color: () => COLORS.accent2(),
  },
  {
    key: 'pane',
    title: 'pane-info',
    sample: 'PaneInfo(first=True, last=True, timing=UNKNOWN)',
    note: 'Firing metadata for triggers. In bounded mode there is a single, final pane.',
    color: () => COLORS.late(),
  },
];

export function mount(selectorOrEl) {
  const el = typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
  if (!el) return;

  const W = 460, H = 320;
  const svg = createSvg(el, W, H);

  // `revealed` = how many layers are currently shown (0..4). 0 means just the stub card.
  let revealed = 0;

  const cardX = 26;
  const cardW = W - 52;
  const top = 64;
  const rowH = 50;
  const rowGap = 8;

  function draw() {
    svg.selectAll('*').remove();

    // --- title -----------------------------------------------------------
    svg.append('text').attr('x', W / 2).attr('y', 24).attr('text-anchor', 'middle')
      .attr('fill', COLORS.soft()).attr('font-size', 13).attr('font-weight', 700)
      .text('one PCollection element');
    svg.append('text').attr('x', W / 2).attr('y', 44).attr('text-anchor', 'middle')
      .attr('fill', COLORS.text()).attr('font-size', 12).attr('font-weight', 600)
      .text('value + event-timestamp + window(s) + pane-info');

    // --- the stacked card ------------------------------------------------
    LAYERS.forEach((layer, i) => {
      const y = top + i * (rowH + rowGap);
      const isOn = i < revealed;
      const c = layer.color();

      const g = svg.append('g').style('cursor', 'pointer')
        .on('click', () => { revealed = i + 1; draw(); });

      // layer slab
      g.append('rect').attr('x', cardX).attr('y', y).attr('width', cardW).attr('height', rowH)
        .attr('rx', 9)
        .attr('fill', isOn
          ? 'color-mix(in srgb,' + c + ' 14%, transparent)'
          : COLORS.surface())
        .attr('stroke', isOn ? c : COLORS.border())
        .attr('stroke-width', isOn ? 2.4 : 1.4)
        .attr('stroke-dasharray', isOn ? null : '4 4');

      // accent index dot on the left edge
      g.append('circle').attr('cx', cardX + 16).attr('cy', y + rowH / 2).attr('r', 7)
        .attr('fill', isOn ? c : COLORS.surface2())
        .attr('stroke', isOn ? c : COLORS.border()).attr('stroke-width', 1.5);
      g.append('text').attr('x', cardX + 16).attr('y', y + rowH / 2 + 3.5)
        .attr('text-anchor', 'middle').attr('font-size', 9).attr('font-weight', 800)
        .attr('fill', isOn ? '#fff' : COLORS.soft()).text(i + 1);

      // layer title
      g.append('text').attr('x', cardX + 34).attr('y', y + 20)
        .attr('font-size', 11.5).attr('font-weight', 700)
        .attr('fill', isOn ? c : COLORS.soft()).text(layer.title);

      // sample value (monospace-ish), only once revealed
      g.append('text').attr('x', cardX + 34).attr('y', y + 38)
        .attr('font-size', 10).attr('font-family', 'ui-monospace, Menlo, monospace')
        .attr('fill', isOn ? COLORS.text() : COLORS.border())
        .text(isOn ? layer.sample : '— hidden —');
    });

    // --- explanation strip for the most-recently revealed layer ----------
    const stripY = top + LAYERS.length * (rowH + rowGap) + 6;
    if (revealed > 0) {
      const cur = LAYERS[revealed - 1];
      svg.append('rect').attr('x', cardX).attr('y', stripY).attr('width', cardW).attr('height', 30)
        .attr('rx', 7).attr('fill', COLORS.surface2()).attr('stroke', COLORS.border());
      svg.append('text').attr('x', cardX + 10).attr('y', stripY + 19)
        .attr('font-size', 9.5).attr('fill', COLORS.soft())
        .text(clip(cur.note, 74));
    } else {
      svg.append('text').attr('x', W / 2).attr('y', stripY + 19).attr('text-anchor', 'middle')
        .attr('font-size', 10.5).attr('fill', COLORS.soft())
        .text('press Step / Play to peel back each layer (or click a slab)');
    }
  }

  // Truncate long notes so they never overflow the SVG width.
  function clip(s, n) { return s.length > n ? s.slice(0, n - 1) + '…' : s; }

  const ticker = makeTicker(() => {
    revealed = revealed >= LAYERS.length ? 0 : revealed + 1;
    draw();
  }, 1400);

  wireControls(el, {
    play: () => (ticker.running ? ticker.stop() : ticker.start()),
    step: () => { ticker.stop(); revealed = revealed >= LAYERS.length ? 0 : revealed + 1; draw(); },
    reset: () => { ticker.stop(); revealed = 0; draw(); },
  });

  draw();
}
