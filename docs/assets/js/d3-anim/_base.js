/* ============================================================================
   _base.js — shared helpers for the chapter D3 animations.
   Every chapter animation module exports `mount(selector, config)` and uses these
   utilities so look-and-feel + play/step/reset wiring stay consistent.
   D3 is loaded globally (vendored) as `window.d3`.
   ========================================================================== */

export const d3 = window.d3;

/** Read a CSS custom property (design token) so animations match the theme. */
export function token(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

export const COLORS = {
  beam: () => token('--c-beam', '#f26d21'),
  flink: () => token('--c-flink', '#e6526f'),
  accent: () => token('--c-accent', '#4f7cff'),
  accent2: () => token('--c-accent-2', '#16a08a'),
  idle: () => token('--state-idle', '#3b82f6'),
  busy: () => token('--state-busy', '#ef4444'),
  back: () => token('--state-back', '#111827'),
  ok: () => token('--state-ok', '#16a34a'),
  late: () => token('--state-late', '#f59e0b'),
  text: () => token('--text', '#1d2433'),
  soft: () => token('--text-soft', '#5a6577'),
  border: () => token('--border', '#d8deea'),
  surface: () => token('--surface', '#fff'),
  surface2: () => token('--surface-2', '#eef1f7'),
};

/** Create a responsive SVG (viewBox-scaled) inside `el`. Returns the d3 <svg> selection. */
export function createSvg(el, width, height) {
  d3.select(el).selectAll('*').remove();
  return d3
    .select(el)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .attr('role', 'img');
}

/**
 * Wire the standard play/step/reset control bar that sits next to the diagram.
 * `handlers` = { play, step, reset }. Looks for `.anim-controls` within the diagram card.
 */
export function wireControls(el, handlers) {
  const card = el.closest('.diagram-card') || el.parentElement || document;
  const bar = card.querySelector('.anim-controls');
  if (!bar) return;
  bar.querySelectorAll('button[data-act]').forEach((btn) => {
    const act = btn.getAttribute('data-act');
    btn.addEventListener('click', () => {
      if (handlers[act]) handlers[act]();
    });
  });
}

/** A tiny play-loop driver: calls `step()` every `interval` ms until it returns false. */
export function makeTicker(step, interval) {
  let timer = null;
  return {
    start() {
      if (timer) return;
      timer = setInterval(() => {
        if (step() === false) this.stop();
      }, interval);
    },
    stop() {
      if (timer) { clearInterval(timer); timer = null; }
    },
    get running() { return timer != null; },
  };
}

/** Resolve the mount element from a selector-or-element. */
export function resolve(selectorOrEl) {
  return typeof selectorOrEl === 'string' ? document.querySelector(selectorOrEl) : selectorOrEl;
}

/** Linear-ish easing helper for manual interpolation when not using transitions. */
export function lerp(a, b, t) { return a + (b - a) * t; }
