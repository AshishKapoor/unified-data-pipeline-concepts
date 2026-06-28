/* Initialise Mermaid for any <pre class="mermaid"> block, themed to the brand palette.
   Mermaid is vendored (offline-capable). Adapts to the OS colour-scheme. */
(function () {
  'use strict';
  if (typeof mermaid === 'undefined') return;
  var dark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;

  function tok(name, fallback) {
    var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  }
  var beam = tok('--c-beam', '#ff6a1a');
  var accent = tok('--c-accent', '#5b7cfa');
  var surface = dark ? '#161d31' : '#ffffff';
  var line = dark ? '#46567c' : '#b9c4dd';
  var text = dark ? '#e7ecfb' : '#1a2030';

  mermaid.initialize({
    startOnLoad: true,
    theme: 'base',
    securityLevel: 'strict',
    flowchart: { curve: 'basis', htmlLabels: true, padding: 14, nodeSpacing: 44, rankSpacing: 52 },
    themeVariables: {
      fontFamily: 'Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif',
      fontSize: '14px',
      background: 'transparent',
      primaryColor: surface,
      primaryBorderColor: beam,
      primaryTextColor: text,
      secondaryColor: dark ? '#1b2440' : '#eef2fc',
      secondaryBorderColor: accent,
      tertiaryColor: dark ? '#11182b' : '#f6f8fd',
      lineColor: line,
      textColor: text,
      clusterBkg: dark ? 'rgba(91,124,250,.08)' : 'rgba(91,124,250,.06)',
      clusterBorder: accent,
      edgeLabelBackground: surface,
    },
  });
})();
