/* Initialise Mermaid for any <pre class="mermaid"> block. Mermaid is vendored (offline-capable).
   Theme follows the OS colour-scheme so diagrams read well in light and dark. */
(function () {
  'use strict';
  if (typeof mermaid === 'undefined') return;
  var dark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  mermaid.initialize({
    startOnLoad: true,
    theme: dark ? 'dark' : 'neutral',
    securityLevel: 'strict',
    flowchart: { curve: 'basis', htmlLabels: true },
    themeVariables: { fontFamily: 'system-ui, -apple-system, Segoe UI, Roboto, sans-serif' },
  });
})();
