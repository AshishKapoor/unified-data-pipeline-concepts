/* ============================================================================
   site.js — shared chapter behaviour
   - "Run on Flink" button -> POST /api/pipelines/:concept/run
   - live SSE log viewer    -> EventSource('/api/runs/:id/events')
   - status badge, Flink deep-link, cancel
   The API base is same-origin (the NestJS app serves these docs).
   ========================================================================== */
(function () {
  'use strict';

  var FLINK_UI = 'http://localhost:8081';

  function $(sel, root) { return (root || document).querySelector(sel); }

  function setStatus(badge, status) {
    if (!badge) return;
    badge.textContent = status;
    badge.setAttribute('data-status', status);
  }

  function appendLog(logEl, text, cls) {
    if (!logEl) return;
    var atBottom = logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 24;
    var line = document.createElement('div');
    if (cls) line.className = cls;
    line.textContent = text;
    logEl.appendChild(line);
    if (atBottom) logEl.scrollTop = logEl.scrollHeight;
  }

  function wireChapterRunner() {
    var runBtn = $('#run-btn');
    if (!runBtn) return; // index page has no runner
    var concept = runBtn.getAttribute('data-concept');
    var badge = $('#run-status');
    var logEl = $('#run-log');
    var flinkLink = $('#flink-link');
    var cancelBtn = $('#cancel-btn');
    var current = { runId: null, source: null };

    function closeSource() {
      if (current.source) { current.source.close(); current.source = null; }
    }

    function onEvent(payload) {
      if (payload.type === 'log') {
        appendLog(logEl, payload.line || '', payload.stream === 'stderr' ? 'err' : null);
      } else if (payload.type === 'status') {
        setStatus(badge, payload.status);
      } else if (payload.type === 'flink') {
        if (flinkLink) {
          flinkLink.href = FLINK_UI + '/#/job/running/' + payload.flinkJobId;
          flinkLink.style.display = 'inline';
        }
        appendLog(logEl, '↳ linked Flink job ' + payload.flinkJobId, 'meta');
      } else if (payload.type === 'end') {
        setStatus(badge, payload.status);
        appendLog(logEl, '■ run ' + payload.status.toLowerCase(), 'meta');
        runBtn.disabled = false;
        if (cancelBtn) cancelBtn.disabled = true;
        closeSource();
      }
    }

    function openStream(runId) {
      var src = new EventSource('/api/runs/' + runId + '/events');
      current.source = src;
      ['log', 'status', 'flink', 'end'].forEach(function (name) {
        src.addEventListener(name, function (e) {
          try { onEvent(JSON.parse(e.data)); } catch (_) { /* ignore */ }
        });
      });
      src.onerror = function () {
        // Stream closes normally when the run ends; only note unexpected drops.
        if (current.runId === runId && runBtn.disabled) {
          appendLog(logEl, '… stream closed', 'meta');
        }
      };
    }

    runBtn.addEventListener('click', function () {
      runBtn.disabled = true;
      if (logEl) logEl.innerHTML = '';
      setStatus(badge, 'PENDING');
      if (flinkLink) flinkLink.style.display = 'none';
      appendLog(logEl, '▶ submitting ' + concept + ' to Flink…', 'meta');

      fetch('/api/pipelines/' + concept + '/run', { method: 'POST' })
        .then(function (r) {
          if (!r.ok) throw new Error('HTTP ' + r.status);
          return r.json();
        })
        .then(function (run) {
          current.runId = run.runId;
          appendLog(logEl, '✓ runId ' + run.runId + ' (job_name ' + run.jobName + ')', 'meta');
          if (cancelBtn) cancelBtn.disabled = false;
          openStream(run.runId);
        })
        .catch(function (err) {
          appendLog(logEl, '✗ could not start run: ' + err.message +
            '  — is the stack up? (./scripts/up.sh)', 'err');
          setStatus(badge, 'FAILED');
          runBtn.disabled = false;
        });
    });

    if (cancelBtn) {
      cancelBtn.disabled = true;
      cancelBtn.addEventListener('click', function () {
        if (!current.runId) return;
        cancelBtn.disabled = true;
        fetch('/api/runs/' + current.runId + '/cancel', { method: 'POST' })
          .then(function () { appendLog(logEl, '⏹ cancel requested', 'meta'); })
          .catch(function () { /* ignore */ });
      });
    }
  }

  document.addEventListener('DOMContentLoaded', wireChapterRunner);
})();
