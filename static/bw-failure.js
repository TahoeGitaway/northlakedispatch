/* ================================================================
   BREEZEWAY LOAD-FAILURE WORDING — shared by every page that reports
   properties it couldn't load (batcher, map import, route discrepancy,
   occupancy check, assignee monitor, owner-clean scan).

   The per-property fetch now reports WHY each house failed, as
   failure_statuses: {"429": 240, "timeout": 2}. Previously every cause
   was reported as "it throttled or errored", which named a cause the
   code had no evidence for — a rejected credential and a rate limit
   look identical to the user but need opposite responses.

   Two formatters, because the surfaces differ:
     bwFailureCause(d)        → full sentence for banners
     bwFailureCauseShort(d)   → parenthetical for one-line subtitles

   Both return {text, retry}. `retry` is false when no failure class is
   transient, so callers can suppress a retry button that cannot work.
================================================================ */
(function (global) {
  "use strict";

  var INFO = {
    '429':     { label: 'Breezeway rate-limited the request (HTTP 429)',           short: 'HTTP 429 — rate limited',   retry: true  },
    '401':     { label: 'Breezeway rejected the credentials (HTTP 401)',           short: 'HTTP 401 — auth rejected',  retry: false },
    '403':     { label: 'Breezeway refused access to these properties (HTTP 403)', short: 'HTTP 403 — access denied',  retry: false },
    '404':     { label: 'Breezeway did not recognise the property id (HTTP 404)',  short: 'HTTP 404 — unknown id',     retry: false },
    '400':     { label: 'Breezeway rejected the query as malformed (HTTP 400)',    short: 'HTTP 400 — bad query',      retry: false },
    'timeout': { label: 'Breezeway did not respond within 15 s',                   short: 'no response (timeout)',     retry: true  },
    // 598 is OURS, not Breezeway's: the app's own limiter declined to send rather
    // than pile onto an API already refusing us. It is not a real status — the
    // number only exists so existing "retry anything >= 500" logic keeps working.
    // Never print it: 598 is informally used elsewhere for "network read timeout",
    // which is close to the opposite of what it means here, so showing it misleads
    // anyone who recognises it.
    // Never sent: the scan's time budget expired before this property's turn. NOT
    // a timeout — a timeout means we asked and got no answer. Conflating the two
    // reported never-sent requests as slow responses, and made the API log (which
    // only records requests actually sent) look wrong when it was right.
    'unreached': { label: "not reached before the scan ran out of time",
                   short: 'not reached (ran out of time)', retry: true, noCode: true },
    '598':     { label: "held back by this app to avoid piling on",
                 short: 'held back by the app', retry: true, noCode: true },
  };

  function info(code) {
    return INFO[code] || {
      label: 'Breezeway returned HTTP ' + code,
      short: 'HTTP ' + code,
      // 5xx is transient and worth retrying; anything else unrecognised is not.
      retry: Number(code) >= 500,
    };
  }

  function entriesFor(d) {
    return Object.entries((d && d.failure_statuses) || {}).sort(function (a, b) { return b[1] - a[1]; });
  }

  /* Full sentence, e.g.
     "⚠ 242 properties couldn't be loaded — Breezeway rate-limited the request (HTTP 429)" */
  function bwFailureCause(d) {
    var n = d && d.failed_properties;
    if (!n) return { text: "", retry: false };

    var noun    = 'propert' + (n === 1 ? 'y' : 'ies');
    var entries = entriesFor(d);
    // A cached payload from before this field existed — stay vague rather than guess.
    if (!entries.length) {
      return { text: '⚠ ' + n + ' ' + noun + " couldn't be loaded from Breezeway", retry: true };
    }

    var topCode = entries[0][0], topN = entries[0][1];
    var retry   = entries.some(function (e) { return info(e[0]).retry; });
    if (entries.length === 1 || topN === n) {
      return { text: '⚠ ' + n + ' ' + noun + " couldn't be loaded — " + info(topCode).label, retry: retry };
    }
    // Describe each cause in words. Printing "3×HTTP 598" invents a status code
    // Breezeway never sent — that number is ours and means we didn't ask at all.
    var breakdown = entries.map(function (e) {
      var i = info(e[0]);
      if (e[0] === 'timeout')   return e[1] + ' timed out';
      if (e[0] === 'unreached') return e[1] + ' not reached in time';
      if (i.noCode)             return e[1] + ' held back by this app';
      // 429 is NOT a refusal — it is a throttle, and the most retryable class in
      // the list. Calling it "refused" alongside 401/403 read as permanent, which
      // is the opposite of what the user should do about it.
      if (e[0] === '429')       return e[1] + ' rate-limited (HTTP 429)';
      return e[1] + ' refused (HTTP ' + e[0] + ')';
    }).join(', ');
    // No trailing "Mostly: <top cause>". The breakdown is sorted largest-first and
    // already names that cause in words, so the clause restated the line's own
    // opening item — "(208 not reached in time, …). Mostly: not reached before the
    // scan ran out of time."
    return {
      text: '⚠ ' + n + ' ' + noun + " couldn't be loaded from Breezeway (" + breakdown + ')',
      retry: retry,
    };
  }

  /* Parenthetical for cramped one-line subtitles, e.g. "(HTTP 429 — rate limited)".
     Returns text:"" when there is no per-status detail, so callers can fall back to
     their existing wording without printing an empty bracket. */
  function bwFailureCauseShort(d) {
    var entries = entriesFor(d);
    if (!(d && d.failed_properties) || !entries.length) return { text: "", retry: true };
    var retry = entries.some(function (e) { return info(e[0]).retry; });
    var topN  = entries[0][1];
    var text  = entries.length === 1 || topN === d.failed_properties
      ? info(entries[0][0]).short
      : entries.map(function (e) {
          var i = info(e[0]);
          if (e[0] === 'timeout')   return e[1] + ' timed out';
          if (e[0] === 'unreached') return e[1] + ' not reached';
          if (i.noCode)             return e[1] + ' held back';
          return e[1] + ' × HTTP ' + e[0];
        }).join(', ');
    return { text: text, retry: retry };
  }

  /* Copy a diagnostics object to the clipboard, with a visible confirmation on the
     button. Falls back to prompt() because the clipboard API needs a secure context
     and can be blocked outright — the text must stay obtainable either way. */
  function bwCopyDiagnostics(obj, btn) {
    var text = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
    var restore = btn ? btn.textContent : null;
    var done = function (msg) {
      if (!btn) return;
      btn.textContent = msg;
      setTimeout(function () { btn.textContent = restore; }, 1800);
    };
    if (global.navigator && navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () { done('Copied ✓'); },
                                              function () { global.prompt('Copy the details below:', text); done('Copied ✓'); });
    } else {
      global.prompt('Copy the details below:', text);
      done('Copied ✓');
    }
  }

  global.bwFailureCause      = bwFailureCause;
  global.bwFailureCauseShort = bwFailureCauseShort;
  global.bwCopyDiagnostics   = bwCopyDiagnostics;
})(window);
