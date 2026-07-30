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
    // 598 is ours, not Breezeway's: the app's own rate limiter declined to send
    // rather than pile onto an API that was already refusing us. Naming it
    // separately matters — reporting it as "Breezeway rate-limited us" would
    // blame the vendor for our own back-pressure.
    '598':     { label: "held back by this app's rate limiter — not sent to Breezeway",
                 short: 'held back locally', retry: true },
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
    var breakdown = entries.map(function (e) {
      return e[1] + '×' + (e[0] === 'timeout' ? 'timeout' : 'HTTP ' + e[0]);
    }).join(', ');
    return {
      text: '⚠ ' + n + ' ' + noun + " couldn't be loaded from Breezeway (" + breakdown + '). Mostly: ' + info(topCode).label,
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
      : entries.map(function (e) { return e[1] + '×' + (e[0] === 'timeout' ? 'timeout' : e[0]); }).join(', ');
    return { text: text, retry: retry };
  }

  global.bwFailureCause      = bwFailureCause;
  global.bwFailureCauseShort = bwFailureCauseShort;
})(window);
