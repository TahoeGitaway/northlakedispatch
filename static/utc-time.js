/* ================================================================
   UTC TIMESTAMP RENDERING — shared by every page that shows a
   backend-written timestamp.

   The problem this exists to prevent
   ----------------------------------
   The backend writes timestamps as `datetime.utcnow().isoformat()`,
   which produces "2026-08-01T20:48:33.123456" — genuinely UTC, but
   with NO "Z" and no offset. Per ES2015, a date-time string with no
   offset is parsed as LOCAL time, so `new Date(that)` reads 20:48 as
   8:48 PM Pacific rather than 1:48 PM. Passing
   `timeZone: "America/Los_Angeles"` to toLocaleString does NOT save
   you: by then the Date already holds the wrong instant, so the
   conversion is a no-op and the label says "PT" over a UTC clock
   reading. That shipped as a visible 7-hour error on the routes page.

   Breezeway's own timestamps have the same shape with a space instead
   of a "T" ("2026-08-01 20:48:33"), which is worse — space-separated
   date-times aren't in the spec at all, so engines differ.

   Sources that DO carry an offset (Asana's created_at, anything
   written client-side via toISOString()) must be left alone, or
   tagging them again would shift them a second time. Hence the regex
   test rather than an unconditional += "Z".

   Use utcDate()/fmtUtc() for anything that came from the server.
   Do not call `new Date()` on a backend timestamp directly.
================================================================ */
(function (global) {
  "use strict";

  var HAS_ZONE = /[zZ]|[+\-]\d\d:?\d\d$/;

  /* Parse a backend timestamp into a correct Date.
     Returns null for empty/unparseable input so callers can render a
     dash instead of "Invalid Date". */
  function utcDate(value) {
    if (!value) return null;
    var s = String(value).trim();
    if (!s) return null;
    // Breezeway sends "2026-08-01 20:48:33"; normalise to ISO first.
    if (s.indexOf("T") === -1) s = s.replace(" ", "T");
    if (!HAS_ZONE.test(s)) s += "Z";
    var d = new Date(s);
    return isNaN(d.getTime()) ? null : d;
  }

  /* Format a backend timestamp for display. Defaults to Pacific,
     because this is a Tahoe dispatch app and the times on screen are
     always local operating times — never the viewer's device zone,
     which would silently differ for anyone travelling. */
  function fmtUtc(value, opts) {
    var d = utcDate(value);
    if (!d) return "";
    var o = Object.assign(
      { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" },
      opts || {}
    );
    if (!o.timeZone) o.timeZone = "America/Los_Angeles";
    return d.toLocaleString("en-US", o);
  }

  global.utcDate = utcDate;
  global.fmtUtc  = fmtUtc;
})(window);
