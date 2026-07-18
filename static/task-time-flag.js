/* ============================================================
   TIME-SENSITIVE TASK FLAG — shared matcher (single source of truth)

   Decides whether a task TITLE should be flagged purple because it calls out a
   date / time / timing-sensitive keyword. Used by BOTH the Group Assign page
   and the map sidebar task list so the rule is identical everywhere — change
   the patterns here and every surface updates together.

   A title is flagged when it contains ANY of:
     • a date            7/14, 07/14/26, 11-3, "July 14", "Jul 14", "the 3rd", "14th"
     • an explicit time  11:00, 11AM, 11 am, 3 p.m., 11:00pm
     • a written time     noon, midday, midnight, o'clock, "half past",
                          "quarter to", "eleven AM/o'clock",
                          morning/afternoon/evening/tonight,
                          "first thing", "end of day", EOD, COB, ASAP
     • the word "Issue"  (issue / issues)
     • "HO Request"       (HO request, H.O. request, H/O request, homeowner request)

   Known, accepted trade-offs (broad by design — she asked to catch everything):
     • "1/2 bath" reads as a date (1/2) and will flag. "1st/2nd/3rd floor" is
       excluded, but other ordinals ("14th") flag as a day-of-month.
   ============================================================ */
(function () {
  const MONTHS  = "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?";
  const NUMWORD = "one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve";

  const PATTERNS = [
    // ── Explicit clock time ──
    /\b\d{1,2}:\d{2}\b/,                                   // 11:00, 9:30
    /\b\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)\b/i,     // 11AM, 11 am, 3 p.m., 11:00pm

    // ── Dates ──
    /\b\d{1,2}[\/\-]\d{1,2}(?:[\/\-]\d{2,4})?\b/,          // 7/14, 07/14/26, 11-3
    new RegExp("\\b(?:" + MONTHS + ")\\.?\\s+\\d{1,2}\\b", "i"),  // "July 14", "Jul 14", "dec 3"
    /\b\d{1,2}(?:st|nd|rd|th)\b(?!\s*(?:floor|fl\b))/i,    // "the 3rd", "14th" — but not "1st floor"

    // ── Written-out time ──
    new RegExp("\\b(?:" + NUMWORD + ")\\s*(?:o'?clock|a\\.?m\\.?|p\\.?m\\.?)\\b", "i"), // "eleven AM", "two o'clock"
    /\bnoon\b/i,
    /\bmidday\b/i,
    /\bmidnight\b/i,
    /\bo'?clock\b/i,
    /\bhalf past\b/i,
    /\bquarter\s+(?:past|to|of|till|'?til)\b/i,

    // ── Parts of day / common human time refs ──
    /\bmorning\b/i,
    /\bafternoon\b/i,
    /\bevening\b/i,
    /\btonight\b/i,
    /\bfirst thing\b/i,
    /\bend of day\b/i,
    /\beod\b/i,
    /\bcob\b/i,
    /\basap\b/i,

    // ── Keywords ──
    /\bissues?\b/i,                                        // Issue / issues
    /\bh\.?\/?o\.?\s*request\b/i,                          // HO Request, H.O./H/O request
    /\bhomeowner\s+request\b/i,
  ];

  // Routine task TYPES that always carry a date ("... for 7/16") by habit, so the
  // date must NOT flag them purple. Matched by their fixed name phrase regardless
  // of the date. If a title matches any of these, it's never time-flagged.
  const EXCLUSIONS = [
    /\bwalk[\s-]*thru\b/i,
    /\bwalk[\s-]*through\b/i,
    /\bpost\s+rental\s+inspection\b/i,
    /\barrival\s+hot\s+tub\s+service\b/i,
  ];

  function isTimeSensitiveTitle(title) {
    if (!title || typeof title !== "string") return false;
    if (EXCLUSIONS.some(re => re.test(title))) return false;
    return PATTERNS.some(re => re.test(title));
  }

  window.NLD = window.NLD || {};
  window.NLD.TIME_FLAG_COLOR    = "#7c3aed";              // violet-600 — same purple as the PCI badge
  window.NLD.isTimeSensitiveTitle = isTimeSensitiveTitle;

  // Apply the purple "left bar" to a task row without shifting its layout.
  // Uses an inset box-shadow (not a border) so nothing reflows. Callers style
  // the title text purple themselves per their own conventions.
  window.NLD.markTimeFlagRow = function (rowEl, padLeftPx) {
    if (!rowEl) return;
    rowEl.style.boxShadow = "inset 3px 0 0 " + window.NLD.TIME_FLAG_COLOR;
    if (padLeftPx != null) rowEl.style.paddingLeft = padLeftPx + "px";
  };

  /* ── VIP badge ──
     Any task whose TITLE contains "VIP" gets a gold ⭐ VIP badge. Two builders
     so both string-HTML surfaces (Group Assign) and DOM surfaces (map sidebar)
     stay identical: vipBadgeHtml() returns markup, makeVipBadge() a node. */
  window.NLD.isVipTitle = function (title) {
    return !!title && typeof title === "string" && /\bvips?\b/i.test(title);
  };

  // One gold style, shared by both builders so the badge looks the same everywhere.
  const VIP_STYLE =
    "display:inline-block;font-size:0.62rem;font-weight:700;padding:1px 6px;" +
    "border-radius:99px;background:#facc15;color:#713f12;border:1px solid #eab308;";

  window.NLD.vipBadgeHtml = function () {
    return '<span data-vip-badge="1" title="VIP task" style="' + VIP_STYLE + '">⭐ VIP</span>';
  };

  // House-level banner. Same gold family as the task pill, but sized/weighted to
  // match the PCI banner on the schedule card so a VIP house reads at the same
  // glance-distance as a by-noon check-in.
  window.NLD.vipBannerHtml = function () {
    return '<span data-vip-banner="1" title="VIP house — a task here is flagged VIP" ' +
      'style="display:inline-block;background:#facc15;color:#713f12;font-weight:800;' +
      'font-size:0.72rem;padding:3px 10px;border-radius:6px;letter-spacing:0.03em;' +
      'box-shadow:0 0 0 2px #fef08a;">⭐ VIP</span>';
  };

  window.NLD.makeVipBadge = function () {
    const b = document.createElement("span");
    b.dataset.vipBadge = "1";
    b.title = "VIP task";
    b.style.cssText = VIP_STYLE;
    b.textContent = "⭐ VIP";
    return b;
  };

  /* ── Per-task flag DISMISSAL (shared across all users, persisted server-side) ──
     A task_id in this set means "hide BOTH the purple flag and the VIP badge for
     that task." Loaded once from the server; the ✕ control POSTs a dismissal and
     re-renders. Restore is server-side only for now (no un-dismiss UI yet). */
  window.NLD.dismissedFlags = new Set();

  window.NLD.isFlagDismissed = function (taskId) {
    if (taskId == null || taskId === "") return false;   // unkeyed tasks can't be dismissed
    return window.NLD.dismissedFlags.has(String(taskId));
  };

  window.NLD.loadDismissedFlags = function () {
    return fetch("/task-flags/dismissed", { credentials: "same-origin" })
      .then(r => (r.ok ? r.json() : { ids: [] }))
      .then(d => {
        window.NLD.dismissedFlags = new Set((d.ids || []).map(String));
        // Re-render whichever surface is on the page now that we know the dismissals,
        // so a render that ran before this fetch resolved drops any stale flags.
        // typeof-guard: these page globals don't exist on every page.
        try { if (typeof _syncSidebarToSchedule === "function") _syncSidebarToSchedule(); } catch (e) {}
        try { if (typeof renderScan === "function") renderScan(); } catch (e) {}
        // Schedule cards carry the house-level VIP banner, which is derived from these
        // dismissals too — re-render them or a dismissed VIP house keeps its banner.
        try { if (typeof isOptimized !== "undefined" && isOptimized && typeof renderSchedule === "function") renderSchedule(); } catch (e) {}
      })
      .catch(() => {});
  };

  window.NLD.dismissFlag = function (taskId) {
    if (taskId == null || taskId === "") return Promise.resolve(false);
    return fetch("/task-flags/dismiss", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ task_id: String(taskId) }),
    })
      .then(r => r.ok)
      .then(ok => { if (ok) window.NLD.dismissedFlags.add(String(taskId)); return ok; })
      .catch(() => false);
  };

  // A small "✕" control that removes a task's flags. On click it dismisses the
  // task_id (server + local set) then calls afterDismiss() to re-render the list.
  window.NLD.makeFlagRemoveX = function (taskId, afterDismiss) {
    const x = document.createElement("span");
    x.textContent = "✕";
    x.title = "Remove flag from this task (hides it for everyone)";
    x.style.cssText = "cursor:pointer;color:#9ca3af;font-size:0.7rem;font-weight:700;margin-left:4px;line-height:1;user-select:none;";
    x.addEventListener("mouseover", () => { x.style.color = "#dc2626"; });
    x.addEventListener("mouseout",  () => { x.style.color = "#9ca3af"; });
    x.addEventListener("click", (e) => {
      e.stopPropagation(); e.preventDefault();
      x.style.pointerEvents = "none"; x.style.opacity = "0.4";
      window.NLD.dismissFlag(taskId).then((ok) => {
        if (ok && typeof afterDismiss === "function") afterDismiss();
        else if (!ok) { x.style.pointerEvents = ""; x.style.opacity = ""; }
      });
    });
    return x;
  };

  // Kick off the load immediately (pages are behind auth; a 401 just yields an
  // empty set). Fires in <head> so the set is usually ready before any render.
  window.NLD.loadDismissedFlags();
})();
