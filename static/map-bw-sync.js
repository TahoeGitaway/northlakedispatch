/*
 * map-bw-sync.js — Breezeway task time sync.
 * Uses the current displayed schedule (optimizedSchedule).
 * Requires a saved route (currentRouteId) so unsaved-only routes can't sync.
 */

/* Stops worth contacting Breezeway about a second time. A "failed" or "partial"
   stop is one whose task we could see but couldn't finish writing — a throttled
   lookup or a timed-out run — so another attempt can genuinely fix it. An
   "unmatched" stop cannot: its name matches no Breezeway property, and no number
   of retries changes that until someone edits the name. Keeping unmatched out is
   the whole point of a targeted retry; including it would guarantee a failure. */
function _bwRetryableNames(outcome) {
  return ((outcome && outcome.notApplied) || [])
    .filter(x => x.status === "failed" || x.status === "partial")
    .map(x => x.name)
    .filter(Boolean);
}

/* The retry control. Two skins for the two places it appears: the sidebar result
   box (Tailwind, like everything around it) and the fixed banner (inline styles,
   since the banner builds its own markup outside the stylesheet's reach). */
function _bwRetryButtonHtml(names, where) {
  const n     = names.length;
  const label = `&#8635; Retry only ${n} that didn't sync`;
  if (where === "banner") {
    return '<button type="button" onclick="bwRetryNotApplied()" '
         + 'style="margin-left:14px;background:#fff;border:none;color:#b91c1c;'
         + 'border-radius:5px;padding:3px 10px;cursor:pointer;font-weight:700;'
         + 'font-size:0.8rem;flex-shrink:0;white-space:nowrap;">' + label + '</button>';
  }
  return '<button type="button" onclick="bwRetryNotApplied()" '
       + 'class="w-full rounded-lg bg-amber-600 hover:bg-amber-700 text-white '
       + 'font-semibold text-xs px-3 py-1.5">' + label + '</button>';
}

/* Re-sync ONLY the stops that didn't get a time last run. The stops that already
   applied are left alone — re-writing them is harmless but it spends the same
   40s server budget that starved the failures in the first place, which is why a
   plain re-run tends to fail the same stops again. */
function bwRetryNotApplied() {
  const outcome = window.__bwSyncOutcome;
  const names   = _bwRetryableNames(outcome);
  if (!names.length) {
    alert("There's nothing left that a retry can fix.");
    return;
  }
  // Carry the running total of stops already written so this run's summary can't
  // read as "only 4 stops on this route".
  const prior = (window.__bwSyncPriorApplied || 0)
              + (((outcome && outcome.summary) || {}).updated || 0);
  // Re-arm the top-of-screen banner: the previous run's observer disconnected
  // when it reported, so without this a retry would update the sidebar silently.
  // Only for the Save & Sync flow that armed it — see _watchBwSync.
  if (window.__bwSyncWatchArmed && typeof _watchBwSync === "function") _watchBwSync();
  bwSyncTimes({ onlyNames: names, priorApplied: prior });
}

function bwSyncTimes(opts) {
  opts = opts || {};
  const onlyNames = (opts.onlyNames && opts.onlyNames.length)
    ? new Set(opts.onlyNames.map(n => String(n).trim().toLowerCase()))
    : null;

  if (!currentRouteId) {
    alert("Save the route first before syncing to Breezeway.");
    return;
  }

  let real = optimizedSchedule.filter(s => !s.isLunch && !s.isGap && s.lat);
  if (!real.length) {
    alert("No stops on screen to sync.");
    return;
  }

  if (onlyNames) {
    real = real.filter(s => onlyNames.has((s.name || "").trim().toLowerCase()));
    if (!real.length) {
      // The route changed under the retry (stop removed or renamed). Say so
      // rather than silently syncing nothing.
      alert("Those stops are no longer on this route, so there's nothing to retry.\n\n"
            + "Re-optimize and run a full sync instead.");
      return;
    }
  }

  const assignee = (document.getElementById("assignedToField").value || "").trim();
  const date     = (document.getElementById("routeDateField").value || "").trim();

  if (!date) {
    alert("Set a route date before syncing.");
    return;
  }

  const warning = onlyNames
    ? `Retry ${real.length} stop${real.length === 1 ? "" : "s"} that did NOT get a time in Breezeway, `
      + `for ${assignee || "this route"} on ${date}:`
      + `\n\n${real.map(s => "• " + s.name).join("\n")}`
      + `\n\nStops that already synced will be left completely alone. Nothing will be created or deleted.`
      + `\n\nContinue?`
    : `This will update start times on Breezeway tasks for ${assignee || "this route"} on ${date}.`
      + `\n\nOnly existing tasks assigned to "${assignee || "this employee"}" will be changed. Nothing will be created or deleted.`
      + `\n\nContinue?`;
  if (!confirm(warning)) return;

  // A full sync starts the tally over; a retry inherits the count from the runs
  // before it.
  window.__bwSyncPriorApplied = onlyNames ? (opts.priorApplied || 0) : 0;
  window.__bwSyncWasRetry     = !!onlyNames;

  const stops     = real.map(s => ({ name: s.name, eta_minutes: s.eta_minutes + (s.serviceMinutes || 0) }));
  const btn       = document.getElementById("bwSyncBtn");
  const resultDiv = document.getElementById("bwSyncResult");

  btn.disabled    = true;
  btn.textContent = "Syncing…";
  resultDiv.classList.remove("hidden");
  resultDiv.innerHTML = '<span class="text-gray-500">Contacting Breezeway…</span>';
  // Clear last run's verdict — a stale "all applied" must never let this run
  // navigate away, and every exit path below sets a fresh one.
  window.__bwSyncOutcome = null;

  fetch("/api/bw-sync-times", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ date, assignee, stops }),
  })
    .then(r => r.text())
    .then(raw => {
      let data;
      try {
        data = JSON.parse(raw);
      } catch (_) {
        // Non-JSON body (the hosting proxy's plain-text "upstream error") = the sync
        // ran longer than the gateway waits, so it returned its own error instead of
        // our JSON. The work most likely still finished — but "most likely" is not
        // confirmation, so treat it as unresolved and don't navigate away.
        window.__bwSyncOutcome = { allApplied: false, notApplied: [], summary: {},
                                   unresolved: true };
        resultDiv.innerHTML =
          `<div class="text-amber-700 font-semibold">⏳ The sync took longer than the server waits — it most likely still went through.</div>`
          + `<div class="text-gray-600 text-xs mt-1">This usually means a lot of tasks/stops in one sync, or Breezeway being busy or lagging behind. `
          + `Click <b>Sync Times to Breezeway</b> again to confirm — it should come back quickly, and re-syncing the same times is harmless.</div>`;
        return;
      }
      if (data.error) {
        window.__bwSyncOutcome = { allApplied: false, notApplied: [], summary: {},
                                   hardError: data.error };
        resultDiv.innerHTML = `<span class="text-red-600">Error: ${data.error}</span>`;
        return;
      }
      // Record the OUTCOME for the banner/redirect watcher. It used to infer this
      // by regex-scraping this box's text, which is why a partial sync could read
      // as "done" and navigate away. all_applied comes straight from the server.
      const prior    = window.__bwSyncPriorApplied || 0;
      const isRetry  = !!window.__bwSyncWasRetry;
      // The wrong-day heuristic reads "no stop has a task that day" as a wrong
      // date. That inference doesn't hold on a retry that follows successful
      // writes — those stops proved the date is right — so telling the user to
      // check the date would send them after the wrong problem.
      const wrongDay = !!data.wrong_day_suspected && !(isRetry && prior > 0);

      window.__bwSyncOutcome = {
        allApplied:  data.all_applied !== false,
        notApplied:  data.not_applied || [],
        summary:     data.summary || {},
        diagnostics: data.diagnostics || null,
        wrongDay:    wrongDay,
        wrongDayMsg: wrongDay ? (data.wrong_day_message || "") : "",
      };

      // Nothing written and every stop had no task that day — almost always the
      // wrong date. Interrupt rather than let it read as a quiet success.
      if (wrongDay) {
        alert("⚠ Nothing was synced.\n\n" + data.wrong_day_message
              + "\n\nThe route's date is what gets synced — check it matches the day "
              + "you're planning before trying again.");
      }

      const s  = data.summary || {};
      let html = `<div class="font-semibold mb-1">`;
      if (isRetry) html += `<span class="text-gray-500">retry:</span> `;
      html += `${s.updated || 0} updated &nbsp;·&nbsp; ${s.skipped || 0} skipped`;
      if (s.failed)    html += ` &nbsp;·&nbsp; <span class="text-red-600">${s.failed} failed</span>`;
      if (s.unmatched) html += ` &nbsp;·&nbsp; <span class="text-red-600">${s.unmatched} name not found in Breezeway</span>`;
      html += `</div>`;

      // Targeted retry, right where the failures are listed.
      const retryable = _bwRetryableNames(window.__bwSyncOutcome);
      if (retryable.length) {
        html += `<div class="my-1.5">${_bwRetryButtonHtml(retryable, "sidebar")}</div>`;
      }
      if (s.unmatched) {
        html += `<div class="text-red-600 mb-1">`
              + `${s.unmatched} name${s.unmatched === 1 ? "" : "s"} can't be fixed by retrying — `
              + `correct the spelling to match Breezeway first.</div>`;
      }

      for (const r of (data.results || [])) {
        const bad   = r.status === "failed" || r.status === "unmatched" || r.status === "partial";
        const color = r.status === "updated" ? "text-green-700"
                    : bad                    ? "text-red-600"
                    : "text-gray-500";
        const icon  = r.status === "updated"   ? "&#10003;"
                    : r.status === "unmatched" ? "&#9888;"
                    : bad                      ? "&#10007;"
                    : "&ndash;";
        html += `<div class="${color} text-xs leading-snug mb-1">`;
        html += `${icon} <b>${r.name}</b>`;
        if (r.time)      html += ` &rarr; ${r.time}`;
        if (r.reason)    html += ` <span class="text-gray-400">(${r.reason})</span>`;
        if (r.task_keys) html += `<div class="text-gray-400 pl-3">fields: ${r.task_keys.join(", ")}</div>`;
        if (r.linked_reso !== undefined) html += `<div class="text-gray-400 pl-3">linked_reso: ${JSON.stringify(r.linked_reso)}</div>`;
        for (const t of (r.tasks || [])) {
          const tmsg = t.ok
            ? `<span class="text-green-600">${t.msg}</span>`
            : `<span class="text-red-600">FAIL: ${t.msg}</span>`;
          html += `<div class="pl-3 text-gray-500">${t.task_name}: ${tmsg}</div>`;
        }
        html += `</div>`;
      }
      resultDiv.innerHTML = html;
    })
    .catch(e => {
      window.__bwSyncOutcome = { allApplied: false, notApplied: [], summary: {},
                                 hardError: e && e.message ? e.message : String(e) };
      resultDiv.innerHTML = `<span class="text-red-600">Error: ${e.message}</span>`;
    })
    .finally(() => {
      btn.disabled    = false;
      btn.textContent = "Sync Times to Breezeway";
    });
}
