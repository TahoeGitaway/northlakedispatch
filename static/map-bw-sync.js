/*
 * map-bw-sync.js — Breezeway task time sync.
 * Uses the current displayed schedule (optimizedSchedule).
 * Requires a saved route (currentRouteId) so unsaved-only routes can't sync.
 */

function bwSyncTimes() {
  if (!currentRouteId) {
    alert("Save the route first before syncing to Breezeway.");
    return;
  }

  const real = optimizedSchedule.filter(s => !s.isLunch && !s.isGap && s.lat);
  if (!real.length) {
    alert("No stops on screen to sync.");
    return;
  }

  const assignee = (document.getElementById("assignedToField").value || "").trim();
  const date     = (document.getElementById("routeDateField").value || "").trim();

  if (!date) {
    alert("Set a route date before syncing.");
    return;
  }

  const warning = `This will update start times on Breezeway tasks for ${assignee || "this route"} on ${date}.`
                + `\n\nOnly existing tasks assigned to "${assignee || "this employee"}" will be changed. Nothing will be created or deleted.`
                + `\n\nContinue?`;
  if (!confirm(warning)) return;

  const stops     = real.map(s => ({ name: s.name, eta_minutes: s.eta_minutes + (s.serviceMinutes || 0) }));
  const btn       = document.getElementById("bwSyncBtn");
  const resultDiv = document.getElementById("bwSyncResult");

  btn.disabled    = true;
  btn.textContent = "Syncing…";
  resultDiv.classList.remove("hidden");
  resultDiv.innerHTML = '<span class="text-gray-500">Contacting Breezeway…</span>';

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
        // our JSON. The work most likely still finished in the background.
        resultDiv.innerHTML =
          `<div class="text-amber-700 font-semibold">⏳ The sync took longer than the server waits — it most likely still went through.</div>`
          + `<div class="text-gray-600 text-xs mt-1">This usually means a lot of tasks/stops in one sync, or Breezeway being busy or lagging behind. `
          + `Click <b>Sync Times to Breezeway</b> again to confirm — it should come back quickly, and re-syncing the same times is harmless.</div>`;
        return;
      }
      if (data.error) {
        resultDiv.innerHTML = `<span class="text-red-600">Error: ${data.error}</span>`;
        return;
      }
      const s  = data.summary || {};
      let html = `<div class="font-semibold mb-1">`;
      html += `${s.updated || 0} updated &nbsp;·&nbsp; ${s.skipped || 0} skipped`;
      if (s.failed) html += ` &nbsp;·&nbsp; <span class="text-red-600">${s.failed} failed</span>`;
      html += `</div>`;

      for (const r of (data.results || [])) {
        const color = r.status === "updated" ? "text-green-700"
                    : r.status === "failed"  ? "text-red-600"
                    : "text-gray-500";
        const icon  = r.status === "updated" ? "&#10003;"
                    : r.status === "failed"  ? "&#10007;"
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
      resultDiv.innerHTML = `<span class="text-red-600">Error: ${e.message}</span>`;
    })
    .finally(() => {
      btn.disabled    = false;
      btn.textContent = "Sync Times to Breezeway";
    });
}
