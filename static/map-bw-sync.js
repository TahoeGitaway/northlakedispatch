/*
 * map-bw-sync.js — Breezeway task time sync.
 * Reads the current optimizedSchedule and PATCHes matching Breezeway
 * task start_times to match route ETAs. Isolated: reads global state
 * (optimizedSchedule, routeDateField, assignedToField) but never writes it.
 */

function bwSyncTimes() {
  const real = optimizedSchedule.filter(s => !s.isLunch && !s.isGap && s.lat);
  if (!real.length) {
    alert("Optimize the route first before syncing times to Breezeway.");
    return;
  }

  const date = (document.getElementById("routeDateField").value || "").trim();
  if (!date) {
    alert("Set a route date before syncing to Breezeway.");
    return;
  }

  const assignee = (document.getElementById("assignedToField").value || "").trim();

  const stops = real.map(s => ({ name: s.name, eta_minutes: s.eta_minutes }));

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
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        resultDiv.innerHTML = `<span class="text-red-600">Error: ${data.error}</span>`;
        return;
      }
      const s   = data.summary || {};
      let html  = `<div class="font-semibold mb-1">`;
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
        html += `<div class="${color} text-xs leading-snug">`;
        html += `${icon} <b>${r.name}</b>`;
        if (r.time)   html += ` &rarr; ${r.time}`;
        if (r.reason) html += ` <span class="text-gray-400">(${r.reason})</span>`;
        html += `</div>`;
      }
      resultDiv.innerHTML = html;
    })
    .catch(e => {
      resultDiv.innerHTML = `<span class="text-red-600">Network error: ${e.message}</span>`;
    })
    .finally(() => {
      btn.disabled    = false;
      btn.textContent = "Sync Times to Breezeway";
    });
}
