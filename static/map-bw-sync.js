/*
 * map-bw-sync.js — Breezeway task time sync.
 * Fetches the saved route from the server and PATCHes matching Breezeway
 * task start_times to the saved ETAs. Only works after a route is saved.
 */

function bwSyncTimes() {
  if (!currentRouteId) {
    alert("Save the route first before syncing to Breezeway.");
    return;
  }

  const btn       = document.getElementById("bwSyncBtn");
  const resultDiv = document.getElementById("bwSyncResult");

  btn.disabled    = true;
  btn.textContent = "Syncing…";
  resultDiv.classList.remove("hidden");
  resultDiv.innerHTML = '<span class="text-gray-500">Loading saved route…</span>';

  fetch(`/routes/${currentRouteId}`)
    .then(r => r.json())
    .then(route => {
      const schedule = route.schedule || [];
      const real     = schedule.filter(s => !s.isLunch && !s.isGap && s.lat);

      if (!real.length) {
        resultDiv.innerHTML = '<span class="text-red-600">No stops found in saved route.</span>';
        btn.disabled    = false;
        btn.textContent = "Sync Times to Breezeway";
        return;
      }

      const stops = real.map(s => ({ name: s.name, eta_minutes: s.eta_minutes }));

      resultDiv.innerHTML = '<span class="text-gray-500">Contacting Breezeway…</span>';

      return fetch("/api/bw-sync-times", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({
          date:     route.route_date,
          assignee: route.assigned_to || "",
          stops,
        }),
      })
        .then(r => r.json())
        .then(data => {
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
            html += `<div class="${color} text-xs leading-snug">`;
            html += `${icon} <b>${r.name}</b>`;
            if (r.time)   html += ` &rarr; ${r.time}`;
            if (r.reason) html += ` <span class="text-gray-400">(${r.reason})</span>`;
            html += `</div>`;
          }
          resultDiv.innerHTML = html;
        });
    })
    .catch(e => {
      resultDiv.innerHTML = `<span class="text-red-600">Error: ${e.message}</span>`;
    })
    .finally(() => {
      btn.disabled    = false;
      btn.textContent = "Sync Times to Breezeway";
    });
}
