/* ================================================================
   OPTIMIZE — optimize, save, update, load route, Google Maps export
   Depends on: all state globals, guardResponse, render functions
================================================================ */

/* ── TEAM SIDEBAR INIT ── */
document.addEventListener("DOMContentLoaded", function() {
  const sel = document.getElementById("sidebarTeamId");
  const row = document.getElementById("sidebarTeamRow");
  if (!sel || !row || !window.APP_TEAMS || !APP_TEAMS.length) return;
  APP_TEAMS.forEach(t => {
    const opt = document.createElement("option");
    opt.value = t.id;
    opt.textContent = t.name;
    if (t.id === window.USER_TEAM_ID) opt.selected = true;
    sel.appendChild(opt);
  });
  row.classList.remove("hidden");
});

/* ── TOAST ── */
function showToast(msg, durationMs = 3500) {
  const t = document.createElement("div");
  t.textContent = msg;
  t.style.cssText = [
    "position:fixed","bottom:24px","left:50%","transform:translateX(-50%)",
    "background:#16a34a","color:#fff","font-size:14px","font-weight:600",
    "padding:10px 20px","border-radius:8px","box-shadow:0 4px 12px rgba(0,0,0,0.2)",
    "z-index:9999","transition:opacity 0.4s","pointer-events:none"
  ].join(";");
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = "0"; }, durationMs - 400);
  setTimeout(() => { t.remove(); }, durationMs);
}

/* ── STALE-ROUTE HELPERS ── */
function markRouteStale() {
  if (!isOptimized) return;
  const btn  = document.getElementById("optimizeBtn");
  const gBtn = document.getElementById("googleOptimizeBtn");
  if (btn._stale) return; // already marked
  btn._stale  = true;
  gBtn._stale = true;
  btn.textContent  = "⚠ Re-optimize Route";
  btn.classList.remove("bg-indigo-600","hover:bg-indigo-700");
  btn.classList.add("bg-amber-500","hover:bg-amber-600");
  const gSpan = gBtn.querySelector("span:first-child");
  if (gSpan) gSpan.textContent = "⚠ Re-optimize with Google Maps";
  gBtn.classList.remove("bg-emerald-700","hover:bg-emerald-800");
  gBtn.classList.add("bg-amber-600","hover:bg-amber-700");
}

function clearRouteStale() {
  const btn  = document.getElementById("optimizeBtn");
  const gBtn = document.getElementById("googleOptimizeBtn");
  btn._stale  = false;
  gBtn._stale = false;
  btn.textContent = "Optimize Route";
  btn.classList.remove("bg-amber-500","hover:bg-amber-600");
  btn.classList.add("bg-indigo-600","hover:bg-indigo-700");
  const gSpan = gBtn.querySelector("span:first-child");
  if (gSpan) gSpan.textContent = "🗺 Optimize with Google Maps";
  gBtn.classList.remove("bg-amber-600","hover:bg-amber-700");
  gBtn.classList.add("bg-emerald-700","hover:bg-emerald-800");
}

/* ── OPTIMIZE ── */
async function optimizeRoute(useGoogleMatrix = false) {
  if (!selectedStops.length) { alert("Add at least one stop first."); return; }
  clearRouteStale();
  _lunchWasMoved = false;

  // If re-optimizing an existing route, rebuild selectedStops from the current
  // schedule (captures any service-time / check-in changes made post-optimize)
  // and wipe stale state so the optimizer starts completely fresh.
  if (isOptimized) {
    selectedStops = optimizedSchedule
      .filter(s => !s.isLunch && !s.isGap)
      .map(s => ({
        _id: s._id, name: s.name, lat: s.lat, lng: s.lng,
        arrival: s.arrival, priority_checkin: s.priority_checkin || false,
        serviceMinutes: s.serviceMinutes
      }));
    isOptimized      = false;
    optimizedSchedule = [];
    durationMatrix    = [];
  }

  document.getElementById("loadingOverlay").classList.add("active");
  document.getElementById("loadingOverlay").querySelector(".lo-label").textContent =
    useGoogleMatrix ? "Optimizing with Google Maps…" : "Optimizing…";
  document.getElementById("optimizeBtn").disabled = true;
  document.getElementById("googleOptimizeBtn").disabled = true;

  try {
    const res  = await fetch("/optimize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        stops: selectedStops.map(s => ({
          name:s.name, lat:s.lat, lng:s.lng,
          arrival:s.arrival, priority_checkin:s.priority_checkin, serviceMinutes:s.serviceMinutes
        })),
        start:             startLocation,
        end:               endLocation,
        startTime:         document.getElementById("startTime").value,
        drive_only:        false,
        use_google_matrix: useGoogleMatrix,
      })
    });
    const data = await guardResponse(res);
    document.getElementById("loadingOverlay").classList.remove("active");
    document.getElementById("optimizeBtn").disabled = false;
    document.getElementById("googleOptimizeBtn").disabled = false;
    if (data.error) { alert(data.error); return; }

    durationMatrix = data.duration_matrix || [];
    startMinutes   = data.start_minutes || hhmmToMinutes(document.getElementById("startTime").value);

    optimizedSchedule = data.schedule.map(entry => {
      const orig = selectedStops.find(s => s.name === entry.name);
      return { ...entry, _id: orig?._id || makeStopId() };
    });
    isOptimized = true;

    const lunchMins = hhmmToMinutes(document.querySelector('input[name="lunchTime"]:checked').value);
    if (getLunchEnabled()) {
      insertLunchAt(lunchMins);
      recalculateTimes();
      if (_guardLunchAgainstCheckins()) recalculateTimes();
    } else {
      recalculateTimes();
    }

    lastStats = {
      total_duration:   data.total_duration,
      driving_duration: data.driving_duration,
      service_duration: data.service_duration,
      distance:         data.distance,
    };

    document.getElementById("totalTime").textContent   = (data.total_duration   / 3600).toFixed(2) + " hrs";
    document.getElementById("drivingTime").textContent = (data.driving_duration / 3600).toFixed(2) + " hrs";
    document.getElementById("serviceTime").textContent = (data.service_duration / 3600).toFixed(2) + " hrs";
    document.getElementById("distance").textContent    = data.distance
      ? (data.distance / 1609).toFixed(1) + " miles" : "—";

    const wb = document.getElementById("warningBox");
    wb.className = "text-sm hidden p-2 rounded"; wb.innerHTML = "";
    const warns = [];
    if (data.total_duration > 10 * 3600) warns.push("⚠ Route exceeds 10-hour shift.");
    if (data.late_priority_checkins?.length) warns.push(`⚠ Priority check-ins after 12PM: ${data.late_priority_checkins.join(", ")}`);
    if (data.late_checkins?.length) warns.push(`⚠ Late check-ins (after 4PM): ${data.late_checkins.join(", ")}`);
    if (_lunchWasMoved) warns.push("⚠ Lunch moved after last check-in — check-ins take priority over efficiency.");
    if (warns.length) {
      wb.classList.remove("hidden");
      wb.classList.add((data.late_checkins?.length || data.late_priority_checkins?.length) ? "deadline-warning" : "shift-warning");
      wb.innerHTML = warns.join("<br>");
    }

    document.getElementById("scheduleSection").classList.remove("hidden");
    document.getElementById("workInSection").classList.remove("hidden");
    document.getElementById("addMoreBtn").classList.remove("hidden");
    document.getElementById("changeStartBtn").classList.remove("hidden");
    if (currentRouteId) {
      document.getElementById("saveRouteBtn").classList.add("hidden");
      document.getElementById("updateRouteBtn").classList.remove("hidden");
    } else {
      document.getElementById("saveRouteBtn").classList.remove("hidden");
      document.getElementById("updateRouteBtn").classList.add("hidden");
    }
    if (!document.getElementById("saveRouteDate").value)
      document.getElementById("saveRouteDate").value = new Date().toISOString().split("T")[0];
    if (!document.getElementById("routeDateField").value)
      document.getElementById("routeDateField").value = new Date().toISOString().split("T")[0];

    renderStops();
    renderSchedule();
    await redrawRouteOnMap(data.route_polyline || null);
    if (useGoogleMatrix) showToast("✓ Optimized with Google Maps real drive times");
  } catch(err) {
    if (err === "session_expired") return;
    document.getElementById("loadingOverlay").classList.remove("active");
    document.getElementById("optimizeBtn").disabled = false;
    document.getElementById("googleOptimizeBtn").disabled = false;
    alert("Optimize failed: " + (err.message || err));
  }
}

/* ── SAVE MODAL ── */
function openSaveModal() {
  if (!optimizedSchedule.filter(s => !s.isLunch).length) { alert("Optimize a route first."); return; }
  document.getElementById("saveError").classList.add("hidden");
  document.getElementById("saveSuccess").classList.add("hidden");

  const sidebarName     = document.getElementById("routeNameField").value.trim();
  const sidebarAssigned = document.getElementById("assignedToField").value.trim();
  const sidebarDate     = document.getElementById("routeDateField").value;

  document.getElementById("saveRouteName").value  = sidebarName;
  document.getElementById("saveAssignedTo").value = sidebarAssigned;
  document.getElementById("saveRouteDate").value  = sidebarDate || new Date().toISOString().split("T")[0];

  // Sync team from sidebar to modal
  const sidebarTeam = document.getElementById("sidebarTeamId");
  const modalTeam   = document.getElementById("saveTeamId");
  if (sidebarTeam && modalTeam) modalTeam.value = sidebarTeam.value;

  document.getElementById("saveModal").classList.remove("hidden");
}
function closeSaveModal() { document.getElementById("saveModal").classList.add("hidden"); }

async function submitSaveRoute() {
  const name        = document.getElementById("saveRouteName").value.trim();
  const assignedTo  = document.getElementById("saveAssignedTo").value.trim();
  const routeDate   = document.getElementById("saveRouteDate").value;
  const notes       = document.getElementById("routeNotesField").value.trim();
  const notesPublic = document.getElementById("notesPublicField").checked;
  const teamEl      = document.getElementById("saveTeamId");
  const teamId      = teamEl ? parseInt(teamEl.value) || null : null;
  const errorEl     = document.getElementById("saveError");
  const successEl   = document.getElementById("saveSuccess");
  const saveBtn     = document.getElementById("saveSubmitBtn");
  errorEl.classList.add("hidden"); successEl.classList.add("hidden");

  if (!name)      { errorEl.textContent = "Please enter a route name."; errorEl.classList.remove("hidden"); return; }
  if (!routeDate) { errorEl.textContent = "Please choose a date.";      errorEl.classList.remove("hidden"); return; }

  saveBtn.disabled = true;
  const real = optimizedSchedule.filter(s => !s.isLunch);
  const url  = currentRouteId ? `/routes/${currentRouteId}/update` : "/routes/save";

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, assigned_to:assignedTo, route_date:routeDate, schedule:real, stats:lastStats, notes, notes_public:notesPublic, team_id:teamId })
    });
    if (res.redirected || res.url.includes("/login")) {
      document.getElementById("sessionBanner").style.display = "block";
      closeSaveModal(); return;
    }
    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("json")) {
      errorEl.textContent = `Server error (${res.status}). Please try again.`;
      errorEl.classList.remove("hidden"); saveBtn.disabled = false; return;
    }
    const data = await res.json();
    if (data.error) {
      errorEl.textContent = data.error; errorEl.classList.remove("hidden");
      saveBtn.disabled = false;
    } else {
      if (data.id) currentRouteId = data.id;
      successEl.textContent = "Route saved! Redirecting…";
      successEl.classList.remove("hidden");
      const savedDate = document.getElementById("routeDateField").value || document.getElementById("saveRouteDate").value;
      setTimeout(() => { window.location.href = savedDate ? `/routes?date=${savedDate}` : "/routes"; }, 1000);
    }
  } catch(e) {
    errorEl.textContent = "Save failed: " + e.message; errorEl.classList.remove("hidden");
    saveBtn.disabled = false;
  }
}

/* ── UPDATE ROUTE ── */
async function submitUpdateRoute() {
  const name        = document.getElementById("routeNameField").value.trim();
  const assignedTo  = document.getElementById("assignedToField").value.trim();
  const routeDate   = document.getElementById("routeDateField").value;
  const notes       = document.getElementById("routeNotesField").value.trim();
  const notesPublic = document.getElementById("notesPublicField").checked;
  const sidebarTeam = document.getElementById("sidebarTeamId");
  const modalTeam   = document.getElementById("saveTeamId");
  const teamEl      = sidebarTeam || modalTeam;
  const teamId      = teamEl ? parseInt(teamEl.value) || null : null;
  const btn         = document.getElementById("updateRouteBtn");

  if (!currentRouteId) { alert("No loaded route to update. Use Save Route instead."); return; }
  if (!name)            { alert("Please enter a route name before updating."); return; }
  if (!routeDate)       { alert("Please select a date before updating."); return; }

  btn.disabled = true; btn.textContent = "Saving…";
  const real = optimizedSchedule.filter(s => !s.isLunch);

  try {
    const res = await fetch(`/routes/${currentRouteId}/update`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, assigned_to:assignedTo, route_date:routeDate, schedule:real, stats:lastStats, notes, notes_public:notesPublic, team_id:teamId })
    });
    if (res.redirected || res.url.includes("/login")) {
      document.getElementById("sessionBanner").style.display = "block"; return;
    }
    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("json")) {
      alert(`Server error (${res.status}). Please try again.`);
      btn.disabled = false; btn.textContent = "↑ Update"; return;
    }
    const data = await res.json();
    if (data.error) {
      alert("Update failed: " + data.error);
      btn.disabled = false; btn.textContent = "↑ Update";
    } else {
      btn.textContent = "✓ Updated! Redirecting…";
      btn.classList.remove("bg-blue-600","hover:bg-blue-700");
      btn.classList.add("bg-green-600");
      const savedDate = document.getElementById("routeDateField").value;
      setTimeout(() => {
        window.location.href = savedDate ? `/routes?date=${savedDate}` : "/routes";
      }, 900);
    }
  } catch(e) {
    alert("Update failed: " + e.message);
    btn.disabled = false; btn.textContent = "↑ Update";
  }
}

/* ── LOAD FROM PROJECT CLUSTER ── */
(async function checkPropsParam() {
  const params     = new URLSearchParams(window.location.search);
  const propsParam = params.get("props");
  const autoopt    = params.get("autooptimize") === "1";
  // Never auto-optimize when a saved route is also being loaded
  if (!propsParam || params.get("load")) return;
  window.history.replaceState({}, "", window.location.pathname);
  try {
    const res  = await fetch(`/projects/properties?ids=${encodeURIComponent(propsParam)}`);
    const data = await res.json();
    if (data.properties && data.properties.length) {
      data.properties.forEach(p => addStop({...p, serviceMinutes: 15}));
      if (autoopt) optimizeRoute(false);
    }
  } catch (e) {
    console.error("Failed to load project properties:", e);
  }
})();

/* ── LOAD ROUTE ── */
(async function checkLoadParam() {
  const params = new URLSearchParams(window.location.search);
  const loadId = params.get("load");
  if (!loadId) {
    const dateParam = params.get("date");
    if (dateParam) {
      window.history.replaceState({}, "", window.location.pathname);
      document.getElementById("routeDateField").value = dateParam;
      document.getElementById("saveRouteDate").value  = dateParam;
    }
    return;
  }
  window.history.replaceState({}, "", window.location.pathname);

  const overlay  = document.getElementById("routeLoadOverlay");
  const errorBar = document.getElementById("routeLoadError");
  const errorMsg = document.getElementById("routeLoadErrorMsg");

  function showLoadError(msg) {
    overlay.classList.remove("active");
    errorMsg.textContent = msg;
    errorBar.classList.add("active");
  }

  overlay.classList.add("active");
  try {
    const res = await fetch(`/routes/${loadId}`);
    if (res.redirected || res.url.includes("/login")) {
      overlay.classList.remove("active");
      document.getElementById("sessionBanner").style.display = "block"; return;
    }
    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("json")) {
      showLoadError(`Server error (${res.status}). Please try again.`); return;
    }
    const data = await res.json();
    if (data.error) { showLoadError("Could not load route: " + data.error); return; }

    currentRouteId = data.id;
    _stopIdCounter = 0;

    optimizedSchedule = data.schedule.map((s, i) => ({
      ...s, _id: makeStopId(), matrix_index: i + 1,
    }));

    selectedStops = optimizedSchedule.map(s => ({
      _id: s._id, name:s.name, lat:s.lat, lng:s.lng,
      arrival:s.arrival, priority_checkin:s.priority_checkin || false, serviceMinutes:s.serviceMinutes
    }));

    isOptimized = true;

    // Use saved eta_minutes of first stop as a starting approximation
    if (optimizedSchedule.length > 0 && optimizedSchedule[0].eta_minutes != null) {
      startMinutes = optimizedSchedule[0].eta_minutes;
    } else {
      startMinutes = hhmmToMinutes(document.getElementById("startTime").value);
    }

    // Build drive-time matrix directly from saved eta_minutes — no API call needed.
    // These values were computed from the Google Maps matrix when the route was first
    // optimized, so drive-time pills are exact and the route loads instantly.
    {
      const n = optimizedSchedule.length + 1;
      durationMatrix = Array.from({length: n}, () => Array(n).fill(0));
      for (let i = 0; i < optimizedSchedule.length; i++) {
        const s       = optimizedSchedule[i];
        const prev    = i === 0 ? null : optimizedSchedule[i - 1];
        const prevEta = prev ? prev.eta_minutes + prev.serviceMinutes : startMinutes;
        const driveSec = (s.drive_seconds != null)
          ? s.drive_seconds
          : Math.max(0, (s.eta_minutes - prevEta) * 60);
        durationMatrix[i][i + 1] = driveSec;
        durationMatrix[i + 1][i] = driveSec;
      }
      optimizedSchedule.forEach((s, i) => { s.matrix_index = i + 1; });
    }

    lastStats = {
      total_duration:   data.total_duration   || 0,
      driving_duration: data.driving_duration || 0,
      service_duration: data.service_duration || 0,
      distance:         data.distance         || 0,
    };

    if (data.total_duration)   document.getElementById("totalTime").textContent   = (data.total_duration   / 3600).toFixed(2) + " hrs";
    if (data.driving_duration) document.getElementById("drivingTime").textContent = (data.driving_duration / 3600).toFixed(2) + " hrs";
    if (data.service_duration) document.getElementById("serviceTime").textContent = (data.service_duration / 3600).toFixed(2) + " hrs";
    if (data.distance)         document.getElementById("distance").textContent    = (data.distance / 1609).toFixed(1) + " miles";

    document.getElementById("scheduleSection").classList.remove("hidden");
    document.getElementById("workInSection").classList.remove("hidden");
    document.getElementById("addMoreBtn").classList.remove("hidden");
    document.getElementById("changeStartBtn").classList.remove("hidden");
    document.getElementById("saveRouteBtn").classList.add("hidden");
    document.getElementById("updateRouteBtn").classList.remove("hidden");
    document.getElementById("saveRouteName").value   = data.name;
    document.getElementById("saveAssignedTo").value  = data.assigned_to || "";
    document.getElementById("saveRouteDate").value   = data.route_date;
    document.getElementById("routeNameField").value   = data.name;
    document.getElementById("assignedToField").value  = data.assigned_to || "";
    document.getElementById("routeDateField").value   = data.route_date;
    document.getElementById("routeNotesField").value  = data.notes || "";
    document.getElementById("notesPublicField").checked = data.notes_public || false;
    const teamEl      = document.getElementById("saveTeamId");
    const sidebarTeam = document.getElementById("sidebarTeamId");
    if (data.team_id) {
      if (teamEl)      teamEl.value      = data.team_id;
      if (sidebarTeam) sidebarTeam.value = data.team_id;
    }

    const lunchMins = hhmmToMinutes(document.querySelector('input[name="lunchTime"]:checked').value);
    if (getLunchEnabled()) insertLunchAt(lunchMins);

    recalculateTimes();
    renderStops();
    renderSchedule();
    await redrawRouteOnMap();
    overlay.classList.remove("active");
  } catch(e) {
    const msg = e?.message && e.message !== "session_expired"
      ? "Could not load route: " + e.message
      : "Could not load route. Check your connection and try again.";
    showLoadError(msg);
  }
})();

document.getElementById("saveModal").addEventListener("click", function(e) {
  if (e.target === this) closeSaveModal();
});

/* ── GOOGLE MAPS EXPORT ── */
function exportToGoogleMaps() {
  const real = optimizedSchedule.filter(s => !s.isLunch && s.lat);
  if (!real.length) { alert("Optimize route first."); return; }
  if (real.length > 10)
    alert(`Note: Google Maps displays up to 10 waypoints. All ${real.length} stops are included but Google may truncate the display.`);
  const coords = [
    `${startLocation.lat},${startLocation.lng}`,
    ...real.map(s => `${s.lat},${s.lng}`),
    `${endLocation.lat},${endLocation.lng}`
  ];
  window.open("https://www.google.com/maps/dir/" + coords.join("/"), "_blank");
}
