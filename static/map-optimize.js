/* ================================================================
   OPTIMIZE — optimize, save, update, load route, Google Maps export
   Depends on: all state globals, guardResponse, render functions
================================================================ */

/* ── OPTIMIZE ── */
async function optimizeRoute() {
  if (!selectedStops.length) { alert("Add at least one stop first."); return; }
  document.getElementById("loadingOverlay").classList.add("active");
  document.getElementById("optimizeBtn").disabled = true;

  // Fetch OSRM matrix from the browser — the deploy server can't reach router.project-osrm.org.
  // Fall back to haversine approximation if OSRM is unavailable so the app always works.
  const allLocs = [startLocation, ...selectedStops];
  let clientMatrix = null;
  try {
    const coordStr = allLocs.map(s => `${s.lng},${s.lat}`).join(";");
    const mResp    = await fetch(
      `https://router.project-osrm.org/table/v1/driving/${coordStr}?annotations=duration`
    );
    const mData    = await mResp.json();
    clientMatrix   = mData.durations || null;
  } catch(_) {}

  if (!clientMatrix) {
    clientMatrix = haversineMatrix(allLocs);
  }

  try {
    const res  = await fetch("/optimize", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        stops: selectedStops.map(s => ({
          name:s.name, lat:s.lat, lng:s.lng,
          arrival:s.arrival, priority_checkin:s.priority_checkin, serviceMinutes:s.serviceMinutes
        })),
        start:           startLocation,
        startTime:       document.getElementById("startTime").value,
        drive_only:      false,
        duration_matrix: clientMatrix,
      })
    });
    const data = await guardResponse(res);
    document.getElementById("loadingOverlay").classList.remove("active");
    document.getElementById("optimizeBtn").disabled = false;
    if (data.error) { alert(data.error); return; }

    durationMatrix = clientMatrix;
    startMinutes   = data.start_minutes || hhmmToMinutes(document.getElementById("startTime").value);

    optimizedSchedule = data.schedule.map(entry => {
      const orig = selectedStops.find(s => s.name === entry.name);
      return { ...entry, _id: orig?._id || makeStopId() };
    });
    isOptimized = true;

    const lunchMins = hhmmToMinutes(document.querySelector('input[name="lunchTime"]:checked').value);
    if (getLunchEnabled()) insertLunchAt(lunchMins);
    recalculateTimes();

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
    if (warns.length) {
      wb.classList.remove("hidden");
      wb.classList.add((data.late_checkins?.length || data.late_priority_checkins?.length) ? "deadline-warning" : "shift-warning");
      wb.innerHTML = warns.join("<br>");
    }

    document.getElementById("scheduleSection").classList.remove("hidden");
    document.getElementById("workInSection").classList.remove("hidden");
    document.getElementById("addMoreBtn").classList.remove("hidden");
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
    await redrawRouteOnMap();
  } catch(err) {
    if (err === "session_expired") return;
    document.getElementById("loadingOverlay").classList.remove("active");
    document.getElementById("optimizeBtn").disabled = false;
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
  document.getElementById("saveModal").classList.remove("hidden");
}
function closeSaveModal() { document.getElementById("saveModal").classList.add("hidden"); }

async function submitSaveRoute() {
  const name        = document.getElementById("saveRouteName").value.trim();
  const assignedTo  = document.getElementById("saveAssignedTo").value.trim();
  const routeDate   = document.getElementById("saveRouteDate").value;
  const notes       = document.getElementById("routeNotesField").value.trim();
  const notesPublic = document.getElementById("notesPublicField").checked;
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
      body: JSON.stringify({ name, assigned_to:assignedTo, route_date:routeDate, schedule:real, stats:lastStats, notes, notes_public:notesPublic })
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
      body: JSON.stringify({ name, assigned_to:assignedTo, route_date:routeDate, schedule:real, stats:lastStats, notes, notes_public:notesPublic })
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
      btn.textContent = "✓ Updated!";
      btn.classList.remove("bg-blue-600","hover:bg-blue-700");
      btn.classList.add("bg-green-600");
      setTimeout(() => {
        btn.textContent = "↑ Update"; btn.disabled = false;
        btn.classList.remove("bg-green-600");
        btn.classList.add("bg-blue-600","hover:bg-blue-700");
      }, 2000);
    }
  } catch(e) {
    alert("Update failed: " + e.message);
    btn.disabled = false; btn.textContent = "↑ Update";
  }
}

/* ── LOAD ROUTE ── */
(async function checkLoadParam() {
  const loadId = new URLSearchParams(window.location.search).get("load");
  if (!loadId) return;
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

    // Fetch the drive time matrix so drive-time pills show real values.
    // 5-second abort — fall through to synthesis fallback immediately if OSRM is slow.
    try {
      const allLocs = [
        { lat: startLocation.lat, lng: startLocation.lng },
        ...optimizedSchedule.map(s => ({ lat: s.lat, lng: s.lng }))
      ];
      const coordStr = allLocs.map(s => `${s.lng},${s.lat}`).join(";");
      const ctrl = new AbortController();
      setTimeout(() => ctrl.abort(), 5000);
      const mResp = await fetch(
        `https://router.project-osrm.org/table/v1/driving/${coordStr}?annotations=duration`,
        { signal: ctrl.signal }
      );
      const mData = await mResp.json();
      durationMatrix = mData.durations || [];

      optimizedSchedule.forEach((s, i) => { s.matrix_index = i + 1; });

      // Derive true startMinutes = first stop ETA minus depot→stop[0] drive time
      if (durationMatrix.length > 0 && optimizedSchedule.length > 0) {
        const depotDriveSec = durationMatrix[0][1] || 0;
        const derived = optimizedSchedule[0].eta_minutes - (depotDriveSec / 60);
        startMinutes = Math.max(0, Math.round(derived));
      }

      const h = Math.floor(startMinutes / 60) % 24;
      const m = startMinutes % 60;
      document.getElementById("startTime").value =
        `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;

    } catch(_) {
      // OSRM unavailable — synthesize drive times from saved eta_minutes.
      // These are the exact values used when the route was originally optimized,
      // so drive-time pills will be accurate even without a live matrix.
      const n = optimizedSchedule.length + 1;
      durationMatrix = Array.from({length: n}, () => Array(n).fill(0));
      for (let i = 0; i < optimizedSchedule.length; i++) {
        const s       = optimizedSchedule[i];
        const prev    = i === 0 ? null : optimizedSchedule[i - 1];
        const prevEta = prev ? prev.eta_minutes + prev.serviceMinutes : startMinutes;
        const driveSec = Math.max(0, (s.eta_minutes - prevEta) * 60);
        durationMatrix[i][i + 1] = driveSec;
        durationMatrix[i + 1][i] = driveSec;
      }
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

    const lunchMins = hhmmToMinutes(document.querySelector('input[name="lunchTime"]:checked').value);
    if (getLunchEnabled()) insertLunchAt(lunchMins);

    optimizedSchedule.forEach(s => {
      if (!s.isLunch && s.eta_minutes != null)
        s.eta = minutesToHHMM(s.eta_minutes);
    });

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
    `${startLocation.lat},${startLocation.lng}`
  ];
  window.open("https://www.google.com/maps/dir/" + coords.join("/"), "_blank");
}
