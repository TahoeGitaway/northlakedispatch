/* ================================================================
   SCHEDULE — render, time recalc, stats, lunch, view mode,
              stop setters, move/drag/reverse/restart
   Depends on: all state globals, map, markers (globals)
================================================================ */

/* ── LIVE STATS ── */
function updateStatsDisplay() {
  const real = optimizedSchedule.filter(s => !s.isLunch);
  if (!real.length) return;

  let drivingSec = 0;
  let prevIdx = 0;
  real.forEach(stop => {
    if (durationMatrix[prevIdx] && durationMatrix[prevIdx][stop.matrix_index]) {
      drivingSec += durationMatrix[prevIdx][stop.matrix_index];
    }
    prevIdx = stop.matrix_index;
  });

  const lunchMin   = getLunchEnabled() ? 30 : 0;
  const gapMin     = optimizedSchedule.filter(s => s.isGap).reduce((sum, s) => sum + s.serviceMinutes, 0);
  const serviceSec = real.reduce((sum, s) => sum + s.serviceMinutes * 60, 0);
  const totalSec   = drivingSec + serviceSec + (lunchMin + gapMin) * 60;

  document.getElementById("totalTime").textContent   = (totalSec   / 3600).toFixed(2) + " hrs";
  document.getElementById("drivingTime").textContent = (drivingSec / 3600).toFixed(2) + " hrs";
  document.getElementById("serviceTime").textContent = (serviceSec / 3600).toFixed(2) + " hrs";
}

/* ── TIME RECALCULATION ── */
function recalculateTimes() {
  let running = startMinutes;
  let prevMatrixIdx = 0;

  optimizedSchedule.forEach(stop => {
    if (stop.isLunch || stop.isGap) {
      stop.eta_minutes = Math.round(running);
      stop.eta         = minutesToHHMM(stop.eta_minutes);
      running         += stop.serviceMinutes;
      return;
    }

    let driveSec = 0;
    if (
      durationMatrix.length > 0 &&
      prevMatrixIdx != null &&
      stop.matrix_index != null &&
      durationMatrix[prevMatrixIdx] != null &&
      durationMatrix[prevMatrixIdx][stop.matrix_index] != null
    ) {
      driveSec = durationMatrix[prevMatrixIdx][stop.matrix_index];
    }

    running          += driveSec / 60;
    stop.eta_minutes  = Math.round(running);
    stop.eta          = minutesToHHMM(stop.eta_minutes);
    running          += stop.serviceMinutes;
    prevMatrixIdx     = stop.matrix_index;

    const fin          = stop.eta_minutes + stop.serviceMinutes;
    stop.late          = stop.arrival          && fin > DEADLINE_MINS;
    stop.priority_late = stop.priority_checkin && fin > PRIORITY_MINS;
  });

  updateStatsDisplay();
}

/* ── VIEW MODE ── */
function setViewMode(mode) {
  viewMode = mode;
  document.getElementById('modeFullBtn').classList.toggle('active', mode === 'full');
  document.getElementById('modeDriveBtn').classList.toggle('active', mode === 'drive');
  if (isOptimized) renderSchedule();
}

/* ── LUNCH ── */
function makeLunchSentinel(etaMinutes) {
  return {
    _id: "lunch", name: "Lunch Break", isLunch: true,
    serviceMinutes: 30, eta_minutes: etaMinutes, eta: minutesToHHMM(etaMinutes),
    lat: null, lng: null, matrix_index: null,
  };
}

function getLunchEnabled() { return document.getElementById("lunchEnabled").checked; }

function insertLunchAt(etaMinutes) {
  optimizedSchedule = optimizedSchedule.filter(s => !s.isLunch);
  if (!getLunchEnabled()) return;
  let idx = optimizedSchedule.length;
  for (let i = 0; i < optimizedSchedule.length; i++) {
    if (optimizedSchedule[i].eta_minutes >= etaMinutes) { idx = i; break; }
  }
  optimizedSchedule.splice(idx, 0, makeLunchSentinel(etaMinutes));
}

function toggleLunch() {
  if (!isOptimized) return;
  if (!getLunchEnabled()) {
    optimizedSchedule = optimizedSchedule.filter(s => !s.isLunch);
  } else {
    const lunchMins = hhmmToMinutes(document.querySelector('input[name="lunchTime"]:checked').value);
    insertLunchAt(lunchMins);
    recalculateTimes();
  }
  renderSchedule();
}

function moveLunchTo(lunchMins) {
  if (!isOptimized || !getLunchEnabled()) return;
  insertLunchAt(lunchMins);
  recalculateTimes();
  renderSchedule();
}

/* ── SETTERS ── */
function setArrival(id, val) {
  [selectedStops, optimizedSchedule].forEach(arr => {
    const s = arr.find(s => s._id === id);
    if (s) { s.arrival = val; if (!val) s.priority_checkin = false; }
  });
  if (isOptimized) { markRouteStale(); recalculateTimes(); renderSchedule(); } else renderStops();
}

function setPriorityCheckin(id, val) {
  [selectedStops, optimizedSchedule].forEach(arr => {
    const s = arr.find(s => s._id === id);
    if (s) { s.priority_checkin = val; if (val) s.arrival = true; }
  });
  if (isOptimized) { markRouteStale(); recalculateTimes(); renderSchedule(); }
}

function toggleScheduleCheckin(id) {
  [selectedStops, optimizedSchedule].forEach(arr => {
    const s = arr.find(s => s._id === id);
    if (s) { s.arrival = !s.arrival; if (!s.arrival) s.priority_checkin = false; }
  });
  markRouteStale();
  recalculateTimes();
  renderSchedule();
}

function toggleSchedulePriority(id) {
  [selectedStops, optimizedSchedule].forEach(arr => {
    const s = arr.find(s => s._id === id);
    if (s) {
      s.priority_checkin = !s.priority_checkin;
      if (s.priority_checkin) s.arrival = true;
    }
  });
  markRouteStale();
  recalculateTimes();
  renderSchedule();
}

function setServiceMinutes(id, val) {
  [selectedStops, optimizedSchedule].forEach(arr => {
    const s = arr.find(s => s._id === id);
    if (s) s.serviceMinutes = val;
  });
  if (isOptimized) { recalculateTimes(); renderSchedule(); }
}

function setGapMinutes(id, val) {
  const s = optimizedSchedule.find(s => s._id === id);
  if (s) s.serviceMinutes = val;
  recalculateTimes(); renderSchedule();
}

function addGap() {
  optimizedSchedule.push({
    _id: makeStopId(), isGap: true, name: "Gap",
    serviceMinutes: 30, eta_minutes: 0, eta: "—",
    lat: null, lng: null, matrix_index: null,
  });
  recalculateTimes(); renderSchedule();
}

/* ── PRE-OPT STOP LIST ── */
function _updateGoogleCostHint() {
  const n    = selectedStops.length + 1; // +1 for depot node
  const cost = (n * n * 0.005).toFixed(2);
  const hint = document.getElementById("googleCostHint");
  const note = document.getElementById("googleStopNote");
  if (hint) hint.textContent = `~$${cost} / ${selectedStops.length} stops`;
  if (note) {
    if (selectedStops.length > 10) note.classList.remove("hidden");
    else note.classList.add("hidden");
  }
}

function renderStops() {
  const container = document.getElementById("selectedStops");
  const countEl   = document.getElementById("stopCount");

  _updateGoogleCostHint();

  if (isOptimized) {
    container.innerHTML = "";
    countEl.textContent = "";
    document.getElementById("preOptSection").classList.add("hidden");
    document.getElementById("preOptSearch").classList.add("hidden");
    return;
  }

  document.getElementById("preOptSection").classList.remove("hidden");
  document.getElementById("preOptSearch").classList.remove("hidden");
  container.innerHTML = "";
  countEl.textContent = selectedStops.length ? `(${selectedStops.length})` : "";

  if (!selectedStops.length) {
    container.innerHTML = '<p class="text-sm text-gray-400">No stops added yet.</p>';
    return;
  }

  selectedStops.forEach(s => {
    const div = document.createElement("div");
    div.className = "stop-card bg-gray-50 p-2 rounded shadow-sm text-sm";

    div.innerHTML = `
      <div class="font-medium text-gray-800 truncate mb-1" title="${s.name}">${s.name}</div>
      <div class="stop-card-row items-center">
        <label class="flex items-center gap-1 cursor-pointer">
          <input type="checkbox" class="accent-green-600" data-role="checkin"
                 ${s.arrival ? "checked" : ""}>
          <span class="text-gray-600 text-xs">Check-in</span>
        </label>
        <label class="flex items-center gap-1 cursor-pointer priority-label"
               style="${s.arrival ? '' : 'display:none'}">
          <input type="checkbox" class="accent-violet-600" data-role="priority"
                 ${s.priority_checkin ? "checked" : ""}>
          <span class="text-violet-700 font-medium text-xs">Priority (by 12PM)</span>
        </label>
        <select class="border rounded px-1 py-0.5 text-xs" data-role="service">
          ${generateTimeOptions(s.serviceMinutes)}
        </select>
      </div>
      <button data-role="remove"
              class="text-red-400 hover:text-red-600 text-xs mt-1">✕ Remove</button>`;

    const checkinCb   = div.querySelector('[data-role="checkin"]');
    const priorityCb  = div.querySelector('[data-role="priority"]');
    const priorityLbl = div.querySelector('.priority-label');
    const serviceEl   = div.querySelector('[data-role="service"]');
    const removeBtn   = div.querySelector('[data-role="remove"]');

    checkinCb.addEventListener("change", () => {
      s.arrival = checkinCb.checked;
      if (!s.arrival) { s.priority_checkin = false; priorityCb.checked = false; }
      priorityLbl.style.display = s.arrival ? "" : "none";
    });
    priorityCb.addEventListener("change", () => { s.priority_checkin = priorityCb.checked; });
    serviceEl.addEventListener("change", () => { s.serviceMinutes = parseInt(serviceEl.value); });
    removeBtn.addEventListener("click", () => {
      selectedStops = selectedStops.filter(x => x._id !== s._id);
      div.remove();
      countEl.textContent = selectedStops.length ? `(${selectedStops.length})` : "";
      if (!selectedStops.length)
        container.innerHTML = '<p class="text-sm text-gray-400">No stops added yet.</p>';
    });

    container.appendChild(div);
  });

  // Update live cost hint on the Google optimize button
  const hint = document.getElementById("googleCostHint");
  if (hint) {
    const n = selectedStops.length;
    if (!n) {
      hint.textContent = "~$0.60 / 10 stops";
    } else {
      const cost = ((n + 1) * (n + 1) * 0.005).toFixed(2);
      hint.textContent = `~$${cost} / ${n} stop${n !== 1 ? "s" : ""}`;
    }
  }
}

/* ── MOVE UP / DOWN ── */
function moveUp(btn) {
  const li     = btn.closest("li");
  const lis    = [...document.getElementById("routeList").querySelectorAll("li")];
  const domIdx = lis.indexOf(li);
  if (domIdx <= 0) return;
  [optimizedSchedule[domIdx-1], optimizedSchedule[domIdx]] =
    [optimizedSchedule[domIdx], optimizedSchedule[domIdx-1]];
  markRouteStale(); recalculateTimes(); renderSchedule(); redrawRouteOnMap();
}

function moveDown(btn) {
  const li     = btn.closest("li");
  const lis    = [...document.getElementById("routeList").querySelectorAll("li")];
  const domIdx = lis.indexOf(li);
  if (domIdx >= optimizedSchedule.length - 1) return;
  [optimizedSchedule[domIdx], optimizedSchedule[domIdx+1]] =
    [optimizedSchedule[domIdx+1], optimizedSchedule[domIdx]];
  markRouteStale(); recalculateTimes(); renderSchedule(); redrawRouteOnMap();
}

/* ── REVERSE ── */
function reverseRoute() {
  if (!isOptimized) { selectedStops.reverse(); renderStops(); return; }
  const real  = optimizedSchedule.filter(s => !s.isLunch);
  const lunch = optimizedSchedule.find(s => s.isLunch) || null;
  real.reverse();
  optimizedSchedule = lunch ? [...real, lunch] : real;
  markRouteStale(); recalculateTimes(); renderSchedule(); redrawRouteOnMap();
}

/* ── RESTART ── */
function restartRoute() {
  if ((selectedStops.length || isOptimized) &&
      !confirm("Start over? This will clear all stops and the current route.")) return;
  selectedStops = []; optimizedSchedule = []; isOptimized = false;
  durationMatrix = []; startMinutes = 9 * 60; currentRouteId = null;
  lastStats = { total_duration:0, driving_duration:0, service_duration:0, distance:0 };
  clearRouteMarkers();
  if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }
  document.getElementById("preOptSection").classList.remove("hidden");
  document.getElementById("preOptSearch").classList.remove("hidden");
  renderStops();
  document.getElementById("scheduleSection").classList.add("hidden");
  document.getElementById("workInSection").classList.add("hidden");
  document.getElementById("saveRouteBtn").classList.add("hidden");
  document.getElementById("updateRouteBtn").classList.add("hidden");
  document.getElementById("addMoreBtn").classList.add("hidden");
  document.getElementById("changeStartBtn").classList.add("hidden");
  document.getElementById("warningBox").classList.add("hidden");
  document.getElementById("workInBox").value = "";
  document.getElementById("saveRouteName").value  = "";
  document.getElementById("saveAssignedTo").value = "";
  document.getElementById("saveRouteDate").value  = "";
  document.getElementById("routeNameField").value  = "";
  document.getElementById("assignedToField").value = "";
  document.getElementById("routeDateField").value  = "";
  document.getElementById("routeNotesField").value   = "";
  document.getElementById("notesPublicField").checked = false;
  ["totalTime","drivingTime","serviceTime","distance"].forEach(id =>
    document.getElementById(id).textContent = "—");
}

/* ── REMOVE STOP ── */
function removeStop(id) {
  if (id === "lunch") {
    document.getElementById("lunchEnabled").checked = false;
    optimizedSchedule = optimizedSchedule.filter(s => !s.isLunch);
    recalculateTimes(); renderSchedule(); return;
  }

  const entry = optimizedSchedule.find(s => s._id === id);
  if (entry && markers[entry.name]) { map.removeLayer(markers[entry.name]); delete markers[entry.name]; }

  selectedStops     = selectedStops.filter(s => s._id !== id);
  optimizedSchedule = optimizedSchedule.filter(s => s._id !== id);

  if (isOptimized) {
    const real = optimizedSchedule.filter(s => !s.isLunch);
    if (!real.length) { restartRoute(); return; }
    recalculateTimes(); renderSchedule(); redrawRouteOnMap();
  } else {
    renderStops();
  }
}

/* ── ADD STOP (pre-opt) ── */
function addStop(property, asCheckin = false, asPriority = false) {
  if (selectedStops.find(s => s.name === property.name)) return;
  selectedStops.push({
    _id: makeStopId(), name:property.name, lat:property.lat, lng:property.lng,
    arrival: asCheckin || asPriority, priority_checkin: asPriority, serviceMinutes: 60
  });
  renderStops();
}

/* ── DRAG AND DROP ── */
let dragSrcIdx = null;

function wireDragEvents(li, scheduleIdx) {
  li.addEventListener("dragstart", e => {
    dragSrcIdx = scheduleIdx;
    li.classList.add("dragging");
    e.dataTransfer.effectAllowed = "move";
  });
  li.addEventListener("dragend", () => {
    li.classList.remove("dragging");
    document.querySelectorAll("#routeList li").forEach(el => el.classList.remove("drag-over"));
  });
  li.addEventListener("dragover", e => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    document.querySelectorAll("#routeList li").forEach(el => el.classList.remove("drag-over"));
    li.classList.add("drag-over");
  });
  li.addEventListener("dragleave", () => { li.classList.remove("drag-over"); });
  li.addEventListener("drop", e => {
    e.preventDefault();
    li.classList.remove("drag-over");
    const dropIdx = scheduleIdx;
    if (dragSrcIdx === null || dragSrcIdx === dropIdx) return;
    const moved    = optimizedSchedule.splice(dragSrcIdx, 1)[0];
    const insertAt = dragSrcIdx < dropIdx ? dropIdx - 1 : dropIdx;
    optimizedSchedule.splice(insertAt < 0 ? 0 : insertAt, 0, moved);
    dragSrcIdx = null;
    markRouteStale(); recalculateTimes(); renderSchedule(); redrawRouteOnMap();
  });
}

/* ── RENDER SCHEDULE ── */
function renderSchedule() {
  const list = document.getElementById("routeList");
  list.innerHTML = "";
  let num = 1;

  optimizedSchedule.forEach((stop, si) => {
    const li = document.createElement("li");

    // ── LUNCH ROW ──
    if (stop.isLunch) {
      li.className = "lunch-row";
      li.draggable = true;
      li.dataset.scheduleIdx = si;
      li.innerHTML = `
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-1 font-medium text-orange-800 text-sm">
            <span class="drag-handle">⠿</span>
            🍽 Lunch Break <span class="lunch-badge">30 MIN</span>
          </div>
          <button onclick="removeStop('lunch')"
                  class="move-btn !bg-red-50 !text-red-500 !border-red-200 hover:!bg-red-100">✕</button>
        </div>
        <div class="text-xs text-orange-600 mt-0.5">
          ${stop.eta} – ${minutesToHHMM(stop.eta_minutes + 30)}
        </div>`;
      wireDragEvents(li, si);
      list.appendChild(li); return;
    }

    // ── GAP ROW ──
    if (stop.isGap) {
      li.className = "gap-row";
      li.draggable = true;
      li.dataset.scheduleIdx = si;
      li.innerHTML = `
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-2 font-medium text-gray-600 text-sm">
            <span class="drag-handle">⠿</span>
            ⏸ Gap
            <select class="border rounded px-1 py-0.5 text-xs font-normal text-gray-700"
                    onchange="setGapMinutes('${stop._id}', parseInt(this.value))">
              ${generateTimeOptions(stop.serviceMinutes)}
            </select>
          </div>
          <button onclick="removeStop('${stop._id}')"
                  class="move-btn !bg-red-50 !text-red-500 !border-red-200 hover:!bg-red-100">✕</button>
        </div>
        <div class="text-xs text-gray-400 mt-0.5">
          ${stop.eta} – ${minutesToHHMM(stop.eta_minutes + stop.serviceMinutes)}
        </div>`;
      wireDragEvents(li, si);
      list.appendChild(li); return;
    }

    // ── DRIVE TIMES VIEW ──
    if (viewMode === 'drive') {
      let driveMin = 0;
      // Walk back past any lunch sentinel to find the real previous matrix index
      let prevMatrixIdx = 0; // default: depot
      for (let j = si - 1; j >= 0; j--) {
        if (!optimizedSchedule[j].isLunch) { prevMatrixIdx = optimizedSchedule[j].matrix_index; break; }
      }
      if (stop.matrix_index != null && durationMatrix[prevMatrixIdx] != null) {
        driveMin = Math.round((durationMatrix[prevMatrixIdx][stop.matrix_index] || 0) / 60);
      }

      li.className = "p-2 border-l-4 border-l-indigo-300 border border-gray-100 rounded bg-white hover:bg-indigo-50 transition-colors";
      li.innerHTML = `
        <div class="flex items-center justify-between gap-1">
          <div class="font-medium text-sm flex items-center gap-2 flex-1 min-w-0">
            <span class="text-indigo-600 font-bold shrink-0">${num}.</span>
            <span class="truncate max-w-[160px]" title="${stop.name}">${stop.name}</span>
          </div>
          <div class="flex gap-1 shrink-0">
            <button class="move-btn" onclick="moveUp(this)"   ${si===0?"disabled":""}>▲</button>
            <button class="move-btn" onclick="moveDown(this)" ${si===optimizedSchedule.length-1?"disabled":""}>▼</button>
            <button onclick="removeStop('${stop._id}')"
                    class="move-btn !bg-red-50 !text-red-500 !border-red-200 hover:!bg-red-100">✕</button>
          </div>
        </div>
        <div class="text-xs text-gray-400 mt-0.5">
          🚗 ${driveMin < 1 ? '< 1' : driveMin} min drive${stop.arrival
            ? (stop.priority_checkin
                ? ' &nbsp;·&nbsp; <span style="background:#ede9fe;color:#6d28d9;font-weight:700;padding:1px 6px;border-radius:999px;font-size:0.6rem;">CHECK-IN by 12PM</span>'
                : ' &nbsp;·&nbsp; <span style="background:#dcfce7;color:#15803d;font-weight:700;padding:1px 6px;border-radius:999px;font-size:0.6rem;">CHECK-IN</span>')
            : ''}
        </div>`;
      li.addEventListener("mouseenter", () => { const m=markers[stop.name]; if(m){map.panTo(m.getLatLng());m.openPopup();} });
      li.addEventListener("mouseleave", () => { const m=markers[stop.name]; if(m) m.closePopup(); });
      list.appendChild(li); num++; return;
    }

    // ── FULL SCHEDULE VIEW ──
    const dep = minutesToHHMM(stop.eta_minutes + stop.serviceMinutes);

    // Drive time from previous real stop (or depot) to this stop
    let driveMin = 0;
    const prevStop = si > 0 ? optimizedSchedule[si - 1] : null;
    const fromIdx  = prevStop && !prevStop.isLunch
      ? prevStop.matrix_index
      : prevStop && prevStop.isLunch
        ? (() => {
            for (let j = si - 2; j >= 0; j--) {
              if (!optimizedSchedule[j].isLunch) return optimizedSchedule[j].matrix_index;
            }
            return 0;
          })()
        : 0; // depot
    if (
      durationMatrix.length > 0 &&
      fromIdx != null &&
      stop.matrix_index != null &&
      durationMatrix[fromIdx] != null &&
      durationMatrix[fromIdx][stop.matrix_index] != null
    ) {
      driveMin = Math.round(durationMatrix[fromIdx][stop.matrix_index] / 60);
    }

    if (si > 0) {
      const driveEl = document.createElement("div");
      driveEl.className = "flex items-center gap-2 px-1 py-0.5";
      const prevWasLunch = prevStop && prevStop.isLunch;
      driveEl.innerHTML = `
        <div class="flex-1 border-t border-dashed border-gray-200"></div>
        <span class="text-xs text-gray-400 font-medium shrink-0">
          🚗 ${driveMin < 1 ? '&lt;1' : driveMin} min${prevWasLunch ? ' (after lunch)' : ''}
        </span>
        <div class="flex-1 border-t border-dashed border-gray-200"></div>`;
      list.appendChild(driveEl);
    }

    let badge = "";
    if (stop.priority_checkin && stop.priority_late)
      badge = `<span class="late-badge">PRIORITY LATE</span>`;
    else if (stop.priority_checkin)
      badge = `<span class="checkin-badge">CHECK-IN</span><span class="priority-badge">by 12PM</span>`;
    else if (stop.arrival && stop.late)
      badge = `<span class="late-badge">LATE</span>`;
    else if (stop.arrival)
      badge = `<span class="checkin-badge">CHECK-IN</span>`;

    let actionBtns = "";
    if (stop.arrival && stop.priority_checkin) {
      actionBtns = `
        <button onclick="toggleSchedulePriority('${stop._id}')"
                class="text-xs text-violet-700 hover:text-gray-500 font-medium">★ Priority</button>
        <button onclick="toggleScheduleCheckin('${stop._id}')"
                class="text-xs text-green-700 hover:text-red-500 font-medium">✓ Check-in</button>`;
    } else if (stop.arrival) {
      actionBtns = `
        <button onclick="toggleSchedulePriority('${stop._id}')"
                class="text-xs text-gray-400 hover:text-violet-600 font-medium">+ Priority</button>
        <button onclick="toggleScheduleCheckin('${stop._id}')"
                class="text-xs text-green-700 hover:text-red-500 font-medium">✓ Check-in</button>`;
    } else {
      actionBtns = `
        <button onclick="toggleScheduleCheckin('${stop._id}')"
                class="text-xs text-gray-400 hover:text-green-600 font-medium">+ Check-in</button>`;
    }

    li.className = "p-2 border rounded bg-white hover:bg-indigo-50 transition-colors";
    li.draggable = true;
    li.dataset.scheduleIdx = si;
    li.innerHTML = `
      <div class="flex items-start justify-between gap-1">
        <div class="font-medium text-sm flex items-center flex-wrap gap-1 flex-1 min-w-0">
          <span class="drag-handle" title="Drag to reorder">⠿</span>
          <span class="text-indigo-600 font-bold shrink-0">${num}.</span>
          <span class="truncate max-w-[100px]" title="${stop.name}">${stop.name}</span>
          ${badge}
        </div>
        <div class="flex gap-1 shrink-0">
          <button onclick="removeStop('${stop._id}')"
                  class="move-btn !bg-red-50 !text-red-500 !border-red-200 hover:!bg-red-100">✕</button>
        </div>
      </div>
      <div class="text-xs text-gray-500 mt-0.5 flex items-center gap-3 flex-wrap">
        <span>Arrive: ${stop.eta} &nbsp;|&nbsp; <span class="text-sm font-bold text-gray-800">Depart: ${dep}</span></span>
        <select class="service-time-select"
                onchange="setServiceMinutes('${stop._id}', parseInt(this.value))">
          ${generateTimeOptions(stop.serviceMinutes)}
        </select>
        ${actionBtns}
      </div>`;

    li.addEventListener("mouseenter", () => { const m=markers[stop.name]; if(m){map.panTo(m.getLatLng());m.openPopup();} });
    li.addEventListener("mouseleave", () => { const m=markers[stop.name]; if(m) m.closePopup(); });
    wireDragEvents(li, si);
    list.appendChild(li);
    num++;
  });
}
