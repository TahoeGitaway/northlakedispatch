/* ================================================================
   SEARCH — main search box, work-in search, add-more search
   Depends on: properties, selectedStops, optimizedSchedule,
               durationMatrix, startLocation (globals)
================================================================ */

/* ── MAIN SEARCH BOX ── */
const searchBox   = document.getElementById("searchBox");
const suggestions = document.getElementById("suggestions");

searchBox.addEventListener("input", function() {
  const text = this.value.toLowerCase().trim();
  currentSuggestions = []; activeIndex = -1;
  if (text.length < 2) { closeSuggestions(false); return; }

  const already = new Set([
    ...selectedStops.map(s => s.name),
    ...optimizedSchedule.filter(s => !s.isLunch).map(s => s.name)
  ]);
  currentSuggestions = properties
    .filter(p => p.name && p.name.toLowerCase().includes(text) && !already.has(p.name))
    .slice(0, 10);
  renderSuggestions();
});

searchBox.addEventListener("keydown", function(e) {
  if (!currentSuggestions.length) return;
  if (e.key === "ArrowDown") {
    e.preventDefault(); activeIndex = (activeIndex + 1) % currentSuggestions.length; updateHighlight();
  } else if (e.key === "ArrowUp") {
    e.preventDefault(); activeIndex = (activeIndex - 1 + currentSuggestions.length) % currentSuggestions.length; updateHighlight();
  } else if (e.key === "Enter") {
    e.preventDefault(); addStop(currentSuggestions[activeIndex >= 0 ? activeIndex : 0], false, false); closeSuggestions(true);
  } else if (e.key === "c" || e.key === "C") {
    if (activeIndex >= 0) { e.preventDefault(); addStop(currentSuggestions[activeIndex], true, false); closeSuggestions(true); }
  } else if (e.key === "p" || e.key === "P") {
    if (activeIndex >= 0) { e.preventDefault(); addStop(currentSuggestions[activeIndex], true, true); closeSuggestions(true); }
  } else if (e.key === "Escape") { closeSuggestions(false); }
});

document.addEventListener("click", e => {
  if (!suggestions.contains(e.target) && e.target !== searchBox) closeSuggestions();
});

function renderSuggestions() {
  suggestions.innerHTML = "";
  if (!currentSuggestions.length) { suggestions.classList.add("hidden"); return; }
  suggestions.classList.remove("hidden");

  currentSuggestions.forEach((p, idx) => {
    const div = document.createElement("div");
    div.className = `sugg-item${idx === activeIndex ? " active" : ""}`;

    const nameSpan = document.createElement("span");
    nameSpan.className = "sugg-item-name";
    nameSpan.textContent = p.name;
    nameSpan.addEventListener("click", () => { addStop(p, false, false); closeSuggestions(true); });

    const btnWrap = document.createElement("span");
    btnWrap.className = "sugg-type-btns";
    [["stop","+ Stop",false,false],["checkin","✓ Check-in",true,false],["priority","★ Priority",true,true]]
      .forEach(([cls, label, ci, pr]) => {
        const btn = document.createElement("button");
        btn.className = `sugg-type-btn ${cls}`;
        btn.textContent = label;
        btn.addEventListener("click", e => { e.stopPropagation(); addStop(p, ci, pr); closeSuggestions(true); });
        btnWrap.appendChild(btn);
      });

    div.addEventListener("mouseenter", () => { activeIndex = idx; updateHighlight(); });
    div.appendChild(nameSpan);
    div.appendChild(btnWrap);
    suggestions.appendChild(div);
  });

  // "Search any address" fallback row
  const anyAddr = document.createElement("div");
  anyAddr.className = "sugg-item sugg-any-address";
  const anyText = searchBox.value.trim();
  anyAddr.innerHTML = `<span class="sugg-item-name" style="color:#6366f1;">
    📍 Add "${anyText}" as address…</span>`;
  anyAddr.addEventListener("click", () => geocodeAndAddStop(anyText, false, false));
  suggestions.appendChild(anyAddr);

  const hint = document.createElement("div");
  hint.className = "suggestion-hint";
  hint.innerHTML = `<kbd>Enter</kbd> add &nbsp;<kbd>C</kbd> check-in &nbsp;<kbd>P</kbd> priority &nbsp;<kbd>↑↓</kbd> navigate`;
  suggestions.appendChild(hint);
}

function updateHighlight() {
  [...suggestions.querySelectorAll(".sugg-item")].forEach((el, i) =>
    el.classList.toggle("active", i === activeIndex));
}
function closeSuggestions(clearInput = false) {
  suggestions.classList.add("hidden"); suggestions.innerHTML = "";
  if (clearInput) searchBox.value = "";
  currentSuggestions = []; activeIndex = -1;
}

/* ── WORK-IN SEARCH BOX ── */
const workInBox         = document.getElementById("workInBox");
const workInSuggestions = document.getElementById("workInSuggestions");
let workInCurrent = [];
let workInIndex   = -1;

workInBox.addEventListener("input", function() {
  const text = this.value.toLowerCase().trim();
  workInCurrent = []; workInIndex = -1;
  if (text.length < 2) { closeWorkIn(false); return; }

  const already = new Set(optimizedSchedule.filter(s => !s.isLunch).map(s => s.name));
  workInCurrent = properties
    .filter(p => p.name && p.name.toLowerCase().includes(text) && !already.has(p.name))
    .slice(0, 10);
  renderWorkInSuggestions();
});

workInBox.addEventListener("keydown", function(e) {
  if (!workInCurrent.length) return;
  if (e.key === "ArrowDown") { e.preventDefault(); workInIndex = (workInIndex + 1) % workInCurrent.length; updateWorkInHighlight(); }
  else if (e.key === "ArrowUp") { e.preventDefault(); workInIndex = (workInIndex - 1 + workInCurrent.length) % workInCurrent.length; updateWorkInHighlight(); }
  else if (e.key === "Enter") { e.preventDefault(); workInStop(workInCurrent[workInIndex >= 0 ? workInIndex : 0], false, false); closeWorkIn(true); }
  else if (e.key === "c" || e.key === "C") { if (workInIndex >= 0) { e.preventDefault(); workInStop(workInCurrent[workInIndex], true, false); closeWorkIn(true); } }
  else if (e.key === "p" || e.key === "P") { if (workInIndex >= 0) { e.preventDefault(); workInStop(workInCurrent[workInIndex], true, true); closeWorkIn(true); } }
  else if (e.key === "Escape") { closeWorkIn(false); }
});

document.addEventListener("click", e => {
  if (!workInSuggestions.contains(e.target) && e.target !== workInBox) closeWorkIn(false);
});

function renderWorkInSuggestions() {
  workInSuggestions.innerHTML = "";
  if (!workInCurrent.length) { workInSuggestions.classList.add("hidden"); return; }
  workInSuggestions.classList.remove("hidden");

  workInCurrent.forEach((p, idx) => {
    const div = document.createElement("div");
    div.className = `sugg-item${idx === workInIndex ? " active" : ""}`;

    const nameSpan = document.createElement("span");
    nameSpan.className = "sugg-item-name";
    nameSpan.textContent = p.name;
    nameSpan.addEventListener("click", () => { workInStop(p, false, false); closeWorkIn(true); });

    const btnWrap = document.createElement("span");
    btnWrap.className = "sugg-type-btns";
    [["stop","+ Stop",false,false],["checkin","✓ Check-in",true,false],["priority","★ Priority",true,true]]
      .forEach(([cls, label, ci, pr]) => {
        const btn = document.createElement("button");
        btn.className = `sugg-type-btn ${cls}`;
        btn.textContent = label;
        btn.addEventListener("click", e => { e.stopPropagation(); workInStop(p,ci,pr); closeWorkIn(true); });
        btnWrap.appendChild(btn);
      });

    div.addEventListener("mouseenter", () => { workInIndex = idx; updateWorkInHighlight(); });
    div.appendChild(nameSpan);
    div.appendChild(btnWrap);
    workInSuggestions.appendChild(div);
  });

  // "Search any address" fallback row
  const anyAddr = document.createElement("div");
  anyAddr.className = "sugg-item sugg-any-address";
  const anyText = workInBox.value.trim();
  anyAddr.innerHTML = `<span class="sugg-item-name" style="color:#6366f1;">
    📍 Add "${anyText}" as address…</span>`;
  anyAddr.addEventListener("click", () => geocodeAndWorkIn(anyText, false, false));
  workInSuggestions.appendChild(anyAddr);

  const hint = document.createElement("div");
  hint.className = "suggestion-hint";
  hint.innerHTML = `<kbd>Enter</kbd> add &nbsp;<kbd>C</kbd> check-in &nbsp;<kbd>P</kbd> priority`;
  workInSuggestions.appendChild(hint);
}

function updateWorkInHighlight() {
  [...workInSuggestions.querySelectorAll(".sugg-item")].forEach((el, i) =>
    el.classList.toggle("active", i === workInIndex));
}
function closeWorkIn(clearInput = false) {
  workInSuggestions.classList.add("hidden"); workInSuggestions.innerHTML = "";
  if (clearInput) workInBox.value = "";
  workInCurrent = []; workInIndex = -1;
}

/* ── WORK-IN STOP (insert mid-schedule, no re-optimize) ── */
async function workInStop(property, asCheckin, asPriority) {
  const existingReal = optimizedSchedule.filter(s => !s.isLunch);
  const allExisting  = [
    { lat: startLocation.lat, lng: startLocation.lng },
    ...existingReal.map(s => ({ lat: s.lat, lng: s.lng }))
  ];

  const overlay = document.getElementById("workInOverlay");
  overlay.classList.add("active");

  let fromNew = [], toNew = [];
  try {
    const res  = await fetch("/matrix-row", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_stop: { lat: property.lat, lng: property.lng }, existing_stops: allExisting })
    });
    const data = await res.json();
    fromNew = data.from_new || [];
    toNew   = data.to_new   || [];
  } catch(_) {}

  overlay.classList.remove("active");

  const newIdx = durationMatrix.length;
  const newRow = [...fromNew.slice(0, newIdx), 0];
  while (newRow.length <= newIdx) newRow.push(0);
  durationMatrix.push(newRow);

  durationMatrix.forEach((row, i) => {
    if (i === newIdx) return;
    row.push(i < toNew.length ? toNew[i] : 0);
  });

  const newStop = {
    _id: makeStopId(), name: property.name, lat: property.lat, lng: property.lng,
    arrival: asCheckin || asPriority, priority_checkin: asPriority,
    serviceMinutes: 60, matrix_index: newIdx,
    eta_minutes: 0, eta: "—", late: false, priority_late: false,
  };

  selectedStops.push({
    _id: newStop._id, name:property.name, lat:property.lat, lng:property.lng,
    arrival: newStop.arrival, priority_checkin: newStop.priority_checkin, serviceMinutes: 60
  });

  optimizedSchedule.push(newStop);
  recalculateTimes(); renderSchedule(); redrawRouteOnMap();
}

/* ── ADD MORE STOPS SEARCH ── */
const addMoreBox         = document.getElementById("addMoreBox");
const addMoreSuggestions = document.getElementById("addMoreSuggestions");
let addMoreStops   = [];
let addMoreCurrent = [];
let addMoreIndex   = -1;

addMoreBox.addEventListener("input", function() {
  const text = this.value.toLowerCase().trim();
  addMoreCurrent = []; addMoreIndex = -1;
  if (text.length < 2) { closeAddMoreSugg(false); return; }

  const already = new Set([
    ...optimizedSchedule.filter(s => !s.isLunch).map(s => s.name),
    ...addMoreStops.map(s => s.name)
  ]);
  addMoreCurrent = properties
    .filter(p => p.name && p.name.toLowerCase().includes(text) && !already.has(p.name))
    .slice(0, 10);
  renderAddMoreSugg();
});

addMoreBox.addEventListener("keydown", function(e) {
  if (!addMoreCurrent.length) return;
  if (e.key === "ArrowDown") { e.preventDefault(); addMoreIndex = (addMoreIndex+1) % addMoreCurrent.length; updateAddMoreHighlight(); }
  else if (e.key === "ArrowUp") { e.preventDefault(); addMoreIndex = (addMoreIndex-1+addMoreCurrent.length) % addMoreCurrent.length; updateAddMoreHighlight(); }
  else if (e.key === "Enter") { e.preventDefault(); stageStop(addMoreCurrent[addMoreIndex >= 0 ? addMoreIndex : 0], false, false); closeAddMoreSugg(true); }
  else if (e.key === "c" || e.key === "C") { if (addMoreIndex >= 0) { e.preventDefault(); stageStop(addMoreCurrent[addMoreIndex], true, false); closeAddMoreSugg(true); } }
  else if (e.key === "p" || e.key === "P") { if (addMoreIndex >= 0) { e.preventDefault(); stageStop(addMoreCurrent[addMoreIndex], true, true); closeAddMoreSugg(true); } }
  else if (e.key === "Escape") { closeAddMoreSugg(false); }
});

document.addEventListener("click", e => {
  if (!addMoreSuggestions.contains(e.target) && e.target !== addMoreBox) closeAddMoreSugg(false);
});

function renderAddMoreSugg() {
  addMoreSuggestions.innerHTML = "";
  if (!addMoreCurrent.length) { addMoreSuggestions.classList.add("hidden"); return; }
  addMoreSuggestions.classList.remove("hidden");

  addMoreCurrent.forEach((p, idx) => {
    const div = document.createElement("div");
    div.className = `sugg-item${idx === addMoreIndex ? " active" : ""}`;
    const nameSpan = document.createElement("span");
    nameSpan.className = "sugg-item-name";
    nameSpan.textContent = p.name;
    nameSpan.addEventListener("click", () => { stageStop(p, false, false); closeAddMoreSugg(true); });

    const btnWrap = document.createElement("span");
    btnWrap.className = "sugg-type-btns";
    [["stop","+ Stop",false,false],["checkin","✓ Check-in",true,false],["priority","★ Priority",true,true]]
      .forEach(([cls, label, ci, pr]) => {
        const btn = document.createElement("button");
        btn.className = `sugg-type-btn ${cls}`;
        btn.textContent = label;
        btn.addEventListener("click", ev => { ev.stopPropagation(); stageStop(p,ci,pr); closeAddMoreSugg(true); });
        btnWrap.appendChild(btn);
      });

    div.addEventListener("mouseenter", () => { addMoreIndex = idx; updateAddMoreHighlight(); });
    div.appendChild(nameSpan);
    div.appendChild(btnWrap);
    addMoreSuggestions.appendChild(div);
  });

  const hint = document.createElement("div");
  hint.className = "suggestion-hint";
  hint.innerHTML = `<kbd>Enter</kbd> add &nbsp;<kbd>C</kbd> check-in &nbsp;<kbd>P</kbd> priority`;
  addMoreSuggestions.appendChild(hint);
}

function updateAddMoreHighlight() {
  [...addMoreSuggestions.querySelectorAll(".sugg-item")].forEach((el,i) =>
    el.classList.toggle("active", i === addMoreIndex));
}
function closeAddMoreSugg(clearInput = false) {
  addMoreSuggestions.classList.add("hidden"); addMoreSuggestions.innerHTML = "";
  if (clearInput) addMoreBox.value = "";
  addMoreCurrent = []; addMoreIndex = -1;
}

function stageStop(property, asCheckin, asPriority) {
  if (addMoreStops.find(s => s.name === property.name)) return;
  const stop = {
    _id: makeStopId(), name: property.name, lat: property.lat, lng: property.lng,
    arrival: asCheckin || asPriority, priority_checkin: asPriority, serviceMinutes: 60
  };
  addMoreStops.push(stop);
  renderAddMoreList();
}

function renderAddMoreList() {
  const container = document.getElementById("addMoreStops");
  container.innerHTML = "";
  addMoreStops.forEach(s => {
    const div = document.createElement("div");
    div.className = "flex items-center justify-between bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm";
    div.innerHTML = `
      <span class="truncate text-gray-800 font-medium flex-1">${s.name}</span>
      <span class="text-xs mx-2 ${s.priority_checkin ? 'text-violet-600 font-bold' : s.arrival ? 'text-green-600 font-medium' : 'text-gray-400'}">
        ${s.priority_checkin ? '★ Priority' : s.arrival ? '✓ Check-in' : 'Stop'}
      </span>
      <button class="text-red-400 hover:text-red-600 text-xs ml-1">✕</button>`;
    div.querySelector("button").addEventListener("click", () => {
      addMoreStops = addMoreStops.filter(x => x._id !== s._id);
      renderAddMoreList();
    });
    container.appendChild(div);
  });
}

function openAddMore() {
  addMoreStops = [];
  document.getElementById("addMoreStops").innerHTML = "";
  document.getElementById("addMoreBox").value = "";
  document.getElementById("addMoreSection").classList.remove("hidden");
  document.getElementById("addMoreBox").focus();
}

function closeAddMore() {
  addMoreStops = [];
  document.getElementById("addMoreStops").innerHTML = "";
  document.getElementById("addMoreSection").classList.add("hidden");
}

/* ── CUSTOM START LOCATION ── */
/* ── POST-OPTIMIZE CHANGE START FORM (inline, no scrolling needed) ── */
function _updateStartEndPill() {
  const startName = startLocation.name || "Custom Start";
  const endName   = endLocation.name   || "Custom End";
  const short = n => n.length > 35 ? n.slice(0, 35) + "…" : n;
  document.getElementById("customStartLabel").textContent = short(startName);
  const endLabel = document.getElementById("customEndLabel");
  const same = (Math.abs(startLocation.lat - endLocation.lat) < 1e-5 &&
                Math.abs(startLocation.lng - endLocation.lng) < 1e-5);
  if (same) {
    endLabel.classList.add("hidden");
  } else {
    endLabel.textContent = "→ End: " + short(endName);
    endLabel.classList.remove("hidden");
  }
}

function toggleChangeStartForm() {
  const form = document.getElementById("changeStartForm");
  const isHidden = form.classList.toggle("hidden");
  if (!isHidden) {
    document.getElementById("changeStartInput").value = "";
    document.getElementById("changeStartError").classList.add("hidden");
    document.getElementById("changeStartCurrent").textContent = startLocation.name;
    document.getElementById("changeEndInput").value = "";
    document.getElementById("changeEndError").classList.add("hidden");
    document.getElementById("changeEndCurrent").textContent = endLocation.name;
    document.getElementById("changeStartInput").focus();
  }
}

function closeChangeStartForm() {
  document.getElementById("changeStartForm").classList.add("hidden");
}

async function applyChangeStart() {
  const input   = document.getElementById("changeStartInput");
  const errEl   = document.getElementById("changeStartError");
  const spinner = document.getElementById("changeStartSpinner");
  const address = input.value.trim();
  if (!address) return;

  errEl.classList.add("hidden");
  spinner.classList.remove("hidden");
  input.disabled = true;

  try {
    const loc = await geocodeAddress(address);
    startLocation = { name: loc.name, lat: loc.lat, lng: loc.lng };
    document.getElementById("changeStartCurrent").textContent = loc.name;
    input.value = "";
    _updateStartEndPill();
    _showStartChangedBanner();
  } catch (e) {
    errEl.textContent = "Address not found — try a more specific address.";
    errEl.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    input.disabled = false;
  }
}

function resetStartFromForm() {
  startLocation = { ...DEFAULT_START_LOCATION };
  document.getElementById("changeStartCurrent").textContent = DEFAULT_START_LOCATION.name;
  _updateStartEndPill();
  _showStartChangedBanner();
}

async function applyChangeEnd() {
  const input   = document.getElementById("changeEndInput");
  const errEl   = document.getElementById("changeEndError");
  const spinner = document.getElementById("changeEndSpinner");
  const address = input.value.trim();
  if (!address) return;

  errEl.classList.add("hidden");
  spinner.classList.remove("hidden");
  input.disabled = true;

  try {
    const loc = await geocodeAddress(address);
    endLocation = { name: loc.name, lat: loc.lat, lng: loc.lng };
    document.getElementById("changeEndCurrent").textContent = loc.name;
    input.value = "";
    _updateStartEndPill();
    _showStartChangedBanner();
  } catch (e) {
    errEl.textContent = "Address not found — try a more specific address.";
    errEl.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    input.disabled = false;
  }
}

function resetEndFromForm() {
  endLocation = { ...DEFAULT_END_LOCATION };
  document.getElementById("changeEndCurrent").textContent = DEFAULT_END_LOCATION.name;
  _updateStartEndPill();
  _showStartChangedBanner();
}

// Enter key support for the inline form inputs
document.getElementById("changeStartInput")
  .addEventListener("keydown", e => { if (e.key === "Enter") { e.preventDefault(); applyChangeStart(); } });
document.getElementById("changeEndInput")
  .addEventListener("keydown", e => { if (e.key === "Enter") { e.preventDefault(); applyChangeEnd(); } });

function toggleCustomStart() {
  const panel  = document.getElementById("customStartPanel");
  const hidden = panel.classList.toggle("hidden");
  if (!hidden) document.getElementById("customStartInput").focus();
}

async function applyCustomStart() {
  const input   = document.getElementById("customStartInput");
  const errEl   = document.getElementById("customStartError");
  const spinner = document.getElementById("customStartSpinner");
  const address = input.value.trim();
  if (!address) return;

  errEl.classList.add("hidden");
  spinner.classList.remove("hidden");
  input.disabled = true;

  try {
    const loc = await geocodeAddress(address);
    startLocation = { name: loc.name, lat: loc.lat, lng: loc.lng };
    document.getElementById("customStartPanel").classList.add("hidden");
    input.value = "";
    _updateStartEndPill();
    if (isOptimized) {
      _showStartChangedBanner();
    }
  } catch (e) {
    errEl.textContent = "Address not found — try a more specific address.";
    errEl.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    input.disabled = false;
  }
}

function resetStart() {
  startLocation = { ...DEFAULT_START_LOCATION };
  document.getElementById("customStartInput").value = "";
  document.getElementById("customStartError").classList.add("hidden");
  document.getElementById("customStartPanel").classList.add("hidden");
  _updateStartEndPill();
  if (isOptimized) {
    _showStartChangedBanner();
  }
}

function _showStartChangedBanner() {
  // Show a prompt in the warningBox telling the user to re-optimize
  const wb = document.getElementById("warningBox");
  wb.className = "text-sm p-2 rounded shift-warning";
  wb.innerHTML = `
    <div class="font-medium mb-1.5">Start location changed — re-optimize to update stop order.</div>
    <div class="flex gap-2">
      <button onclick="optimizeRoute(false)"
              class="flex-1 bg-amber-600 hover:bg-amber-700 text-white text-xs font-medium py-1.5 rounded-lg transition-colors">
        Re-optimize (free)
      </button>
      <button onclick="optimizeRoute(true)"
              class="flex-1 bg-emerald-700 hover:bg-emerald-800 text-white text-xs font-medium py-1.5 rounded-lg transition-colors">
        Re-optimize (Google Maps)
      </button>
    </div>`;
  wb.classList.remove("hidden");
  wb.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

async function applyCustomEnd() {
  const input   = document.getElementById("pillEndInput");
  const errEl   = document.getElementById("pillEndError");
  const spinner = document.getElementById("pillEndSpinner");
  const address = input.value.trim();
  if (!address) return;

  errEl.classList.add("hidden");
  spinner.classList.remove("hidden");
  input.disabled = true;

  try {
    const loc = await geocodeAddress(address);
    endLocation = { name: loc.name, lat: loc.lat, lng: loc.lng };
    input.value = "";
    _updateStartEndPill();
    if (isOptimized) _showStartChangedBanner();
  } catch (e) {
    errEl.textContent = "Address not found — try a more specific address.";
    errEl.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    input.disabled = false;
  }
}

function resetEnd() {
  endLocation = { ...DEFAULT_END_LOCATION };
  document.getElementById("pillEndInput").value = "";
  document.getElementById("pillEndError").classList.add("hidden");
  _updateStartEndPill();
  if (isOptimized) _showStartChangedBanner();
}

// Allow pressing Enter in the custom start/end inputs
document.getElementById("customStartInput")
  .addEventListener("keydown", e => { if (e.key === "Enter") { e.preventDefault(); applyCustomStart(); } });
document.getElementById("pillEndInput")
  .addEventListener("keydown", e => { if (e.key === "Enter") { e.preventDefault(); applyCustomEnd(); } });

/* ── ADDRESS FALLBACK in main search (any address outside DB) ── */
async function geocodeAndAddStop(address, asCheckin, asPriority) {
  try {
    const loc = await geocodeAddress(address);
    addStop({ name: loc.name, lat: loc.lat, lng: loc.lng }, asCheckin, asPriority);
    closeSuggestions(true);
  } catch (_) {
    alert("Address not found. Try a more specific address.");
  }
}

async function geocodeAndWorkIn(address, asCheckin, asPriority) {
  try {
    const loc = await geocodeAddress(address);
    workInStop({ name: loc.name, lat: loc.lat, lng: loc.lng }, asCheckin, asPriority);
    closeWorkIn(true);
  } catch (_) {
    alert("Address not found. Try a more specific address.");
  }
}

function reOptimize() {
  if (!addMoreStops.length) { alert("Add at least one new stop first."); return; }
  const currentReal = optimizedSchedule.filter(s => !s.isLunch);
  selectedStops = [
    ...currentReal.map(s => ({
      _id: s._id, name: s.name, lat: s.lat, lng: s.lng,
      arrival: s.arrival, priority_checkin: s.priority_checkin || false,
      serviceMinutes: s.serviceMinutes
    })),
    ...addMoreStops
  ];
  isOptimized = false;
  optimizedSchedule = [];
  durationMatrix = [];
  closeAddMore();
  optimizeRoute();
}
