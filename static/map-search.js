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
  renderAddMoreSugg(this.value.trim());
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

function renderAddMoreSugg(rawText = "") {
  addMoreSuggestions.innerHTML = "";
  if (!addMoreCurrent.length && !rawText) { addMoreSuggestions.classList.add("hidden"); return; }
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

  if (rawText) {
    const anyAddr = document.createElement("div");
    anyAddr.className = "sugg-item sugg-any-address";
    anyAddr.innerHTML = `<span class="sugg-item-name" style="color:#6366f1;">📍 Add "${rawText}" as address…</span>`;
    anyAddr.addEventListener("click", () => geocodeAndStageStop(rawText, false, false));
    addMoreSuggestions.appendChild(anyAddr);
  }

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
    div.className = "flex items-center justify-between bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm gap-2";
    div.innerHTML = `
      <span class="truncate text-gray-800 font-medium flex-1">${s.name}</span>
      <span class="text-xs ${s.priority_checkin ? 'text-violet-600 font-bold' : s.arrival ? 'text-green-600 font-medium' : 'text-gray-400'} shrink-0">
        ${s.priority_checkin ? '★ Priority' : s.arrival ? '✓ Check-in' : 'Stop'}
      </span>
      <select class="border rounded px-1 py-0.5 text-xs shrink-0">
        ${generateTimeOptions(s.serviceMinutes)}
      </select>
      <button class="text-red-400 hover:text-red-600 text-xs shrink-0">✕</button>`;
    div.querySelector("select").addEventListener("change", function() {
      s.serviceMinutes = parseInt(this.value);
    });
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
function _isDefaultLocation(loc) {
  return (Math.abs(loc.lat - DEFAULT_START_LOCATION.lat) < 1e-4 &&
          Math.abs(loc.lng - DEFAULT_START_LOCATION.lng) < 1e-4);
}

function _highlightCustomDepot() {
  const pill    = document.querySelector("button[onclick='toggleCustomStart()']");
  const label   = document.getElementById("customStartLabel");
  const isCustomStart = !_isDefaultLocation(startLocation);
  const isCustomEnd   = !_isDefaultLocation(endLocation);
  const isCustom = isCustomStart || isCustomEnd;

  if (isCustom) {
    pill.classList.remove("bg-gray-50");
    pill.classList.add("bg-amber-50", "border-amber-300", "ring-1", "ring-amber-300");
    label.classList.add("text-amber-700");
    label.classList.remove("text-gray-800");
  } else {
    pill.classList.add("bg-gray-50");
    pill.classList.remove("bg-amber-50", "border-amber-300", "ring-1", "ring-amber-300");
    label.classList.remove("text-amber-700");
    label.classList.add("text-gray-800");
  }
}

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
  _highlightCustomDepot();
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

async function geocodeAndStageStop(address, asCheckin, asPriority) {
  try {
    const loc = await geocodeAddress(address);
    stageStop({ name: loc.name, lat: loc.lat, lng: loc.lng }, asCheckin, asPriority);
    closeAddMoreSugg(true);
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

/* ── BREEZEWAY IMPORT ── */
async function runBwImport() {
  // Auto-fill from route fields if import fields are empty
  const dateInput     = document.getElementById("bwImportDate");
  const assigneeInput = document.getElementById("bwImportAssignee");
  if (!dateInput.value) {
    const routeDate = document.getElementById("routeDateField").value;
    if (routeDate) dateInput.value = routeDate;
  }
  if (!assigneeInput.value.trim()) {
    const routeAssignee = document.getElementById("assignedToField").value.trim();
    if (routeAssignee) assigneeInput.value = routeAssignee;
  }

  const date      = dateInput.value;
  const rawNames  = assigneeInput.value.trim();
  const resultEl  = document.getElementById("bwImportResult");
  const btn       = document.getElementById("bwImportBtn");

  if (!date) {
    _bwImportMsg("Please select a date.", "red");
    return;
  }

  // Parse comma-separated names into list
  const assignees = rawNames ? rawNames.split(",").map(s => s.trim()).filter(Boolean) : [];
  const payload   = assignees.length > 1
    ? {date, assignees}
    : {date, assignee: assignees[0] || ""};

  btn.disabled    = true;
  btn.textContent = "Importing…";
  resultEl.classList.add("hidden");

  try {
    const res  = await fetch("/api/bw-import", {
      method:  "POST",
      headers: {"Content-Type": "application/json"},
      body:    JSON.stringify(payload),
    });
    const data = await res.json();

    if (data.error)   { _bwImportMsg(data.error,   "red");  return; }
    if (data.message) { _bwImportMsg(data.message, "gray"); return; }

    if (data.by_assignee) {
      // Multi-employee: load first employee's stops, show tabbed sidebar
      _bwShowTaskSidebarMulti(date, data.by_assignee);
      document.getElementById("routeDateField").value = date;
      _bwImportMsg(
        `Loaded ${Object.keys(data.by_assignee).length} employees — tab to switch routes.`,
        "green"
      );
    } else {
      // Single employee
      let added = 0;
      for (const p of (data.matched || [])) {
        if (!selectedStops.find(s => s.name === p.name)) {
          addStop(p, !!p.arrival, false);
          added++;
        }
      }
      let msg   = added === 0 ? "All properties already in the list." : `Added ${added} stop${added !== 1 ? "s" : ""}.`;
      let color = "green";
      const unmatched = data.unmatched || [];
      if (unmatched.length) {
        msg  += ` Not found: ${unmatched.join(", ")}.`;
        color = added > 0 ? "amber" : "red";
      }
      _bwImportMsg(msg, color);
      _bwShowTaskSidebar(date, data.matched || []);
      _bwPlaceMarkers();
      document.getElementById("routeDateField").value  = date;
      document.getElementById("assignedToField").value = assignees[0] || "";
    }
  } catch (_) {
    _bwImportMsg("Network error — could not reach server.", "red");
  } finally {
    btn.disabled      = false;
    btn.textContent   = "Import Stops";
    btn.style.cssText = "";
  }
}

function _bwImportMsg(text, color) {
  const el = document.getElementById("bwImportResult");
  const styleMap = {
    green: "background:#f0fdf4; color:#15803d;",
    amber: "background:#fffbeb; color:#b45309;",
    red:   "background:#fef2f2; color:#b91c1c;",
    gray:  "background:#f9fafb; color:#4b5563;",
  };
  el.style.cssText = styleMap[color] || styleMap.gray;
  el.textContent = text;
  el.classList.remove("hidden");
}

// Stored multi-employee data for tab switching
let _bwByAssignee     = null;
let _bwActiveDate     = null;
let _bwTasksByPropName = {};  // {propertyName: [{task_name, assignees}]} — keyed for sync

let _bwSidebarMinimized = true;  // starts minimized

function bwSidebarMinimize() {
  _bwSidebarMinimized = !_bwSidebarMinimized;
  const sidebar = document.getElementById("bwTaskSidebar");
  const chevron = document.getElementById("bwSidebarChevron");
  const header  = document.getElementById("bwTaskSidebarHeader");
  const tabs    = document.getElementById("bwTaskTabs");
  const content = document.getElementById("bwTaskSidebarContent");
  if (_bwSidebarMinimized) {
    sidebar.style.width   = "2.5rem";
    header.style.display  = "none";
    tabs.style.display    = "none";
    content.style.display = "none";
    chevron.textContent   = "‹";
    chevron.title         = "Expand";
  } else {
    sidebar.style.width   = "18rem";
    header.style.display  = "";
    content.style.display = "";
    chevron.textContent   = "›";
    chevron.title         = "Minimize";
    // Show tabs only when there's tabbed content (saved routes or BW import)
    if (document.querySelectorAll("#bwTaskTabs button").length > 0) {
      tabs.style.display = "";
    }
    // Fill stop list immediately if a route is already loaded
    if (typeof _syncSidebarToSchedule === "function") _syncSidebarToSchedule();
    // Load daily routes only when not in BW-task mode (avoid clobbering BW state)
    if (!_dailyRoutesLoaded && !_bwByAssignee) _loadDailyRoutes();
  }
}


/* ── DAILY ROUTES PANEL ─────────────────────────────────────────── */

let _dailyRoutesLoaded = false;
let _dailyRoutesList   = [];   // [{id, name, assigned_to, route_date}, ...]
let _activeRouteTab    = null; // currently active route id or BW employee name

// Called on page load — wire up the date picker and load today's routes
(function initDailyRoutes() {
  const dateEl = document.getElementById("dailyRoutesDate");
  if (!dateEl) return;
  const today = new Date().toISOString().slice(0, 10);
  dateEl.value = today;
  dateEl.addEventListener("change", () => {
    _dailyRoutesLoaded = false;
    _loadDailyRoutes();
  });
})();

async function _loadDailyRoutes() {
  const dateEl = document.getElementById("dailyRoutesDate");
  const date   = dateEl ? dateEl.value : new Date().toISOString().slice(0, 10);
  try {
    const res  = await fetch(`/api/routes-for-date?date=${date}`);
    const data = await res.json();
    _dailyRoutesList   = data.routes || [];
    _dailyRoutesLoaded = true;
    _renderDailyRouteTabs();
  } catch (_) {}
}

function _renderDailyRouteTabs() {
  const tabsEl  = document.getElementById("bwTaskTabs");
  const content = document.getElementById("bwTaskSidebarContent");

  // Clear any BW-import tabs state
  _bwByAssignee = null;

  tabsEl.innerHTML = "";

  if (!_dailyRoutesList.length) {
    tabsEl.style.display = "none";
    content.innerHTML = `<div class="text-xs text-gray-400 text-center py-4">No routes saved for this date.</div>`;
    return;
  }

  tabsEl.style.display = "";
  for (const r of _dailyRoutesList) {
    const label = r.assigned_to || r.name;
    const btn = document.createElement("button");
    btn.className = "px-3 py-2.5 text-xs font-medium border-b-2 border-transparent text-gray-500 hover:text-gray-800 whitespace-nowrap cursor-pointer bg-transparent shrink-0";
    btn.textContent = label;
    btn.dataset.routeId = r.id;
    btn.addEventListener("click", () => _selectDailyRouteTab(r.id, label));
    tabsEl.appendChild(btn);
  }

  // Auto-select first route only if no route is currently active
  if (_dailyRoutesList.length && !currentRouteId) {
    const first = _dailyRoutesList[0];
    _selectDailyRouteTab(first.id, first.assigned_to || first.name);
  }
}

function _selectDailyRouteTab(routeId, label) {
  _activeRouteTab = routeId;

  // Update tab styles
  const tabsEl = document.getElementById("bwTaskTabs");
  for (const btn of tabsEl.querySelectorAll("button")) {
    const active = String(btn.dataset.routeId) === String(routeId);
    btn.className = active
      ? "px-3 py-2.5 text-xs font-medium border-b-2 border-indigo-500 text-indigo-700 whitespace-nowrap cursor-pointer bg-transparent shrink-0"
      : "px-3 py-2.5 text-xs font-medium border-b-2 border-transparent text-gray-500 hover:text-gray-800 whitespace-nowrap cursor-pointer bg-transparent shrink-0";
  }

  // Show loading state in content
  const content = document.getElementById("bwTaskSidebarContent");
  content.innerHTML = `<div class="text-xs text-gray-400 text-center py-4">Loading…</div>`;

  // Load the route onto the map using the shared loader
  loadRouteById(routeId).then(() => {
    // After load, show stop list in the panel
    content.innerHTML = "";
    const stops = optimizedSchedule.filter(s => !s.isLunch && !s.isGap);
    if (!stops.length) {
      content.innerHTML = `<div class="text-xs text-gray-400 text-center py-4">No stops.</div>`;
      return;
    }
    stops.forEach((s, i) => {
      const row = document.createElement("div");
      row.className = "flex items-start gap-2 py-1.5 border-b border-gray-100 last:border-0";
      const num = document.createElement("span");
      num.className = "text-xs text-gray-400 font-medium w-4 shrink-0 pt-px";
      num.textContent = i + 1;
      const name = document.createElement("span");
      name.className = "text-xs text-gray-700 leading-snug";
      name.textContent = s.name;
      if (s.arrival) name.className += " font-medium text-green-700";
      row.appendChild(num);
      row.appendChild(name);
      content.appendChild(row);
    });
  }).catch(() => {
    content.innerHTML = `<div class="text-xs text-red-400 text-center py-4">Failed to load.</div>`;
  });
}

/* ── BREEZEWAY TASK OVERLAY (after import) ─────────────────────── */

// Single-employee: show task content without tabs
function _bwShowTaskSidebar(date, matched) {
  if (!matched.length) return;
  _bwByAssignee = null;
  _bwActiveDate = date;
  _bwTasksByPropName = {};
  for (const p of matched) _bwTasksByPropName[p.name] = p.tasks || [];
  _syncSidebarToSchedule();
  _expandSidebarIfMinimized();
}

// Multi-employee: replace tabs with employee tabs
function _bwShowTaskSidebarMulti(date, byAssignee) {
  _bwByAssignee = byAssignee;
  _bwActiveDate = date;

  const tabsEl = document.getElementById("bwTaskTabs");
  tabsEl.innerHTML = "";
  tabsEl.style.display = "";

  for (const name of Object.keys(byAssignee)) {
    const btn = document.createElement("button");
    btn.className = "px-3 py-2.5 text-xs font-medium border-b-2 border-transparent text-gray-500 hover:text-gray-800 whitespace-nowrap cursor-pointer bg-transparent shrink-0";
    btn.textContent = name;
    btn.dataset.employee = name;
    btn.addEventListener("click", () => _bwSelectTab(name));
    tabsEl.appendChild(btn);
  }

  const firstName = Object.keys(byAssignee)[0];
  if (firstName) _bwSelectTab(firstName);

  _expandSidebarIfMinimized();
}

function _expandSidebarIfMinimized() {
  if (_bwSidebarMinimized) {
    _bwSidebarMinimized = false;
    const sidebar = document.getElementById("bwTaskSidebar");
    const chevron = document.getElementById("bwSidebarChevron");
    const header  = document.getElementById("bwTaskSidebarHeader");
    const content = document.getElementById("bwTaskSidebarContent");
    sidebar.style.width   = "18rem";
    header.style.display  = "";
    content.style.display = "";
    chevron.textContent   = "›";
    chevron.title         = "Minimize";
    if (typeof _syncSidebarToSchedule === "function") _syncSidebarToSchedule();
  }
}

function _bwSelectTab(name) {
  _activeRouteTab = name;
  const tabsEl = document.getElementById("bwTaskTabs");
  for (const btn of tabsEl.querySelectorAll("button")) {
    const active = btn.dataset.employee === name;
    btn.className = active
      ? "px-3 py-2.5 text-xs font-medium border-b-2 border-indigo-500 text-indigo-700 whitespace-nowrap cursor-pointer bg-transparent shrink-0"
      : "px-3 py-2.5 text-xs font-medium border-b-2 border-transparent text-gray-500 hover:text-gray-800 whitespace-nowrap cursor-pointer bg-transparent shrink-0";
  }

  const data = _bwByAssignee[name] || {};
  clearRouteMarkers();
  if (typeof routeLayer !== "undefined" && routeLayer) {
    map.removeLayer(routeLayer);
    routeLayer = null;
  }
  selectedStops     = [];
  optimizedSchedule = [];
  isOptimized       = false;
  durationMatrix    = [];

  // Hide stale post-opt DOM so old schedule cards don't linger
  document.getElementById("scheduleSection").classList.add("hidden");
  document.getElementById("workInSection").classList.add("hidden");
  document.getElementById("recalcTimesBtn").classList.add("hidden");
  document.getElementById("changeStartBtn").classList.add("hidden");
  document.getElementById("saveRouteBtn").classList.add("hidden");
  document.getElementById("updateRouteBtn").classList.add("hidden");
  document.getElementById("bwSyncResult").classList.add("hidden");

  // Build prop→tasks map for sidebar sync
  _bwTasksByPropName = {};
  for (const p of (data.matched || [])) _bwTasksByPropName[p.name] = p.tasks || [];

  renderStops();
  for (const p of (data.matched || [])) addStop(p, !!p.arrival, false);
  _bwPlaceMarkers();
  document.getElementById("assignedToField").value = name;
  _syncSidebarToSchedule();
}

function _bwPlaceMarkers() {
  clearRouteMarkers();
  const bounds = [];
  for (const stop of selectedStops) {
    if (!stop.lat || !stop.lng) continue;
    const m = L.marker([stop.lat, stop.lng], { icon: pickStopIcon(stop) })
      .addTo(map)
      .bindPopup(`<b>${stop.name}</b>${stop.arrival ? "<br><span style='color:#16a34a;font-weight:600'>Check-in</span>" : ""}`);
    activeRouteMarkers.push(m);
    markers[stop.name] = m;
    bounds.push([stop.lat, stop.lng]);
  }
  if (bounds.length) map.fitBounds(bounds, { padding: [60, 60], maxZoom: 14 });
  _syncSidebarToSchedule();
}

function _syncSidebarToSchedule() {
  const content = document.getElementById("bwTaskSidebarContent");
  if (!content || _bwSidebarMinimized) return;

  // Determine current stop order
  const stops = isOptimized
    ? optimizedSchedule.filter(s => !s.isLunch && !s.isGap)
    : selectedStops;

  const hasBwTasks = Object.keys(_bwTasksByPropName).length > 0;

  if (!stops.length) {
    if (hasBwTasks) content.innerHTML = `<div class="text-xs text-gray-400 text-center py-4">No stops yet.</div>`;
    return;
  }

  // Daily-routes mode (no BW tasks): keep stop list in sync with schedule order
  if (!hasBwTasks) {
    content.innerHTML = "";
    stops.forEach((s, i) => {
      const row = document.createElement("div");
      row.className = "flex items-start gap-2 py-1.5 border-b border-gray-100 last:border-0";
      const num = document.createElement("span");
      num.className = "text-xs text-gray-400 font-medium w-4 shrink-0 pt-px";
      num.textContent = i + 1;
      const name = document.createElement("span");
      name.className = "text-xs leading-snug " + (s.arrival ? "font-medium text-green-700" : "text-gray-700");
      name.textContent = s.name;
      row.appendChild(num); row.appendChild(name);
      content.appendChild(row);
    });
    return;
  }

  // BW-tasks mode: show stops in current schedule order with their tasks
  content.innerHTML = "";
  stops.forEach((s, i) => {
    const tasks = _bwTasksByPropName[s.name] || [];

    const card = document.createElement("div");
    card.className = "flex gap-2 py-1.5 border-b border-gray-100 last:border-0";

    const num = document.createElement("span");
    num.className = "text-xs text-gray-400 font-medium w-4 shrink-0 pt-0.5";
    num.textContent = i + 1;

    const body = document.createElement("div");
    body.className = "flex-1 min-w-0";

    const propName = document.createElement("div");
    propName.className = "text-xs font-semibold truncate " + (s.arrival ? "text-green-700" : "text-gray-800");
    propName.textContent = s.name;
    body.appendChild(propName);

    for (const t of tasks) {
      const taskRow = document.createElement("div");
      taskRow.className = "flex items-baseline gap-1 mt-0.5";
      const tname = document.createElement("span");
      tname.className = "text-xs text-gray-600";
      tname.textContent = t.task_name;
      taskRow.appendChild(tname);
      if (t.assignees && t.assignees.length) {
        const asgn = document.createElement("span");
        asgn.className = "text-xs text-gray-400";
        asgn.textContent = "· " + t.assignees.join(", ");
        taskRow.appendChild(asgn);
      }
      body.appendChild(taskRow);
    }

    card.appendChild(num); card.appendChild(body);
    content.appendChild(card);
  });
}

function _bwRenderTaskContent(matched) {
  const content = document.getElementById("bwTaskSidebarContent");
  content.innerHTML = "";

  if (!matched.length) {
    content.innerHTML = `<div class="text-xs text-gray-400 px-1 py-3 text-center">No stops found.</div>`;
    return;
  }

  for (const p of matched) {
    const card = document.createElement("div");
    card.className = "rounded-lg border border-gray-100 bg-gray-50 px-3 py-2";

    const title = document.createElement("div");
    title.className = "text-xs font-semibold text-gray-800 mb-1.5 truncate";
    title.textContent = p.name;
    card.appendChild(title);

    for (const t of (p.tasks || [])) {
      const row   = document.createElement("div");
      row.className = "mb-1";
      const tname = document.createElement("div");
      tname.className = "text-xs font-medium text-gray-700";
      tname.textContent = t.task_name;
      row.appendChild(tname);
      if (t.assignees && t.assignees.length) {
        const asgn = document.createElement("div");
        asgn.className = "text-xs text-gray-500 pl-2";
        asgn.textContent = t.assignees.join(", ");
        row.appendChild(asgn);
      }
      card.appendChild(row);
    }
    content.appendChild(card);
  }
}
