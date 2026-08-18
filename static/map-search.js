/* ================================================================
   SEARCH — main search box, work-in search, add-more search
   Depends on: properties, selectedStops, optimizedSchedule,
               durationMatrix, startLocation (globals)
================================================================ */

/* bwFailureCause() / bwFailureCauseShort() come from static/bw-failure.js,
   loaded globally in base.html so every page words these warnings identically. */

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
    📍 Add "${anyText}" as address…</span><span style="font-size:0.6rem;color:#9ca3af;white-space:nowrap;margin-left:6px;">💲 Google lookup</span>`;
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
    📍 Add "${anyText}" as address…</span><span style="font-size:0.6rem;color:#9ca3af;white-space:nowrap;margin-left:6px;">💲 Google lookup</span>`;
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
    anyAddr.innerHTML = `<span class="sugg-item-name" style="color:#6366f1;">📍 Add "${rawText}" as address…</span><span style="font-size:0.6rem;color:#9ca3af;white-space:nowrap;margin-left:6px;">💲 Google lookup</span>`;
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
  const short = n => n.length > 35 ? n.slice(0, 35) + "…" : n;
  document.getElementById("customStartLabel").textContent = short(startName);
  // End address is not user-editable — never surface it in the pill.
  const endLabel = document.getElementById("customEndLabel");
  if (endLabel) endLabel.classList.add("hidden");
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
      go_first: s.go_first || false, serviceMinutes: s.serviceMinutes
    })),
    ...addMoreStops
  ];
  isOptimized = false;
  optimizedSchedule = [];
  durationMatrix = [];
  closeAddMore();
  optimizeRoute();
}

/* ── ESTIMATED SERVICE TIME (tentative on import; editable after) ── */
// Per task-type estimates. A stop's tentative time = sum of its tasks' estimates.
function estTaskMinutes(name) {
  const t = " " + String(name || "").toLowerCase().replace(/[^a-z0-9]+/g, " ") + " ";
  if (t.includes(" light walk thru ") || t.includes(" light walk through ")) return 15;
  if (t.includes(" walk thru ") || t.includes(" walk through ")) return 30;
  if (t.includes(" hot tub ")) return 30;
  if (t.includes(" post rental inspection ") || t.includes(" pri ")) return 60;  // PRI
  if (t.includes(" managed service")) return 60;                                  // inspection or arrival
  if (t.includes(" bear fence ")) return 0;                                       // disarm bear fence — no time
  if (t.includes(" property check ")) return 15;
  return 30;   // unknown task — modest default
}
function estServiceMinutes(tasks) {
  if (!tasks || !tasks.length) return 60;
  let sum = 0;
  for (const t of tasks) sum += estTaskMinutes(t.task_name || t.name || t);
  sum = Math.round(sum / 15) * 15;             // snap to 15-min steps (the dropdown's increments)
  return Math.max(15, Math.min(60, sum));      // auto-allot caps at 60 min; user can raise it via the dropdown
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

  // Multiple employees: open ONE new window with remaining names comma-separated.
  // That window runs the same logic, peels off the next name, opens another window, etc.
  // Browsers only allow one popup per user gesture — cascading handles any count.
  if (assignees.length > 1) {
    const [, ...rest] = assignees;
    window.open(`/?bw_date=${encodeURIComponent(date)}&bw_assignee=${encodeURIComponent(rest.join(","))}`, "_blank");
    // fall through — load first employee in this window
  }

  btn.disabled    = true;
  btn.textContent = "Importing…";
  resultEl.classList.add("hidden");
  const uncertainBox = document.getElementById("bwImportUncertain");
  if (uncertainBox) { uncertainBox.innerHTML = ""; uncertainBox.classList.add("hidden"); }
  _bwShowScanning();

  // Capture enough context to report a failure. A gateway timeout returns an HTML
  // error page, so res.json() throws and the real detail (status, body, how long it
  // took) is exactly what a developer needs — and exactly what used to be discarded.
  const _t0 = (window.performance && performance.now) ? performance.now() : Date.now();
  const _reqCtx = () => ({
    when_utc:   new Date().toISOString(),
    endpoint:   "/api/bw-import",
    request:    {date, assignee: assignees[0] || ""},
    elapsed_ms: Math.round(((window.performance && performance.now) ? performance.now() : Date.now()) - _t0),
    page_url:   location.pathname,
  });

  try {
    const res  = await fetch("/api/bw-import", {
      method:  "POST",
      headers: {"Content-Type": "application/json"},
      body:    JSON.stringify({date, assignee: assignees[0] || ""}),
    });

    // Read as TEXT first. A 502/504 from the platform gateway is an HTML page, and
    // res.json() would throw away the status and body along with the exception.
    const bodyText = await res.text();
    let data;
    try {
      data = JSON.parse(bodyText);
    } catch (parseErr) {
      _bwImportFail({
        ..._reqCtx(),
        failure: res.ok ? "server returned a non-JSON body"
                        : `server returned HTTP ${res.status}`,
        http_status: res.status,
        http_status_text: res.statusText,
        response_body: (bodyText || "").slice(0, 800),
        parse_error: String(parseErr),
      }, res.status === 502 || res.status === 504
           // "The scan may still be running — try again in a moment" told the user
           // nothing they could act on, and wasn't even reliably true. Say what
           // happened and what to do about it.
           ? "This took too long and the connection was cut, so nothing loaded. "
             + "If you started imports for more than one person at once, run them "
             + "ONE AT A TIME — each looks up every property, and together they slow "
             + "each other down. Click Import Stops to try again."
           : `The server returned HTTP ${res.status} instead of data.`);
      return;
    }

    // A server-reported error is still a failure worth reporting — route it through
    // the same path so it gets a Copy button. This is the COMMON case (auth
    // failures, misconfiguration) and it was the one left without any detail.
    if (data.error) {
      _bwImportFail({
        ..._reqCtx(),
        failure:            "server returned an error",
        http_status:        res.status,
        server_error:       data.error,
        failure_statuses:   data.failure_statuses || null,
        failed_properties:  (data.failed_properties ?? null),
        server_diagnostics: data.diagnostics || null,
      }, data.error);
      return;
    }
    // "No Breezeway tasks found for that date/assignee" is NOT the same claim as "this
    // person has nothing on". It is reached when the sweep returned tasks but none
    // survived the assignee filter — so if this person's houses were among the ones
    // Breezeway never answered for, the honest answer is "not yet", and the second or
    // third pass is what finds them.
    //
    // Only a COMPLETE sweep can say "nothing here" and stop. Anything less falls
    // through to the normal path below, because returning here skipped _bwAutoReset,
    // _bwLastImport and _bwAutoAfterResult — so the retry was never armed, and the one
    // result that most needed another pass was the only one that never got one. It was
    // also gray, which reads as "fine", with no count and no retry button.
    //
    // This lands hardest on the lightest routes: someone with fifteen stops almost
    // always has one house land in the first partial sweep, which keeps the retry
    // alive. Someone with two often doesn't — and a quiet day is the one nobody
    // questions, because "Trevor has nothing on the 19th" is perfectly plausible.
    if (data.message && !data.failed_properties) {
      _bwImportMsg(data.message, "gray");   // whole portfolio read — genuinely nothing here
      return;
    }

    {  // single employee
      // Confident matches are added immediately
      let added = 0;
      for (const p of (data.matched || [])) {
        if (!selectedStops.find(s => s.name === p.name)) {
          p.serviceMinutes = estServiceMinutes(p.tasks);   // tentative — editable after
          addStop(p, !!p.arrival, !!p.priority_checkin);
          added++;
        }
      }
      // With an incomplete sweep the server's "nothing found" line is the accurate
      // opener; "All matched properties already in the list" would be a lie, because
      // there were no matches at all.
      let msg   = data.message ? data.message
                : (added === 0 ? "All matched properties already in the list."
                               : `Added ${added} stop${added !== 1 ? "s" : ""}.`);
      let color = "green";
      const uncertain = data.uncertain || [];
      const unmatched = data.unmatched || [];
      if (uncertain.length) {
        msg  += ` ${uncertain.length} unsure match${uncertain.length !== 1 ? "es" : ""} — confirm below.`;
        color = "amber";
      }
      if (unmatched.length) {
        // Distinct from an "unsure match" — these homes aren't in the property DB
        // at all (no confident OR plausible match), so there's nothing to confirm.
        msg  += ` Not in your property DB (add ${unmatched.length !== 1 ? "them" : "it"} there first): ${unmatched.join(", ")}.`;
        color = added > 0 ? "amber" : "red";
      }
      // A partial import must still set up the route exactly like a clean one.
      // This used to `return` early when any property failed, which skipped the
      // three lines below — so the route DATE was never filled in, optimize then
      // defaulted it to TODAY, and a route planned for another day got saved and
      // synced to the wrong date. Failures change the message, nothing else.
      let retryCount = 0;
      // The day's check-in list didn't load, so NO stop got a Check-in tick — not
      // because none of these houses has an arrival, but because the question was
      // never answered. Untreated, that is indistinguishable from a correct import
      // of a day with no check-ins, which is how it went unnoticed.
      if (data.arrival_error) {
        msg  += ` ⚠ Check-in ticks are missing: couldn't read the day's arrivals`
              + ` (${data.arrival_error}). Set them by hand or re-import.`;
        color = "red";
      }
      _bwAutoReset();   // a fresh import gets a fresh retry budget, and Stop is forgotten
      if (data.failed_properties) {
        const f = bwFailureCause(data);
        // Offer to retry just the ones that failed. A full re-import spends ~442
        // calls to recover a handful, and those extra requests are what provoke
        // the throttling being retried.
        _bwLastImport = {date, assignee: assignees[0] || "", failed: data.failed_properties};
        retryCount = data.failed_properties;
        if (f.retry) {
          // The expected shape of a first pass: the 45s budget covers ~135 of ~442
          // properties, so the rest are simply not loaded YET. Spelling out the raw
          // failure breakdown here reads as an incident report about the design
          // working correctly — the progress block below says where it is and how
          // long is left, and the breakdown stays available on the copy-details path.
          color = "blue";
        } else {
          msg  += ` ${f.text}.`;   // genuinely stuck — name it in full
          color = "amber";
        }
        // Queue the first automatic retry. recovered=null means "first pass" —
        // nothing to compare against yet, so it starts at the prompt cadence.
        _bwAutoAfterResult(data.failed_properties, f.retry, null);
      }
      // recovered=0: the first pass has no earlier count to difference against, so
      // it contributes the totals but not a rate sample. The ETA falls back to the
      // nominal 180/min until a retry pass supplies a measured one.
      _bwNoteProgress(data, 0);
      _bwImportMsg(msg, color);
      // Must follow _bwImportMsg — that sets textContent and so wipes the controls.
      if (retryCount) _bwAutoRepaintStatus();
      _bwShowTaskSidebar(date, data.matched || []);
      _bwRenderUncertain(date, uncertain);
      _bwPlaceMarkers();
      document.getElementById("routeDateField").value  = date;
      document.getElementById("assignedToField").value = assignees[0] || "";
      if (typeof updateRouteMapOverlay === "function") updateRouteMapOverlay();
    }
  } catch (err) {
    // Was `catch (_)`, which threw the exception away and replaced it with a fixed
    // string — leaving nothing to report. Keep it.
    _bwImportFail({
      ..._reqCtx(),
      failure:    "request never completed",
      error_name: err && err.name,
      error_msg:  String(err && err.message || err),
    }, "Couldn't reach the server. It may have timed out mid-request.");
  } finally {
    btn.disabled      = false;
    btn.textContent   = "Import Stops";
    btn.style.cssText = "";
  }
}

/* "Try the missing N again" — refetch ONLY the properties that failed and merge
   them into what already loaded, instead of re-running the whole ~442-call sweep
   to recover a handful. The server keeps the failed refs for 15 minutes; after
   that a retry falls back to a full import. */
let _bwLastImport = null;

/* ── AUTOMATIC RETRY FOR THE IMPORT ─────────────────────────────────────────
   The import used to leave you watching the panel and clicking "try the missing
   N again" until the count reached zero. Nothing about that needed a human: the
   click always did the same thing, and the only judgement involved was how long
   to wait first.

   So it retries itself. The server re-stamps the held failed-refs list on every
   retry (dispatch.py → _bw_day_cache), so each attempt rolls the 15-minute
   window forward and it never expires underneath us — every gap below is far
   short of that.

   Pacing is PROGRESS-AWARE rather than a fixed ladder, because the two failure
   situations want opposite things. A retry that recovered houses means Breezeway
   is answering again — go again promptly while that holds. A retry that recovered
   nothing means the window is still shut, and hammering it is what caused the
   throttling in the first place — so back off hard. A fixed escalating ladder
   gets this wrong in both directions: too slow when it's working, too fast when
   it isn't. */
// Measured, not guessed: a retry only refetches the handful that failed, and the
// app's own rate gate relaxes after ~3s of quiet (bw_ratelimit.py → _QUIET_DECAY_S),
// so 8s is ample when Breezeway is answering. The backoff tail is capped at 120s
// because the held failed-refs list only survives 15 min between attempts.
const _BW_AUTO_PROGRESS_S = 8;                       // recovered something → go again soon
const _BW_AUTO_BACKOFF_S  = [20, 45, 90, 120, 120];  // recovered nothing → step back
// Whole run finishes in ~1 min if every attempt makes progress, ~10.5 min in the
// worst case where none of them do. Both are unattended, which is the point.
const _BW_AUTO_MAX_TRIES  = 7;

/* ── HOW LONG IS THIS GOING TO TAKE ─────────────────────────────────────────
   Breezeway's limit is confirmed at 200 req/min and the gate paces to 90% of it,
   so throughput is no longer a mystery — it is a constant, and the time to load
   the rest of a day is arithmetic the panel can simply show.

   That matters because a full day (~442 properties) CANNOT load in one pass: at
   ~3 properties/sec a 45s pass covers ~135 of them. A partial first result is the
   designed outcome, not a fault. Until now the panel reported that as "245
   properties couldn't be loaded" — describing an import that was 45% done and
   still working as if it had failed.

   Both constants mirror the server and must be changed with it:
     _BW_RATE_PER_SEC  <- routes/bw_ratelimit.py  _BASE_INTERVAL (200/min x 0.90)
     _BW_PASS_BUDGET_S <- routes/dispatch.py      _BW_IMPORT_BUDGET_S           */
const _BW_RATE_PER_SEC  = 3.0;
const _BW_PASS_BUDGET_S = 45;

let _bwProgress = {
  total:  0,   // properties in the day's sweep — fixed across passes
  loaded: 0,   // how many have come back so far
  rate:   0,   // measured properties/sec; 0 until a pass has been timed
  etaAt:  0,   // wall-clock ms the whole thing should be done, for a live countdown
};
let _bwPassStartedAt = 0;

/* Seconds to load `remaining`, including the pauses between passes. Uses the rate
   actually observed when there is one — the nominal figure ignores the arrival
   lookup, the DB join and the geocoding a pass also pays for, so it reads optimistic
   against a real import. */
function _bwEtaSeconds(remaining, ratePerSec) {
  if (remaining <= 0) return 0;
  const rate    = ratePerSec > 0 ? ratePerSec : _BW_RATE_PER_SEC;
  const perPass = Math.max(1, Math.floor(rate * _BW_PASS_BUDGET_S));
  const passes  = Math.ceil(remaining / perPass);
  return remaining / rate + Math.max(0, passes - 1) * _BW_AUTO_PROGRESS_S;
}

function _bwFmtDuration(s) {
  s = Math.max(0, Math.round(s));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), r = s % 60;
  return r ? `${m}m ${String(r).padStart(2, "0")}s` : `${m}m`;
}

/* The first pass blocks for up to the whole budget and there is no server-side
   progress to stream from it — but the budget IS a known constant, so the wait can
   be given a shape instead of a disabled button and a hidden panel. */
function _bwShowScanning() {
  _bwProgress      = { total: 0, loaded: 0, rate: 0,
                       etaAt: Date.now() + _BW_PASS_BUDGET_S * 1000 };
  _bwPassStartedAt = Date.now();
  const el = document.getElementById("bwImportResult");
  if (!el) return;
  _bwImportMsg("Scanning Breezeway for the day's properties…", "blue");
  const wrap = document.createElement("div");
  wrap.setAttribute("data-bw-status", "");
  wrap.innerHTML = `<div style="margin-top:5px;font-size:12px;">`
                 + `Up to <span data-bw-eta>…</span> for this first pass.</div>`;
  el.appendChild(wrap);
  _bwAutoTick();
  _bwSyncTick();
}

// Fold a finished pass into the estimate. An EMA rather than the latest reading,
// so one slow pass doesn't make the countdown lurch.
function _bwNoteProgress(data, recovered) {
  const total  = Number(data && data.properties_total)  || 0;
  const loaded = Number(data && data.properties_loaded) || 0;
  if (total) { _bwProgress.total = total; _bwProgress.loaded = loaded; }
  if (_bwPassStartedAt && recovered > 0) {
    const secs = (Date.now() - _bwPassStartedAt) / 1000;
    if (secs > 0.5) {
      const observed = recovered / secs;
      _bwProgress.rate = _bwProgress.rate ? (_bwProgress.rate * 0.5 + observed * 0.5) : observed;
    }
  }
  const remaining = Math.max(0, _bwProgress.total - _bwProgress.loaded);
  _bwProgress.etaAt = remaining
    ? Date.now() + _bwEtaSeconds(remaining, _bwProgress.rate) * 1000
    : 0;
}

let _bwAuto = {
  attempt:  0,      // automatic retries fired for this import
  stalls:   0,      // consecutive retries that recovered nothing (indexes the backoff)
  timerId:  null,
  tickId:   null,
  dueAt:    0,
  stopped:  false,  // user pressed Stop, or we gave up
  inFlight: false,
  // A POST to /api/bw-import is open right now. Distinct from `inFlight`, which is
  // display state that _bwAutoHalt clears deliberately without cancelling the
  // request it describes. Only the request's own finally clears this one.
  requestOpen: false,
  // Whether the outstanding failures are the kind a retry can fix (429/timeout/598
  // yes, 401/403/404 no). The repaint needs it to decide whether to offer the manual
  // button at all — without it, a repaint on a credentials failure offered a "try
  // now" that bwFailureCause had already ruled out.
  retryable: false,
  note:     "",
};

function _bwAutoClearTimers() {
  if (_bwAuto.timerId) clearTimeout(_bwAuto.timerId);
  if (_bwAuto.tickId)  clearInterval(_bwAuto.tickId);
  _bwAuto.timerId = _bwAuto.tickId = null;
  _bwAuto.dueAt   = 0;
}

// A brand-new import: forget that Stop was ever pressed and start counting over.
function _bwAutoReset() {
  _bwAutoClearTimers();
  _bwAuto.attempt = _bwAuto.stalls = 0;
  _bwAuto.stopped = _bwAuto.inFlight = false;
  _bwAuto.retryable = false;
  _bwAuto.note    = "";
}

// Stop retrying and say why, without touching an in-flight request.
function _bwAutoHalt(note) {
  _bwAutoClearTimers();
  _bwAuto.stopped  = true;
  _bwAuto.inFlight = false;
  _bwAuto.note     = note || "";
}

/* Tick the "about Xm Ys left" countdown. Runs while a pass is in flight as well as
   during the gap between passes — an estimate that freezes for the 45 seconds the
   app is actually working is exactly when it looks broken.

   Never counts to zero and sits there: if the estimate runs out while there is
   still work, it holds at "a few more seconds" rather than claiming to be done. */
function _bwAutoTick() {
  const el = document.querySelector("[data-bw-eta]");
  if (!el) return;
  const left = (_bwProgress.etaAt || 0) - Date.now();
  el.textContent = left > 1000 ? _bwFmtDuration(left / 1000) : "a few more seconds";
}

/* Decide what happens after an import or retry result lands.
   `recovered` is how many properties that attempt actually pulled in — the signal
   the pacing is built on. Pass null for the first import (nothing to compare). */
function _bwAutoAfterResult(stillFailed, retryable, recovered) {
  _bwAuto.inFlight  = false;
  _bwAuto.retryable = !!retryable;
  _bwAutoClearTimers();

  if (!stillFailed) {                 // everything loaded — nothing left to chase
    _bwAutoReset();
    return;
  }
  if (!retryable) {                   // 401/403/404 — retrying cannot fix it
    _bwAuto.note = "";
    return;
  }
  if (_bwAuto.stopped) return;
  if (_bwAuto.attempt >= _BW_AUTO_MAX_TRIES) {
    // Cause-neutral: the stall may be timeouts or this app's own limiter, not a
    // refusal by Breezeway. The message above already names what actually happened.
    _bwAuto.note = `Stopped after ${_BW_AUTO_MAX_TRIES} automatic tries — `
                 + `${stillFailed} still haven't loaded.`;
    return;
  }

  // recovered === null is the first import: treat it as progress so the first
  // retry goes out promptly rather than starting part-way up the backoff.
  if (recovered === null || recovered > 0) _bwAuto.stalls = 0;
  else                                     _bwAuto.stalls++;

  const wait = _bwAuto.stalls === 0
    ? _BW_AUTO_PROGRESS_S
    : _BW_AUTO_BACKOFF_S[Math.min(_bwAuto.stalls - 1, _BW_AUTO_BACKOFF_S.length - 1)];

  _bwAuto.dueAt   = Date.now() + wait * 1000;
  _bwAuto.timerId = setTimeout(_bwAutoFire, wait * 1000);
  // The 1s tick is NOT started here. It belongs to whatever is on screen, and the
  // countdown has to keep moving during a pass as well as between passes — started
  // alongside the schedule, it died every time a request went out and the estimate
  // froze for the 45 seconds the app was actually working. The repaint owns it.
}

function _bwAutoFire() {
  _bwAutoClearTimers();
  const el = document.getElementById("bwImportResult");
  // Panel gone, or the import context was replaced — stop rather than keep
  // spending Breezeway calls on a result nobody can see.
  if (_bwAuto.stopped || !el || el.classList.contains("hidden") || !_bwLastImport) {
    if (!_bwAuto.stopped) _bwAutoHalt("the import panel closed");
    return;
  }
  _bwAuto.attempt++;
  _bwAuto.inFlight = true;
  bwRetryMissingImport(null);          // null = automatic, no button to disable
}

function _bwAutoStop() {
  _bwAutoHalt("");
  _bwAutoRepaintStatus();
}

function _bwAutoResume() {
  _bwAuto.stopped = false;
  _bwAuto.stalls  = _bwAuto.attempt = 0;
  _bwAuto.note    = "";
  _bwAutoAfterResult((_bwLastImport && _bwLastImport.failed) || 0, true, null);
  _bwAutoRepaintStatus();
}

/* The progress block under the import message. Appended to #bwImportResult —
   _bwImportMsg sets textContent and so wipes children, which is why this always
   runs AFTER the message, never before.

   Leads with WHERE IT IS and HOW LONG IS LEFT, because at 200 req/min those are
   the two facts about a multi-pass import that a person actually needs. Whether
   the app is mid-request or waiting out an 8-second gap is machinery — it used to
   be the headline ("Loading the missing 61 automatically in 10s (try 2 of 7)")
   while the things worth knowing, how far along and how much longer, appeared
   nowhere at all. */
function _bwAutoStatusHtml(n) {
  const btn = (attr, label) =>
    `<button ${attr} style="text-decoration:underline;font-weight:700;color:inherit;`
    + `background:none;border:none;padding:0;cursor:pointer;font-size:12px;">${label}</button>`;

  const p    = _bwProgress;
  const have = p.total > 0;
  const done = have ? Math.min(100, Math.round((p.loaded / p.total) * 100)) : 0;

  // The bar. Rendered from the fraction that has LOADED, so it only ever advances —
  // a bar driven by the failure count would jump backwards on a stalled pass.
  const bar = have
    ? `<div style="margin-top:5px;height:4px;background:rgba(0,0,0,.10);border-radius:2px;overflow:hidden;">`
      + `<div data-bw-bar style="height:100%;width:${done}%;background:currentColor;opacity:.55;`
      + `transition:width .4s ease;"></div></div>`
    : "";
  const count = have ? `${p.loaded} of ${p.total} properties` : `${n} still to load`;

  if (_bwAuto.stopped) {
    return `<div style="margin-top:5px;font-size:12px;">${count} — paused`
         + (_bwAuto.note ? ` (${_escHtml(_bwAuto.note)})` : "")
         + `. ${btn("data-bw-resume", "Resume")}</div>${bar}`;
  }
  if (_bwAuto.note) {          // gave up, or a cause a retry cannot fix
    return `<div style="margin-top:5px;font-size:12px;">${count}. ${_escHtml(_bwAuto.note)}</div>${bar}`;
  }
  if (_bwAuto.inFlight || _bwAuto.timerId) {
    // One line for both states. The ETA already accounts for the gap between
    // passes, so "requesting" vs "waiting 8s" is a distinction without a difference
    // to anyone watching it.
    return `<div style="margin-top:5px;font-size:12px;">`
         + `Loading ${count} — about <span data-bw-eta>…</span> left `
         + `${btn("data-bw-stop", "Stop")}</div>${bar}`;
  }
  return "";
}

/* Should "Try the missing N now" be on screen at this moment?
   Two states where it must not be:

   * A retry is already talking to Breezeway. The button used to stay live through
     an automatic attempt, so the panel read "👉 Try the missing 61 now" directly
     above "↻ Loading the missing 61 now…" — two contradictory statements, and
     clicking fired a SECOND concurrent refetch of the same properties.
   * We are backing off after an attempt that recovered nothing. That wait exists
     precisely because Breezeway is throttling us; skipping it re-creates the
     condition it is there to clear, so "now" is the one thing that cannot help.
     Stop is the escape hatch during a backoff, not an immediate retry.

   The prompt 8s cadence after a productive attempt is left clickable — Breezeway is
   answering, so going early is harmless. */
function _bwManualRetryAllowed() {
  if (!_bwAuto.retryable) return false;
  if (_bwAuto.inFlight || _bwAuto.requestOpen) return false;
  return !(_bwAuto.timerId && _bwAuto.stalls > 0);
}

// Repaint the retry controls from state, leaving the message above them alone.
// The manual button is repainted here TOO, not just appended once at result time —
// it is retry state like everything else, and leaving it out of the repaint is what
// let it sit live above an in-flight attempt.
function _bwAutoRepaintStatus() {
  const el = document.getElementById("bwImportResult");
  if (!el) return;
  const n = (_bwLastImport && _bwLastImport.failed) || 0;

  const oldStatus = el.querySelector("[data-bw-status]");
  if (oldStatus) oldStatus.remove();
  const oldBtn = el.querySelector("[data-bw-manual]");
  if (oldBtn) oldBtn.remove();

  if (n && _bwLastImport && _bwManualRetryAllowed()) el.appendChild(_bwManualRetryBtn(n));

  const html = n ? _bwAutoStatusHtml(n) : "";
  if (html) {
    const wrap = document.createElement("div");
    wrap.setAttribute("data-bw-status", "");
    wrap.innerHTML = html;
    el.appendChild(wrap);
    _bwAutoTick();
  }
  // Always — including the nothing-to-show path, which is where the interval would
  // otherwise be left running against a panel that has finished.
  _bwSyncTick();
}

// Run the 1s countdown exactly when there is a countdown on screen. Tying it to the
// rendered element rather than to the retry schedule is what keeps it alive across
// the in-flight/waiting boundary, and what stops it leaking once the panel is done.
function _bwSyncTick() {
  const wanted = !!document.querySelector("[data-bw-eta]");
  if (wanted && !_bwAuto.tickId)  _bwAuto.tickId = setInterval(_bwAutoTick, 1000);
  if (!wanted && _bwAuto.tickId) { clearInterval(_bwAuto.tickId); _bwAuto.tickId = null; }
}

document.addEventListener("click", function (ev) {
  const t = ev.target;
  if (!t || !t.closest) return;
  if (t.closest("[data-bw-stop]"))   { ev.preventDefault(); _bwAutoStop();   return; }
  if (t.closest("[data-bw-resume]")) { ev.preventDefault(); _bwAutoResume(); return; }
});

/* The manual button stays. Auto-retry handles the normal case, but the button is
   what you reach for after pressing Stop, or when you don't want to wait out the
   countdown — and it's the fallback if the schedule ever gives up. Whether it is
   on screen at all is _bwManualRetryAllowed()'s call. */
function _bwManualRetryBtn(n) {
  const btn = document.createElement("button");
  btn.setAttribute("data-bw-manual", "");
  btn.textContent = `👉 Try the missing ${n} now`;
  btn.style.cssText = "display:block;margin-top:5px;text-decoration:underline;font-weight:700;"
                    + "background:none;border:none;padding:0;cursor:pointer;color:#b45309;font-size:12px;";
  btn.onclick = () => {
    _bwAutoClearTimers();          // hand the pending automatic attempt to this one
    // A manual try costs Breezeway exactly what an automatic one does, so it spends
    // from the same budget. Untracked, clicking repeatedly walked straight past
    // _BW_AUTO_MAX_TRIES — the ceiling only ever counted the timer's own attempts.
    _bwAuto.attempt++;
    _bwAuto.inFlight = true;
    bwRetryMissingImport(btn);
  };
  return btn;
}


/* btn is null when the retry was fired by the automatic schedule rather than a
   click — there is no button to disable, and the status line reports progress
   instead. */
async function bwRetryMissingImport(btn) {
  if (!_bwLastImport) return;
  // Last line of defence against two retries running at once. The button is hidden
  // while a request is open, but a stray timer, a double-click landing inside the
  // same tick, or a future caller must not be able to put a second POST on the wire:
  // both attempts refetch the same refs, and their responses race to merge into
  // selectedStops.
  if (_bwAuto.requestOpen) return;
  _bwAuto.requestOpen = true;
  const { date, assignee } = _bwLastImport;
  const original = btn ? btn.textContent : null;
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Retrying…";
  }
  _bwAuto.inFlight = true;
  _bwPassStartedAt = Date.now();   // times this pass, to keep the ETA honest
  _bwAutoRepaintStatus();
  try {
    const res  = await fetch("/api/bw-import", {
      method:  "POST",
      headers: {"Content-Type": "application/json"},
      body:    JSON.stringify({date, assignee, retry_failed: true}),
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); }
    catch (e) {
      // Stop the schedule, same as an error payload does. This path used to leave
      // inFlight set with nothing to clear it, so the panel claimed a load was
      // still running long after the request had come back unreadable.
      _bwAutoHalt("the retry didn't come back as data");
      _bwImportFail({ when_utc: new Date().toISOString(), endpoint: "/api/bw-import",
                      request: {date, assignee, retry_failed: true},
                      failure: `server returned HTTP ${res.status}`,
                      http_status: res.status, response_body: (text || "").slice(0, 800) },
                    "The retry didn't come back as data.");
      return;
    }
    if (data.error) {
      // A retry that comes back unusable stops the schedule and says why, rather
      // than counting down to another attempt that will fail the same way. The note
      // is left empty because the red message directly above already carries the
      // reason — repeating it in the status line just says it twice.
      _bwAutoHalt("");
      _bwImportMsg(data.error, "red");
      return;
    }

    // Add ONLY the stops this retry actually recovered.
    //
    // data.matched is the full merged list, not just the new houses — so adding
    // anything "not currently in selectedStops" silently resurrected stops the
    // user had deliberately deleted. _bwTasksByPropName holds every property the
    // previous pass already knew about, so anything in there is not new.
    const knownBefore = new Set(Object.keys(_bwTasksByPropName || {}));
    let added = 0;
    for (const p of (data.matched || [])) {
      if (knownBefore.has(p.name)) continue;                       // not new — leave it alone
      if (selectedStops.find(s => s.name === p.name)) continue;    // already on the route
      // Must go through addStop, same as the first-pass import. A raw push skips
      // renderStops(), so the recovered stop lands in selectedStops with a marker
      // and a sidebar row but NO card in the Selected Stops panel — and the search
      // box then refuses to re-offer it, because it filters out names already on
      // the route. It also skips _id/serviceMinutes, which remove and optimize need.
      p.serviceMinutes = estServiceMinutes(p.tasks);
      addStop(p, !!p.arrival, !!p.priority_checkin);
      added++;
    }
    if (added) {
      _bwPlaceMarkers();
      _bwShowTaskSidebar(date, data.matched || []);
    }
    // Report BOTH numbers. A retry that loads 11 properties none of which have a
    // task for this person adds 0 stops — reporting only "recovered 0 stops" reads
    // as "nothing happened" when real progress was made, and hides that clicking
    // again will keep working.
    const still     = data.failed_properties || 0;
    const wasFailed = (_bwLastImport && _bwLastImport.failed) || 0;
    const recovered = Math.max(0, wasFailed - still);
    const stopsBit  = added
      ? `added ${added} more stop${added === 1 ? "" : "s"}`
      : `no new stops for ${assignee || "this route"} among them`;

    if (still) {
      const f     = bwFailureCause(data);
      // Name what actually held them up. "Breezeway refused them again" was hardcoded,
      // so a stall caused by timeouts, or by this app's own limiter declining to send,
      // was reported as a refusal Breezeway never made.
      const cause = (typeof bwFailureCauseShort === "function")
        ? (bwFailureCauseShort(data).text || "") : "";
      _bwLastImport = {date, assignee, failed: still};
      // Settle the schedule BEFORE painting, so the status line already knows
      // whether another attempt is queued and how long the wait is.
      _bwAutoAfterResult(still, f.retry, recovered);
      _bwNoteProgress(data, recovered);
      // Blue, not amber, while this is still going: the progress block underneath
      // is now carrying the count and the ETA, so this line only has to report what
      // the pass just did. A run that is proceeding exactly as designed should not
      // be painted as a warning.
      _bwImportMsg(
        recovered === 0
          ? `No new properties that time${cause ? ` (${cause})` : ""} — easing off, then trying again.`
          : `Loaded ${recovered} more propert${recovered === 1 ? "y" : "ies"} — ${stopsBit}.`,
        f.retry ? "blue" : "amber");
      // One repaint either way — it reads f.retry back off _bwAuto.retryable and
      // decides for itself whether the manual button belongs on screen.
      _bwAutoRepaintStatus();
    } else {
      // Clear the held count as well as the schedule. It used to keep the last
      // non-zero `failed` after a fully successful retry, so anything that repainted
      // from state afterwards would render retry controls for properties that had
      // all loaded.
      _bwLastImport = {date, assignee, failed: 0};
      _bwAutoAfterResult(0, false, recovered);     // done — clears the schedule
      _bwNoteProgress(data, recovered);
      const total = _bwProgress.total;
      _bwImportMsg(
        `Loaded the last ${recovered} propert${recovered === 1 ? "y" : "ies"} — ${stopsBit}. `
        + `All ${total ? total + " " : ""}properties loaded.`, "green");
    }
  } catch (e) {
    if (btn) {
      btn.disabled = false;
      btn.textContent = original;
    }
    // Don't count down to another attempt after a transport failure — say it
    // stopped and leave the manual button as the way back in.
    _bwAutoHalt(`couldn't reach the server (${e.message})`);
    _bwImportMsg(`Retry failed: ${e.message}`, "red");
  } finally {
    // Release the guard BEFORE the final repaint, or the manual button — the way
    // back in after a transport failure — is suppressed by a request that has
    // already finished.
    _bwAuto.requestOpen = false;
    _bwAutoRepaintStatus();
  }
}

/* Show a human message AND keep the technical detail reachable. Previously an
   import failure printed a fixed string and dropped everything else, so there was
   nothing to send anyone — the user could only describe what they saw. */
let _bwLastImportError = null;
function _bwImportFail(diag, humanMsg) {
  _bwLastImportError = diag;
  const el = document.getElementById("bwImportResult");
  el.style.cssText = "background:#fef2f2; color:#b91c1c;";
  el.innerHTML = "";

  const p = document.createElement("div");
  p.textContent = humanMsg || "The import failed.";
  el.appendChild(p);

  const btn = document.createElement("button");
  btn.textContent = "Copy error details";
  btn.style.cssText = "margin-top:4px;text-decoration:underline;background:none;"
                    + "border:none;padding:0;cursor:pointer;color:#7f1d1d;font-size:11px;";
  btn.onclick = () => bwCopyDiagnostics(_bwLastImportError, btn);
  el.appendChild(btn);

  el.classList.remove("hidden");
  _bwSyncTick();   // the countdown's element went with the innerHTML wipe
  // Also log it, so it's recoverable from the console even if the box is dismissed.
  console.error("[bw-import] failed", diag);
}

function _bwImportMsg(text, color) {
  const el = document.getElementById("bwImportResult");
  const styleMap = {
    green: "background:#f0fdf4; color:#15803d;",
    amber: "background:#fffbeb; color:#b45309;",
    red:   "background:#fef2f2; color:#b91c1c;",
    gray:  "background:#f9fafb; color:#4b5563;",
    // Work in progress. A full day takes several passes by design now, and painting
    // that in warning-amber told people something was wrong when nothing was —
    // amber is reserved for outcomes that actually want attention.
    blue:  "background:#eff6ff; color:#1d4ed8;",
  };
  el.style.cssText = styleMap[color] || styleMap.gray;
  el.textContent = text;
  el.classList.remove("hidden");
}

function _fmtTaskDate(ds) {
  if (!ds) return "";
  const d = new Date(ds + "T00:00:00");
  return isNaN(d.getTime()) ? ds : d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// "PCI" as a standalone token in a task title = priority check-in (arrive by noon).
// Any punctuation counts as a separator, so "(PCI)", "PCI.", "PCI*" all still match.
function _titleHasPci(title) {
  return (" " + String(title || "").toLowerCase().replace(/[^a-z0-9]+/g, " ") + " ").includes(" pci ");
}

// Make the live Breezeway scan AUTHORITATIVE for check-in / priority-check-in flags.
//
// A stop's `arrival` can be wrong for reasons the route itself can't see: it was
// saved before the arrival moved, it was added by hand with the check-in button, it
// was worked in from a stale panel. Previously only PCI-titled houses were ever
// reconciled and the correction was one-directional everywhere else — the sidebar
// ORs a live arrival ON for display but never off, and never writes back. So a stop
// flagged as a check-in when it isn't stayed that way through every re-check, and
// this is not cosmetic: `arrival` drives the 4 PM deadline constraint in the
// optimizer and the lunch guard, so a wrong flag bends the actual route.
//
// The scan answers "is this house a check-in today" from the day's reservations, so
// for any house it actually read, FALSE is a finding rather than an absence. Those
// houses are set to match it in both directions. Everything else is left alone:
//
//   * a failed arrival lookup (arrival_error) → reconcile nothing at all, or one bad
//     fetch silently strips every check-in off the route
//   * a house whose own task fetch failed (unverified) → not evidence of anything
//   * a house the scan never covered → left exactly as the user left it
function _reconcileFlagsFromScan(data) {
  const currentTasks = data && data.current_tasks;
  if (!Array.isArray(currentTasks) || !currentTasks.length) return;
  // Couldn't read today's arrivals — every house would look like a non-arrival, and
  // clearing the route on the strength of that is the failure this guards against.
  // The panel says so in red; the flags stay untouched.
  if (data.arrival_error) return;

  const wantByProp = new Map();   // propLower -> {arrival, pci}
  for (const c of currentTasks) {
    // Older payloads without `pci` fall back to the title rule, as before.
    const hasPciTitle = (c.tasks || []).some(t =>
      _titleHasPci(typeof t === "string" ? t : ((t && (t.name || t.task_name || t.title)) || "")));
    const pci = ("pci" in c) ? !!c.pci : hasPciTitle;
    wantByProp.set((c.property || "").toLowerCase(),
                   { arrival: ("arrival" in c) ? (!!c.arrival || pci) : pci, pci });
  }
  // Houses Breezeway refused to answer for. "No task returned" and "never asked" look
  // identical from here, so they are not reconciled — the same rule that keeps them
  // out of the remove action.
  for (const u of (data.unverified || [])) {
    wantByProp.delete((u.property || "").toLowerCase());
  }
  if (!wantByProp.size) return;

  let changed = false;
  const allPci   = new Map();   // nameLower -> display name, for EVERY PCI stop on the route
  const promoted = new Set();   // nameLower of stops that just BECAME a PCI since save
  const gained   = new Map();   // nameLower -> display name: became a check-in
  const lost     = new Map();   // nameLower -> display name: was flagged, actually isn't
  const apply = s => {
    if (!s || s.isLunch || s.isGap) return;
    const key  = (s.name || "").toLowerCase();
    const want = wantByProp.get(key);
    if (want === undefined) return;                 // not covered by this scan → leave as-is
    if (!!s.arrival !== want.arrival) {
      (want.arrival ? gained : lost).set(key, s.name || "(unnamed stop)");
      s.arrival = want.arrival;
      changed = true;
    }
    if (!!s.priority_checkin !== want.pci) {
      // Track PCIs that flipped on since the save — an existing PCI is just as easy
      // to overlook in a long list, so the alert below lists them all either way.
      if (want.pci) promoted.add(key);
      s.priority_checkin = want.pci;
      changed = true;
    }
    if (want.pci) allPci.set(key, s.name || "(unnamed stop)");
  };
  (typeof selectedStops !== "undefined" ? selectedStops : []).forEach(apply);
  (typeof optimizedSchedule !== "undefined" ? optimizedSchedule : []).forEach(apply);

  // Never move a flag silently. A check-in appearing or disappearing changes when the
  // stop has to be finished, so it has to be visible as an event, not just a badge
  // that quietly looks different from a moment ago.
  if (gained.size || lost.size) {
    _rcFlagNotice([...gained.values()], [...lost.values()]);
  }

  // Repaint only when a flag actually moved, but ALERT whenever the route carries any
  // PCI — so existing priority check-ins are surfaced on reopen, not just new ones.
  if (changed) {
    if (typeof isOptimized !== "undefined" && isOptimized) {
      if (typeof recalculateTimes === "function") recalculateTimes();
      if (typeof renderSchedule === "function") renderSchedule();
      if (typeof redrawRouteOnMap === "function") redrawRouteOnMap();
    } else if (typeof renderStops === "function") {
      renderStops();
    }
  }

  if (allPci.size) {
    _alertPciStops([...allPci.entries()].map(([key, name]) => ({ name, isNew: promoted.has(key) })));
  }
}

// Name the stops whose check-in status just changed. A badge that quietly differs from
// how it looked a moment ago is not something anyone re-reads a saved route to catch,
// and a check-in appearing or disappearing changes when that stop has to be finished.
function _rcFlagNotice(gained, lost) {
  const bits = [];
  if (gained.length) bits.push(`now check-ins: ${gained.join(", ")}`);
  if (lost.length)   bits.push(`no longer check-ins: ${lost.join(", ")}`);
  if (!bits.length) return;
  try { console.info("[route-changes] check-in flags reconciled —", { gained, lost }); } catch (e) {}
  try {
    _bwImportMsg(`Breezeway check-in flags updated — ${bits.join(" · ")}. `
               + `Re-optimize if the timing matters.`, "amber");
  } catch (e) { /* message box isn't on this page — the console line still carries it */ }
}

// Hard-to-miss alert listing every PRIORITY CHECK-IN (arrive-by-noon) on a reopened
// saved route. Stops that flipped to PCI since the save (Breezeway added "PCI" to the
// title overnight) are tagged NEW; the rest are existing PCIs that are still easy to
// lose in a long list. Stops are repainted purple too, but a passive color change is
// easy to scan past — this blocks with a dismiss so they can't be missed. Re-entrant
// safe: a second detection merges into the open alert rather than stacking modals.
function _alertPciStops(stops) {
  if (!stops || !stops.length) return;
  let overlay = document.getElementById("pciAlert");
  let known = [];
  if (overlay) known = JSON.parse(overlay.dataset.stops || "[]");
  // Merge by name; a NEW flag from any detection wins (a flip is the louder fact).
  const byName = new Map();
  for (const s of [...known, ...stops]) {
    const prev = byName.get(s.name);
    byName.set(s.name, { name: s.name, isNew: !!(s.isNew || (prev && prev.isNew)) });
  }
  const merged = [...byName.values()].sort((a, b) => (b.isNew - a.isNew));   // NEW first

  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "pciAlert";
    overlay.style.cssText = [
      "position:fixed","inset:0","background:rgba(17,24,39,0.55)",
      "z-index:10000","display:flex","align-items:center","justify-content:center","padding:20px"
    ].join(";");
    document.body.appendChild(overlay);
  }
  overlay.dataset.stops = JSON.stringify(merged);

  const rows = merged.map(s => {
    const newTag = s.isNew
      ? ` <span style="background:#dc2626;color:#fff;font-size:10px;font-weight:800;padding:1px 6px;border-radius:4px;vertical-align:middle;">NEW</span>`
      : "";
    return `<div style="font-weight:700;color:#5b21b6;background:#f5f3ff;border-left:4px solid #7c3aed;border-radius:0 6px 6px 0;padding:6px 10px;margin-top:6px;">⚡ ${_escHtml(s.name)}${newTag}</div>`;
  }).join("");
  const newCount = merged.filter(s => s.isNew).length;
  const plural = merged.length > 1;

  overlay.innerHTML =
    `<div role="alertdialog" aria-modal="true" style="background:#fff;max-width:440px;width:100%;border-radius:14px;box-shadow:0 20px 50px rgba(0,0,0,0.35);overflow:hidden;">
       <div style="background:#7c3aed;color:#fff;font-weight:800;font-size:15px;letter-spacing:0.02em;padding:14px 18px;">
         ⚡ ${merged.length} PRIORITY CHECK-IN${plural ? "S" : ""} ON THIS ROUTE
       </div>
       <div style="padding:16px 18px;color:#374151;font-size:13px;line-height:1.5;">
         <div>${plural ? "These stops are" : "This stop is"} <b>arrive-by-noon</b> and show purple on the route — turning <b style="color:#dc2626;">red</b> if scheduled past noon.${newCount ? ` <b>${newCount}</b> tagged <b style="color:#dc2626;">NEW</b> had "PCI" added to the Walk-Thru title since this route was saved.` : ""}</div>
         ${rows}
       </div>
       <div style="padding:0 18px 16px;text-align:right;">
         <button id="pciAlertOk" style="background:#7c3aed;color:#fff;font-weight:700;font-size:13px;border:none;border-radius:8px;padding:9px 22px;cursor:pointer;">Got it</button>
       </div>
     </div>`;

  const close = () => overlay.remove();
  overlay.querySelector("#pciAlertOk").addEventListener("click", close);
  overlay.addEventListener("click", e => { if (e.target === overlay) close(); });
}

// Low-confidence name matches — let the user keep or reject each before it
// becomes a stop. Prevents a Breezeway house that isn't in the system yet from
// silently matching the closest wrong home.
function _bwRenderUncertain(date, list) {
  const box = document.getElementById("bwImportUncertain");
  if (!box) return;
  box.innerHTML = "";
  if (!list || !list.length) { box.classList.add("hidden"); return; }
  box.classList.remove("hidden");

  const hdr = document.createElement("div");
  hdr.className = "text-xs font-semibold text-amber-700";
  hdr.textContent = "Unsure about these matches — confirm each:";
  box.appendChild(hdr);

  const dropIfEmpty = () => {
    if (!box.querySelector(".uncertain-row")) { box.innerHTML = ""; box.classList.add("hidden"); }
  };

  for (const p of list) {
    const pct = Math.round((p.match_score || 0) * 100);
    const row = document.createElement("div");
    row.className = "uncertain-row rounded-lg border border-amber-200 bg-amber-50 px-2 py-1.5 text-xs";
    row.innerHTML =
      `<div class="text-gray-700 leading-snug">Breezeway: <b>${_escHtml(p.bw_name)}</b></div>` +
      `<div class="text-gray-500 leading-snug">matched → <b>${_escHtml(p.name)}</b> ` +
      `<span class="text-gray-400">(${pct}% match)</span></div>`;

    const btns = document.createElement("div");
    btns.className = "flex gap-1.5 mt-1";

    const keep = document.createElement("button");
    keep.className = "flex-1 bg-indigo-600 hover:bg-indigo-700 text-white rounded px-2 py-1 font-medium";
    keep.textContent = "Keep";
    keep.addEventListener("click", () => {
      if (!selectedStops.find(s => s.name === p.name)) {
        p.serviceMinutes = estServiceMinutes(p.tasks);   // tentative — editable after
        addStop(p, !!p.arrival, !!p.priority_checkin);
      }
      _bwTasksByPropName[p.name] = p.tasks || [];
      if (p.property_id) _bwPropIdByName[p.name] = p.property_id;
      _syncSidebarToSchedule();
      _bwPlaceMarkers();
      row.remove();
      dropIfEmpty();
    });

    const skip = document.createElement("button");
    skip.className = "flex-1 bg-gray-100 hover:bg-gray-200 text-gray-600 rounded px-2 py-1 font-medium";
    skip.textContent = "Reject";
    skip.addEventListener("click", () => { row.remove(); dropIfEmpty(); });

    btns.appendChild(keep); btns.appendChild(skip);
    row.appendChild(btns);
    box.appendChild(row);
  }
}

// Stored multi-employee data for tab switching
let _bwByAssignee     = null;
let _bwActiveDate     = null;
let _bwTasksByPropName = {};  // {propertyName: [{task_name, assignees}]} — keyed for sync
let _bwPropIdByName    = {};  // {propertyName: breezeway home_id} — for the 📅 calendar link

// House occupancy for the route's date: {String(breezeway pid): {kind, until}}.
// Same fact as the Group Batcher (guest/tenant/owner/block). Only occupied houses
// appear. Loaded once per date and cached; _loadOccupancy re-renders when it lands.
let _occByPid   = {};
let _occDate    = null;
let _occLoading = false;

function _loadOccupancy(date) {
  if (!date || date === _occDate || _occLoading) return;   // cache: one fetch per date
  _occLoading = true;
  const forDate = date;
  fetch(`/route/occupancy?date=${encodeURIComponent(date)}`, { credentials: "same-origin" })
    .then(r => (r.ok ? r.json() : { occupancy: {} }))
    .then(d => {
      if (d && d.error) console.warn("Occupancy lookup:", d.error);   // real failure, not "everyone vacant"
      _occByPid = (d && d.occupancy) || {}; _occDate = forDate;
    })
    .catch(() => { _occByPid = {}; _occDate = forDate; })   // set date so we don't retry-storm
    .finally(() => { _occLoading = false; _syncSidebarToSchedule(); });
}

// A small occupancy badge for a house, or null when vacant/unknown. Colors match
// the Group Batcher's guest/tenant/owner/block pills; the date it's occupied
// "until" rides in the tooltip to keep the narrow sidebar readable.
function _occupancyBadge(pid) {
  const occ = (pid != null && pid !== "") ? _occByPid[String(pid)] : null;
  if (!occ || !occ.kind) return null;
  const M = {
    guest: ["#fef3c7", "#92400e", "#fde68a", "🛏️ Guest",  "Guest in house"],
    lease: ["#ffedd5", "#9a3412", "#fdba74", "🔑 Tenant", "Long-term tenant in house"],
    owner: ["#dbeafe", "#1e40af", "#93c5fd", "👤 Owner",  "Owner in house"],
    block: ["#e2e8f0", "#475569", "#cbd5e1", "🚫 Block",  "House blocked / on hold"],
  };
  const m = M[occ.kind];
  if (!m) return null;
  const until = occ.until ? (typeof _fmtTaskDate === "function" ? _fmtTaskDate(occ.until) : String(occ.until).slice(5)) : "";
  const b = document.createElement("span");
  b.className = "shrink-0 text-[0.58rem] font-bold rounded px-1.5 leading-tight";
  b.style.cssText = `background:${m[0]};color:${m[1]};border:1px solid ${m[2]};`;
  b.textContent = m[3];
  b.title = m[4] + (until ? ` · until ${until}` : "") + " (on this route's date)";
  return b;
}

// Build a "📅 calendar ↗" link to a property's Breezeway calendar, matching the
// style used elsewhere in the app (occupancy check, hot tub billing). Returns null
// when we don't have the Breezeway property id for this house.
function _bwCalendarLink(name) {
  const pid = _bwPropIdByName[name];
  if (!pid) return null;
  const a = document.createElement("a");
  a.href      = `https://app.breezeway.io/property/${encodeURIComponent(pid)}/calendar`;
  a.target    = "_blank";
  a.rel       = "noopener";
  a.className = "text-indigo-500 hover:underline text-[11px] font-normal ml-1 whitespace-nowrap";
  a.title     = `Open ${name}'s calendar in Breezeway`;
  a.textContent = "📅 ↗";
  a.addEventListener("click", e => e.stopPropagation());  // don't trigger row/stop handlers
  return a;
}

// Return a task-title element for the sidebar: an <a> linking straight to the task in
// Breezeway when we have its id, otherwise a plain <span>. `className` styles the text
// either way (so it matches the surrounding rows); the link just adds a hover underline
// and stops the click from bubbling to the row/stop handlers.
function _bwTaskLabel(taskId, text, className) {
  // Time/date-sensitive titles (a date, an explicit or written-out time, "Issue",
  // "HO Request") render purple + bold via the shared matcher. Inline color wins
  // over the Tailwind text-* class. Callers add the purple left bar to the row.
  // Suppressed when this task_id has had its flag dismissed (the ✕). Routine task
  // types (Walk Thru / Post Rental Inspection / Arrival Hot Tub Service) are
  // excluded inside the shared matcher itself.
  const timeFlag = !!(window.NLD && !NLD.isFlagDismissed(taskId) && NLD.isTimeSensitiveTitle(text));
  const paint = (el) => {
    if (timeFlag) {
      el.style.color     = window.NLD.TIME_FLAG_COLOR || "#7c3aed";
      el.style.fontWeight = "700";
      el.dataset.timeFlag = "1";
    }
  };
  if (taskId != null && taskId !== "") {
    const a = document.createElement("a");
    a.href      = `https://app.breezeway.io/task/${encodeURIComponent(taskId)}`;
    a.target    = "_blank";
    a.rel       = "noopener";
    a.className  = className + " hover:underline";
    a.title      = "Open this task in Breezeway";
    a.textContent = text;
    a.addEventListener("click", e => e.stopPropagation());
    paint(a);
    return a;
  }
  const span = document.createElement("span");
  span.className   = className;
  span.textContent = text;
  paint(span);
  return span;
}

// A task title as an HTML STRING, for the string-built CHANGES-vs-Breezeway panel —
// the counterpart of _bwTaskLabel(), which builds DOM nodes for the stop list. Same
// deal: an <a> straight to the task in Breezeway when we have its id, else a plain
// <span>, so every task title in the sidebar is clickable the same way.
function _bwTaskLinkHtml(taskId, text, className) {
  const cls = className || "";
  if (taskId == null || taskId === "") return `<span class="${cls}">${_escHtml(text)}</span>`;
  return `<a href="https://app.breezeway.io/task/${encodeURIComponent(taskId)}"`
       + ` target="_blank" rel="noopener" title="Open this task in Breezeway"`
       + ` class="${cls} hover:underline">${_escHtml(text)}</a>`;
}

// 📅 link (as an HTML string) to a property's Breezeway calendar, for the string-built
// CHANGES-vs-Breezeway panel. Lets her open a changed/removed house's calendar to see
// what happened. Returns "" when we don't have the property id. Style matches the sidebar.
function _bwCalLinkHtml(pid, name) {
  if (pid == null || pid === "") return "";
  return ` <a href="https://app.breezeway.io/property/${encodeURIComponent(pid)}/calendar"`
       + ` target="_blank" rel="noopener"`
       + ` class="text-indigo-500 hover:underline text-[11px] font-normal whitespace-nowrap"`
       + ` title="Open ${_escHtml(name || "")}'s calendar in Breezeway" onclick="event.stopPropagation()">📅 ↗</a>`;
}

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
    // A reopen reuses the cached discrepancy result — the check runs once per route
    // load (and on an explicit Check again), not on every reopen. Re-running it here was
    // hammering the heavy all-houses endpoint past the gateway timeout (→ HTTP 503).
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

  // Load the route, then render stops + the Breezeway comparison via the single
  // render path (_syncSidebarToSchedule), so later redraws can't wipe the panel.
  _routeChangesCache = { routeId: null, html: null };   // force a fresh check for this route
  _rcAutoReset(routeId);                                // and a fresh retry schedule with it
  loadRouteById(routeId).then(() => {
    _syncSidebarToSchedule();
  }).catch(() => {
    content.innerHTML = `<div class="text-xs text-red-400 text-center py-4">Failed to load.</div>`;
  });
}

/* ── ROUTE DISCREPANCY CHECK (saved route vs live Breezeway) ─────── */

let _routeChangesCache    = { routeId: null, html: null };
let _routeChangesInflight = { routeId: null, promise: null, controller: null };
let _appliedRouteChanges  = new Set();   // route ids whose changes have been applied (hide the Apply button)
// Per-route UI state for the "Changes vs Breezeway" box: after Apply we collapse it and
// mark it applied (the shown list no longer reflects a fresh check). Check again clears both.
let _routeChangesUiState  = { routeId: null, collapsed: false, stale: false };

// Force the NEXT render to re-pull from Breezeway instead of serving the cached
// result. Used on an explicit Check again and to avoid caching an error; a page reload
// gets a fresh run for free (in-memory cache starts empty). Passive reopens and
// redraws within a session reuse the cache, so the check runs once per route load.
function _invalidateRouteChanges() {
  _routeChangesCache    = { routeId: null, html: null };
  _routeChangesInflight = { routeId: null, promise: null, controller: null };
}

/* ── AUTOMATIC RETRY OF THE PROPERTIES BREEZEWAY DIDN'T RETURN ─────────────
   The server holds the list of WHICH properties failed for 15 minutes
   (_ROUTE_DISC_RETRY_WINDOW) so a retry can refetch just those few instead of
   re-sweeping all ~442 houses. That list used to expire while the panel sat
   there waiting for someone to click "load just the missing N" — and once it
   was gone the only thing left was the expensive full re-scan the retry exists
   to avoid ("The list of which properties failed has expired…").

   So retry on our own, on a backoff that stays comfortably inside the window.
   Every partial result re-stamps the held list server-side, so each attempt
   rolls the window forward and it never expires while we're still working.

   Retrying stops by itself when nothing is failing any more, when the failure
   is one a retry cannot fix (401/403/404), when the attempts run out, or when
   the panel is no longer on screen — no background hammering of Breezeway for
   something nobody is looking at.

   Stop cancels the pending timer AND aborts a retry already in flight. A Stop
   that leaves a request running isn't a stop. */
const _RC_RETRY_DELAYS_S = [15, 30, 60, 120, 240];   // 5 tries over ~7.5 min, all inside the 15-min window
let _rcAuto = {
  routeId:  null,   // route the schedule belongs to
  attempt:  0,      // automatic retries already fired
  timerId:  null,   // pending setTimeout for the next one
  tickId:   null,   // countdown repaint interval
  dueAt:    0,      // epoch ms of the next attempt
  stopped:  false,  // user pressed Stop (or a retry came back unrecoverable)
  inFlight: false,  // an automatic retry is running right now
  note:     "",     // why we're no longer retrying, shown in the panel
};

function _rcAutoClearTimers() {
  if (_rcAuto.timerId) clearTimeout(_rcAuto.timerId);
  if (_rcAuto.tickId)  clearInterval(_rcAuto.tickId);
  _rcAuto.timerId = _rcAuto.tickId = null;
  _rcAuto.dueAt   = 0;
}

// A different route loaded, or the user asked for a fresh check: forget that
// they ever pressed Stop and start the attempt count over.
function _rcAutoReset(routeId) {
  _rcAutoClearTimers();
  _rcAuto.routeId  = routeId || null;
  _rcAuto.attempt  = 0;
  _rcAuto.stopped  = false;
  _rcAuto.inFlight = false;
  _rcAuto.note     = "";
}

// Stop retrying and say why, WITHOUT touching an in-flight request. Used when a
// retry itself comes back unusable — the request is already finished.
function _rcAutoHalt(note) {
  _rcAutoClearTimers();
  _rcAuto.stopped  = true;
  _rcAuto.inFlight = false;
  _rcAuto.note     = note || "";
}

// The panel body currently on screen, if any. The sidebar rebuilds this element
// on every sync, so it has to be looked up at use time — a reference captured
// when the timer was set would point at a detached node.
function _rcBody() { return document.querySelector("[data-rc-body]"); }

// Repaint the panel from the data we already have — no server call. Used when
// only the retry status line changed (scheduled / stopped / resumed). Returns
// false when there's nothing cached to paint, so callers can say something else
// rather than leave a stale "Checking…" on screen.
function _rcRepaint() {
  const body = _rcBody();
  if (!body || _routeChangesCache.routeId !== _rcAuto.routeId || !_routeChangesCache.data) return false;
  body.innerHTML = _renderChangesHtml(_routeChangesCache.data);
  _rcTick();
  return true;
}

// The header button disables itself on click and relies on the success path
// rebuilding the whole panel to come back. A stopped or aborted check never
// reaches that path, which left the button dead and the panel unusable.
function _rcRestoreRefreshBtn() {
  const body = _rcBody();
  const btn  = body && body.parentElement && body.parentElement.querySelector("[data-refresh]");
  if (!btn) return;
  btn.disabled = false;
  const has = _routeChangesCache.routeId === _rcAuto.routeId && !!_routeChangesCache.data;
  btn.textContent = (has || _routeChangesUiState.stale) ? "Check again" : "Check now";
}

/* How long until every property has been read, not how long until the next attempt.
   Breezeway's limit is confirmed at 200/min and the gate paces to 90% of it, so the
   remaining time is arithmetic: the wait still to serve, plus the fetching itself,
   plus the gaps before any further passes.

   The ladder below escalates (15s, 30s, 60s...), so the gaps dominate once a few
   attempts have gone by — which is exactly why "in 34s (try 3 of 5)" was so
   misleading. It named the smallest number on screen while the real answer was
   minutes away. */
function _rcEtaSeconds(remaining) {
  if (remaining <= 0) return 0;
  const perPass = Math.max(1, Math.floor(_BW_RATE_PER_SEC * _BW_PASS_BUDGET_S));
  const passes  = Math.ceil(remaining / perPass);
  let secs = _rcAuto.dueAt ? Math.max(0, (_rcAuto.dueAt - Date.now()) / 1000) : 0;
  secs += remaining / _BW_RATE_PER_SEC;
  for (let i = 1; i < passes; i++) {
    secs += _RC_RETRY_DELAYS_S[Math.min(_rcAuto.attempt + i, _RC_RETRY_DELAYS_S.length - 1)];
  }
  return secs;
}

// Drive the "about Xm Ys left" countdown. Ticks during an in-flight attempt as well
// as between attempts — an estimate that freezes while the app is working is exactly
// when it looks broken.
function _rcTick() {
  const el = document.querySelector("[data-rc-eta]");
  _rcSyncTick();
  if (!el) return;
  const d    = _routeChangesCache.data;
  const left = _rcEtaSeconds((d && d.failed_properties) || 0);
  el.textContent = left > 1 ? _bwFmtDuration(left) : "a few more seconds";
}

// Run the countdown exactly when there is a countdown on screen. Tying it to the
// rendered element rather than to the retry schedule keeps it alive across the
// in-flight/waiting boundary, and stops it leaking once the panel is done.
function _rcSyncTick() {
  const wanted = !!document.querySelector("[data-rc-eta]");
  if (wanted && !_rcAuto.tickId)  _rcAuto.tickId = setInterval(_rcTick, 1000);
  if (!wanted && _rcAuto.tickId) { clearInterval(_rcAuto.tickId); _rcAuto.tickId = null; }
}

// Decide what happens after any check/retry result lands. Called BEFORE the
// panel is painted so the status line it renders is already correct.
function _rcAutoAfterResult(routeId, data) {
  if (routeId !== _rcAuto.routeId) return;   // panel has moved to another route
  _rcAuto.inFlight = false;
  _rcAutoClearTimers();

  if (!data.failed_properties) {             // everything loaded — nothing to chase
    _rcAuto.attempt = 0;
    _rcAuto.note    = "";
    return;
  }
  const f = (typeof bwFailureCause === "function") ? bwFailureCause(data) : { retry: false };
  if (!f.retry) {                            // 401/403/404 — retrying cannot fix it
    _rcAuto.note = "";
    return;
  }
  if (_rcAuto.stopped) return;
  if (_rcAuto.attempt >= _RC_RETRY_DELAYS_S.length) {
    // Cause-neutral: the stall may be timeouts, or this app's own limiter declining
    // to send, and calling either a refusal blames Breezeway for something it never
    // did. The breakdown line above already names what actually happened.
    _rcAuto.note = `Stopped after ${_RC_RETRY_DELAYS_S.length} automatic tries — `
                 + `${data.failed_properties} still haven't loaded.`;
    return;
  }
  const wait = _RC_RETRY_DELAYS_S[_rcAuto.attempt] * 1000;
  _rcAuto.dueAt   = Date.now() + wait;
  _rcAuto.timerId = setTimeout(() => _rcAutoFire(routeId), wait);
  // The 1s tick is NOT started here. Started alongside the schedule it died every
  // time a request went out, so the estimate froze for the whole of each attempt —
  // precisely when it looks broken. _rcSyncTick ties it to what is on screen.
}

function _rcAutoFire(routeId) {
  _rcAutoClearTimers();
  const body = _rcBody();
  // Panel closed, or a different route is loaded: give up rather than keep
  // spending Breezeway calls on a result nobody can see.
  if (_rcAuto.stopped || !body || currentRouteId !== routeId) {
    if (!_rcAuto.stopped) _rcAutoHalt("the panel was closed");
    return;
  }
  // Changes were applied: the shown list has been superseded, and a retry would
  // paint an old comparison over the "these have been applied" message.
  if (_routeChangesUiState.routeId === routeId && _routeChangesUiState.stale) {
    _rcAutoHalt("the changes were applied");
    return;
  }
  _rcAuto.attempt++;
  _rcAuto.inFlight = true;
  _rcRepaint();                                    // "Retrying now…" in place of the countdown
  _renderRouteChangesInto(routeId, body, false, true, true);
}

// The Stop button. Kills the timer and the request that's already out.
function _rcAutoStop() {
  _rcAutoHalt("");
  if (_routeChangesInflight.controller) {
    try { _routeChangesInflight.controller.abort(); } catch (_) {}
  }
  _routeChangesInflight = { routeId: null, promise: null, controller: null };
  _rcRestoreRefreshBtn();
  if (!_rcRepaint()) {
    // Nothing cached to fall back on — a first-ever check was stopped part-way.
    const body = _rcBody();
    if (body) {
      body.innerHTML = `<span class="text-gray-500 leading-snug">Stopped. Nothing was checked — `
                     + `use <b>Check now</b> above to start again.</span>`;
    }
  }
}

function _rcAutoResume() {
  _rcAuto.stopped = false;
  _rcAuto.note    = "";
  _rcAuto.attempt = 0;
  if (_routeChangesCache.routeId === _rcAuto.routeId && _routeChangesCache.data) {
    _rcAutoAfterResult(_rcAuto.routeId, _routeChangesCache.data);
  }
  _rcRepaint();
}

// The line under the "couldn't be loaded" warning that says what the retrying is
// doing right now, and offers the button to stop it.
function _rcAutoStatusHtml(d) {
  if (_rcAuto.routeId !== currentRouteId) return "";
  const btn = (attr, label) =>
    `<button ${attr} style="text-decoration:underline;font-weight:700;color:inherit;background:none;`
    + `border:none;padding:0;cursor:pointer;font-size:11px;">${label}</button>`;

  const total  = Number(d && d.scanned_properties) || 0;
  const loaded = total ? Math.max(0, total - (d.failed_properties || 0)) : 0;
  const pct    = total ? Math.min(100, Math.round((loaded / total) * 100)) : 0;
  // Rendered from what has LOADED so the bar only ever advances; one driven by the
  // failure count jumps backwards whenever a pass recovers nothing.
  const bar = total
    ? `<div class="mt-1" style="height:3px;background:rgba(0,0,0,.10);border-radius:2px;overflow:hidden;">`
      + `<div style="height:100%;width:${pct}%;background:currentColor;opacity:.55;transition:width .4s ease;"></div></div>`
    : "";
  const count = total ? `${loaded} of ${total} properties` : `${d.failed_properties} still to load`;

  if (_rcAuto.stopped) {
    return `<div class="mt-1 text-[11px] text-gray-600">${count} — paused`
         + (_rcAuto.note ? ` (${_escHtml(_rcAuto.note)})` : "")
         + `. ${btn("data-rc-resume", "Resume")}</div>${bar}`;
  }
  if (_rcAuto.note) {
    return `<div class="mt-1 text-[11px] text-gray-600">${count}. ${_escHtml(_rcAuto.note)}</div>${bar}`;
  }
  if (_rcAuto.inFlight || _rcAuto.timerId) {
    // One line for both states. Whether the app is mid-request or waiting out a gap
    // is machinery; the ETA already covers the gap, and "in 34s (try 3 of 5)" told
    // you about the machinery while the thing you wanted to know went unsaid.
    return `<div class="mt-1 text-[11px]">Loading ${count} — about `
         + `<span data-rc-eta>…</span> left ${btn("data-rc-stop", "Stop")}</div>${bar}`;
  }
  return "";
}

// Append the "Changes vs Breezeway" block for the currently-loaded saved route.
// Cheap to re-run: re-renders from cache on later panel redraws, and shares a
// single in-flight request per route so redraws don't re-hit the heavy endpoint.
function _appendRouteChanges(content) {
  if (!currentRouteId) return;
  const rid = currentRouteId;
  // Reset collapse/stale when a DIFFERENT route loads; otherwise preserve it across the
  // many sidebar re-renders so the panel stays minimized (and Stale) after Apply.
  if (_routeChangesUiState.routeId !== rid) {
    _routeChangesUiState = { routeId: rid, collapsed: false, stale: false };
  }
  // Same rule for the retry schedule: only when the route actually changes, or
  // every sidebar sync would wipe a pending countdown and un-press Stop.
  if (_rcAuto.routeId !== rid) _rcAutoReset(rid);
  const box = document.createElement("div");
  box.className = "mt-3 pt-3 border-t border-gray-200";
  box.innerHTML = `
    <div class="flex items-center justify-between mb-2">
      <button data-toggle class="flex items-center gap-1.5 text-xs font-semibold text-gray-700 uppercase tracking-wide hover:text-gray-900">
        <span data-caret class="text-gray-400 text-[10px] leading-none"></span>
        <span>Changes vs Breezeway</span>
        <span data-stale class="hidden normal-case tracking-normal text-[10px] font-bold text-amber-700 bg-amber-100 border border-amber-200 rounded px-1.5 py-px">Applied</span>
      </button>
      <button data-refresh class="text-xs text-indigo-500 hover:text-indigo-700 font-medium"></button>
    </div>
    <div data-body data-rc-body class="text-xs text-gray-400"></div>`;
  content.appendChild(box);
  const body   = box.querySelector("[data-body]");
  const caret  = box.querySelector("[data-caret]");
  const staleB   = box.querySelector("[data-stale]");
  const refreshB = box.querySelector("[data-refresh]");

  // ONE action button, labelled for the state it's in. There used to be two — a
  // header "↻ Recheck" plus a body "Check for changes" — which did nearly the same
  // thing, and "Recheck" meant nothing before anything had been checked.
  const _hasResult = () => _routeChangesCache.routeId === rid && !!_routeChangesCache.data;

  const paintChrome = () => {
    const st = _routeChangesUiState;
    caret.textContent  = st.collapsed ? "▸" : "▾";
    staleB.classList.toggle("hidden", !st.stale);
    body.style.display = st.collapsed ? "none" : "";
    // "Check now" the first time, "Check again" once there's something to replace.
    refreshB.textContent = (_hasResult() || st.stale) ? "Check again" : "Check now";
  };

  // The check sweeps EVERY property (~442 Breezeway calls). It used to run
  // automatically whenever the panel rendered, so opening a route spent the rate
  // limit whether or not anyone wanted the answer. It now waits to be asked.
  const paintBody = () => {
    if (_routeChangesUiState.stale) {
      body.innerHTML = `<span class="text-amber-700">These changes have been applied, so this list no `
                     + `longer matches. Use <b>Check again</b> above to compare with Breezeway.</span>`;
      return;
    }
    if (_hasResult()) {
      body.innerHTML = _renderChangesHtml(_routeChangesCache.data);
      _rcTick();          // the countdown span is brand new — fill it before the next second ticks
      return;
    }
    body.innerHTML =
      `<span class="text-gray-400 leading-snug">Not checked yet. Use <b>Check now</b> above — `
      + `it looks up every property, so it only runs when you ask.</span>`;
  };

  box.querySelector("[data-toggle]").addEventListener("click", () => {
    _routeChangesUiState.collapsed = !_routeChangesUiState.collapsed;
    paintChrome();
    if (!_routeChangesUiState.collapsed) paintBody();
  });
  refreshB.addEventListener("click", () => {
    refreshB.disabled = true;
    refreshB.textContent = "Checking…";
    _invalidateRouteChanges();
    _rcAutoReset(rid);                         // fresh check → fresh retry budget, and Stop is forgotten
    _appliedRouteChanges.delete(rid);          // a fresh check brings the Apply button back
    _routeChangesUiState.stale     = false;    // fresh data → no longer applied-and-stale
    _routeChangesUiState.collapsed = false;    // and re-open it to show the result
    body.style.display = "";
    caret.textContent  = "▾";
    staleB.classList.add("hidden");
    // force=1 only when re-running over an existing result; the first check can
    // ride the server's short cache.
    _renderRouteChangesInto(rid, body, _hasResult());
  });

  paintChrome();
  if (!_routeChangesUiState.collapsed) paintBody();
}

// "Load just the missing N" — delegated, NOT bound per render.
//
// The panel is rebuilt on every sidebar sync (_appendRouteChanges) and again when
// it renders from cache, and each rebuild replaces the DOM. A listener attached to
// the button at render time was therefore discarded almost immediately: the button
// stayed on screen and did nothing. Delegating from the document survives every
// re-render.
document.addEventListener("click", function (ev) {
  const t = ev.target;
  if (!t || !t.closest) return;

  const retry = t.closest("[data-retry-missing]");
  if (retry && !retry.disabled) {
    const body = retry.closest("[data-body]");
    if (!body || !currentRouteId) return;
    ev.preventDefault();
    // Hand the pending automatic attempt over to this one, or both would fire.
    _rcAutoClearTimers();
    // A manual try costs Breezeway exactly what an automatic one does, so it spends
    // from the same budget. Untracked, clicking repeatedly walked straight past the
    // _RC_RETRY_DELAYS_S ceiling — it only ever counted the timer's own attempts.
    _rcAuto.attempt++;
    _rcAuto.inFlight = true;
    // quiet: repaint in place so the warning box — and its Stop button — stay on
    // screen while the retry runs. Wiping the body to "Checking Breezeway…" took
    // Stop away at exactly the moment there was something to stop.
    if (!_rcRepaint()) {
      retry.disabled = true;
      retry.textContent = "Loading the missing ones…";
    }
    _renderRouteChangesInto(currentRouteId, body, false, true, true);
    return;
  }

  // Stop the automatic retrying — the pending timer AND whatever is already out.
  if (t.closest("[data-rc-stop]")) {
    ev.preventDefault();
    _rcAutoStop();
    return;
  }

  if (t.closest("[data-rc-resume]")) {
    ev.preventDefault();
    _rcAutoResume();
    return;
  }

  // "Mark these task changes as seen" — record the list now on screen as the new
  // baseline. Posts back what the check already sent us, so it costs no Breezeway
  // calls at all.
  const ack = t.closest("[data-ack-tasks]");
  if (ack && !ack.disabled) {
    ev.preventDefault();
    _ackTaskChanges(ack);
    return;
  }

});

// Record the currently-shown task list as this route's accepted baseline.
//
// Sends only the houses the check actually READ (current_tasks holds exactly
// those), so a house Breezeway refused keeps whatever baseline it already had
// rather than being recorded as having no tasks.
async function _ackTaskChanges(btn) {
  const data = _routeChangesCache.data;
  if (!data || !currentRouteId) return;
  btn.disabled = true;
  btn.textContent = "Recording…";
  try {
    const resp = await fetch("/api/route-task-baseline", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        route_id: currentRouteId,
        houses: (data.current_tasks || []).map(c => ({
          property: c.property,
          tasks: (c.tasks || []).map(t => (t && typeof t === "object")
            ? { id: t.id, name: t.name }
            : { id: null, name: t }),
        })),
      }),
    });
    const out = await resp.json();
    if (out.error) throw new Error(out.error);
    // The panel on screen was computed against the OLD baseline, so re-check rather
    // than leaving changes visible that have just been accepted.
    _routeChangesCache = { routeId: null, html: null };
    const body = btn.closest("[data-body]");
    if (body) _renderRouteChangesInto(currentRouteId, body, true);
  } catch (e) {
    btn.disabled = false;
    btn.textContent = "Couldn't record that — try again";
    console.error("[route-changes] baseline ack failed:", e);
  }
}

function _renderRouteChangesInto(routeId, body, force, retryFailed, quiet) {
  // Re-render from cached DATA (not a frozen html string) so the panel reflects
  // the CURRENT list each time — manual or applied fixes clear resolved changes.
  // A retry must always go to the server, or it would just repaint the same gaps.
  if (!retryFailed && _routeChangesCache.routeId === routeId && _routeChangesCache.data) {
    body.innerHTML = _renderChangesHtml(_routeChangesCache.data);
    _rcTick();
    return;
  }
  // A retry only needs the in-flight slot cleared so it actually goes out. Dropping
  // the CACHED RESULT too (as this used to) means an aborted or failed retry leaves
  // the panel with nothing to fall back on — and Stop would blank the very list the
  // user was reading. The guard above already keeps a retry from being short-circuited.
  if (retryFailed) _routeChangesInflight = { routeId: null, promise: null, controller: null };
  // quiet: an automatic retry repaints in place (the status line says it's running)
  // instead of wiping the whole comparison every time the timer fires.
  if (!quiet) {
    // Stop stays reachable during a full check too. Without it the only way out of
    // a long all-houses sweep was to close the sidebar and hope.
    body.innerHTML = `<span class="text-gray-400">Checking Breezeway…</span> `
      + `<button data-rc-stop style="text-decoration:underline;font-weight:700;color:#6b7280;`
      + `background:none;border:none;padding:0;cursor:pointer;font-size:11px;">Stop</button>`;
  }
  if (_routeChangesInflight.routeId !== routeId || !_routeChangesInflight.promise) {
    // AbortController so Stop can actually cut a retry that's already out. Without
    // it "Stop" only cancelled the NEXT one and the current sweep ran to completion.
    const ctrl = (typeof AbortController !== "undefined") ? new AbortController() : null;
    _routeChangesInflight = {
      routeId,
      controller: ctrl,
      // no-store: the GET is otherwise HTTP-cacheable, which made a page reload show
      // the stale browser-cached result instead of a live re-check. force=1 (explicit
      // an explicit re-check only) tells the server to skip ITS short-lived cache; passive
      // reopens omit it so they ride the cache and don't re-run the heavy all-houses
      // scan (which is what was timing out at the gateway → HTTP 503).
      promise: fetch(`/api/route-discrepancies?route_id=${routeId}`
                     + (force ? "&force=1" : "")
                     + (retryFailed ? "&retry_failed=1" : ""),
                     { cache: "no-store", signal: ctrl ? ctrl.signal : undefined })
        .then(r => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json();
        }),
    };
  }
  _routeChangesInflight.promise.then(data => {
    if (data.error) {
      console.error("[route-changes] route", routeId, "server error:", data.error);
      // A retry that comes back unusable stops the schedule and says why, but must
      // NOT throw away the comparison we already have on screen.
      if (retryFailed && _routeChangesCache.routeId === routeId && _routeChangesCache.data) {
        _rcAutoHalt(data.error);
        body.innerHTML = _renderChangesHtml(_routeChangesCache.data);
        return;
      }
      _rcAutoHalt(data.error);
      body.innerHTML = `<span class="text-red-500">${_escHtml(data.error)} — reopen the sidebar or use Check again.</span>`;
      _invalidateRouteChanges();   // never cache an error — let a reopen/recheck retry
      return;
    }
    if (data.failed_properties) {
      console.warn("[route-changes] route", routeId, "—", data.failed_properties,
                   "propert(y/ies) failed to load from Breezeway; task list may be incomplete");
    }
    // Settle the retry schedule BEFORE painting, so the status line the panel
    // renders already knows whether another attempt is queued.
    _rcAutoAfterResult(routeId, data);
    const html = _renderChangesHtml(data);
    body.innerHTML = html;
    _rcTick();
    _routeChangesCache = { routeId, html, data };
    _reconcileFlagsFromScan(data);   // the live scan is the authority on check-in / PCI flags
    // Remember each house's Breezeway property_id from the live scan so the saved-route
    // sidebar can render 📅 calendar links (the imported-BW path fills this in separately).
    for (const c of (data.current_tasks || [])) {
      if (c.property_id && !_bwPropIdByName[c.property]) _bwPropIdByName[c.property] = c.property_id;
    }
    _syncSidebarToSchedule();   // re-paint stops now that we have each property's tasks
  }).catch(e => {
    // Stop aborted it on purpose — that path has already repainted the panel.
    if (e && e.name === "AbortError") return;
    console.error("[route-changes] route", routeId, "fetch failed:", e);
    _rcAuto.inFlight = false;
    // Same as the server-error path: a failed retry keeps the result it was
    // trying to improve, and just reports that the retrying has stopped.
    if (retryFailed && _routeChangesCache.routeId === routeId && _routeChangesCache.data) {
      _rcAutoHalt(`Couldn't reach Breezeway (${e.message})`);
      body.innerHTML = _renderChangesHtml(_routeChangesCache.data);
      return;
    }
    body.innerHTML = `<span class="text-red-500">Could not check Breezeway: ${_escHtml(e.message)} — reopen the sidebar or use Check again.</span>`;
    _invalidateRouteChanges();   // don't cache the failed promise, or every retry reuses it
  });
}

// Task titles for a given stop name, from the last discrepancy fetch.
function _tasksForStop(name) {
  const data = _routeChangesCache.data;
  if (!data || !data.current_tasks) return null;
  const key = (name || "").toLowerCase();
  const hit = data.current_tasks.find(c => (c.property || "").toLowerCase() === key);
  return hit ? hit.tasks : null;
}

// Does this house have a live VIP task? True when ANY of its tasks has a VIP title
// and that task's flag hasn't been dismissed — so dismissing the last VIP task's ✕
// clears the house flag too. Reads both task sources: the BW-tasks payload and the
// discrepancy-scan payload (daily-routes mode), since only one is populated at a time.
// Shared by both sidebars so they can never disagree about which house is VIP.
function _stopHasVip(name) {
  if (!window.NLD) return false;
  const vip = (title, id) => NLD.isVipTitle(title) && !NLD.isFlagDismissed(id);

  const bw = _bwTasksByPropName[name] || [];
  if (bw.some(t => vip(t.task_name, t.task_id))) return true;

  const scan = _tasksForStop(name) || [];
  return scan.some(t => (t && typeof t === "object")
    ? vip(t.name, t.id)
    : vip(t, null));
}

// After a task's ✕ dismissal, re-render BOTH sidebars: the task list here AND the
// schedule cards, whose house-level VIP banner is derived from these same flags and
// would otherwise sit stale until the next optimize.
function _afterFlagDismiss() {
  _syncSidebarToSchedule();
  try { if (isOptimized && typeof renderSchedule === "function") renderSchedule(); } catch (e) {}
}

// Live same-day arrival flag from the latest scan — so a check-in that moved to today
// lights up the sidebar badge even though the SAVED route (s.arrival) predates the move.
function _arrivalForStop(name) {
  const data = _routeChangesCache.data;
  if (!data || !data.current_tasks) return false;
  const key = (name || "").toLowerCase();
  const hit = data.current_tasks.find(c => (c.property || "").toLowerCase() === key);
  return hit ? !!hit.arrival : false;
}

function _fmtChangeWhen(w) {
  if (!w) return "";
  // Breezeway sends these timestamps in UTC but often WITHOUT a timezone marker, so
  // `new Date("2026-07-10T15:47:00")` would be read as LOCAL time — showing a morning
  // event (8:47 AM Pacific) as 3:47 PM. If there's no trailing Z/offset, treat it as UTC,
  // then render in Tahoe (Pacific) time explicitly.
  let s = String(w).trim();
  const hasTz = /(Z|[+-]\d{2}:?\d{2})$/.test(s);
  if (!hasTz && /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(s)) {
    s = s.replace(" ", "T") + "Z";
  }
  const d = new Date(s);
  return isNaN(d.getTime())
    ? w
    : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric",
                                  minute: "2-digit", timeZone: "America/Los_Angeles" });
}

function _escHtml(s) {
  return String(s == null ? "" : s).replace(/[&<>"]/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function _renderChangesHtml(d) {
  // Show only what's still OUTSTANDING vs the CURRENT working list — once a stop
  // is added or removed (by hand or via Apply), it drops out of this panel.
  const _cur = new Set(
    (isOptimized ? optimizedSchedule.filter(s => !s.isLunch && !s.isGap) : selectedStops)
      .map(s => (s.name || "").toLowerCase())
  );
  const added   = (d.added   || []).filter(a => !_cur.has((a.property || "").toLowerCase()));
  const removed = (d.removed || []).filter(r =>  _cur.has((r.property || "").toLowerCase()));
  const moved   = d.moved || [];
  // Houses already on the route that became a same-day check-in — only relevant while the
  // stop is still in the working list.
  const newCheckin = (d.new_checkin || []).filter(c => _cur.has((c.property || "").toLowerCase()));
  let h = "";

  // The day's check-in list didn't load. This one goes FIRST and in red, because it is
  // worse than a missing house: every CHECK-IN badge on the route is derived from that
  // list, so when it comes back short the whole route quietly reads "not a check-in"
  // and looks perfectly normal. Nothing else on this panel would hint at it.
  if (d.arrival_error) {
    h += `<div class="mb-2 text-[11px] text-red-800 bg-red-50 border border-red-300 rounded px-2 py-1 leading-snug">`
       + `<span class="font-bold">⚠ Check-in flags are NOT reliable.</span> `
       + `Couldn't read today's arrivals from Breezeway, so the CHECK-IN badges below `
       + `(and on the route) may be missing or wrong — they have been left exactly as `
       + `they were rather than guessed at.`
       + `<div class="text-red-600 mt-1">${_escHtml(d.arrival_error)}</div>`
       + `<div class="text-gray-600 mt-1">Use Check again — this usually clears on a retry.</div>`
       + `</div>`;
  }

  // Loud, non-silent warning when Breezeway dropped some houses — the comparison
  // (and the auto-loaded task titles) are then incomplete, so don't trust "no changes".
  if (d.failed_properties) {
    const f = bwFailureCause(d);
    // Still working vs actually stuck. At 200 req/min a full day takes several
    // passes by design, so a check that is mid-flight is not a fault and is no
    // longer painted as one — amber is kept for the cases that want you. The
    // failure breakdown moves to a quiet second line for the same reason: it is
    // detail about work in progress, not the headline.
    const working = f.retry && !_rcAuto.stopped && (_rcAuto.inFlight || _rcAuto.timerId);
    const tone = working
      ? `text-blue-700 bg-blue-50 border-blue-200`
      : `text-amber-700 bg-amber-50 border-amber-200`;
    h += `<div class="mb-2 text-[11px] ${tone} border rounded px-2 py-1 leading-snug">`
       + (working
            ? `Still reading the day's tasks — the comparison below is incomplete until it finishes.`
              + _rcAutoStatusHtml(d)
              + `<div class="text-gray-500 mt-1">${_escHtml(f.text.replace(/^⚠\s*/, ""))}.</div>`
            // Not running: say what went wrong, and offer the button. This is the
            // only state where a manual retry makes sense — with an attempt already
            // scheduled or in flight, the button duplicated it, and clicking during
            // a backoff skipped the wait that exists because Breezeway is throttling
            // us. The panel used to show "Click here to load just the missing 8 now"
            // directly above "Retrying automatically in 34s".
            : `${f.text} — some tasks may be missing.`
              + (f.retry
                   ? `<br><button data-retry-missing="${d.failed_properties}"`
                     + ` style="margin-top:4px;text-decoration:underline;font-weight:700;color:inherit;`
                     + `background:none;border:none;padding:0;cursor:pointer;font-size:11px;">`
                     + `👉 Load just the missing ${d.failed_properties} now</button>`
                     + _rcAutoStatusHtml(d)
                     + `<div class="text-gray-500 mt-1">Only re-checks the ones that failed — much faster, and far less likely`
                     + ` to be throttled again. (Check again at the top re-scans all ${d.scanned_properties || "the"} properties.)</div>`
                   : ` Checking again won't help; this needs a fix in Breezeway or the app's settings.`))
       + `</div>`;
  }

  // ── New same-day check-ins (arrival moved to today on a stop you already have) ──
  // Shown first because it's time-critical: a check-in is arrive-by-noon.
  if (newCheckin.length) {
    h += `<div class="font-semibold text-amber-800 mb-1">☀️ New check-in today (${newCheckin.length})</div>`;
    for (const c of newCheckin) {
      h += `<div class="mb-1.5 leading-snug bg-amber-50 border-l-2 border-amber-400 rounded-r pl-2 pr-1 py-1">`;
      h += `<div class="text-gray-800 font-medium">${_escHtml(c.property)}${_bwCalLinkHtml(c.property_id, c.property)}`
         + ` <span class="inline-block align-middle bg-amber-500 text-white text-[10px] font-bold px-1.5 py-0.5 rounded">☀️ NEW CHECK-IN</span>`
         + (c.pci ? ` <span class="inline-block align-middle bg-violet-600 text-white text-[10px] font-bold px-1.5 py-0.5 rounded">⚡ BY NOON</span>` : "")
         + `</div>`;
      for (const t of (c.tasks || [])) {
        h += `<div class="text-[11px] text-gray-500 pl-3 leading-snug">• ${_escHtml(t)}</div>`;
      }
      h += `<div class="text-[11px] text-amber-700 pl-3 italic">Already on your route — arrival moved to today.</div>`;
      h += `</div>`;
    }
  }

  // Task-level changes at houses already on the route. Kept OUT of the _cur name
  // filter used above: these are keyed on Breezeway task ids, which are stable and
  // global, so they stay correct even where a house name doesn't line up.
  const addedTasks   = d.added_tasks   || [];
  const removedTasks = d.removed_tasks || [];

  // ── What changed since the route was saved ──
  if (!added.length && !removed.length && !moved.length && !newCheckin.length
      && !addedTasks.length && !removedTasks.length) {
    h += `<div class="text-green-600 mb-1">✓ No changes — the list matches the saved route.</div>`;
    if (d.task_baseline === "seeded") {
      h += `<div class="text-[11px] text-gray-400 mb-1 leading-snug">`
         + `Recorded this task list as the starting point — from now on a task added to `
         + `or removed from a house already on the route will show up here.</div>`;
    }
  }
  if (added.length) {
    // "Added to list" claimed these were NEW since the route was saved. The check
    // can't actually tell that: it reports any task in Breezeway that isn't on the
    // route, so a task that was always there but whose property failed to load on
    // an earlier throttled check shows up here the first time it loads cleanly.
    // "Additional tasks" states what's true — they're on Breezeway and not on the
    // route — without asserting when they got there.
    h += `<div class="font-semibold text-red-700 mb-1">➕ Additional tasks (${added.length})</div>`;
    // Group by property so it reads like the stop list above: house header + bulleted tasks.
    const byProp = {};
    for (const a of added) (byProp[a.property] = byProp[a.property] || []).push(a);
    for (const prop of Object.keys(byProp)) {
      // A priority check-in (PCI) = arrive by noon. Mark it loudly: a badge on the
      // house header AND a highlighted, badged line for each PCI task.
      // `a.pci` already requires a same-day arrival; a next-day PCI stays unflagged.
      const propPci = byProp[prop].some(a => a.pci);
      // VIP is independent of PCI — a house that's both gets BOTH badges.
      const propVip = !!(window.NLD && byProp[prop].some(
        a => NLD.isVipTitle(a.task_name) && !NLD.isFlagDismissed(a.task_id)));
      h += `<div class="mb-1.5 leading-snug">`;
      h += `<div class="text-gray-800 font-medium">${_escHtml(prop)}${_bwCalLinkHtml(byProp[prop][0].property_id, prop)}`
         + (propPci ? ` <span class="inline-block align-middle bg-violet-600 text-white text-[10px] font-bold px-1.5 py-0.5 rounded">⚡ PRIORITY CHECK-IN</span>` : "")
         + (propVip ? ` ${NLD.vipBadgeHtml()}` : "")
         + `</div>`;
      for (const a of byProp[prop]) {
        const isPci = !!a.pci;
        // Breezeway has no "added to the list" event, so this is the task's CREATION
        // (created_at/created_by). Labelled honestly: "created … by …" when a person made
        // it, "auto-created …" for nightly system-generated tasks with no creator.
        const who  = a.history && a.history.who  ? _escHtml(a.history.who) : null;
        const when = a.history && a.history.when ? _fmtChangeWhen(a.history.when) : null;
        const note = when
          ? (who
              ? ` <span class="text-gray-400">(created ${_escHtml(when)} by ${who})</span>`
              : ` <span class="text-gray-400">(auto-created ${_escHtml(when)})</span>`)
          : "";
        const pciBadge = isPci
          ? ` <span class="inline-block bg-violet-600 text-white text-[10px] font-bold px-1 rounded">⚡ BY NOON</span>`
          : "";
        const lineCls = isPci
          ? "text-[11px] text-violet-900 font-semibold bg-violet-50 border-l-2 border-violet-500 rounded-r pl-2 pr-1 py-0.5 leading-snug"
          : "text-[11px] text-gray-500 pl-3 leading-snug";
        const vipBadge = (window.NLD && NLD.isVipTitle(a.task_name) && !NLD.isFlagDismissed(a.task_id))
          ? ` ${NLD.vipBadgeHtml()}` : "";
        // Title links to the task in Breezeway, same as every other task line in this
        // sidebar. Inherits lineCls so the PCI highlight styling is unchanged.
        h += `<div class="${lineCls}">• ${_bwTaskLinkHtml(a.task_id, a.task_name, "")}`
           + `${pciBadge}${vipBadge}${note}</div>`;
      }
      h += `</div>`;
    }
  }
  if (removed.length) {
    // Mirror of "Additional tasks": all that's known is Breezeway returned no task
    // for this person at this house. "No longer on list" asserted a removal, which
    // the check can't establish — the task may have been reassigned, finished, or
    // never existed. (Houses whose lookup FAILED are held back separately under
    // "Couldn't check" and never appear here.)
    h += `<div class="font-semibold text-amber-700 mt-3 mb-1">➖ No tasks in Breezeway (${removed.length})</div>`;
    for (const r of removed) h += `<div class="text-gray-700 mb-1">${_escHtml(r.property)}${_bwCalLinkHtml(r.property_id, r.property)}</div>`;
  }
  // Stops we could NOT check because Breezeway refused the lookup. Shown, but
  // deliberately kept OUT of the remove action — "no task found" and "couldn't
  // ask" look identical from here, and dropping a stop on missing data would
  // take a house off someone's route that is still assigned to them.
  const unverified = (d.unverified || []).filter(r => _cur.has((r.property || "").toLowerCase()));
  if (unverified.length) {
    h += `<div class="font-semibold text-amber-700 mt-3 mb-1">❔ Couldn't check (${unverified.length})</div>`;
    h += `<div class="text-gray-500 text-xs mb-1">Breezeway didn't return these, so we can't tell whether they changed. `
       + `They're left on the route — use Check again to try them once more.</div>`;
    for (const r of unverified) h += `<div class="text-gray-700 mb-1">${_escHtml(r.property)}${_bwCalLinkHtml(r.property_id, r.property)}</div>`;
  }
  // ── Task changes at houses you already have ──
  // The case the house-level comparison is blind to: the stop was always there, but
  // its task list is not what it was. Grouped by house so it reads like the stop list.
  if (addedTasks.length || removedTasks.length) {
    const byHouse = {};
    for (const t of addedTasks)
      (byHouse[t.property] = byHouse[t.property] || { add: [], del: [] }).add.push(t);
    for (const t of removedTasks)
      (byHouse[t.property] = byHouse[t.property] || { add: [], del: [] }).del.push(t);

    const nAdd = addedTasks.length, nDel = removedTasks.length;
    const label = [nAdd ? `${nAdd} new` : null, nDel ? `${nDel} gone` : null]
                    .filter(Boolean).join(", ");
    h += `<div class="font-semibold text-red-700 mt-3 mb-1">🔔 Task changes on stops you already have (${label})</div>`;
    for (const prop of Object.keys(byHouse).sort((a, b) => a.localeCompare(b))) {
      const g = byHouse[prop];
      h += `<div class="mb-1.5 leading-snug">`;
      h += `<div class="text-gray-800 font-medium">${_escHtml(prop)}`
         + `${_bwCalLinkHtml((g.add[0] || g.del[0] || {}).property_id, prop)}</div>`;
      for (const t of g.add) {
        const vipBadge = (window.NLD && NLD.isVipTitle(t.task_name) && !NLD.isFlagDismissed(t.task_id))
          ? ` ${NLD.vipBadgeHtml()}` : "";
        h += `<div class="text-[11px] text-red-700 pl-3 leading-snug">`
           + `+ ${_bwTaskLinkHtml(t.task_id, t.task_name, "")}${vipBadge}</div>`;
      }
      for (const t of g.del) {
        h += `<div class="text-[11px] text-gray-500 pl-3 leading-snug line-through">`
           + `− ${_escHtml(t.task_name || "Task")}</div>`;
      }
      h += `</div>`;
    }
    // Nothing about a task change alters which STOPS are on the route, so this has
    // its own action rather than riding on "Apply to route" — which adds and drops
    // houses and would be the wrong verb entirely.
    h += `<button data-ack-tasks="1"`
       + ` class="w-full mt-1 mb-1 bg-gray-100 hover:bg-gray-200 text-gray-700 text-[11px]`
       + ` font-semibold py-1.5 rounded-lg transition-colors">`
       + `Mark these task changes as seen</button>`;
  }

  if (moved.length) {
    h += `<div class="font-semibold text-blue-700 mt-3 mb-1">🕑 Time changed (${moved.length})</div>`;
    for (const m of moved) {
      h += `<div class="text-gray-700 mb-1">${_escHtml(m.property)}${_bwCalLinkHtml(m.property_id, m.property)}: `
         + `<span class="text-gray-400">${_escHtml(m.was)} → </span>${_escHtml(m.now)}</div>`;
    }
  }

  // Apply-to-route button: add the added properties / drop the removed ones,
  // then leave the route in the editable state for manual reorder + optimize.
  // Hidden once applied (until the next check).
  // Button reflects only OUTSTANDING changes — it disappears on its own once the
  // list matches (manual fix or Apply), no separate "applied" flag needed.
  if (added.length || removed.length) {
    const nAdd = new Set(added.map(a => a.property)).size;
    h += `<button onclick="reapproachWithChanges()"
            class="w-full mt-3 bg-indigo-600 hover:bg-indigo-700 text-white text-xs
                   font-semibold py-2 rounded-lg transition-colors">`
       + `↘ Apply to route — add ${nAdd}, remove ${removed.length}</button>`;
  }
  return h;
}

// Apply the right-panel changes to the route. When the route is OPTIMIZED, each
// added stop is WORKED IN at the end (just like the Work-In feature) and removed
// stops are dropped in place — the optimized order is preserved, nothing is undone.
// When not yet optimized, it just builds the editable list. Only OUTSTANDING
// changes (vs the current list) are applied, so a manual fix is never re-applied.
async function reapproachWithChanges() {
  const data = _routeChangesCache.data;
  if (!data) { alert("Open the route first so the Breezeway changes have loaded."); return; }

  const curNames = new Set(
    (isOptimized ? optimizedSchedule.filter(s => !s.isLunch && !s.isGap) : selectedStops)
      .map(s => (s.name || "").toLowerCase())
  );
  const added   = (data.added   || []).filter(a => !curNames.has((a.property || "").toLowerCase()));
  const removed = (data.removed || []).filter(r =>  curNames.has((r.property || "").toLowerCase()));
  if (!added.length && !removed.length) {
    if (typeof _syncSidebarToSchedule === "function") _syncSidebarToSchedule();
    alert("Nothing left to apply — the list already matches.");
    return;
  }

  const removedSet = new Set(removed.map(r => (r.property || "").toLowerCase()));
  const meta = {};
  for (const a of added) {
    const k = (a.property || "").toLowerCase();
    if (!meta[k]) meta[k] = { arrival: false, pci: false };
    if (a.arrival) meta[k].arrival = true;
    if (a.pci)     meta[k].pci     = true;
  }
  const addedNames = [...new Set(added.map(a => a.property).filter(Boolean))];
  const notFound = [];
  let addedCount = 0, removedCount = 0;

  const _lookup = name => (typeof properties !== "undefined")
    ? properties.find(pr => (pr.name || "").toLowerCase() === name.toLowerCase()) : null;

  if (isOptimized) {
    // ── Preserve the optimized route ──
    // 1) Drop removed properties in place (same as removing a stop).
    if (removedSet.size) {
      const before = optimizedSchedule.filter(s => !s.isLunch && !s.isGap).length;
      optimizedSchedule
        .filter(s => !s.isLunch && !s.isGap && removedSet.has((s.name || "").toLowerCase()))
        .forEach(s => { if (markers[s.name]) { map.removeLayer(markers[s.name]); delete markers[s.name]; } });
      optimizedSchedule = optimizedSchedule.filter(s => s.isLunch || s.isGap || !removedSet.has((s.name || "").toLowerCase()));
      selectedStops     = selectedStops.filter(s => !removedSet.has((s.name || "").toLowerCase()));
      removedCount = before - optimizedSchedule.filter(s => !s.isLunch && !s.isGap).length;
    }
    // 2) Work each added property in AT THE END (Work-In behaviour).
    const present = new Set(optimizedSchedule.filter(s => !s.isLunch && !s.isGap).map(s => (s.name || "").toLowerCase()));
    for (const name of addedNames) {
      const key = name.toLowerCase();
      if (present.has(key)) continue;
      const p = _lookup(name);
      if (!p) { notFound.push(name); continue; }
      const m = meta[key] || {};
      await workInStop(p, !!(m.arrival || m.pci), !!m.pci);   // appends + updates drive times
      present.add(key); addedCount++;
    }
    recalculateTimes(); renderSchedule(); redrawRouteOnMap();
  } else {
    // ── Not optimized — build the editable list, stay pre-optimize ──
    const origReal = selectedStops.filter(s => !s.isLunch && !s.isGap);
    const kept = origReal.filter(s => !removedSet.has((s.name || "").toLowerCase()));
    removedCount = origReal.length - kept.length;
    const have = new Set(kept.map(s => (s.name || "").toLowerCase()));
    for (const name of addedNames) {
      const key = name.toLowerCase();
      if (have.has(key)) continue;
      const p = _lookup(name);
      if (!p) { notFound.push(name); continue; }
      const m = meta[key] || {};
      kept.push({ _id: makeStopId(), name: p.name, lat: p.lat, lng: p.lng,
                  arrival: !!(m.arrival || m.pci), priority_checkin: !!m.pci, serviceMinutes: 60 });
      have.add(key); addedCount++;
    }
    selectedStops = kept;
    renderStops();
    if (typeof _bwPlaceMarkers === "function") _bwPlaceMarkers();
  }

  // Applied: the shown change list no longer reflects a fresh Breezeway check. Collapse
  // the panel and mark it applied — re-expanding says so; Check again refreshes it.
  _routeChangesUiState = { routeId: currentRouteId, collapsed: true, stale: true };
  if (typeof _syncSidebarToSchedule === "function") _syncSidebarToSchedule();

  if (notFound.length) {
    alert(`Applied: +${addedCount} added, −${removedCount} removed.\n\n`
        + `Couldn't add (not in your property DB): ${notFound.join(", ")}`);
  }
}

/* ── BREEZEWAY TASK OVERLAY (after import) ─────────────────────── */

// Single-employee: show task content without tabs
function _bwShowTaskSidebar(date, matched) {
  if (!matched.length) return;
  _bwByAssignee = "bw";   // truthy — prevents _loadDailyRoutes from clobbering BW state
  _bwActiveDate = date;
  _bwTasksByPropName = {};
  _bwPropIdByName    = {};
  for (const p of matched) {
    _bwTasksByPropName[p.name] = p.tasks || [];
    if (p.property_id) _bwPropIdByName[p.name] = p.property_id;
  }
  _syncSidebarToSchedule();
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
    // Reopening reuses the cached discrepancy result (runs once per route load + on
    // an explicit check) — re-pulling here re-ran the heavy scan on every reopen.
    if (typeof _syncSidebarToSchedule === "function") _syncSidebarToSchedule();
  }
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

  // Who's in each house on this route's date (guest/tenant/owner/block). Fetched
  // once per date; when it lands it re-renders and the badges appear. The date is
  // the sidebar's route date (set on load/optimize), falling back to the BW date.
  const _routeDate = (document.getElementById("routeDateField") || {}).value || _bwActiveDate;
  if (_routeDate) _loadOccupancy(_routeDate);

  // Daily-routes mode (no BW tasks): keep stop list in sync with schedule order
  if (!hasBwTasks) {
    content.innerHTML = "";
    stops.forEach((s, i) => {
      const card = document.createElement("div");
      card.className = "py-1.5 border-b border-gray-100 last:border-0";
      const row = document.createElement("div");
      row.className = "flex items-start gap-2";
      const num = document.createElement("span");
      num.className = "text-xs text-gray-400 font-medium w-4 shrink-0 pt-px";
      num.textContent = i + 1;
      // Arrival badge reflects the LIVE scan, not just the saved route — so a check-in
      // that moved to today shows even though this route was saved before the move.
      const liveArrival = s.arrival || _arrivalForStop(s.name);
      const name = document.createElement("span");
      name.className = "text-xs leading-snug " + (liveArrival ? "font-medium text-green-700" : "text-gray-700");
      name.textContent = s.name;
      const cal = _bwCalendarLink(s.name);   // 📅 → property's Breezeway calendar (when we have its id)
      if (cal) name.appendChild(cal);
      row.appendChild(num); row.appendChild(name);
      if (liveArrival) {
        const badge = document.createElement("span");
        badge.className = "shrink-0 text-[0.6rem] font-bold text-green-700 bg-green-100 rounded px-1.5 leading-tight mt-px";
        badge.textContent = "CHECK-IN";
        row.appendChild(badge);
      }
      if (_stopHasVip(s.name)) {                               // gold flag on the house itself
        const vb = NLD.makeVipBadge(); vb.classList.add("shrink-0"); vb.style.marginTop = "1px";
        vb.title = "VIP house — a task here is flagged VIP";
        row.appendChild(vb);
      }
      const occB = _occupancyBadge(_bwPropIdByName[s.name]);   // who's in the house that day
      if (occB) { occB.classList.add("mt-px"); row.appendChild(occB); }
      card.appendChild(row);
      // Auto-loaded tasks for this property that day, for this person. Each links to the
      // task in Breezeway when we have its id (older payloads sent plain title strings).
      const tasks = _tasksForStop(s.name);
      if (tasks && tasks.length) {
        const tl = document.createElement("div");
        tl.className = "pl-6 mt-0.5 space-y-0.5";
        for (const t of tasks) {
          const title = (t && typeof t === "object") ? (t.name || "") : t;
          const tid   = (t && typeof t === "object") ? t.id : null;
          const line  = document.createElement("div");
          line.className = "text-[11px] text-gray-400 leading-snug";
          line.appendChild(document.createTextNode("• "));
          const lbl = _bwTaskLabel(tid, title, "text-gray-400");
          line.appendChild(lbl);
          const vipHere = !!(window.NLD && !NLD.isFlagDismissed(tid) && NLD.isVipTitle(title));
          if (vipHere) { const vb = NLD.makeVipBadge(); vb.style.marginLeft = "5px"; line.appendChild(vb); }
          if (lbl.dataset.timeFlag && window.NLD) NLD.markTimeFlagRow(line, 6);
          if ((lbl.dataset.timeFlag || vipHere) && tid != null && tid !== "" && window.NLD) {
            line.appendChild(NLD.makeFlagRemoveX(tid, _afterFlagDismiss));
          }
          tl.appendChild(line);
        }
        card.appendChild(tl);
      }
      content.appendChild(card);
    });
    _appendRouteChanges(content);
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
    propName.className = "text-xs font-semibold " + (s.arrival ? "text-green-700" : "text-gray-800");
    const propNameText = document.createElement("span");
    propNameText.className = "align-middle";
    propNameText.textContent = s.name;
    propName.appendChild(propNameText);
    const cal = _bwCalendarLink(s.name);
    if (cal) { cal.classList.add("align-middle"); propName.appendChild(cal); }
    if (_stopHasVip(s.name)) {                              // gold flag on the house itself
      const vb = NLD.makeVipBadge();
      vb.classList.add("align-middle"); vb.style.marginLeft = "5px";
      vb.title = "VIP house — a task here is flagged VIP";
      propName.appendChild(vb);
    }
    const occB = _occupancyBadge(_bwPropIdByName[s.name]);   // who's in the house that day
    if (occB) { occB.classList.add("align-middle"); occB.style.marginLeft = "5px"; propName.appendChild(occB); }
    body.appendChild(propName);

    for (const t of tasks) {
      const taskRow = document.createElement("div");
      taskRow.className = "flex items-baseline gap-1 mt-0.5";
      const tnameClass = (s.priority_checkin && _titleHasPci(t.task_name)) ? "text-xs font-bold text-violet-700" : "text-xs text-gray-600";
      const tLbl = _bwTaskLabel(t.task_id, t.task_name, tnameClass);  // → task in Breezeway when we have its id
      taskRow.appendChild(tLbl);
      const vipHere = !!(window.NLD && !NLD.isFlagDismissed(t.task_id) && NLD.isVipTitle(t.task_name));
      if (vipHere) { const vb = NLD.makeVipBadge(); vb.style.marginLeft = "4px"; taskRow.appendChild(vb); }
      if (tLbl.dataset.timeFlag && window.NLD) NLD.markTimeFlagRow(taskRow, 6);
      if ((tLbl.dataset.timeFlag || vipHere) && t.task_id != null && t.task_id !== "" && window.NLD) {
        taskRow.appendChild(NLD.makeFlagRemoveX(t.task_id, _afterFlagDismiss));
      }
      if (t.assignees && t.assignees.length) {
        const asgn = document.createElement("span");
        asgn.className = "text-xs text-gray-400";
        asgn.textContent = "· " + t.assignees.join(", ");
        taskRow.appendChild(asgn);
      }
      if (t.date) {
        const dt = document.createElement("span");
        dt.className = "text-[10px] text-gray-300";  // discreet date confirmation
        dt.textContent = "· " + _fmtTaskDate(t.date);
        taskRow.appendChild(dt);
      }
      body.appendChild(taskRow);
    }

    card.appendChild(num); card.appendChild(body);
    content.appendChild(card);
  });
  _appendRouteChanges(content);
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
      tname.className = (p.priority_checkin && _titleHasPci(t.task_name)) ? "text-xs font-bold text-violet-700" : "text-xs font-medium text-gray-700";
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

// Auto-import when opened as a new window for a specific employee
(function () {
  const params   = new URLSearchParams(window.location.search);
  const bwDate   = params.get("bw_date");
  const bwAsgn   = params.get("bw_assignee");
  if (!bwDate || !bwAsgn) return;
  document.getElementById("bwImportDate").value     = bwDate;
  document.getElementById("bwImportAssignee").value = bwAsgn;
  // Wait for map to initialise before firing
  window.addEventListener("load", () => runBwImport());
})();
