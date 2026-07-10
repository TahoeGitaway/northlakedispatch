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
  return Math.max(15, Math.min(240, sum));     // clamp to the dropdown range (15–240)
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

  try {
    const res  = await fetch("/api/bw-import", {
      method:  "POST",
      headers: {"Content-Type": "application/json"},
      body:    JSON.stringify({date, assignee: assignees[0] || ""}),
    });
    const data = await res.json();

    if (data.error)   { _bwImportMsg(data.error,   "red");  return; }
    if (data.message) { _bwImportMsg(data.message, "gray"); return; }

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
      let msg   = added === 0 ? "All matched properties already in the list." : `Added ${added} stop${added !== 1 ? "s" : ""}.`;
      let color = "green";
      const uncertain = data.uncertain || [];
      const unmatched = data.unmatched || [];
      if (uncertain.length) {
        msg  += ` ${uncertain.length} unsure match${uncertain.length !== 1 ? "es" : ""} — confirm below.`;
        color = "amber";
      }
      if (unmatched.length) {
        msg  += ` Not found: ${unmatched.join(", ")}.`;
        color = added > 0 ? "amber" : "red";
      }
      if (data.failed_properties) {
        msg  += ` ⚠ ${data.failed_properties} propert${data.failed_properties === 1 ? "y" : "ies"} couldn't be loaded from Breezeway — re-import to retry so no tasks are missed.`;
        color = "amber";
      }
      _bwImportMsg(msg, color);
      _bwShowTaskSidebar(date, data.matched || []);
      _bwRenderUncertain(date, uncertain);
      _bwPlaceMarkers();
      document.getElementById("routeDateField").value  = date;
      document.getElementById("assignedToField").value = assignees[0] || "";
      if (typeof updateRouteMapOverlay === "function") updateRouteMapOverlay();
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

// A loaded saved route stores priority_checkin from when it was saved — which can be
// stale BOTH ways: a PCI added in Breezeway later (needs flagging), OR a flag left on
// a walk-thru whose arrival is actually another day (needs clearing — e.g. a route
// saved before the same-day rule existed). So whenever we have the route's LIVE
// Breezeway tasks, the same-day rule is AUTHORITATIVE for every property that has a
// PCI-titled task: backend `c.pci` true → flag it; false → demote it to a plain stop.
// Properties with NO PCI task are left untouched, so manual Priority / Check-in
// toggles on ordinary stops are preserved.
function _flagPciFromTasks(currentTasks) {
  if (!Array.isArray(currentTasks) || !currentTasks.length) return;
  const wantByProp = new Map();   // propLower -> should-be-PCI (boolean)
  for (const c of currentTasks) {
    const hasPciTitle = (c.tasks || []).some(t =>
      _titleHasPci(typeof t === "string" ? t : ((t && (t.task_name || t.title)) || "")));
    if (!hasPciTitle) continue;
    // Trust the backend `pci` flag (already requires a SAME-DAY arrival). Older
    // payloads without the field fall back to "has PCI title" = treat as same-day.
    const sameDay = ("pci" in c) ? !!c.pci : true;
    wantByProp.set((c.property || "").toLowerCase(), sameDay);
  }
  if (!wantByProp.size) return;

  let changed = false;
  const allPci   = new Map();   // nameLower -> display name, for EVERY PCI stop on the route
  const promoted = new Set();   // nameLower of stops that just BECAME a PCI since save
  const apply = s => {
    if (!s) return;
    const key  = (s.name || "").toLowerCase();
    const want = wantByProp.get(key);
    if (want === undefined) return;                 // no PCI task here → leave as-is
    if (want) {
      // This stop is (or has just become) an arrive-by-noon priority check-in. Track
      // every one on the route — an existing PCI is just as easy to overlook among a
      // long list as a brand-new one — and flag the ones that flipped since the save.
      if (!s.priority_checkin) {
        promoted.add(key);
        s.priority_checkin = true; s.arrival = true; changed = true;
      }
      allPci.set(key, s.name || "(unnamed stop)");
    } else if (s.priority_checkin || s.arrival) {
      // Stale same-day PCI flag (pre-fix save, or an arrival that has since moved to
      // another day) → this walk-thru is for another day, so make it a plain stop.
      s.priority_checkin = false; s.arrival = false; changed = true;
    }
  };
  (typeof selectedStops !== "undefined" ? selectedStops : []).forEach(apply);
  (typeof optimizedSchedule !== "undefined" ? optimizedSchedule : []).forEach(apply);

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
    // load (and on explicit ↻ Recheck), not on every reopen. Re-running it here was
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
  loadRouteById(routeId).then(() => {
    _syncSidebarToSchedule();
  }).catch(() => {
    content.innerHTML = `<div class="text-xs text-red-400 text-center py-4">Failed to load.</div>`;
  });
}

/* ── ROUTE DISCREPANCY CHECK (saved route vs live Breezeway) ─────── */

let _routeChangesCache    = { routeId: null, html: null };
let _routeChangesInflight = { routeId: null, promise: null };
let _appliedRouteChanges  = new Set();   // route ids whose changes have been applied (hide the Apply button)

// Force the NEXT render to re-pull from Breezeway instead of serving the cached
// result. Used on explicit ↻ Recheck and to avoid caching an error; a page reload
// gets a fresh run for free (in-memory cache starts empty). Passive reopens and
// redraws within a session reuse the cache, so the check runs once per route load.
function _invalidateRouteChanges() {
  _routeChangesCache    = { routeId: null, html: null };
  _routeChangesInflight = { routeId: null, promise: null };
}

// Append the "Changes vs Breezeway" block for the currently-loaded saved route.
// Cheap to re-run: re-renders from cache on later panel redraws, and shares a
// single in-flight request per route so redraws don't re-hit the heavy endpoint.
function _appendRouteChanges(content) {
  if (!currentRouteId) return;
  const rid = currentRouteId;
  const box = document.createElement("div");
  box.className = "mt-3 pt-3 border-t border-gray-200";
  box.innerHTML = `
    <div class="flex items-center justify-between mb-2">
      <span class="text-xs font-semibold text-gray-700 uppercase tracking-wide">Changes vs Breezeway</span>
      <button data-refresh class="text-xs text-indigo-500 hover:text-indigo-700 font-medium">↻ Recheck</button>
    </div>
    <div data-body class="text-xs text-gray-400">Checking Breezeway…</div>`;
  content.appendChild(box);
  const body = box.querySelector("[data-body]");
  box.querySelector("[data-refresh]").addEventListener("click", () => {
    _invalidateRouteChanges();
    _appliedRouteChanges.delete(rid);   // a fresh check brings the Apply button back
    _renderRouteChangesInto(rid, body, true);   // explicit Recheck = bypass the server cache
  });
  _renderRouteChangesInto(rid, body);
}

function _renderRouteChangesInto(routeId, body, force) {
  // Re-render from cached DATA (not a frozen html string) so the panel reflects
  // the CURRENT list each time — manual or applied fixes clear resolved changes.
  if (_routeChangesCache.routeId === routeId && _routeChangesCache.data) {
    body.innerHTML = _renderChangesHtml(_routeChangesCache.data);
    return;
  }
  body.innerHTML = `<span class="text-gray-400">Checking Breezeway…</span>`;
  if (_routeChangesInflight.routeId !== routeId || !_routeChangesInflight.promise) {
    _routeChangesInflight = {
      routeId,
      // no-store: the GET is otherwise HTTP-cacheable, which made a page reload show
      // the stale browser-cached result instead of a live re-check. force=1 (explicit
      // Recheck only) tells the server to skip ITS short-lived result cache; passive
      // reopens omit it so they ride the cache and don't re-run the heavy all-houses
      // scan (which is what was timing out at the gateway → HTTP 503).
      promise: fetch(`/api/route-discrepancies?route_id=${routeId}${force ? "&force=1" : ""}`, { cache: "no-store" })
        .then(r => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json();
        }),
    };
  }
  _routeChangesInflight.promise.then(data => {
    if (data.error) {
      console.error("[route-changes] route", routeId, "server error:", data.error);
      body.innerHTML = `<span class="text-red-500">${_escHtml(data.error)} — reopen the sidebar or click ↻ Recheck to retry.</span>`;
      _invalidateRouteChanges();   // never cache an error — let a reopen/recheck retry
      return;
    }
    if (data.failed_properties) {
      console.warn("[route-changes] route", routeId, "—", data.failed_properties,
                   "propert(y/ies) failed to load from Breezeway; task list may be incomplete");
    }
    const html = _renderChangesHtml(data);
    body.innerHTML = html;
    _routeChangesCache = { routeId, html, data };
    _flagPciFromTasks(data.current_tasks);   // a saved route's flag can be stale — re-detect PCI from live tasks
    _syncSidebarToSchedule();   // re-paint stops now that we have each property's tasks
  }).catch(e => {
    console.error("[route-changes] route", routeId, "fetch failed:", e);
    body.innerHTML = `<span class="text-red-500">Could not check Breezeway: ${_escHtml(e.message)} — reopen the sidebar or click ↻ Recheck to retry.</span>`;
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

  // Loud, non-silent warning when Breezeway dropped some houses — the comparison
  // (and the auto-loaded task titles) are then incomplete, so don't trust "no changes".
  if (d.failed_properties) {
    h += `<div class="mb-2 text-[11px] text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1 leading-snug">`
       + `⚠ ${d.failed_properties} propert${d.failed_properties === 1 ? "y" : "ies"} couldn't be loaded from Breezeway — some tasks may be missing. Click ↻ Recheck to retry.`
       + `</div>`;
  }

  // ── New same-day check-ins (arrival moved to today on a stop you already have) ──
  // Shown first because it's time-critical: a check-in is arrive-by-noon.
  if (newCheckin.length) {
    h += `<div class="font-semibold text-amber-800 mb-1">☀️ New check-in today (${newCheckin.length})</div>`;
    for (const c of newCheckin) {
      h += `<div class="mb-1.5 leading-snug bg-amber-50 border-l-2 border-amber-400 rounded-r pl-2 pr-1 py-1">`;
      h += `<div class="text-gray-800 font-medium">${_escHtml(c.property)}`
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

  // ── What changed since the route was saved ──
  if (!added.length && !removed.length && !moved.length && !newCheckin.length) {
    h += `<div class="text-green-600 mb-1">✓ No changes — the list matches the saved route.</div>`;
  }
  if (added.length) {
    h += `<div class="font-semibold text-red-700 mb-1">➕ Added to list (${added.length})</div>`;
    // Group by property so it reads like the stop list above: house header + bulleted tasks.
    const byProp = {};
    for (const a of added) (byProp[a.property] = byProp[a.property] || []).push(a);
    for (const prop of Object.keys(byProp)) {
      // A priority check-in (PCI) = arrive by noon. Mark it loudly: a badge on the
      // house header AND a highlighted, badged line for each PCI task.
      // `a.pci` already requires a same-day arrival; a next-day PCI stays unflagged.
      const propPci = byProp[prop].some(a => a.pci);
      h += `<div class="mb-1.5 leading-snug">`;
      h += `<div class="text-gray-800 font-medium">${_escHtml(prop)}`
         + (propPci ? ` <span class="inline-block align-middle bg-violet-600 text-white text-[10px] font-bold px-1.5 py-0.5 rounded">⚡ PRIORITY CHECK-IN</span>` : "")
         + `</div>`;
      for (const a of byProp[prop]) {
        const isPci = !!a.pci;
        const who  = a.history && a.history.who  ? _escHtml(a.history.who) : null;
        const when = a.history && a.history.when ? _fmtChangeWhen(a.history.when) : null;
        const note = (who || when)
          ? ` <span class="text-gray-400">(${when ? "added " + _escHtml(when) : "added"}${who ? " by " + who : ""})</span>`
          : ` <span class="text-gray-300 italic">(when/who not exposed)</span>`;
        const pciBadge = isPci
          ? ` <span class="inline-block bg-violet-600 text-white text-[10px] font-bold px-1 rounded">⚡ BY NOON</span>`
          : "";
        const lineCls = isPci
          ? "text-[11px] text-violet-900 font-semibold bg-violet-50 border-l-2 border-violet-500 rounded-r pl-2 pr-1 py-0.5 leading-snug"
          : "text-[11px] text-gray-500 pl-3 leading-snug";
        h += `<div class="${lineCls}">• ${_escHtml(a.task_name)}${pciBadge}${note}</div>`;
      }
      h += `</div>`;
    }
  }
  if (removed.length) {
    h += `<div class="font-semibold text-amber-700 mt-3 mb-1">➖ No longer on list (${removed.length})</div>`;
    for (const r of removed) h += `<div class="text-gray-700 mb-1">${_escHtml(r.property)}</div>`;
  }
  if (moved.length) {
    h += `<div class="font-semibold text-blue-700 mt-3 mb-1">🕑 Time changed (${moved.length})</div>`;
    for (const m of moved) {
      h += `<div class="text-gray-700 mb-1">${_escHtml(m.property)}: `
         + `<span class="text-gray-400">${_escHtml(m.was)} → </span>${_escHtml(m.now)}</div>`;
    }
  }

  // Apply-to-route button: add the added properties / drop the removed ones,
  // then leave the route in the editable state for manual reorder + optimize.
  // Hidden once applied (until the next Recheck).
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

  // The changes panel re-renders against the current list (resolved items vanish,
  // button hides on its own). Make sure it repaints now.
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
    // ↻ Recheck) — re-pulling here was re-running the heavy scan on every reopen.
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
      row.appendChild(num); row.appendChild(name);
      if (liveArrival) {
        const badge = document.createElement("span");
        badge.className = "shrink-0 text-[0.6rem] font-bold text-green-700 bg-green-100 rounded px-1.5 leading-tight mt-px";
        badge.textContent = "CHECK-IN";
        row.appendChild(badge);
      }
      card.appendChild(row);
      // Auto-loaded task titles for this property that day, for this person
      const tasks = _tasksForStop(s.name);
      if (tasks && tasks.length) {
        const tl = document.createElement("div");
        tl.className = "pl-6 mt-0.5 space-y-0.5";
        tl.innerHTML = tasks.map(t => `<div class="text-[11px] text-gray-400 leading-snug">• ${_escHtml(t)}</div>`).join("");
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
    body.appendChild(propName);

    for (const t of tasks) {
      const taskRow = document.createElement("div");
      taskRow.className = "flex items-baseline gap-1 mt-0.5";
      const tname = document.createElement("span");
      tname.className = (s.priority_checkin && _titleHasPci(t.task_name)) ? "text-xs font-bold text-violet-700" : "text-xs text-gray-600";
      tname.textContent = t.task_name;
      taskRow.appendChild(tname);
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
