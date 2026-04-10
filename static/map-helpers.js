/* ================================================================
   HELPERS — time formatting, icons, stop IDs, session guard
   Depends on: startLocation (global), DEADLINE_MINS, PRIORITY_MINS
================================================================ */

function makeStopId() { return "stop_" + (++_stopIdCounter); }

function minutesToHHMM(m) {
  m = Math.max(0, Math.round(m));
  const totalMins = m % (24 * 60);
  const h24 = Math.floor(totalMins / 60) % 24;
  const mins = totalMins % 60;
  const ampm = h24 >= 12 ? 'PM' : 'AM';
  const h12  = h24 % 12 === 0 ? 12 : h24 % 12;
  return `${h12}:${String(mins).padStart(2,'0')} ${ampm}`;
}

function hhmmToMinutes(hhmm) {
  const [h, m] = hhmm.split(":").map(Number);
  return h * 60 + m;
}

function generateTimeOptions(selected) {
  let html = "";
  for (let m = 15; m <= 240; m += 15)
    html += `<option value="${m}" ${m===selected?"selected":""}>${m} min</option>`;
  return html;
}

function makeIcon(color) {
  return new L.Icon({
    iconUrl:   `https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-${color}.png`,
    shadowUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-shadow.png",
    iconSize:[25,41], iconAnchor:[12,41]
  });
}

const regularIcon      = makeIcon("blue");
const checkinIcon      = makeIcon("green");
const priorityIcon     = makeIcon("violet");
const nearDeadlineIcon = makeIcon("red");
const lunchIcon        = makeIcon("orange");

function pickStopIcon(stop) {
  if (stop.isLunch)                                 return lunchIcon;
  if (stop.priority_checkin && stop.priority_late)  return nearDeadlineIcon;
  if (stop.priority_checkin)                        return priorityIcon;
  if (stop.arrival && stop.late)                    return nearDeadlineIcon;
  if (stop.arrival)                                 return checkinIcon;
  return regularIcon;
}

/* ── HAVERSINE MATRIX (OSRM fallback) ── */
// Returns an NxN drive-time matrix in seconds using straight-line distance
// × a mountain-road detour factor ÷ average speed. Used when OSRM is down.
function haversineMatrix(locs) {
  const n   = locs.length;
  const mat = Array.from({length: n}, () => Array(n).fill(0));
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < n; j++) {
      if (i === j) continue;
      const R  = 6371000;
      const φ1 = locs[i].lat * Math.PI / 180;
      const φ2 = locs[j].lat * Math.PI / 180;
      const Δφ = (locs[j].lat - locs[i].lat) * Math.PI / 180;
      const Δλ = (locs[j].lng - locs[i].lng) * Math.PI / 180;
      const a  = Math.sin(Δφ/2)**2 + Math.cos(φ1)*Math.cos(φ2)*Math.sin(Δλ/2)**2;
      const d  = R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
      // Tahoe mountain roads ≈ 1.4× straight-line, avg 35 mph (15.6 m/s)
      mat[i][j] = Math.round(d * 1.4 / 15.6);
    }
  }
  return mat;
}

/* ── SESSION GUARD ── */
function guardResponse(res) {
  if (res.redirected || res.url.includes("/login")) {
    document.getElementById("sessionBanner").style.display = "block";
    return Promise.reject("session_expired");
  }
  const ct = res.headers.get("content-type") || "";
  if (!ct.includes("json")) {
    return Promise.reject(new Error(`Server error (${res.status})`));
  }
  return res.json();
}
