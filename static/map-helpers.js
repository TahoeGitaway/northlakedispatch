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

/* ── GEOCODE (address → {name, lat, lng} via server) ── */
async function geocodeAddress(address) {
  const res  = await fetch("/geocode", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ address }),
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error);
  return data; // { name, lat, lng }
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
