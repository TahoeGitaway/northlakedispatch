/* ================================================================
   MAP — Leaflet init, markers, route polyline
   Depends on: startLocation, optimizedSchedule, routeLayer,
               activeRouteMarkers, markers, regularIcon (globals)
================================================================ */

const TAHOE_BOUNDS = L.latLngBounds(
  L.latLng(38.8, -120.8),
  L.latLng(39.8, -119.6)
);

const map = L.map('map', {
  minZoom: 11,
  maxBounds: TAHOE_BOUNDS,
  maxBoundsViscosity: 0.8,
}).setView([39.3279, -120.1833], 12);

L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
  maxZoom: 19,
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>'
}).addTo(map);

/* ── Always-visible specialist + date pill, pinned to the top of the map ── */
(function initRouteMapOverlay() {
  const overlay = document.createElement("div");
  overlay.id = "routeMapOverlay";
  overlay.style.cssText = [
    "position:absolute", "top:12px", "left:50%", "transform:translateX(-50%)",
    "z-index:1000", "background:rgba(255,255,255,0.96)", "border:1px solid #e5e7eb",
    "border-radius:9999px", "box-shadow:0 2px 10px rgba(0,0,0,0.12)",
    "padding:7px 18px", "font-size:13px", "font-weight:600", "color:#374151",
    "display:none", "align-items:center", "gap:10px", "white-space:nowrap",
    "pointer-events:none"
  ].join(";");
  document.getElementById("map").appendChild(overlay);

  ["assignedToField", "routeDateField"].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input",  updateRouteMapOverlay);
    el.addEventListener("change", updateRouteMapOverlay);
  });
  updateRouteMapOverlay();
})();

// Mirror the sidebar's assignee + date onto the map pill. Call this after any
// programmatic change to those fields (import, route load).
function updateRouteMapOverlay() {
  const el = document.getElementById("routeMapOverlay");
  if (!el) return;
  const who  = (document.getElementById("assignedToField")?.value || "").trim();
  const date = (document.getElementById("routeDateField")?.value || "").trim();
  if (!who && !date) { el.style.display = "none"; return; }

  let dateLabel = "";
  if (date) {
    const d = new Date(date + "T00:00:00");
    dateLabel = isNaN(d.getTime())
      ? date
      : d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
  }
  el.innerHTML =
    (who ? `<span>👤 ${who}</span>` : "") +
    (who && dateLabel ? `<span style="color:#d1d5db;">·</span>` : "") +
    (dateLabel ? `<span>📅 ${dateLabel}</span>` : "");
  el.style.display = "flex";
}

function clearRouteMarkers() {
  activeRouteMarkers.forEach(m => map.removeLayer(m));
  activeRouteMarkers = []; markers = {};
}

// In-flight geometry request — aborted when a newer redraw starts
let _routeAbortCtrl = null;

async function redrawRouteOnMap(precomputedPolyline = null) {
  // Cancel any pending geometry fetch from a previous redraw
  if (_routeAbortCtrl) { _routeAbortCtrl.abort(); _routeAbortCtrl = null; }

  clearRouteMarkers();
  if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }

  const real = optimizedSchedule.filter(s => !s.isLunch && s.lat && s.lng);

  const startM = L.marker([startLocation.lat, startLocation.lng], { icon:regularIcon })
    .addTo(map).bindPopup(`<b>Start / End</b><br>${startLocation.name}`);
  activeRouteMarkers.push(startM);

  if (!real.length) return;

  const allPoints = [startLocation, ...real];

  // ── Draw route polyline (wrapped so a bad polyline never blocks marker rendering) ──
  try {
    if (precomputedPolyline && precomputedPolyline.length > 1) {
      routeLayer = L.polyline(precomputedPolyline, { color:"#6366f1", weight:5 }).addTo(map);
      map.invalidateSize();
      map.fitBounds(routeLayer.getBounds(), { padding: [60, 60], maxZoom: 14 });
    } else {
      const latlngs = allPoints.map(s => [s.lat, s.lng]);
      routeLayer = L.polyline(latlngs, {
        color:"#94a3b8", weight:3, dashArray:"8,6", opacity:0.6
      }).addTo(map);
      map.invalidateSize();
      map.fitBounds(routeLayer.getBounds(), { padding: [60, 60], maxZoom: 14 });
    }
  } catch(_) {}

  // ── Add markers (always runs, even if polyline above threw) ──
  let num = 1;
  real.forEach(stop => {
    const dep = minutesToHHMM(stop.eta_minutes + stop.serviceMinutes);
    let sh = "";
    if (stop.priority_checkin && stop.priority_late)
      sh = "<span style='color:#dc2626;font-weight:700;'>PRIORITY — LATE</span>";
    else if (stop.priority_checkin)
      sh = "<span style='color:#7c3aed;font-weight:700;'>PRIORITY CHECK-IN</span>";
    else if (stop.arrival && stop.late)
      sh = "<span style='color:#dc2626;font-weight:700;'>LATE CHECK-IN</span>";
    else if (stop.arrival)
      sh = "<span style='color:#16a34a;font-weight:700;'>CHECK-IN</span>";
    const m = L.marker([stop.lat, stop.lng], { icon: pickStopIcon(stop) })
      .addTo(map)
      .bindPopup(`<b>${stop.name}</b><br>Arrive: ${stop.eta}<br>Depart: ${dep}<br>${sh}`);
    m.bindTooltip(`${num++}`, { permanent:true, direction:"top", className:"route-number" });
    activeRouteMarkers.push(m);
    markers[stop.name] = m;
  });

  // Pre-computed geometry already drawn (Google Matrix optimize) — no need to fetch again
  if (precomputedPolyline && precomputedPolyline.length > 1) return;
  // Dashed straight-line fallback stays — no background Google call
}
