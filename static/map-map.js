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
  minZoom: 10,
  maxBounds: TAHOE_BOUNDS,
  maxBoundsViscosity: 0.8,
}).setView([39.3279, -120.1833], 11);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom:18 }).addTo(map);

function clearRouteMarkers() {
  activeRouteMarkers.forEach(m => map.removeLayer(m));
  activeRouteMarkers = []; markers = {};
}

// In-flight OSRM request — aborted when a newer redraw starts
let _routeAbortCtrl = null;

async function redrawRouteOnMap() {
  // Cancel any pending OSRM call from a previous redraw
  if (_routeAbortCtrl) { _routeAbortCtrl.abort(); _routeAbortCtrl = null; }

  clearRouteMarkers();
  if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }

  const real = optimizedSchedule.filter(s => !s.isLunch && s.lat && s.lng);

  const startM = L.marker([startLocation.lat, startLocation.lng], { icon:regularIcon })
    .addTo(map).bindPopup(`<b>Start / End</b><br>${startLocation.name}`);
  activeRouteMarkers.push(startM);

  if (!real.length) return;

  const allPoints = [startLocation, ...real];

  // ── Draw fallback polyline immediately so the map is always responsive ──
  // Dashed style signals "approximate". Replaced by real geometry if OSRM responds.
  const latlngs = allPoints.map(s => [s.lat, s.lng]);
  routeLayer = L.polyline(latlngs, {
    color: "#6366f1", weight: 4, dashArray: "10,7", opacity: 0.75
  }).addTo(map);

  // ── Add markers now (no OSRM dependency) ──
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

  // ── Try OSRM in the background; replace dashed line with real geometry if it responds ──
  const ctrl = new AbortController();
  _routeAbortCtrl = ctrl;
  setTimeout(() => ctrl.abort(), 5000); // give up after 5 s

  const coordStr = allPoints.map(s => `${s.lng},${s.lat}`).join(";");
  try {
    const resp  = await fetch(
      `https://router.project-osrm.org/route/v1/driving/${coordStr}?overview=full&geometries=geojson`,
      { signal: ctrl.signal }
    );
    const rdata = await resp.json();
    const geo   = rdata.routes?.[0]?.geometry;
    if (geo && !ctrl.signal.aborted) {
      if (routeLayer) map.removeLayer(routeLayer);
      routeLayer = L.geoJSON(geo, { style: { color:"#6366f1", weight:5 } }).addTo(map);
    }
  } catch(_) {
    // Timeout or network error — dashed fallback stays
  } finally {
    if (_routeAbortCtrl === ctrl) _routeAbortCtrl = null;
  }
}
