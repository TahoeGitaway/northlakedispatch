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

  // ── If Google Maps geometry was pre-computed (e.g. from /optimize response), draw it now ──
  if (precomputedPolyline && precomputedPolyline.length > 1) {
    routeLayer = L.polyline(precomputedPolyline, { color:"#6366f1", weight:5 }).addTo(map);
    map.fitBounds(routeLayer.getBounds(), { padding: [40, 40] });
  } else {
    // Draw dashed straight-line fallback immediately while fetching real geometry
    const latlngs = allPoints.map(s => [s.lat, s.lng]);
    routeLayer = L.polyline(latlngs, {
      color:"#94a3b8", weight:3, dashArray:"8,6", opacity:0.6
    }).addTo(map);
    map.fitBounds(routeLayer.getBounds(), { padding: [40, 40] });
  }

  // ── Add markers now (no geometry dependency) ──
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

  // Pre-computed geometry already drawn — no need to fetch again
  if (precomputedPolyline && precomputedPolyline.length > 1) return;

  // ── Fetch real road geometry from server (Google Directions API) in the background ──
  const ctrl = new AbortController();
  _routeAbortCtrl = ctrl;
  setTimeout(() => ctrl.abort(), 10000); // give up after 10 s

  const locations = allPoints.map(s => ({ lat: s.lat, lng: s.lng }));
  try {
    const resp  = await fetch("/route-geometry", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ locations }),
      signal:  ctrl.signal,
    });
    const rdata = await resp.json();
    if (rdata.coords && rdata.coords.length > 1 && !ctrl.signal.aborted) {
      if (routeLayer) map.removeLayer(routeLayer);
      routeLayer = L.polyline(rdata.coords, { color:"#6366f1", weight:5 }).addTo(map);
      map.fitBounds(routeLayer.getBounds(), { padding: [40, 40] });
    }
  } catch(_) {
    // Timeout or error — dashed fallback stays
  } finally {
    if (_routeAbortCtrl === ctrl) _routeAbortCtrl = null;
  }
}
