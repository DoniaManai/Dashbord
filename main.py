# main.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any, List, Set
import json
import os
import math

from nicegui import ui, app
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi import Query

# =========================
# CONFIG
# =========================
# Resolve data paths relative to this file so we can run the app from anywhere
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# Input GeoJSON files (served through the API as-is)
BUILDINGS_PATH = os.path.join(DATA_DIR, 'buildings.geojson')
ROADS_PATH = os.path.join(DATA_DIR, 'roads.geojson')
# Traffic is a GeoJSON of road segments with per-interval traffic properties
TRAFFIC_PATH = os.path.join(DATA_DIR, 'traffic_agg.geojson')

# Static web assets (must contain map_deck.html)
STATIC_DIR = os.path.join(DATA_DIR, 'static')


# =========================
# HELPERS
# =========================
def sanitize_json(obj: Any) -> Any:
    """
    Recursively traverse an object (dict/list/scalar) and replace NaN/Infinity
    with None so the JSON is valid and the browser doesn’t choke on it.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_json(x) for x in obj]
    return obj


def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO-ish datetime string (allowing ' ' or 'T' as separator).
    Returns a datetime or None if parsing fails.
    """
    if not dt_str:
        return None
    dt = dt_str.replace(' ', 'T')
    try:
        return datetime.fromisoformat(dt)
    except Exception:
        return None


def feature_in_range(feat: Dict[str, Any], start: Optional[datetime], end: Optional[datetime]) -> bool:
    """
    Check if a traffic feature’s [begin, end] interval overlaps the [start, end] filter.
    If the feature lacks begin/end, we exclude it.
    """
    p = feat.get('properties', {})
    b = parse_iso(p.get('begin'))
    e = parse_iso(p.get('end'))
    if b is None or e is None:
        return False
    # Completely before the filter window
    if start and e < start:
        return False
    # Completely after the filter window
    if end and b > end:
        return False
    return True


def compute_min_max(features: List[Dict[str, Any]], var: str) -> Dict[str, Optional[float]]:
    """
    Compute min/max for a numeric property `var` across a list of features.
    Skips invalid values (NaN/Inf/non-numeric). Returns {'min': None, 'max': None} if empty.
    """
    vals: List[float] = []
    for ft in features:
        v = ft.get('properties', {}).get(var)
        if isinstance(v, (int, float)):
            vf = float(v)
            if not (math.isnan(vf) or math.isinf(vf)):
                vals.append(vf)
    if not vals:
        return {'min': None, 'max': None}
    return {'min': min(vals), 'max': max(vals)}


def list_variables(example_feature: Dict[str, Any]) -> List[str]:
    """
    From one example feature, list all numeric property names that look like metrics.
    We skip typical non-metric keys such as id/begin/end.
    Sorted alphabetically for stable UI ordering.
    """
    if not example_feature:
        return []
    props = example_feature.get('properties', {}) or {}
    skip: Set[str] = {'id', 'begin', 'end'}
    out: List[str] = []
    for k, v in props.items():
        if k in skip:
            continue
        if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            out.append(k)
    out.sort()
    return out


def latest_interval(features: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    """
    Find the feature with the latest 'end' timestamp and return its [begin, end].
    Used by clients that want a quick “go to latest” without scanning everything.
    """
    latest_end: Optional[datetime] = None
    latest_begin: Optional[datetime] = None
    for ft in features:
        p = ft.get('properties', {})
        b = parse_iso(p.get('begin'))
        e = parse_iso(p.get('end'))
        if b and e:
            if latest_end is None or e > latest_end:
                latest_end = e
                latest_begin = b
    return {
        'begin': latest_begin.isoformat() if latest_begin else None,
        'end': latest_end.isoformat() if latest_end else None,
    }


# =========================
# LOAD DATA INTO MEMORY
# =========================
def safe_load_geojson(path: str) -> Dict[str, Any]:
    """
    Load a GeoJSON file defensively:
    - If missing, return an empty FeatureCollection
    - Sanitize numeric values so they’re JSON-friendly
    """
    if not os.path.exists(path):
        return {'type': 'FeatureCollection', 'features': []}
    with open(path, 'r', encoding='utf-8') as f:
        return sanitize_json(json.load(f))

# Static snapshots in memory; fast to serve in small/medium datasets.
BUILDINGS_GJ: Dict[str, Any] = safe_load_geojson(BUILDINGS_PATH)
ROADS_GJ: Dict[str, Any] = safe_load_geojson(ROADS_PATH)
TRAFFIC_GJ_ALL: Dict[str, Any] = safe_load_geojson(TRAFFIC_PATH)
TRAFFIC_FEATURES_ALL: List[Dict[str, Any]] = TRAFFIC_GJ_ALL.get('features', [])

# Precompute available numeric variables and global min/max per variable
TRAFFIC_VARS: List[str] = list_variables(TRAFFIC_FEATURES_ALL[0] if TRAFFIC_FEATURES_ALL else {})
GLOBAL_STATS: Dict[str, Dict[str, Optional[float]]] = {
    var: compute_min_max(TRAFFIC_FEATURES_ALL, var) for var in TRAFFIC_VARS
}


# =========================
# API ENDPOINTS
# =========================
@app.get('/api/map/buildings')
def api_buildings() -> JSONResponse:
    """Serve buildings GeoJSON as-is."""
    return JSONResponse(BUILDINGS_GJ)


@app.get('/api/map/roads')
def api_roads() -> JSONResponse:
    """Serve roads GeoJSON as-is."""
    return JSONResponse(ROADS_GJ)


@app.get('/api/map/traffic')
def api_traffic(
    start: Optional[str] = Query(default=None, description="ISO YYYY-MM-DDTHH:MM:SS"),
    end: Optional[str]   = Query(default=None, description="ISO YYYY-MM-DDTHH:MM:SS"),
    var: str             = Query(default='vehicles', description="variable used for coloring"),
    scope: str           = Query(default='filtered', description="'filtered' or 'global' for min/max"),
) -> JSONResponse:
    """
    Return a FeatureCollection of traffic segments filtered by [start, end] if provided.
    The payload also includes simple meta: the variable, min/max, feature count, and scope.

    - start/end: ISO strings (space or 'T' accepted). If absent, no time filter is applied.
    - var: which numeric property the client will visualize (e.g., 'vehicles').
    - scope:
        * 'filtered' -> min/max are computed on the filtered subset.
        * 'global'   -> min/max use the precomputed dataset-wide stats.
    """
    dt_start = parse_iso(start)
    dt_end   = parse_iso(end)

    # Filter features by interval overlap only if we have a start or end bound
    feats: List[Dict[str, Any]] = (
        [ft for ft in TRAFFIC_FEATURES_ALL if feature_in_range(ft, dt_start, dt_end)]
        if (dt_start or dt_end) else TRAFFIC_FEATURES_ALL
    )

    # Compute min/max either globally (cached) or on the filtered subset
    stats = GLOBAL_STATS.get(var) if scope == 'global' else compute_min_max(feats, var)
    if scope == 'global' and not stats:
        # Fallback if var wasn’t in the global dict (e.g., new field)
        stats = compute_min_max(TRAFFIC_FEATURES_ALL, var)

    payload: Dict[str, Any] = {
        'type': 'FeatureCollection',
        'features': feats,
        'meta': {
            'var': var,
            'min': (stats or {}).get('min'),
            'max': (stats or {}).get('max'),
            'count': len(feats),
            'scope': scope,
        },
    }
    # Ensure no NaN/Inf sneaks into the response
    return JSONResponse(sanitize_json(payload))


@app.get('/api/map/traffic/minmax')
def api_traffic_minmax(
    var: str = Query(default='vehicles'),
    start: Optional[str] = Query(default=None),
    end: Optional[str]   = Query(default=None),
    scope: str           = Query(default='filtered'),
) -> JSONResponse:
    """
    Return only min/max for a variable.
    - If scope='global' (or no start/end), use the precomputed global stats.
    - Otherwise, compute on the filtered subset in [start, end].
    """
    dt_start = parse_iso(start)
    dt_end   = parse_iso(end)
    if scope == 'global' or (not dt_start and not dt_end):
        stats = GLOBAL_STATS.get(var) or compute_min_max(TRAFFIC_FEATURES_ALL, var)
    else:
        feats = [ft for ft in TRAFFIC_FEATURES_ALL if feature_in_range(ft, dt_start, dt_end)]
        stats = compute_min_max(feats, var)
    return JSONResponse({'min': stats.get('min'), 'max': stats.get('max')})


@app.get('/api/map/traffic/latest')
def api_traffic_latest() -> JSONResponse:
    """
    Convenience endpoint: return the latest [begin, end] seen in the dataset
    so clients can jump to the most recent interval.
    """
    info = latest_interval(TRAFFIC_FEATURES_ALL)
    return JSONResponse(info)


@app.get('/api/map/traffic/vars')
def api_traffic_vars() -> JSONResponse:
    """List the numeric variables available for coloring (sorted)."""
    return JSONResponse({'variables': TRAFFIC_VARS})


# (optional) noise endpoint if you later provide a GeoJSON
@app.get('/api/map/noise')
def api_noise() -> JSONResponse:
    """
    Placeholder for a future noise layer. Replace with your dataset when ready.
    Must return a GeoJSON FeatureCollection.
    """
    return JSONResponse({'type': 'FeatureCollection', 'features': []})


# =========================
# STATIC FILES & ROOT ROUTE
# =========================
# Expose the /static folder (serves map_deck.html and assets)
app.add_static_files('/static', STATIC_DIR)

@app.get('/', include_in_schema=False)
def root() -> RedirectResponse:
    """
    Redirect the root URL to the dashboard HTML.
    Add a cache-busting query param when you update the file.
    """
    return RedirectResponse(url='/static/map_deck.html?v=10')


# =========================
# RUN
# =========================
if __name__ in {"__main__", "__mp_main__"}:
    """
    Launch the NiceGUI/Starlette app.
    - title: browser tab title
    - port:  development port (change if needed)
    - reload: auto-reload on code changes (dev only)
    """
    ui.run(title='Carte Trafic & Bruit', port=8080, reload=True)
