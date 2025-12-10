from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict, Any, List
import json, os, math
from nicegui import ui, app
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi import Query

# CONFIG
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
BUILDINGS_PATH = os.path.join(DATA_DIR, 'buildings.geojson')
ROADS_PATH = os.path.join(DATA_DIR, 'roads.geojson')
TRAFFIC_PATH = os.path.join(DATA_DIR, 'traffic_agg.geojson')
INTERVALS_PATH = os.path.join(DATA_DIR, 'intervals.json')
STATIC_DIR = os.path.join(DATA_DIR, 'static')

def sanitize_json(obj: Any) -> Any:
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
    if not dt_str: return None
    s = str(dt_str).strip().replace(' ', 'T')
    if s.endswith('Z'): s = s[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def feature_in_range(feat, start, end):
    p = feat.get('properties', {}) or {}
    b = parse_iso(p.get('begin')) or parse_iso(p.get('start'))
    e = parse_iso(p.get('end')) or b
    if not b and not e: return False
    if start and e and e < start: return False
    if end and b and b > end: return False
    return True

def compute_min_max(features, var):
    vals = [float(f["properties"].get(var, 0)) for f in features
            if isinstance(f.get("properties", {}).get(var), (int, float))]
    return {'min': min(vals) if vals else None, 'max': max(vals) if vals else None}

def safe_load_json(path, default):
    if not os.path.exists(path): return default
    with open(path, 'r', encoding='utf-8') as f: return json.load(f)

def safe_load_geojson(path): return safe_load_json(path, {'type':'FeatureCollection','features':[]})

# --- charger les données
BUILDINGS_GJ = safe_load_geojson(BUILDINGS_PATH)
ROADS_GJ = safe_load_geojson(ROADS_PATH)
TRAFFIC_GJ_ALL = safe_load_geojson(TRAFFIC_PATH)
TRAFFIC_FEATURES_ALL = TRAFFIC_GJ_ALL.get('features', [])
INTERVALS_RAW = safe_load_json(INTERVALS_PATH, {"intervals":[]})

# === NOUVEAU: exposer la vraie liste des variables numériques ===
def list_variables(example_feature: Dict[str, Any]) -> List[str]:
    if not example_feature:
        return []
    props = example_feature.get('properties', {}) or {}
    skip = {'id', 'begin', 'end', 'start'}
    out: List[str] = []
    for k, v in props.items():
        if k in skip:
            continue
        if isinstance(v, (int, float)):
            out.append(k)
    out.sort()
    return out

TRAFFIC_VARS = list_variables(TRAFFIC_FEATURES_ALL[0] if TRAFFIC_FEATURES_ALL else {})

# --- API
@app.get('/api/map/buildings')
def api_buildings(): return JSONResponse(BUILDINGS_GJ)

@app.get('/api/map/roads')
def api_roads(): return JSONResponse(ROADS_GJ)

@app.get('/api/map/traffic')
def api_traffic(start:Optional[str]=None, end:Optional[str]=None, var:str='vehicles', scope:str='filtered'):
    dt_start, dt_end = parse_iso(start), parse_iso(end)
    feats = [f for f in TRAFFIC_FEATURES_ALL if feature_in_range(f, dt_start, dt_end)] if (dt_start or dt_end) else TRAFFIC_FEATURES_ALL
    stats = compute_min_max(feats, var)
    return JSONResponse({'type':'FeatureCollection','features':feats,'meta':{'min':stats['min'],'max':stats['max']}})

@app.get('/api/map/traffic/minmax')
def api_traffic_minmax(var:str='vehicles', start:Optional[str]=None, end:Optional[str]=None, scope:str='filtered'):
    dt_start, dt_end = parse_iso(start), parse_iso(end)
    feats = [f for f in TRAFFIC_FEATURES_ALL if feature_in_range(f, dt_start, dt_end)] if (dt_start or dt_end) else TRAFFIC_FEATURES_ALL
    return JSONResponse(compute_min_max(feats, var))

@app.get('/api/map/traffic/intervals')
def api_traffic_intervals():
    if INTERVALS_RAW.get("intervals"):
        return JSONResponse({'intervals': INTERVALS_RAW["intervals"]})
    return JSONResponse({'intervals': []})

# === NOUVEAU: endpoint pour la liste des variables ===
@app.get('/api/map/traffic/vars')
def api_traffic_vars():
    return JSONResponse({'variables': TRAFFIC_VARS})

# static & root
app.add_static_files('/static', STATIC_DIR)

@app.get('/', include_in_schema=False)
def root(): return RedirectResponse(url='/static/map_deck.html?v=23')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='Carte Trafic & Bruit', port=8080, reload=True)
