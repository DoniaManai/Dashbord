# prepare_data.py
import json
import pandas as pd
import numpy as np

from shapely import from_wkb
from shapely.geometry import mapping
from shapely.ops import transform as shp_transform
from pyproj import Transformer

# ========= CONFIG =========
ROADS_PARQUET = "GEOM_V1.parquet"
BUILDINGS_PARQUET = "BUILDINGS_GEOM_v1.parquet"
TRAFFIC_PARQUET = "traffic_res.parquet"

BUILDINGS_OUT = "buildings.geojson"
ROADS_OUT = "roads.geojson"
TRAFFIC_OUT = "traffic_agg.geojson"
INTERVALS_OUT = "intervals.json"   # <<< NEW

SRC_EPSG = 3003
DST_EPSG = 4326
TRANSFORMER = Transformer.from_crs(SRC_EPSG, DST_EPSG, always_xy=True)

# ========= UTILITAIRES =========
def reproject_geom(geom):
    return shp_transform(lambda x, y, z=None: TRANSFORMER.transform(x, y), geom)

def decode_wkb_to_geojson_features(df, id_col, geom_col, extra_props=None, reproject=True):
    features = []
    extra_props = extra_props or []
    for _, row in df.iterrows():
        if pd.isna(row[geom_col]):
            continue
        g = from_wkb(row[geom_col])
        if g is None:
            continue
        if reproject:
            g = reproject_geom(g)
        props = {id_col: row[id_col]}
        for p in extra_props:
            props[p] = row[p]
        features.append({"type": "Feature", "geometry": mapping(g), "properties": props})
    return {"type": "FeatureCollection", "features": features}

def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def safe_num(s):
    return pd.to_numeric(s, errors="coerce")

def to_iso_seconds(ts: pd.Timestamp | None) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    # on force naïf en local (pas de timezone dans le JSON)
    if ts.tz is not None:
        ts = ts.tz_convert(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")

# ========= CHARGEMENT =========
roads = pd.read_parquet(ROADS_PARQUET)
buildings = pd.read_parquet(BUILDINGS_PARQUET)
traffic = pd.read_parquet(TRAFFIC_PARQUET)

# Harmoniser id (clé de jointure)
roads["id"] = roads["id"].astype(str)
traffic["id"] = traffic["id"].astype(str)

# ========= TEMPS : parsing robuste =========
# Beaucoup de parquets ont déjà des datetime64 ; ces lignes rendent robuste si string / formats variés
traffic["begin"] = pd.to_datetime(traffic["begin"], errors="coerce", utc=False)
traffic["end"]   = pd.to_datetime(traffic["end"],   errors="coerce", utc=False)

# Sanity check de base
print("=== TRAFFIC brut ===")
print("rows:", len(traffic))
print("intervals uniques parquet:", traffic[["begin","end"]].drop_duplicates().shape[0])
print("begin min (parquet):", traffic["begin"].min(), "| end max (parquet):", traffic["end"].max())

# ========= MÉTRIQUES =========
traffic["entered"] = safe_num(traffic["entered"])
traffic["left"] = safe_num(traffic["left"])
traffic["speed"] = safe_num(traffic["speed"])
traffic["speedRelative"] = safe_num(traffic["speedRelative"])

traffic["vehicles_row"] = (traffic["entered"] + traffic["left"]) / 2.0
gcols = ["id", "begin", "end"]

totals = traffic.groupby(gcols, as_index=False).agg(
    vehicles=("vehicles_row", "sum"),
    w_speed_num=("speed", lambda s: float(np.nansum(s * traffic.loc[s.index, "vehicles_row"]))),
    w_sprel_num=("speedRelative", lambda s: float(np.nansum(s * traffic.loc[s.index, "vehicles_row"]))),
    w_den=("vehicles_row", "sum"),
)
totals["speed"] = np.where(totals["w_den"] > 0, totals["w_speed_num"] / totals["w_den"], np.nan)
totals["speedRelative"] = np.where(totals["w_den"] > 0, totals["w_sprel_num"] / totals["w_den"], np.nan)
totals = totals.drop(columns=["w_speed_num", "w_sprel_num", "w_den"])

class_counts = traffic.pivot_table(index=gcols, columns="vclass", values="vehicles_row", aggfunc="sum", fill_value=0.0)
traffic["w_speed_row"] = traffic["speed"] * traffic["vehicles_row"]
class_speed_num = traffic.pivot_table(index=gcols, columns="vclass", values="w_speed_row", aggfunc="sum", fill_value=0.0)
class_speed = class_speed_num / class_counts.replace({0.0: np.nan})
class_speed.columns = [f"{c}_s" for c in class_speed.columns]

agg = totals.set_index(gcols).join(class_counts, how="left").join(class_speed, how="left").reset_index()
agg["id"] = agg["id"].astype(str)

# ========= JOINTURE GÉOMÉTRIE =========
roads_small = roads[["id", "geometry"]].copy()
traffic_geo = agg.merge(roads_small, on="id", how="left")  # left: on conserve tous les intervalles

print("\n=== APRÈS MERGE ===")
print("rows traffic_geo:", len(traffic_geo))
print("rows avec geometry non nulle:", traffic_geo["geometry"].notna().sum())

# ========= EXPORTS =========
# A) Bâtiments
buildings_gj = decode_wkb_to_geojson_features(buildings, id_col="PK", geom_col="geometry", extra_props=["HEIGHT", "POP"], reproject=True)
write_json(BUILDINGS_OUT, buildings_gj)

# B) Routes
roads_gj = decode_wkb_to_geojson_features(roads, id_col="id", geom_col="geometry", extra_props=[], reproject=True)
write_json(ROADS_OUT, roads_gj)

# C) Trafic : on exporte même sans géométrie (geometry: null) pour préserver la timeline
classes = sorted(traffic["vclass"].dropna().unique().tolist())
traffic_features = []
for _, row in traffic_geo.iterrows():
    g_json = None
    if pd.notna(row["geometry"]):
        g = from_wkb(row["geometry"])
        if g is not None:
            g = reproject_geom(g)
            g_json = mapping(g)

    props = {
        "id": row["id"],
        "begin": to_iso_seconds(row["begin"]),
        "end": to_iso_seconds(row["end"]),
        "vehicles": float(row["vehicles"]) if pd.notna(row["vehicles"]) else None,
        "speed": float(row["speed"]) if pd.notna(row["speed"]) else None,
        "speedRelative": float(row["speedRelative"]) if pd.notna(row["speedRelative"]) else None,
    }
    for cls in classes:
        cval = row.get(cls, np.nan)
        sval = row.get(f"{cls}_s", np.nan)
        props[cls] = float(cval) if pd.notna(cval) else None
        props[f"{cls}_s"] = float(sval) if pd.notna(sval) else None

    traffic_features.append({"type": "Feature", "geometry": g_json, "properties": props})

traffic_gj = {"type": "FeatureCollection", "features": traffic_features}
write_json(TRAFFIC_OUT, traffic_gj)

# D) ⚠️ NEW: Intervalles “bruts” issus DIRECTEMENT du parquet trafic (garanti complet)
uni = traffic[["begin", "end"]].dropna().drop_duplicates().sort_values(["begin", "end"])
intervals = [{"begin": to_iso_seconds(b), "end": to_iso_seconds(e)} for b, e in zip(uni["begin"], uni["end"])]
write_json(INTERVALS_OUT, {"intervals": intervals})

print("\nExport OK :", BUILDINGS_OUT, ROADS_OUT, TRAFFIC_OUT, INTERVALS_OUT)
print("Bornes parquet :", to_iso_seconds(traffic['begin'].min()), "→", to_iso_seconds(traffic['end'].max()))
print("Nb intervalles (parquet) :", len(intervals))
