#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prépare les données de bruit pour le frontend deck.gl :
- joint les mesures de bruit aux géométries de bâtiments via PK
- normalise begin/end en ISO (UTC)
- reprojette en EPSG:4326
- exporte noise.parquet et noise.geojson

Usage:
  python prepare_noise.py \
    --buildings ./data/buildings.geojson \
    --noise ./data/noise_source.parquet \
    --vars Lden LAeq \
    --out-parquet ./data/noise.parquet \
    --out-geojson ./data/noise.geojson
"""

from __future__ import annotations
import argparse
import pandas as pd
import geopandas as gpd
from pathlib import Path

def read_buildings(path: str) -> gpd.GeoDataFrame:
    if path.lower().endswith(".parquet"):
        gdf = gpd.read_parquet(path)
    else:
        gdf = gpd.read_file(path)
    if "PK" not in gdf.columns:
        raise ValueError("Le fichier bâtiments doit contenir une colonne 'PK' (identifiant bâtiment).")
    if gdf.crs is None:
        # Ajuste ce CRS si tu connais le CRS source exact
        gdf = gdf.set_crs(3857, allow_override=True)
    gdf = gdf.to_crs(4326)
    # on garde les colonnes utiles: PK, HEIGHT, POP, geometry (les autres ne gênent pas)
    return gdf

def read_noise(path: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path)
    elif path.lower().endswith(".csv"):
        df = pd.read_csv(path)
    else:
        # tente lecture générique (xls, etc.)
        if path.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(path)
        else:
            raise ValueError(f"Format non supporté pour le bruit: {path}")
    return df

def coerce_ts(col):
    return pd.to_datetime(col, utc=True, errors="coerce")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--buildings", required=True, help="Chemin des bâtiments (geojson/parquet) avec PK & geometry")
    ap.add_argument("--noise", required=True, help="Chemin des mesures de bruit (csv/parquet)")
    ap.add_argument("--vars", nargs="+", required=True, help="Nom(s) de variable(s) bruit à conserver (ex: Lden LAeq)")
    ap.add_argument("--pk-col", default="PK", help="Nom de la colonne identifiant bâtiment dans les deux sources (default: PK)")
    ap.add_argument("--begin-col", default="begin", help="Nom de la colonne début (default: begin)")
    ap.add_argument("--end-col", default="end", help="Nom de la colonne fin (default: end)")
    ap.add_argument("--out-parquet", default="./noise.parquet", help="Fichier de sortie parquet")
    ap.add_argument("--out-geojson", default="./noise.geojson", help="Fichier de sortie geojson")
    args = ap.parse_args()

    # 1) charge
    b_gdf = read_buildings(args.buildings)
    n_df = read_noise(args.noise)

    # 2) vérifs colonnes
    required_noise_cols = {args.pk_col, args.begin_col, args.end_col}
    missing = [c for c in required_noise_cols if c not in n_df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans le bruit: {missing}. Reçues: {list(n_df.columns)}")

    missing_vars = [v for v in args.vars if v not in n_df.columns]
    if missing_vars:
        raise ValueError(f"Variables bruit absentes: {missing_vars}. Reçues: {list(n_df.columns)}")

    # 3) normalisation temps
    n_df = n_df.copy()
    n_df[args.begin_col] = coerce_ts(n_df[args.begin_col])
    n_df[args.end_col]   = coerce_ts(n_df[args.end_col])
    # écarte les lignes sans intervalle valide
    n_df = n_df[n_df[args.begin_col].notna() & n_df[args.end_col].notna()]
    if n_df.empty:
        raise ValueError("Aucune ligne de bruit valide après parsing des timestamps.")

    # 4) join avec géométrie (left join sur PK)
    join_cols = [args.pk_col, args.begin_col, args.end_col] + args.vars
    n_small = n_df[join_cols].copy()
    # assure type du PK compatible
    b_gdf["_PK_key_"] = b_gdf[args.pk_col].astype(str)
    n_small["_PK_key_"] = n_small[args.pk_col].astype(str)

    gdf = b_gdf[["_PK_key_", "geometry", "HEIGHT", "POP"]].merge(
        n_small.drop(columns=[args.pk_col]),
        on="_PK_key_",
        how="right"
    )
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=4326)
    # remet le nom PK original
    gdf[args.pk_col] = gdf["_PK_key_"]
    gdf = gdf.drop(columns=["_PK_key_"])

    # 5) ordonne colonnes
    front_cols = [args.pk_col, args.begin_col, args.end_col] + args.vars + ["HEIGHT", "POP", "geometry"]
    existing = [c for c in front_cols if c in gdf.columns]
    gdf = gdf[existing]

    # 6) tri & dédoublonnage léger
    gdf = gdf.sort_values([args.pk_col, args.begin_col, args.end_col])
    gdf = gdf.drop_duplicates(subset=[args.pk_col, args.begin_col, args.end_col], keep="last")

    # 7) export
    out_parquet = Path(args.out_parquet)
    out_geojson = Path(args.out_geojson)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    out_geojson.parent.mkdir(parents=True, exist_ok=True)

    gdf.to_parquet(out_parquet, index=False)
    gdf.to_file(out_geojson, driver="GeoJSON")

    # petit résumé
    tmin = gdf[args.begin_col].min()
    tmax = gdf[args.end_col].max()
    n_pk = gdf[args.pk_col].nunique()
    print("✅ Export OK")
    print(f"  Bâtiments uniques : {n_pk}")
    print(f"  Fenêtre temporelle: {tmin} → {tmax}")
    print(f"  Sorties : {out_parquet} | {out_geojson}")

if __name__ == "__main__":
    main()
