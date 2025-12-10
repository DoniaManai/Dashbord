import pandas as pd
from glob import glob

# 1) Lis EXACTEMENT le fichier que tu utilises
df = pd.read_parquet("traffic_res.parquet")
print("rows:", len(df))
print("colonnes:", list(df.columns))

# 2) Si ton Parquet a d'autres colonnes temporelles, liste-les
time_like = [c for c in df.columns if any(k in c.lower() for k in ["time","date","hour","begin","end","start","day"])]
print("colonnes temporelles candidates:", time_like)

# 3) Parse begin/end (si présents)
if "begin" in df.columns and "end" in df.columns:
    df["begin"] = pd.to_datetime(df["begin"], errors="coerce")
    df["end"]   = pd.to_datetime(df["end"],   errors="coerce")
    print("begin min:", df["begin"].min(), "| end max:", df["end"].max())
    print("jours distincts (begin.date):", df["begin"].dt.date.nunique())
    print(df[["begin","end"]].drop_duplicates().sort_values("begin").head(10))
else:
    print(" Pas de colonnes 'begin'/'end' dans ce parquet. Montre-moi les noms exacts des colonnes temporelles.")
