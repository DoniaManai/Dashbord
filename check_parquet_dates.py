# convert_parquet_to_csv.py
import pandas as pd

IN_PATH  = "traffic_res.parquet"   # ← ton fichier .parquet
OUT_PATH = "traffic_res.csv"       # ← sortie .csv

df = pd.read_parquet(IN_PATH)      # nécessite pyarrow ou fastparquet
# (facultatif) s'assurer que begin/end soient bien en texte ISO lisible par Excel
# df["begin"] = pd.to_datetime(df["begin"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
# df["end"]   = pd.to_datetime(df["end"],   errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
print(f"Écrit: {OUT_PATH} ({len(df)} lignes)")
