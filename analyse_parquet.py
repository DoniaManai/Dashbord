import pandas as pd

# Liste des fichiers à lire
files = {
    "GEOM_V1": "GEOM_V1.parquet",
    "BUILDINGS_GEOM_v1": "BUILDINGS_GEOM_v1.parquet",
    "traffic_res": "traffic_res.parquet"
}

for name, path in files.items():
    print("="*80)
    print(f" {name}")
    print("="*80)
    try:
        df = pd.read_parquet(path)
        print(f"Nombre de lignes : {len(df)}")
        print(f"Colonnes : {list(df.columns)}")
        print("\nAperçu des premières lignes :")
        print(df.head(5))
    except Exception as e:
        print(f" Erreur lors de la lecture du fichier {path} : {e}")
    print("\n\n")
