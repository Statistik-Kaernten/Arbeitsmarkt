import os
import pandas as pd
from sqlalchemy import create_engine


INPUT_EXCEL_PATH = r"W:\STATSICH\Allgemein\Arbeitsmarkt\Dachverband\Dachverband Daten zusammenfassen.xlsm"
#OUTPUT_CSV_DIR = r"C:\\ordner\\csv-ordner"

DB_CONFIG = {
    "server": "172.21.203.85",
    "database": "statistik",
    "user": "dstabentheiner",
    "password": "statistik123",
}


TABLE_MAPPING = {

"Boe_Alter": {"table": "t_boe_gboe_alter", "typ_id": 2},
"GBoe_Alter": {"table": "t_boe_gboe_alter", "typ_id": 1},
"Boe_Ausländer": {"table": "t_boe_auslaender", "typ_id": None},
"Boe_Ausl_Nat_K": {"table": "t_boe_ausl_nat_k", "typ_id": None},
}

def main():
    print("Lade...")
    sheets = pd.read_excel(INPUT_EXCEL_PATH, sheet_name=None, decimal=",")
    #os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)

    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
    )

    for sheet_name, config in TABLE_MAPPING.items():
        if sheet_name not in sheets:
            print(f"Sheet '{sheet_name}' nicht gefunden. Überspringe.")
            continue
        
        df = sheets[sheet_name]
        df.columns = [col.strip().lower() for col in df.columns]

        if config["typ_id"] is not None:
            df["typ_id"] = config["typ_id"]

        #csv_path = os.path.join(OUTPUT_CSV_DIR, f"{sheet_name}.csv")
        #df.to_csv(csv_path, sep=";", index=False, na_rep="")
        #print(f"Gespeichert als CSV: {csv_path}")

        try:
            df.to_sql(config["table"], engine, if_exists="append", index=False)
            print(f"In DB-Tabelle '{config['table']}' importiert.")
        except Exception as e:
            print(f"Fehler beim Import in '{config['table']}': {e}")
    
    print("Import abgeschlossen.")


if __name__ == "__main__":
    main()






