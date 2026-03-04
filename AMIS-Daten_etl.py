import os
import pandas as pd
from sqlalchemy import create_engine


#Die 4 AMIS Files herunterladen und im entsprechenden Ordner (Jahr) abspeichern. --> W:\STATSICH\Allgemein\Arbeitsmarkt\AMIS
#Danach "AMIS Daten zusammenfassen.xlsm" öffnen und "Start Import" drücken mit dem jeweiligen Monat.
#Danach dieses Script ausführen, welches die CSVs abspeichert in W:\STATSICH\Allgemein\Arbeitsmarkt\AMIS\CSV und in die DB importiert.

INPUT_EXCEL_PATH = r"W:\\STATSICH\\Allgemein\\Arbeitsmarkt\\AMIS\\AMIS Daten zusammenfassen.xlsm"
OUTPUT_CSV_DIR = r"W:\\STATSICH\Allgemein\\Arbeitsmarkt\AMIS\\CSV"

DB_CONFIG = {
    "server": "172.21.203.85",
    "database": "statistik",
    "user": "dstabentheiner",
    "password": "statistik123",
}

TABLE_MAPPING = {
    "AMIS_AL_1": {"table": "t_amis_al1", "typ": None},
    "AMIS_AL_2": {"table": "t_amis_al2_ls1", "typ": 'AL'},
    "AMIS_LS_1": {"table": "t_amis_al2_ls1", "typ": 'LS'},
    "AMIS_LS_2": {"table": "t_amis_ls2", "typ": None},
}

def main():
    print("Lade...")
    sheets = pd.read_excel(INPUT_EXCEL_PATH, sheet_name=None, decimal=",")
    os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)

    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
    )

    for sheet_name, config in TABLE_MAPPING.items():
        if sheet_name not in sheets:
            print(f"Sheet '{sheet_name}' nicht gefunden. Überspringe.")
            continue
        
        df = sheets[sheet_name]
        df.columns = [col.strip().lower() for col in df.columns]

        if config["typ"] is not None:
            df["typ"] = config["typ"]

        csv_path = os.path.join(OUTPUT_CSV_DIR, f"{sheet_name}.csv")
        df.to_csv(csv_path, sep=";", index=False, na_rep="")
        print(f"Gespeichert als CSV: {csv_path}")

        try:
            df.to_sql(config["table"], engine, if_exists="append", index=False)
            print(f"In DB-Tabelle '{config['table']}' importiert.")
        except Exception as e:
            print(f"Fehler beim Import in '{config['table']}': {e}")
    
    print("Import abgeschlossen.")


if __name__ == "__main__":
    main()