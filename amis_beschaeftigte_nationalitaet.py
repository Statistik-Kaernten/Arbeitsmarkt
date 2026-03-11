import os
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine
from nationen_mapping import nationen_dict
from dotenv import load_dotenv

load_dotenv()

ZIELTABELLE = "t_beschaeftigte_nationalitaet"
SCHEMA = "arbeitsmarkt"

DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_SERVER = os.getenv("server")
DB_NAME = os.getenv("database")


pfad = r"W:\STATSICH\Allgemein\Arbeitsmarkt\AMIS\Beschaeftigte_Nationalitaeten_Alter"

def main():
    for filename in os.listdir(pfad):
        if filename.endswith(".json"):
            file_path = os.path.join(pfad, filename)

            with open(file_path, "r", encoding="utf-8") as f:
                js = json.load(f)

            df = pd.DataFrame(js["data"])

            # Monat und Jahr aus Metadata extrahieren
            titel = js["metadata"][0]["Titel"]  # z.B. "Erwerbstätige: Unselbständig Beschäftigte - Januar 2026"
            datum_str = titel.split("-")[-1].strip()  # "Januar 2026"
            monat_name, jahr_str = datum_str.split()
            jahr = int(jahr_str)

            # Mapping für deutsche Monatsnamen
            monat_mapping = {
                "Januar": 1,
                "Februar": 2,
                "März": 3,
                "April": 4,
                "Mai": 5,
                "Juni": 6,
                "Juli": 7,
                "August": 8,
                "September": 9,
                "Oktober": 10,
                "November": 11,
                "Dezember": 12
            }
            monat = monat_mapping[monat_name]

            # Jahr und Monat als neue Spalten ins DataFrame übernehmen
            df["jahr"] = jahr
            df["monat"] = monat

            df.rename(columns={
                "Geschlecht": "geschlecht",
                "Bundesland": "bundesland",
                "Alter 3-Kategorien": "alterskategorie",
                "Nationalität": "nationalitaet",
                "Bestand": "anzahl"
            }, inplace=True)

            df["geschlecht"] = df["geschlecht"].replace({"Frauen": "W", "Männer und altern. Geschl.": "M"})
            
            df["iso3_code"] = df["nationalitaet"].map(nationen_dict).fillna("999")

            # Spaltenreihenfolge:
            cols = ["jahr", "monat", "geschlecht", "bundesland", "alterskategorie", "iso3_code", "nationalitaet", "anzahl"]
            df = df[cols]

            df["anzahl"] = df["anzahl"].astype(int)

            print(df.head())

    # Prüfen, ob dieser Monat schon in der DB ist
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_SERVER)
            cur = conn.cursor()
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{ZIELTABELLE} WHERE jahr = %s AND monat = %s LIMIT 1", (jahr, monat))    
            exists = cur.fetchone()
            cur.close()
            conn.close()

            # Nur einfügen, wenn betreffendes Monat noch nicht vorhanden
            if exists:
                print(f"Monat {monat} vom Jahr {jahr} ist bereits vorhanden. Import übersprungen.")
            else:
                engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")
                df.to_sql(ZIELTABELLE, engine, if_exists="append", index=False, schema=SCHEMA)  
                print(f"Monat {monat} wurde importiert.")


if __name__ == "__main__":
    main()

'''
# Nur für AMIS-Daten Tabelle mit allen 12 Monaten eines Jahres. 
# Dieses Skript ist zur Verarbeitung von Daten über unselbständig Beschäftigte nach Nationalität und OeNACE-Klasse gedacht. 
# Es liest die Daten aus einer JSON-Datei ein, transformiert sie entsprechend und speichert sie in einer PostgreSQL-Datenbank. 
# Dabei wird geprüft, ob die Daten für den betreffenden Monat bereits vorhanden sind, um doppelte Einträge zu vermeiden.

import os
import pandas as pd
import json
from sqlalchemy import create_engine
from nationen_mapping import nationen_dict

ZIELTABELLE = "t_beschaeftigte_nationalitaet"
SCHEMA = "arbeitsmarkt"

DB_CONFIG = {
    "server": "172.21.203.85",
    "database": "statistik",
    "user": "dstabentheiner",
    "password": "statistik123",}


pfad = r"W:\STATSICH\Allgemein\Arbeitsmarkt\AMIS\Beschaeftigte_Nationalitaeten_Alter"

def main():
    engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}")
 
    for filename in os.listdir(pfad):
        if filename.endswith(".json"):
            file_path = os.path.join(pfad, filename)

            with open(file_path, "r", encoding="utf-8") as f:
                js = json.load(f)

            df = pd.DataFrame(js["data"])

            id_vars = ["Geschlecht", "Bundesland", "Alter 3-Kategorien", "Nationalität"]
            value_vars = [c for c in df.columns if c not in id_vars]

            df_long = df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name="jahr_monat",
                value_name="anzahl"
            )

            df_long["jahr"] = df_long["jahr_monat"].str.split("_").str[0].astype(int)
            df_long["monat"] = df_long["jahr_monat"].str.split("_").str[1].astype(int)

            df_long.rename(columns={
                "Geschlecht": "geschlecht",
                "Bundesland": "bundesland",
                "Alter 3-Kategorien": "alterskategorie",
                "Nationalität": "nationalitaet"
            }, inplace=True)


            df_long["geschlecht"] = df_long["geschlecht"].replace({"Frauen": "W", "Männer und altern. Geschl.": "M"})

            df_long['iso3_code'] = df_long['nationalitaet'].map(nationen_dict).fillna("999")         
            
            cols = ["jahr", "monat", "geschlecht", "bundesland", "alterskategorie", "iso3_code", "nationalitaet", "anzahl"]
            df_long = df_long[cols]

            df_long["anzahl"] = df_long["anzahl"].fillna(0).astype(int)
        
            print(df_long.head(5))

            try:
                with engine.begin() as conn:
                    df_long.to_sql(ZIELTABELLE, conn, if_exists="append", index=False, schema=SCHEMA)
                print(f"{filename} importiert!")
            except Exception as e:
                print(f"Fehler beim Import von {filename}: {e}")


if __name__ == "__main__":
    main()   
'''