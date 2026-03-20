import os
import pandas as pd
import json
import psycopg2
from sqlalchemy import create_engine
from nationen_mapping import nationen_dict
from dotenv import load_dotenv

load_dotenv()

ZIELTABELLE = "t_beschaeftigte_nationalitaet_oenace"
SCHEMA = "arbeitsmarkt"

DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_SERVER = os.getenv("server")
DB_NAME = os.getenv("database")

def main():
    pfad = r"W:\STATSICH\Allgemein\Arbeitsmarkt\AMIS\Beschaeftigte_Nationalitaeten_OENACE"

    for filename in os.listdir(pfad):
        if filename.endswith(".json"):
            file_path = os.path.join(pfad, filename)

            with open(file_path, "r", encoding="utf-8") as f:
                js = json.load(f)

            # DataFrame aus "data"-Teil
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
                "Nationalität": "nationalitaet",     
                "Bestand": "anzahl"
            }, inplace=True)
        

            df['iso3_code'] = df['nationalitaet'].map(nationen_dict).fillna("999")


            def transform_oenace_split(x):
                x = str(x).strip()
                
                # Sonderfälle für X
                if x.startswith("X - Kinderbetreuungsgeldbezieher:innen"):
                    return pd.Series(["X - Kinderbetreuungsgeldbezieher:innen", "Kinderbetreuungsgeldbezieher:innen mit aufrechtem Dienstverhältnis"])
                elif x.startswith("X - Präsenzdiener:innen"):
                    return pd.Series(["X - Präsenzdiener:innen", "Präsenzdiener:innen mit aufrechtem Dienstverhältnis"])
                elif x.startswith("X - Sonstige"):
                    return pd.Series(["X - Sonstige", "Sonstige, Wirtschaftsklasse unbekannt"])
                
                # Normalfall: Buchstabe vor dem Bindestrich
                parts = x.split(" - ", 1)
                code = parts[0] if len(parts) > 0 else x[0]  
                lang = parts[1] if len(parts) > 1 else ""
                return pd.Series([code, lang])

            df[["oenace_2025_code", "oenace_2025_bezeichnung"]] = df['Wirtschaftsklasse'].apply(transform_oenace_split)


            # Spaltenreihenfolge:
            cols = ["jahr", "monat", "nationalitaet", "iso3_code", "oenace_2025_code", "oenace_2025_bezeichnung", "anzahl"]
            df = df[cols]
            df["anzahl"] = df["anzahl"].astype(int)
        
            print(df)

            engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")

            
            # Prüfen, ob dieser Monat schon in der DB ist
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_SERVER)
            cur = conn.cursor()
            cur.execute(f"SELECT 1 FROM arbeitsmarkt.{ZIELTABELLE} WHERE jahr = %s AND monat = %s LIMIT 1", (jahr, monat))    
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