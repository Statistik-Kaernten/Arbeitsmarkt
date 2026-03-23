import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# Dieses Skript lädt die monatlichen Beschäftigtendaten des Dachverbands OGD herunter und speichert sie in der Datenbank.
# Sollte mal ein Monat vergessen worden sein, muss der erforderliche Link für das File unter https://opendata.sozialversicherung.at/dvsv_statistik_beschaeftigte.html in die Variable INPUT_CSV_URL kopiert werden. 

INPUT_CSV_URL = "https://opendata-files.sozialversicherung.at/beschaeftigte/DVSV_Statistik_Beschaeftigte_v202404.csv"
ZIELTABELLE = "t_beschaeftigte"
SCHEMA = "arbeitsmarkt"


DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_SERVER = os.getenv("server")
DB_NAME = os.getenv("database")

def main():
    df = pd.read_csv(INPUT_CSV_URL, delimiter=";", decimal=",", encoding="latin1")            
    print(df)

    df.columns = [col.strip().lower() for col in df.columns]
    jahr = str(df["jahr"].unique()[0])
    monat = str(df["monat"].unique()[0])

    # Prüfen, ob dieser Monat schon in der DB ist
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT 1 FROM {SCHEMA}.{ZIELTABELLE} WHERE jahr = :jahr AND monat = :monat LIMIT 1"),{"jahr": jahr, "monat": monat})
            
    exists = result.fetchone()

    # Nur einfügen, wenn betreffendes Monat noch nicht vorhanden
    if exists:
        print(f"Monat {monat} vom Jahr {jahr} ist bereits vorhanden. Import übersprungen.")
    else:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")
        df.to_sql(ZIELTABELLE, engine, if_exists="append", index=False, schema=SCHEMA)
        print(f"Monat {monat} wurde importiert.")

if __name__ == "__main__":
    main()






