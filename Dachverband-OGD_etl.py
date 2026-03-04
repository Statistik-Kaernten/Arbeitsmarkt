import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Dieses Skript lädt die monatlichen Beschäftigtendaten des Dachverbands OGD herunter und speichert sie in der Datenbank.
# Sollte mal ein Monat vergessen worden sein, muss der erforderliche Link für das File unter https://opendata.sozialversicherung.at/dvsv_statistik_beschaeftigte.html in die Variable INPUT_CSV_URL kopiert werden. 

INPUT_CSV_URL = "https://opendata-files.sozialversicherung.at/beschaeftigte/DVSV_Statistik_Beschaeftigte_v202404.csv"
ZIELTABELLE = "t_dachverband_ogd"
SCHEMA = "arbeitsmarkt"

DB_CONFIG = {
    "server": "172.21.203.85",
    "database": "statistik",
    "user": "dstabentheiner",
    "password": "statistik123",
}

def main():
    df = pd.read_csv(INPUT_CSV_URL, delimiter=";", decimal=",", encoding="latin1")            
    print(df)

    df.columns = [col.strip().lower() for col in df.columns]
    jahr = str(df["jahr"].unique()[0])
    monat = str(df["monat"].unique()[0])

    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
    )
    # Prüfen, ob dieser Monat schon in der DB ist
    conn = psycopg2.connect(
        dbname=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['server']
    )
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM {ZIELTABELLE} WHERE jahr = %s AND monat = %s LIMIT 1", (jahr, monat))    
    exists = cur.fetchone()
    cur.close()
    conn.close()

    # Nur einfügen, wenn betreffendes Monat noch nicht vorhanden
    if exists:
        print(f"Monat {monat} vom Jahr {jahr} ist bereits vorhanden. Import übersprungen.")
    else:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
        )
        df.to_sql(ZIELTABELLE, engine, if_exists="append", index=False, schema=SCHEMA)
        print(f"Monat {monat} wurde importiert.")

if __name__ == "__main__":
    main()






