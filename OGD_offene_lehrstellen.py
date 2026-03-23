import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# Dieses Skript lädt die Offenen Lehrstellen herunter und speichert sie in der Datenbank.

INPUT_CSV_URL = "https://www.arbeitsmarktdatenbank.at/opendata/Bestand_LS_OL_Verf_Berufe_RGS.csv"
ZIELTABELLE = "t_offene_lehrstellen"
SCHEMA = "arbeitsmarkt"

DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_SERVER = os.getenv("server")
DB_NAME = os.getenv("database")

def main():
    df = pd.read_csv(INPUT_CSV_URL, delimiter=";", decimal=",", encoding="latin1")            
       
    df['Datum'] = pd.to_datetime(df['Datum'], errors='coerce', format='%Y-%m-%d')

    df['jahr'] = df['Datum'].dt.year
    df['monat'] = df['Datum'].dt.month

    df.drop(columns=['Datum'], inplace=True)

    df = df[df["Typ"] == "OL"]

    df.rename(columns={
        'RGSCode': 'arbeitsmarktbezirk_code',
        'Beruf6Steller': 'berufs_6_steller',
        'Verfuegbarkeit': 'verfuegbarkeit',
        'BESTAND': 'anzahl'
    }, inplace=True)
    
    df["berufs_6_steller"] = df["berufs_6_steller"].str[-6:]

    spalten_behalten = [
        'jahr',
        'monat',
        'arbeitsmarktbezirk_code',
        'berufs_6_steller',
        'verfuegbarkeit',
        'anzahl'     
    ]

    df.drop(columns=[col for col in df.columns if col not in spalten_behalten], inplace=True)

    df = df[spalten_behalten]

    print(df)

    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{ZIELTABELLE}"))
    df.to_sql(ZIELTABELLE, engine, if_exists="append", index=False, schema = SCHEMA)


if __name__ == "__main__":
    main()




