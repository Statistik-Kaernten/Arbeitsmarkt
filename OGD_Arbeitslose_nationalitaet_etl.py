import pandas as pd
from sqlalchemy import create_engine, text
from nationen_mapping import nationen_dict
from dotenv import load_dotenv
import os

load_dotenv()

# Dieses Skript lädt die Arbeitslosen nach Nationalität herunter und speichert sie in der Datenbank.

INPUT_CSV_URL = "https://www.arbeitsmarktdatenbank.at/opendata/AL_Nationalitaet_RGS.csv"
ZIELTABELLE = "t_arbeitslose_nationalitaet"
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

    df.rename(columns={
        'RGSCode': 'arbeitsmarktbezirk_code',
        'Geschlecht': 'geschlecht',
        'Nationalitaet': 'nationalitaet',
        'BESTAND': 'bestand',
        'ZUGANG': 'zugang',
        'ABGANG': 'abgang'
    }, inplace=True)
    
    df["iso3_code"] = df["nationalitaet"].map(nationen_dict).fillna("999")

    spalten_behalten = [
        'jahr',
        'monat',
        'arbeitsmarktbezirk_code',
        'geschlecht',
        'iso3_code',
        'nationalitaet',
        'bestand',
        'zugang',
        'abgang'        
    ]

    df.drop(columns=[col for col in df.columns if col not in spalten_behalten], inplace=True)

    df = df[spalten_behalten]

    print(df)

    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{ZIELTABELLE}"))
    df.to_sql(ZIELTABELLE, engine, if_exists="append", index=False, schema=SCHEMA)


if __name__ == "__main__":
    main()




