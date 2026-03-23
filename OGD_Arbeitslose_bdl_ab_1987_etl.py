import pandas as pd

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# Dieses Skript lädt die Arbeitslosen seit 1987 herunter und speichert sie in der Datenbank.

INPUT_CSV_URL = "https://www.arbeitsmarktdatenbank.at/opendata/AL_SC_Geschlecht_Bdl_ZR.csv"
ZIELTABELLE = "t_arbeitslose_bdl_ab_1987"
SCHEMA = "arbeitsmarkt"

DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_SERVER = os.getenv("server")
DB_NAME = os.getenv("database")


def main():
    
    
    df = pd.read_csv(INPUT_CSV_URL, delimiter=";", decimal=",", encoding="latin1")            
    print(df)
    
    df['Datum'] = pd.to_datetime(df['Datum'], errors='coerce', format='%Y-%m-%d')

    df['jahr'] = df['Datum'].dt.year
    df['monat'] = df['Datum'].dt.month

    df.drop(columns=['Datum'], inplace=True)

    df["Geschlecht"] = df["Geschlecht"].replace({
        "Frauen": "W",
        "Männer": "M"
    })

    bundeslaender = ["Bgld","Ktn","NOe","OOe","Sbg","Stmk","Tirol","Vbg","Wien"]

    df.rename(columns={
        'Geschlecht': 'geschlecht',
        'Status': 'status',
        'StatusBez': 'status_bez'
    }, inplace=True)
    
 
    df = df.melt(
        id_vars=["jahr", "monat", "status", "status_bez", "geschlecht"],
        value_vars=bundeslaender,
        var_name="bundesland",
        value_name="anzahl"
        )
    
    mapping = {
        "Bgld": "Burgenland",
        "Ktn": "Kärnten",
        "NOe": "Niederösterreich",
        "OOe": "Oberösterreich",
        "Sbg": "Salzburg",
        "Stmk": "Steiermark",
        "Tirol": "Tirol",
        "Vbg": "Vorarlberg",
        "Wien": "Wien",
    }

    df["bundesland"] = df["bundesland"].map(mapping)

    spalten_behalten = [
        'jahr',
        'monat',
        'status',
        'geschlecht',
        'bundesland',
        'anzahl'        
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




