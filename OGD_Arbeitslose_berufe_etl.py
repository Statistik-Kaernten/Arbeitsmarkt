import pandas as pd
from sqlalchemy import create_engine, text

# Dieses Skript lädt die Arbeitslosen nach Berufen und Nationalität herunter und speichert sie in der Datenbank.

INPUT_CSV_URL = "https://www.arbeitsmarktdatenbank.at/opendata/AL_Berufe_Nationalitaet_RGS.csv"
ZIELTABELLE = "t_arbeitslose_berufe"
SCHEMA = "arbeitsmarkt"

DB_CONFIG = {
    "server": "172.21.203.85",
    "database": "statistik",
    "user": "dstabentheiner",
    "password": "statistik123",
}

def main():
    df = pd.read_csv(INPUT_CSV_URL, delimiter=";", decimal=",", encoding="latin1")            
       
    df['Datum'] = pd.to_datetime(df['Datum'], errors='coerce', format='%Y-%m-%d')

    df['jahr'] = df['Datum'].dt.year
    df['monat'] = df['Datum'].dt.month

    df.drop(columns=['Datum'], inplace=True)

    # Sicherstellen, dass die Spalte als String behandelt wird
    df['Berufs4Steller'] = df['Berufs4Steller'].astype(str)

    # führende Null hinzufügen, sodass immer 4 Stellen
    df['Berufs4Steller'] = df['Berufs4Steller'].str.zfill(4)

    df.rename(columns={
        'RGSCode': 'arbeitsmarktbezirk_code',
        'Geschlecht': 'geschlecht',
        'Nationalitaet': 'nationalitaet',
        'Berufs4Steller': 'berufs_4_steller',
        'Berufs4StellerBez': 'berufs_4_steller_bezeichnung',
        'BESTAND': 'bestand',
        'ZUGANG': 'zugang',
        'ABGANG': 'abgang'
    }, inplace=True)
    
    spalten_behalten = [ 
        'jahr',
        'monat',
        'arbeitsmarktbezirk_code',
        'geschlecht',
        'nationalitaet',
        'berufs_4_steller',
        'berufs_4_steller_bezeichnung',
        'bestand',
        'zugang',
        'abgang'        
    ]

    df.drop(columns=[col for col in df.columns if col not in spalten_behalten], inplace=True)

    df = df[spalten_behalten]

    print(df)

    engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}"
        )
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{ZIELTABELLE}"))
    df.to_sql(ZIELTABELLE, engine, if_exists="append", index=False, schema=SCHEMA)


if __name__ == "__main__":
    main()




