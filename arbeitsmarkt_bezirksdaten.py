import pandas as pd
import requests
from urllib.parse import urlparse
import os
from sqlalchemy import create_engine,text
from dotenv import load_dotenv

load_dotenv()

ZIELTABELLE = "t_arbeitsmarkt_bezirksdaten"
SCHEMA = "arbeitsmarkt"

DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_SERVER = os.getenv("server")
DB_NAME = os.getenv("database")

#engine = create_engine(f'postgresql://msabitzer:dbadmin@172.21.203.85:5432/statistik')
#conn = engine.raw_connection()
#cur = conn.cursor()

JAHR = '2026' #Hier das gewünschte Jahr eintragen

base_path = r"W:\STATSICH\Allgemein\Arbeitsmarkt\AMS Daten\Bezirksdaten"

#Code zum dowloaden der neuesten Datei:
url = f"https://www.ams.at/content/dam/download/arbeitsmarktdaten/%C3%B6sterreich/berichte-auswertungen/001_amd-polbez_monate_{JAHR}.xlsx"

# Zielordner (Windows-Pfad mit Doppelslashes oder raw string)
ordner = base_path
os.makedirs(ordner, exist_ok=True)

# Dateiname aus URL extrahieren
pfad_aus_url = urlparse(url).path
dateiname =JAHR+".xlsx"
# Vollständiger Speicherpfad
pfad = os.path.join(ordner, dateiname)

# Datei herunterladen (überschreibt, wenn schon vorhanden)
response = requests.get(url, stream=True)
if response.status_code == 200:
    with open(pfad, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Datei erfolgreich gespeichert: {pfad}")
else:
    print(f"Fehler beim Download: {response.status_code}")

def amsBezirksdaten():
   
    dfs =[]

    for filename in os.listdir(base_path):
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            file_path = os.path.join(base_path, filename)
           
            excel_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl',skiprows=9, header=None, nrows=94, decimal="," )
            
            for sheet_name, sheet_data in excel_data.items():     
                jahr, monat = getYearMonth(sheet_name)  
                if jahr is None or monat is None:
                    continue              
                sheet_data["jahr"] = jahr
                sheet_data["monat"] = monat
                dfs.append(sheet_data)
                
    if dfs:
        daten = pd.concat(dfs, ignore_index=True)
    else:
        daten = pd.DataFrame()


    #Trennung nach Geschlechtern

    frauenDaten = daten.iloc[:,[0,2,3,11,12]].copy()
    frauenDaten["geschlecht"] = 'W'
    frauenDaten["beschaeftigte"] = frauenDaten.iloc[:,1].astype(int)
    frauenDaten["arbeitslose"] = frauenDaten.iloc[:,2].astype(int)
    frauenDaten["bezirk"] = frauenDaten.iloc[:,0]  
    frauenDaten = frauenDaten[['jahr','monat','bezirk', 'geschlecht', 'beschaeftigte', 'arbeitslose']]
    
    maennerDaten = daten.iloc[:,[0,5,6,11,12]].copy()
    maennerDaten["geschlecht"] = 'M'
    maennerDaten["beschaeftigte"] = maennerDaten.iloc[:,1].astype(int)
    maennerDaten["arbeitslose"] = maennerDaten.iloc[:,2].astype(int) 
    maennerDaten["bezirk"] = maennerDaten.iloc[:,0]  
    maennerDaten = maennerDaten[['jahr','monat','bezirk', 'geschlecht', 'beschaeftigte', 'arbeitslose']]

    ams_bezirksdaten = pd.concat([maennerDaten,frauenDaten], ignore_index=True)  

    spalten_behalten = [
        'jahr',
        'monat',
        'bezirk',
        'geschlecht',
        'beschaeftigte',
        'arbeitslose',  
    ]

    ams_bezirksdaten.drop(columns=[col for col in ams_bezirksdaten.columns if col not in spalten_behalten], inplace=True)

    print(ams_bezirksdaten[spalten_behalten])

              
   
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{ZIELTABELLE}"))
        ams_bezirksdaten.to_sql(ZIELTABELLE, conn , if_exists="append", index=False, schema=SCHEMA, method="multi",chunksize=1000)

   
def getYearMonth(sheet_name):
    sheet_name_lower = sheet_name.lower().strip()

    monate_dict = {
        "jän": 1, "feb": 2, "mär": 3, "apr": 4, "mai": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "dez": 12   
    }

    # Jahressheet ignorieren
    if sheet_name_lower.startswith("jahr"):
        return None, None

    # Monat erkennen: nur die ersten 3 Buchstaben
    monthShortName = sheet_name_lower[:3]
    if monthShortName not in monate_dict:
        raise ValueError(f"Unbekannter Monat im Sheet: {sheet_name}")

    monat = monate_dict[monthShortName]


    # Jahr aus den letzten 2 Zeichen (z.B. '25' → 2025)
    jahr_suffix = sheet_name_lower[-2:]
    try:
        jahr = int("20" + jahr_suffix)
    except ValueError:
        raise ValueError(f"Ungültiges Jahr im Sheet: {sheet_name}")

    return jahr, monat


def main():
    amsBezirksdaten()

if __name__ == "__main__":
    main()
         
           