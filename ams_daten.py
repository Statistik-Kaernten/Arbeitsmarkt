import pandas as pd
import numpy as np
import requests
from urllib.parse import urlparse
import os
from sqlalchemy import create_engine


engine = create_engine(f'postgresql://msabitzer:dbadmin@172.21.203.85:5432/statistik')
conn = engine.raw_connection()
cur = conn.cursor()

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
                
            #date_id aus sheets erstellen
            for sheet_name, sheet_data in excel_data.items():
                
                dateId = getDateId(sheet_name)
                sheet_data["date_id"] = dateId
               
                dfs.append(sheet_data)
                
    if dfs:
         final_df = pd.concat(dfs, ignore_index=True)
         
    daten = pd.DataFrame(final_df)     
                
    daten = daten.iloc[:, [0, 2, 3, 5, 6, 11]].copy()
    
    #trennung nach Geschlechtern

    frauenDaten = daten.iloc[:,[0,1,2,5]].copy()
    frauenDaten["geschlecht"] = 2
    frauenDaten["beschaeftigte"] = frauenDaten.iloc[:,1].astype(int)
    frauenDaten["arbeitslose"] = frauenDaten.iloc[:,2].astype(int)
    frauenDaten["bezirk"] = frauenDaten.iloc[:,0]  
    frauenDaten = frauenDaten[['date_id','bezirk', 'geschlecht', 'beschaeftigte', 'arbeitslose']]
    
    maennerDaten = daten.iloc[:,[0,3,4,5]].copy()
    maennerDaten["geschlecht"] = 1
    maennerDaten["beschaeftigte"] = maennerDaten.iloc[:,1].astype(int)
    maennerDaten["arbeitslose"] = maennerDaten.iloc[:,2].astype(int) 
    maennerDaten["bezirk"] = maennerDaten.iloc[:,0]  
    maennerDaten = maennerDaten[['date_id','bezirk', 'geschlecht', 'beschaeftigte', 'arbeitslose']]

    ams_bezirksdaten = pd.concat([maennerDaten,frauenDaten], ignore_index=True)  
    
    #Datenbank einspielen
    cur.execute(f"DELETE FROM t_ams_bezirksdaten")
    #Aktuelle Daten in die Datenbank einspielen
    ams_bezirksdaten.to_sql('t_ams_bezirksdaten', engine, if_exists="append", index=False, schema='public')           
    

def getLastDayOfMonth(monat, jahr):
    monat = int(monat)
    jahr = int(jahr)

    # Februar: Schaltjahr prüfen
    if monat == 2:
        if (jahr % 4 == 0 and (jahr % 100 != 0 or jahr % 400 == 0)):
            return 29
        else:
            return 28
    # Monate mit 30 vs. 31 Tagen
    elif monat in [4, 6, 9, 11]:
        return 30
    else:
        return 31


def getDateId(sheet_name):
    """
    Wandelt den Sheet-Namen in ein eindeutiges Datum um.
    - Monats-Sheets (z.B. 'dez25', 'juli23') → 'YYYYMMDD'
    - Jahres-Sheets (z.B. 'Jahr2025') → 'YYYY00jd'
    """
    sheet_name_lower = sheet_name.lower().strip()

    # Liste der Monatsnamen
    monate_dict = {
        "jän": 1, "feb": 2, "mär": 3, "apr": 4, "mai": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "dez": 12   
         }

    # Jahressheet
    if sheet_name_lower.startswith("jahr"):
        jahr_int = int(sheet_name_lower[-4:])  # z.B. 'Jahr2025' → 2025
        return f"{jahr_int}00jd"


    # Monat erkennen: nur die ersten 3 Buchstaben
    monthShortName = sheet_name_lower[:3]

    if monthShortName not in monate_dict:
        raise ValueError(f"Unbekannter Monat im Sheet: {sheet_name}")

    monatZahl = monate_dict[monthShortName]

    # Jahr aus den letzten 2 Zeichen (z.B. '25' → 2025)
    jahr_suffix = sheet_name_lower[-2:]
    try:
        jahr_int = int("20" + jahr_suffix)
    except ValueError:
        raise ValueError(f"Ungültiges Jahr im Sheet: {sheet_name}")

    # Letzten Tag des Monats berechnen
    lastDay = getLastDayOfMonth(monatZahl, jahr_int)

    # Datum zusammensetzen
    return f"{jahr_int}{monatZahl:02d}{lastDay:02d}"



def main():
    amsBezirksdaten()

if __name__ == "__main__":
    main()
    conn.commit()
    cur.close()
    conn.close()         
           