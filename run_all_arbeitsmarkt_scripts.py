import subprocess

#Dieses Skript führt alle Arbeitsmarktdaten-Skripte aus.

skripte = [
    "amis_beschaeftigte_nationalitaet.py",
    "amis_beschaeftigte_nationalitaet_oenace.py ",
    "arbeitsmarkt_bezirksdaten.py",
    "OGD_Arbeitslose_bdl_ab_1987_etl.py",
    "OGD_Arbeitslose_berufe_etl.py", 
    "OGD_Arbeitslose_gemeinden_etl.py", 
    "OGD_Arbeitslose_nationalitaet_etl.py", 
    "OGD_Arbeitslose_oenace_etl.py", 
    "OGD_Arbeitslose_vormerkdauer_etl.py",
    "OGD_Beschaeftigte_Dachverband_etl.py", 
    "OGD_lehrstellensuchende.py", 
    "OGD_offene_lehrstellen.py", 
    "OGD_offene_stellen.py"
]

for skript in skripte:
    print(f"Starte {skript}...")
    result = subprocess.run(["python", skript], capture_output=True, text=True)
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Fehler in {skript}: {result.stderr}")