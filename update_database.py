import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Skapa en logger
desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
database_path = os.path.join(desktop_path, 'Kunskapskontroll2_VG', 'data_processing.log')
logging.basicConfig(filename=database_path, level=logging.INFO)

def read_database(db_path):
    try:
        # Anslut till databasen
        conn = sqlite3.connect(db_path)

        # Välja/Select alla värden (datum, val) från tabellen dates_and_values
        data = pd.read_sql_query("SELECT * FROM dates_and_values", conn)

        # Konvertera datum till datetime-format, ogiltiga värden blir Not A Time (NaT)
        data['date'] = pd.to_datetime(data['date'], errors='coerce')

        # Spara undan rader med ogiltiga datum (NaT)
        invalid_dates = data[data['date'].isna()]

        # Om vi har hittat ogiltiga datum så loggar vi det
        if not invalid_dates.empty:
            logging.warning(f"Ogiltiga datum hittade:\n{invalid_dates[['date', 'value']]}")

        # Ta bort (drop) rader med ogiltig data från tabellen
        data_cleaned = data.dropna(subset=['date'])

        # Stäng anslutningen till databasen
        conn.close()

    except Exception as e:
        # Logga fel om något går fel
        logging.error(f"Ett fel uppstod vid bearbetning av databasen: {e}")
        data_cleaned = pd.DataFrame()  # Returnera tom DataFrame vid fel

    return data_cleaned


def read_data_from_csv(file_path):
    try:
        #  Läs in csv filen och returnera datat, loggar om det gick bra eller dåligt
        data = pd.read_csv(file_path)
        logging.info(f"{datetime.now()}: Data fetched from {file_path}")
        return data  # Returnera data från filen
    except Exception as e:
        logging.error(f"{datetime.now()}: Error fetching data - {e}")
        raise

def process_data(data):
    try:
        #   Ändrar datatyper eller formaterar
        #   errors='coerce' gör att ogiltiga datumsträngar blir ett
        #   speciellt värde som kallas NaT (Not a Time)
        data['date'] = pd.to_datetime(data['date'], errors='coerce')
        logging.info(f"{datetime.now()}: Data processed successfully")
        return data
    except Exception as e:
        logging.error(f"{datetime.now()}: Error processing data - {e}")
        raise

def update_database(data, db_path):
    #   Uppdaterar SQL-tabell med den formaterade datan.
    try:
        # Anslut till databasen
        conn = sqlite3.connect(db_path)
        # Skriv in data i tabellen, om tabellen finns ersätt den
        data.to_sql('dates_and_values', conn, if_exists='replace', index=False)
        # Utför
        conn.commit()
        # Stäng anslutningen
        conn.close()
        logging.info(f"{datetime.now()}: SQL table updated successfully")
    except Exception as e:
        logging.error(f"{datetime.now()}: Error updating SQL table - {e}")
        raise

if __name__ == "__main__":
    # Hämta, formatera och uppdatera SQL-tabellen
    try:
        #  Använder absoluta pathen då filerna annars inte hittas vid scheduleringen
        #  Vi antar att filerna ligger i mappen Kunskapskontroll2_VG
        data_csv_path = os.path.join(desktop_path, 'Kunskapskontroll2_VG', 'data.csv')  # Absoluta pathen till csv-filen
        database_path = os.path.join(desktop_path, 'Kunskapskontroll2_VG', 'database.db')  # Absoluta pathen till databas-filen

        data = read_data_from_csv(data_csv_path)
        processed_data = process_data(data)
        update_database(processed_data, database_path)
        database_value = read_database(database_path)
        logging.info(f"{datetime.now()}:\n\nTable:\n {database_value}\n\n--------------------------")

    except Exception as e:
        logging.error(f"{datetime.now()}: Program failed - {e}")
