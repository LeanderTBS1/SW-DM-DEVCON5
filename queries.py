import sqlite3, csv ,os
from pathlib import Path

class QueriesHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        db_dir = Path(self.db_path).parent
        if not db_dir.exists():
            db_dir.mkdir(parents=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def get_avg(self, date):
        data = self.cursor.execute("""
            SELECT 
            AVG(temperature) AS avg_temperature
            FROM dht_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_min(self,date):
        data = self.cursor.execute("""
            SELECT 
            MIN(temperature) AS min_temperature
            FROM dht_data
            WHERE DATE(timestamp) = ?;                           
        """ , (date,))
        return data.fetchone()

    def get_max(self, date):
        data = self.cursor.execute("""
            SELECT
            MAX(temperature) AS max_temperature
            FROM dht_data
           WHERE DATE(timestamp) = ?;                           
        """ , (date,))
        return data.fetchone()
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, "data.db")

    db = QueriesHandler(db_path)

    date = input("Gib ein Datum im Format JJJJ-MM-TT ein: ")
    try:
        avg = db.get_avg(date)
        max_val = db.get_max(date)
        min_val = db.get_min(date)

        print(f"Durchschnitt: {avg}")
        print(f"Maximum: {max_val}")
        print(f"Minimum: {min_val}")
    except ValueError:
        print("Fehlerhafte Eingabe: Bitte überprüfe das Datumsformat.")
    except KeyError:
        print("Keine Daten für das eingegebene Datum gefunden.")
    except Exception as e:
        print(f"Ein unbekannter Fehler ist aufgetreten: {e}")
