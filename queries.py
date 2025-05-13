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

    def get_avg_temp(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            AVG(temperature) AS avg_temperature
            FROM dht_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_min_temp(self,date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MIN(temperature) AS min_temperature
            FROM dht_data
            WHERE DATE(timestamp) = ?;                           
        """ , (date,))
        return data.fetchone()

    def get_max_temp(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT
            MAX(temperature) AS max_temperature
            FROM dht_data
           WHERE DATE(timestamp) = ?;                           
        """ , (date,))
        return data.fetchone()
    
    def get_max_hum(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MAX(humidity) AS max_humidity
            FROM dht_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_min_hum(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MIN(humidity) AS min_humidity
            FROM dht_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_avg_hum(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            AVG(humidity) AS avg_humidity
            FROM dht_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_max_dustp1(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MAX(p1) AS max_p1
            FROM sds_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_min_dustp1(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MIN(p1) AS max_p1
            FROM sds_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_avg_dustp1(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            AVG(p1) AS avg_p1
            FROM sds_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    

    def get_max_dustp2(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MAX(p2) AS max_p2
            FROM sds_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_min_dustp2(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            MIN(p2) AS max_p2
            FROM sds_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
    def get_avg_dustp2(self, date) -> tuple:
        data = self.cursor.execute("""
            SELECT 
            AVG(p2) AS avg_p2
            FROM sds_data
            WHERE DATE(timestamp) = ?;
        """, (date,))
        return data.fetchone()
    
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, "data.db")

    db = QueriesHandler(db_path)

    date = input("Gib ein Datum im Format JJJJ-MM-TT ein: ")
    try:
        avg_temp = db.get_avg_temp(date)
        max_temp = db.get_max_temp(date)
        min_temp = db.get_min_temp(date)

        avg_hum  = db.get_avg_hum(date)
        max_hum  = db.get_max_hum(date)
        min_hum  = db.get_min_hum(date)

        avg_p1  = db.get_avg_dustp1(date)
        max_p1  = db.get_max_dustp1(date)
        min_p1  = db.get_min_dustp1(date)

        avg_p2  = db.get_avg_dustp2(date)
        max_p2  = db.get_max_dustp2(date)
        min_p2  = db.get_min_dustp2(date)


        print(f"Durchschnittstemperatur: {round(avg_temp[0], 2)} C°")
        print(f"Maximale Temperatur: {max_temp[0]} C°")
        print(f"Minimale Temperatur: {min_temp[0]} C°")

        print(f"Durchschnitliche Luftfeuchtigkeit in Prozent: {round(avg_hum[0], 2)} %")
        print(f"Maximale Luftfeuchtigkeit in Prozent: {max_hum[0]} %")
        print(f"Minimale Luftfeuchtigkeit in Prozent: {min_hum[0]} %")

        print(f"Durchschnitliche Feinstaubwerte P1: {round(avg_p1[0], 2)} mg/m³ P2: {round(avg_p2[0], 2)} mg/m³")
        print(f"Maximale Feinstaubwerte P1: {max_p1[0]} mg/m³ P2: {max_p2[0]} mg/m³")
        print(f"Minimale Feinstaubwerte P1: {min_p1[0]} mg/m³ P2: {min_p2[0]} mg/m³")

    except ValueError:
        print("Fehlerhafte Eingabe: Bitte überprüfe das Datumsformat.")
    except KeyError:
        print("Keine Daten für das eingegebene Datum gefunden.")
    except Exception as e:
        print(f"Ein unbekannter Fehler ist aufgetreten: {e}")
