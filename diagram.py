import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

class FeinstaubGrafik:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_feinstaubdaten(self, datum):
        """Abfrage der Feinstaubdaten für das angegebene Datum."""
        # Verbindet sich mit der SQLite-Datenbank
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Format der Datumseingabe: 'YYYY-MM-DD'
        query = """
        SELECT timestamp, p1, p2
        FROM sds_data
        WHERE timestamp LIKE ?
        ORDER BY timestamp;
        """
        
        # Sucht nach Feinstaubdaten für das angegebene Datum
        cursor.execute(query, (datum + '%',))
        rows = cursor.fetchall()
        
        conn.close()
        
        return rows

    def erstelle_grafik(self, datum):
        """Erstellt eine Grafik der Feinstaubwerte des angegebenen Datums."""
        daten = self.get_feinstaubdaten(datum)

        if not daten:
            print(f"Keine Daten für das Datum {datum} gefunden.")
            return
        
        # Extrahiere Zeitstempel, P1- und P2-Werte
        timestamps = [datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S") for row in daten]  # Format angepasst
        p1_values = [row[1] for row in daten]
        p2_values = [row[2] for row in daten]

        # Erstellen der Grafik
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, p1_values, label='P1 Feinstaubwerte', color='blue')
        plt.plot(timestamps, p2_values, label='P2 Feinstaubwerte', color='red')

        # Formatierung der Grafik
        plt.title(f"Feinstaubwerte für {datum}")
        plt.xlabel('Zeit')
        plt.ylabel('Feinstaubwert (µg/m³)')
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

# Beispielhafte Verwendung:
if __name__ == "__main__":
    db_path = "data.db"  # Der Pfad zur SQLite-Datenbank
    grafik = FeinstaubGrafik(db_path)

    # Datum vom Benutzer eingeben
    datum = input("Geben Sie ein Datum im Format YYYY-MM-DD ein: ")
    grafik.erstelle_grafik(datum)