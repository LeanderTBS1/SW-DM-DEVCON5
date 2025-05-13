import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from typing import List, Tuple

class PlotGraph:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_data(self, datum: str) -> List[Tuple[str, float, float]]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                query = """
                    SELECT timestamp, p1, p2
                    FROM sds_data
                    WHERE timestamp LIKE ?
                    ORDER BY timestamp;
                """
                cursor.execute(query, (datum + '%',))
                return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Fehler beim Zugriff auf die Datenbank: {e}")
            return []

    def create_graphic(self, datum: str) -> None:
        try:
            datetime.strptime(datum, "%Y-%m-%d")
        except ValueError:
            print("Ungültiges Datum. Bitte im Format YYYY-MM-DD eingeben.")
            return

        data = self.get_data(datum)

        if not data:
            print(f"Keine Daten für das Datum {datum} gefunden.")
            return

        try:
            timestamps = [datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S") for row in data]
        except ValueError as ve:
            print(f"Ungültiges Zeitformat in den Daten: {ve}")
            return

        p1_values = [row[1] for row in data]
        p2_values = [row[2] for row in data]

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, p1_values, label='P1 Feinstaubwerte (µg/m³)', color='blue')
        plt.plot(timestamps, p2_values, label='P2 Feinstaubwerte (µg/m³)', color='red')

        plt.title(f"Feinstaubwerte für {datum}")
        plt.xlabel('Uhrzeit')
        plt.ylabel('Feinstaubwert (µg/m³)')
        plt.xticks(rotation=45)

        ax = plt.gca()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H'))

        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    db_path = "data.db"
    plot = PlotGraph(db_path)

    date = input("Geben Sie ein Datum im Format YYYY-MM-DD ein: ").strip()
    plot.create_graphic(date)