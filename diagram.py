import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

class PlotGraph:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_data(self, datum):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
            SELECT timestamp, p1, p2
            FROM sds_data
            WHERE timestamp LIKE ?
            ORDER BY timestamp;
        """

        cursor.execute(query, (datum + '%',))
        rows = cursor.fetchall()

        conn.close()

        return rows

    def create_graphic(self, datum):
        data = self.get_data(datum)

        if not data:
            print(f"Keine Daten für das Datum {datum} gefunden.")
            return

        timestamps = [datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S") for row in data]
        p1_values = [row[1] for row in data]
        p2_values = [row[2] for row in data]

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, p1_values, label='P1 Feinstaubwerte', color='blue')
        plt.plot(timestamps, p2_values, label='P2 Feinstaubwerte', color='red')

        plt.title(f"Feinstaubwerte für {datum}")
        plt.xlabel('Stunde')
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

    date = input("Geben Sie ein Datum im Format YYYY-MM-DD ein: ")
    plot.create_graphic(date)