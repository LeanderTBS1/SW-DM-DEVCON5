import sqlite3, statistics
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict

class TemperatureYearlyPlot:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_yearly_temperature(self, year):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
            SELECT timestamp, temperature
            FROM dht_data
            WHERE strftime('%Y', timestamp) = ?
            AND temperature IS NOT NULL
            ORDER BY timestamp
        """

        cursor.execute(query, (str(year),))
        rows = cursor.fetchall()
        conn.close()

        return rows

    def plot_monthly_avg(self, year):
        data = self.get_yearly_temperature(year)

        if not data:
            print(f"⚠️ Keine Temperaturdaten für das Jahr {year} gefunden.")
            return

        monthly_data = defaultdict(list)
        for timestamp_str, temp in data:
            try:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                monthly_data[timestamp.month].append(temp)
            except ValueError:
                continue

        months = range(1, 13)
        monthly_avg = [
            round(statistics.mean(monthly_data[m]), 2) if m in monthly_data else None
            for m in months
        ]

        plt.figure(figsize=(10, 5))
        plt.plot(months, monthly_avg, marker='o', linestyle='-', color='orange', label="Durchschnittstemperatur")
        plt.xticks(months, [
            "Jan", "Feb", "Mär", "Apr", "Mai", "Jun",
            "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"
        ])
        plt.title(f"Durchschnittstemperatur pro Monat im Jahr {year}")
        plt.xlabel("Monat")
        plt.ylabel("Temperatur (°C)")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    db_path = "data.db"
    year = input("Welches Jahr möchtest du anzeigen (z. B. 2023)? ")

    plotter = TemperatureYearlyPlot(db_path)
    plotter.plot_monthly_avg(year)