import datetime
import urllib.request
import urllib.error
import csv
import gzip
import os
from pathlib import Path

class CSVDownloader:
    def __init__(self, path, start_date, end_date):
        self.sensor_url = "https://archive.sensor.community/"
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        self.path = Path(path) / "CSVFiles"
        self.path.mkdir(parents=True, exist_ok=True)

    def main(self):
        url_lists = self.get_urls()
        reader_lists = self.read_csv(url_lists)
        self.write_csv(reader_lists)
        print("Die CSV-Daten befinden sich nun im Ordner:", self.path)

    def get_urls(self):
        add_a_day = datetime.timedelta(days=1)
        date = self.start_date
        sds_list, dht_list = [], []

        while date <= self.end_date:
            base_path = f"{date.strftime('%Y-%m-%d')}/"
            if 2015 <= date.year <= 2024:
                base_path = f"{date.year}/{base_path}"
                sds_suffix = "_sds011_sensor_321.csv.gz"
                dht_suffix = "_dht22_sensor_322.csv.gz"
            else:
                sds_suffix = "_sds011_sensor_3659.csv"
                dht_suffix = "_dht22_sensor_3660.csv"

            sds_list.append(self.sensor_url + base_path + date.strftime("%Y-%m-%d") + sds_suffix)
            dht_list.append(self.sensor_url + base_path + date.strftime("%Y-%m-%d") + dht_suffix)
            date += add_a_day

        return {"sds": sds_list, "dht": dht_list}

    def _load_csv_from_url(self, url):
        try:
            response = urllib.request.urlopen(url)
            if url.endswith(".gz"):
                with gzip.GzipFile(fileobj=response) as gz:
                    csv_rows = [line.decode("utf-8") for line in gz.readlines()]
            else:
                csv_rows = [line.decode("utf-8") for line in response.readlines()]
            return list(csv.DictReader(csv_rows, delimiter=";"))
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            print(f"Fehler beim Laden von {url}: {e}")
            return None

    def read_csv(self, urls):
        sds_readers = [res for url in urls.get("sds", []) if (res := self._load_csv_from_url(url)) is not None]
        dht_readers = [res for url in urls.get("dht", []) if (res := self._load_csv_from_url(url)) is not None]
        return {"sds": sds_readers, "dht": dht_readers}

    def write_csv(self, readers):
        sds_file = self.path / "Daten-SDS.csv"
        dht_file = self.path / "Daten-DHT.csv"

        with open(sds_file, "w", newline='', encoding="utf-8") as sds_f, \
             open(dht_file, "w", newline='', encoding="utf-8") as dht_f:

            sds_writer = csv.DictWriter(sds_f, fieldnames=["sensor_id", "sensor_type", "location", "lat", "lon",
                                                            "timestamp", "P1", "durP1", "ratioP1", "P2", "durP2", "ratioP2"])
            sds_writer.writeheader()
            for sds_reader in readers["sds"]:
                sds_writer.writerows(sds_reader)

            dht_writer = csv.DictWriter(dht_f, fieldnames=["sensor_id", "sensor_type", "location", "lat", "lon",
                                                            "timestamp", "temperature", "humidity"])
            dht_writer.writeheader()
            for dht_reader in readers["dht"]:
                dht_writer.writerows(dht_reader)

        print(f"Dateien gespeichert: {sds_file}, {dht_file}")


if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    start_date = input("Geben Sie das Startdatum ein (YYYY-MM-DD): ")
    end_date = input("Geben Sie das Enddatum ein (YYYY-MM-DD): ")

    try:
        datetime.datetime.strptime(start_date, '%Y-%m-%d')
        datetime.datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("Ungültiges Datumsformat. Bitte (YYYY-MM-DD) verwenden.")
        exit(1)

    downloader = CSVDownloader(path, start_date, end_date)
    downloader.main()
