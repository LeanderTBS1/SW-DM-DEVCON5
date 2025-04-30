import datetime
import urllib.request
import urllib.error
import csv
import gzip
import os
from pathlib import Path
from typing import List, Dict, Optional


class CSVDownloader:
    def __init__(self, path: str, start_date: datetime.datetime, end_date: datetime.datetime):
        self.sensor_url = "https://archive.sensor.community/"
        self.start_date = start_date
        self.end_date = end_date
        self.path = Path(path) / "CSVFiles"
        self.path.mkdir(parents=True, exist_ok=True)

    def main(self):
        url_lists = self.get_urls()
        reader_lists = self.read_csv(url_lists)
        self.write_csv(reader_lists)
        print("Die CSV-Daten befinden sich nun im Ordner:", self.path)

    def get_urls(self) -> Dict[str, List[str]]:
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

            date_str = date.strftime("%Y-%m-%d")
            sds_list.append(self.sensor_url + base_path + date_str + sds_suffix)
            dht_list.append(self.sensor_url + base_path + date_str + dht_suffix)
            date += add_a_day

        return {"sds": sds_list, "dht": dht_list}

    def _load_csv_from_url(self, url: str) -> Optional[List[Dict[str, str]]]:
        try:
            print(f"Lade: {url}")
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

    def read_csv(self, urls: Dict[str, List[str]]) -> Dict[str, List[List[Dict[str, str]]]]:
        sds_readers = [res for url in urls.get("sds", []) if (res := self._load_csv_from_url(url)) is not None]
        dht_readers = [res for url in urls.get("dht", []) if (res := self._load_csv_from_url(url)) is not None]
        return {"sds": sds_readers, "dht": dht_readers}

    def _write_to_file(self, filename: Path, data: List[List[Dict[str, str]]], fieldnames: List[str]):
        with open(filename, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for dataset in data:
                writer.writerows(dataset)

    def write_csv(self, readers: Dict[str, List[List[Dict[str, str]]]]):
        sds_file = self.path / "Daten-SDS.csv"
        dht_file = self.path / "Daten-DHT.csv"

        self._write_to_file(sds_file, readers["sds"],
                            ["sensor_id", "sensor_type", "location", "lat", "lon",
                             "timestamp", "P1", "durP1", "ratioP1", "P2", "durP2", "ratioP2"])
        self._write_to_file(dht_file, readers["dht"],
                            ["sensor_id", "sensor_type", "location", "lat", "lon",
                             "timestamp", "temperature", "humidity"])

        print(f"Dateien gespeichert: {sds_file}, {dht_file}")


def parse_date(date_str: str) -> datetime.datetime:
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print(f"Ungültiges Datumsformat: {date_str}. Bitte (YYYY-MM-DD) verwenden.")
        exit(1)


if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    start_date_str = input("Geben Sie das Startdatum ein (YYYY-MM-DD): ")
    end_date_str = input("Geben Sie das Enddatum ein (YYYY-MM-DD): ")

    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)

    if start_date > end_date:
        print("Das Startdatum darf nicht nach dem Enddatum liegen.")
        exit(1)

    downloader = CSVDownloader(path, start_date, end_date)
    downloader.main()
