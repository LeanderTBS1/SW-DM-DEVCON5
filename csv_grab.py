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
        urlLists = self.get_urls()
        reader_lists = self.read_csv(urlLists)
        self.write_csv(reader_lists)
        print("Die CSV-Daten befinden sich nun im Ordner: ", self.path)

    def get_urls(self):
        add_a_day = datetime.timedelta(days=1)
        date = self.start_date
        sds_list = []
        dht_list = []
        while date <= self.end_date:
            base_path = f"{date.strftime('%Y-%m-%d')}/"
            if 2015 <= date.year <= 2022:
                base_path = f"{date.strftime('%Y')}/{base_path}"
                sds_suffix = "_sds011_sensor_321.csv.gz" if date.year <= 2022 else "_sds011_sensor_3659.csv"
                dht_suffix = "_dht22_sensor_322.csv.gz" if date.year <= 2022 else "_dht22_sensor_3660.csv"
            else:
                sds_suffix = "_sds011_sensor_3659.csv"
                dht_suffix = "_dht22_sensor_3660.csv"

            sds_list.append(self.sensor_url + base_path + date.strftime("%Y-%m-%d") + sds_suffix)
            dht_list.append(self.sensor_url + base_path + date.strftime("%Y-%m-%d") + dht_suffix)
            date += add_a_day

        return {"sds": sds_list, "dht": dht_list}

    def read_csv(self, urls):
      sds_readers = []
      dht_readers = []
      
      for sds_url in urls['sds']:
          try:
              url_response = urllib.request.urlopen(sds_url)
              if sds_url.endswith('.gz'):
                  with gzip.GzipFile(fileobj=url_response) as gz:
                      csv_rows = [line.decode('utf-8') for line in gz.readlines()]
              else:
                  csv_rows = [line.decode('utf-8') for line in url_response.readlines()]
              reader = csv.DictReader(csv_rows, delimiter=";")
              sds_readers.append(list(reader))
          except (urllib.error.HTTPError, urllib.error.URLError):
              print("Die Daten von", sds_url, "konnten nicht gefunden werden")

      for dht_url in urls['dht']:
          try:
              url_response = urllib.request.urlopen(dht_url)
              if dht_url.endswith('.gz'):
                  with gzip.GzipFile(fileobj=url_response) as gz:
                      csv_rows = [line.decode('utf-8') for line in gz.readlines()]
              else:
                  csv_rows = [line.decode('utf-8') for line in url_response.readlines()]
              reader = csv.DictReader(csv_rows, delimiter=";")
              dht_readers.append(list(reader))
          except (urllib.error.HTTPError, urllib.error.URLError):
              print("Die Daten von", dht_url, "konnten nicht gefunden werden")

      return {"sds": sds_readers, "dht": dht_readers}


    def write_csv(self, readers):
      sds_file_path = self.path / "Daten-SDS.csv"
      dht_file_path = self.path / "Daten-DHT.csv"

      with open(sds_file_path, "w", newline='', encoding="utf-8") as sds_file, \
              open(dht_file_path, "w", newline='', encoding="utf-8") as dht_file:

          sds_writer = csv.DictWriter(sds_file, fieldnames=["sensor_id", "sensor_type", "location", "lat", "lon",
                                                            "timestamp", "P1", "durP1", "ratioP1", "P2", "durP2",
                                                            "ratioP2"])
          sds_writer.writeheader()
          for sds_reader in readers['sds']:
              sds_writer.writerows(sds_reader)

          dht_writer = csv.DictWriter(dht_file, fieldnames=["sensor_id", "sensor_type", "location", "lat", "lon",
                                                            "timestamp", "temperature", "humidity"])
          dht_writer.writeheader()
          for dht_reader in readers['dht']:
              dht_writer.writerows(dht_reader)

      print(f"Dateien gespeichert: {sds_file_path}, {dht_file_path}")

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))
    start_date = "2022-01-01"
    end_date = "2022-12-31"
    downloader = CSVDownloader(path, start_date, end_date)
    downloader.main()