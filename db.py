import sqlite3
import csv
from pathlib import Path

class SQLiteHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        db_dir = Path(self.db_path).parent
        if not db_dir.exists():
            db_dir.mkdir(parents=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        self.cursor.execute("""
         CREATE TABLE IF NOT EXISTS sensor (
            sensor_id INTEGER PRIMARY KEY,
            sensor_type TEXT UNIQUE,
            location_id INTEGER,
            FOREIGN KEY (location_id) REFERENCES location(location_id)
        );
        """)
        
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS location (
            location_id INTEGER PRIMARY KEY,  -- location name as primary key
            lat REAL,
            lon REAL
        );
        """)
        
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS sds_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER,
            timestamp TEXT,
            p1 REAL,
            dur_p1 REAL,
            ratio_p1 REAL,
            p2 REAL,
            dur_p2 REAL,
            ratio_p2 REAL,
            FOREIGN KEY (sensor_id) REFERENCES sensor_type(sensor_id)
        );
        """)
        
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS dht_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER,
            timestamp TEXT,
            temperature REAL,
            humidity REAL,
            FOREIGN KEY (sensor_id) REFERENCES sensor_type(sensor_id)
        );
        """)
        
        self.conn.commit()

    def insert_sensor_type(self, sensor_id, sensor_type_name, location_id):
        self.cursor.execute("SELECT COUNT(*) FROM sensor WHERE sensor_id = ?", (sensor_id,))
        count = self.cursor.fetchone()[0]
        
        self.cursor.execute("""
            INSERT OR IGNORE INTO sensor (sensor_id, sensor_type, location_id)
            VALUES (?, ?, ?)
        """, (sensor_id, sensor_type_name, location_id))
        self.conn.commit()

    def insert_location(self, location_id, lat, lon):
      self.cursor.execute("""
          INSERT OR IGNORE INTO location (location_id, lat, lon)
          VALUES (?, ?, ?)
      """, (location_id, lat, lon))
      self.conn.commit()

    def insert_sds_data(self, sds_data):
        self.cursor.executemany("""
        INSERT INTO sds_data (sensor_id, timestamp, p1, dur_p1, ratio_p1, p2, dur_p2, ratio_p2) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, sds_data)
        self.conn.commit()

    def insert_dht_data(self, dht_data):
        self.cursor.executemany("""
        INSERT INTO dht_data (sensor_id, timestamp, temperature, humidity)
        VALUES (?, ?, ?, ?)
        """, dht_data)
        self.conn.commit()

    def close(self):
        self.conn.close()


class DataInserter:
    def __init__(self, db_path, csv_path):
        self.db_handler = SQLiteHandler(db_path)
        self.csv_path = csv_path
        self.db_handler.create_tables()

    def insert_data_from_csv(self):
      sds_file = open(self.csv_path / "Daten-SDS.csv", "r")
      dht_file = open(self.csv_path / "Daten-DHT.csv", "r")

      sds_reader = csv.DictReader(sds_file)
      dht_reader = csv.DictReader(dht_file)

      print(f"SDS Headers: {sds_reader.fieldnames}")
      print(f"DHT Headers: {dht_reader.fieldnames}")

      sds_data = []
      dht_data = []

      for row in sds_reader:
          sensor_id = int(row['sensor_id']) if row['sensor_id'] else None
          location = row['location'] if row['location'] else None
          lat = float(row['lat']) if row['lat'] else None
          lon = float(row['lon']) if row['lon'] else None
          sensor_type_id = sensor_id
          sensor_type_name = row['sensor_type'] if row['sensor_type'] else None

          if sensor_id is not None and sensor_type_name:
              self.db_handler.insert_sensor_type(sensor_id, sensor_type_name, location)
          if location is not None:
              self.db_handler.insert_location(location, lat, lon)

          sds_data.append((
              sensor_id,
              row['timestamp'] if row['timestamp'] else None,
              float(row['P1']) if row['P1'] else None,
              float(row['durP1']) if row['durP1'] else None,
              float(row['ratioP1']) if row['ratioP1'] else None,
              float(row['P2']) if row['P2'] else None,
              float(row['durP2']) if row['durP2'] else None,
              float(row['ratioP2']) if row['ratioP2'] else None
          ))


      for row in dht_reader:
          sensor_id = int(row['sensor_id']) if row['sensor_id'] else None
          lat = float(row['lat']) if row['lat'] else None
          lon = float(row['lon']) if row['lon'] else None
          sensor_type_id = sensor_id
          sensor_type_name = row['sensor_type'] if row['sensor_type'] else None

          if sensor_id is not None and sensor_type_name:
              self.db_handler.insert_sensor_type(sensor_id, sensor_type_name, location)
          if location is not None:
              self.db_handler.insert_location(location, lat, lon)

          dht_data.append((
              sensor_id,
              row['timestamp'] if row['timestamp'] else None,
              float(row['temperature']) if row['temperature'] else None,
              float(row['humidity']) if row['humidity'] else None
          ))

      self.db_handler.insert_sds_data(sds_data)
      self.db_handler.insert_dht_data(dht_data)

      self.db_handler.close()

      sds_file.close()
      dht_file.close()


# Example usage
if __name__ == "__main__":
    db_path = r'C:\code\Private Repos\SW-DM-DEVCON5\CSVFiles\data.db'
    csv_path = Path(r'C:\code\Private Repos\SW-DM-DEVCON5\CSVFiles')

    data_inserter = DataInserter(db_path, csv_path)
    data_inserter.insert_data_from_csv()

    print("Data has been inserted into the database!")


