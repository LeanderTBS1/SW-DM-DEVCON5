import sqlite3, csv
from pathlib import Path

class SQLiteHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS location (
                location_id INTEGER PRIMARY KEY,
                lat REAL,
                lon REAL
            );

            CREATE TABLE IF NOT EXISTS sensor (
                sensor_id INTEGER PRIMARY KEY,
                sensor_type TEXT,
                location_id INTEGER,
                FOREIGN KEY (location_id) REFERENCES location(location_id)
            );

            CREATE TABLE IF NOT EXISTS sds_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id INTEGER,
                timestamp TEXT,
                p1 REAL, dur_p1 REAL, ratio_p1 REAL,
                p2 REAL, dur_p2 REAL, ratio_p2 REAL,
                FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)
            );

            CREATE TABLE IF NOT EXISTS dht_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id INTEGER,
                timestamp TEXT,
                temperature REAL,
                humidity REAL,
                FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)
            );
        """)
        self.conn.commit()

    def insert_location(self, location_id, lat, lon):
        self.cursor.execute("""
            INSERT OR IGNORE INTO location (location_id, lat, lon)
            VALUES (?, ?, ?)
        """, (location_id, lat, lon))

    def insert_sensor(self, sensor_id, sensor_type, location_id):
        self.cursor.execute("""
            INSERT OR IGNORE INTO sensor (sensor_id, sensor_type, location_id)
            VALUES (?, ?, ?)
        """, (sensor_id, sensor_type, location_id))

    def insert_sds_data(self, data):
        self.cursor.executemany("""
            INSERT INTO sds_data (sensor_id, timestamp, p1, dur_p1, ratio_p1, p2, dur_p2, ratio_p2)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, data)

    def insert_dht_data(self, data):
        self.cursor.executemany("""
            INSERT INTO dht_data (sensor_id, timestamp, temperature, humidity)
            VALUES (?, ?, ?, ?)
        """, data)

    def commit_and_close(self):
        self.conn.commit()
        self.conn.close()


class DataInserter:
    def __init__(self, db_path, csv_path):
        self.db = SQLiteHandler(db_path)
        self.csv_path = Path(csv_path)

    def process_csv_file(self, filename, is_sds=True):
        file_path = self.csv_path / filename
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            data_list = []

            for row in reader:
                sensor_id = int(row['sensor_id']) if row['sensor_id'] else None
                location_id = int(row['location']) if row['location'] else None
                lat = float(row['lat']) if row['lat'] else None
                lon = float(row['lon']) if row['lon'] else None
                sensor_type = row['sensor_type'] if row['sensor_type'] else None

                if location_id is not None:
                    self.db.insert_location(location_id, lat, lon)

                if sensor_id is not None and sensor_type:
                    self.db.insert_sensor(sensor_id, sensor_type, location_id)

                if is_sds:
                    data_list.append((
                        sensor_id,
                        row['timestamp'],
                        self._to_float(row['P1']),
                        self._to_float(row['durP1']),
                        self._to_float(row['ratioP1']),
                        self._to_float(row['P2']),
                        self._to_float(row['durP2']),
                        self._to_float(row['ratioP2']),
                    ))
                else:
                    data_list.append((
                        sensor_id,
                        row['timestamp'],
                        self._to_float(row['temperature']),
                        self._to_float(row['humidity']),
                    ))

        return data_list

    def _to_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def insert_all_data(self):
        sds_data = self.process_csv_file("Daten-SDS.csv", is_sds=True)
        dht_data = self.process_csv_file("Daten-DHT.csv", is_sds=False)

        self.db.insert_sds_data(sds_data)
        self.db.insert_dht_data(dht_data)
        self.db.commit_and_close()


if __name__ == "__main__":
    base_path = Path(__file__).resolve().parent
    db_path = base_path / "data.db"
    csv_path = base_path / "CSVFiles"

    inserter = DataInserter(db_path, csv_path)
    inserter.insert_all_data()

    print("✅ Daten erfolgreich in die Datenbank eingefügt!")
