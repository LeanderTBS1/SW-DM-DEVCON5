CREATE TABLE IF NOT EXISTS sensor (
    sensor_id INTEGER PRIMARY KEY,
    sensor_type TEXT UNIQUE,
    location_id INTEGER,
    FOREIGN KEY (location_id) REFERENCES location(location_id)
);

CREATE TABLE IF NOT EXISTS location (
    location_id INTEGER PRIMARY KEY,
    lat REAL,
    lon REAL
);

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

CREATE TABLE IF NOT EXISTS dht_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id INTEGER,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    FOREIGN KEY (sensor_id) REFERENCES sensor_type(sensor_id)
);
