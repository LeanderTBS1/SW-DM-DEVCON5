-- Maximale, minimale und durchschnittliche Temperatur für den 14.03.2022
SELECT 
    MAX(temperature) AS max_temperature,
    MIN(temperature) AS min_temperature,
    AVG(temperature) AS avg_temperature
FROM dht_data
WHERE DATE(timestamp) = '2022-03-14';

-- Sensoren und Standorte
SELECT 
    sensor.sensor_id, 
    sensor.sensor_type, 
    location.location_id, 
    location.lat, 
    location.lon
FROM sensor
JOIN location ON sensor.location_id = location.location_id;
