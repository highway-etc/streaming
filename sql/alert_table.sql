CREATE TABLE IF NOT EXISTS alert_plate_clone (
    alert_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hphm_mask VARCHAR(16),
    first_station_id INT,
    second_station_id INT,
    time_gap_sec BIGINT,
    distance_km DOUBLE,
    speed_kmh DOUBLE,
    confidence DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_hphm_mask ON alert_plate_clone(hphm_mask);
CREATE INDEX idx_created ON alert_plate_clone(created_at);