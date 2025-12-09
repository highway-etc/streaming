-- 初始化两个分片库：etc_0、etc_1（演练版分库）
CREATE DATABASE IF NOT EXISTS etc_0 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS etc_1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- 分片库 etc_0：建三张业务表
USE etc_0;
CREATE TABLE IF NOT EXISTS traffic_pass_dev (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    gcsj TIMESTAMP NULL,
    xzqhmc VARCHAR(64),
    adcode INT,
    kkmc VARCHAR(128),
    station_id INT,
    fxlx VARCHAR(16),
    hpzl VARCHAR(16),
    hphm_mask VARCHAR(32),
    clppxh VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_station_time (station_id, gcsj),
    KEY idx_created (created_at)
);
CREATE TABLE IF NOT EXISTS stats_realtime (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    station_id INT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    cnt BIGINT NOT NULL,
    by_dir JSON,
    by_type JSON,
    UNIQUE KEY uk_station_window (station_id, window_start, window_end)
);
CREATE TABLE IF NOT EXISTS alert_plate_clone (
    alert_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hphm_mask VARCHAR(16),
    first_station_id INT,
    second_station_id INT,
    time_gap_sec BIGINT,
    distance_km DOUBLE,
    speed_kmh DOUBLE,
    confidence DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_hphm_mask (hphm_mask),
    KEY idx_created (created_at)
);
-- 分片库 etc_1：建同样三张表
USE etc_1;
CREATE TABLE IF NOT EXISTS traffic_pass_dev (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    gcsj TIMESTAMP NULL,
    xzqhmc VARCHAR(64),
    adcode INT,
    kkmc VARCHAR(128),
    station_id INT,
    fxlx VARCHAR(16),
    hpzl VARCHAR(16),
    hphm_mask VARCHAR(32),
    clppxh VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_station_time (station_id, gcsj),
    KEY idx_created (created_at)
);
CREATE TABLE IF NOT EXISTS stats_realtime (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    station_id INT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    cnt BIGINT NOT NULL,
    by_dir JSON,
    by_type JSON,
    UNIQUE KEY uk_station_window (station_id, window_start, window_end)
);
CREATE TABLE IF NOT EXISTS alert_plate_clone (
    alert_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hphm_mask VARCHAR(16),
    first_station_id INT,
    second_station_id INT,
    time_gap_sec BIGINT,
    distance_km DOUBLE,
    speed_kmh DOUBLE,
    confidence DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_hphm_mask (hphm_mask),
    KEY idx_created (created_at)
);