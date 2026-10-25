-- =====================================================================
-- Skema MySQL: Dashboard Simulasi Gempa
-- Jalankan:  mysql -u root -p < schema.sql
-- =====================================================================
CREATE DATABASE IF NOT EXISTS gempa_db
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE gempa_db;

-- ---------------------------------------------------------------------
-- 1. sensor : data master 4 sensor MPU6050 (id tetap, dipakai aplikasi)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor (
  id           TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  kode         VARCHAR(16)  NOT NULL UNIQUE,          -- sama dengan key JSON dari ESP32
  nama         VARCHAR(50)  NOT NULL,
  jenis        ENUM('tanah','bangunan') NOT NULL,
  channel_tca  TINYINT UNSIGNED NOT NULL              -- channel TCA9548A
) ENGINE=InnoDB;

INSERT INTO sensor (id, kode, nama, jenis, channel_tca) VALUES
  (1, 'tanah',  'Tanah / Meja',    'tanah',    1),
  (2, 'kecil',  'Bangunan Kecil',  'bangunan', 2),
  (3, 'sedang', 'Bangunan Sedang', 'bangunan', 4),
  (4, 'tinggi', 'Bangunan Tinggi', 'bangunan', 6)
ON DUPLICATE KEY UPDATE nama = VALUES(nama);

-- ---------------------------------------------------------------------
-- 2. sesi : satu percobaan (misalnya "Sapuan 3 Hz"); selesai NULL = sedang berjalan
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sesi (
  id       INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  nama     VARCHAR(100) NOT NULL,
  catatan  TEXT NULL,
  mulai    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  selesai  DATETIME(3) NULL,
  INDEX idx_sesi_mulai (mulai)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 3. pembacaan : data 1 Hz, satu baris per sensor per detik
--    Semua baris dari satu pesan MQTT memakai nilai 'waktu' yang sama
--    (dipakai untuk menggabungkan bangunan dengan tanah pada analisis sapuan).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pembacaan (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  waktu      DATETIME(3) NOT NULL,
  sesi_id    INT UNSIGNED NULL,
  sensor_id  TINYINT UNSIGNED NOT NULL,
  online     TINYINT(1) NOT NULL,                     -- 1 = sensor terbaca, 0 = putus
  rms        FLOAT NULL COMMENT 'RMS gabungan (g)',
  x          FLOAT NULL COMMENT 'RMS sumbu X (g)',
  y          FLOAT NULL COMMENT 'RMS sumbu Y (g)',
  z          FLOAT NULL COMMENT 'RMS sumbu Z (g)',
  f          FLOAT NULL COMMENT 'frekuensi dominan hasil FFT (Hz)',
  amp        FLOAT NULL COMMENT 'amplitudo puncak spektrum (g)',
  rasio      FLOAT NULL COMMENT 'amplitudo bangunan / tanah pada frekuensi puncak tanah (hanya bangunan)',
  resonansi  TINYINT(1) NULL COMMENT '1 = resonansi terdeteksi (hanya bangunan)',
  INDEX idx_pemb_waktu (waktu),
  INDEX idx_pemb_sensor_waktu (sensor_id, waktu),
  INDEX idx_pemb_sesi (sesi_id),
  CONSTRAINT fk_pemb_sensor FOREIGN KEY (sensor_id) REFERENCES sensor(id),
  CONSTRAINT fk_pemb_sesi   FOREIGN KEY (sesi_id)   REFERENCES sesi(id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 4. spektrum : snapshot spektrum frekuensi (disimpan berkala, bukan tiap pesan)
--    amplitudo = array JSON 64 nilai; bin ke-k berada pada frekuensi k * df
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spektrum (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  waktu      DATETIME(3) NOT NULL,
  sesi_id    INT UNSIGNED NULL,
  sensor_id  TINYINT UNSIGNED NOT NULL,
  df         FLOAT NOT NULL COMMENT 'resolusi frekuensi (Hz per bin)',
  fs         FLOAT NULL COMMENT 'laju sampling terukur (Hz)',
  amplitudo  JSON NOT NULL,
  INDEX idx_spek_sensor_waktu (sensor_id, waktu),
  INDEX idx_spek_sesi (sesi_id),
  CONSTRAINT fk_spek_sensor FOREIGN KEY (sensor_id) REFERENCES sensor(id),
  CONSTRAINT fk_spek_sesi   FOREIGN KEY (sesi_id)   REFERENCES sesi(id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 5. kejadian : event dari ESP32 (sensor putus/pulih, resonansi mulai/selesai)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kejadian (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  waktu       DATETIME(3) NOT NULL,
  sesi_id     INT UNSIGNED NULL,
  sensor_id   TINYINT UNSIGNED NULL,
  tipe        ENUM('offline','pulih','resonansi_mulai','resonansi_selesai') NOT NULL,
  uptime_esp  INT UNSIGNED NULL COMMENT 'detik sejak ESP32 menyala',
  INDEX idx_kej_waktu (waktu),
  CONSTRAINT fk_kej_sensor FOREIGN KEY (sensor_id) REFERENCES sensor(id),
  CONSTRAINT fk_kej_sesi   FOREIGN KEY (sesi_id)   REFERENCES sesi(id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 6. status_perangkat : riwayat online/offline ESP32 (dari topik status)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS status_perangkat (
  id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  waktu   DATETIME(3) NOT NULL,
  status  ENUM('online','offline') NOT NULL,
  INDEX idx_status_waktu (waktu)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- Contoh query berguna
-- ---------------------------------------------------------------------
-- Frekuensi natural per bangunan (metode sapuan): rasio rata-rata per kelompok 0,5 Hz
-- SELECT s.kode, ROUND(t.f / 0.5) * 0.5 AS f_tanah, AVG(b.rasio) AS rasio_rata, COUNT(*) AS n
-- FROM pembacaan b
-- JOIN pembacaan t ON t.waktu = b.waktu AND t.sensor_id = 1
-- JOIN sensor s    ON s.id = b.sensor_id
-- WHERE b.sensor_id <> 1 AND t.f IS NOT NULL AND t.amp >= 0.02 AND b.rasio IS NOT NULL
-- GROUP BY s.kode, f_tanah HAVING n >= 3 ORDER BY s.kode, f_tanah;

-- Hapus data lama (misalnya lebih dari 30 hari) agar database tidak membengkak:
-- DELETE FROM pembacaan WHERE waktu < NOW() - INTERVAL 30 DAY;
-- DELETE FROM spektrum  WHERE waktu < NOW() - INTERVAL 30 DAY;
