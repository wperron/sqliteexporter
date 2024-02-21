-- Backup the start_time and end_time column values and convert existing
-- timestamps to microsecond precision
-- TODO(wperron): remove this before publishing
ALTER TABLE spans RENAME COLUMN start_time TO start_time_bak;
ALTER TABLE spans RENAME COLUMN end_time TO end_time_bak;
ALTER TABLE spans ADD COLUMN start_time INTEGER DEFAULT 0;
ALTER TABLE spans ADD COLUMN end_time INTEGER DEFAULT 0;
UPDATE spans SET start_time = start_time_bak * 1000;
UPDATE spans SET end_time = end_time_bak * 1000;
