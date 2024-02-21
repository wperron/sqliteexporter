ALTER TABLE spans DROP COLUMN start_time;
ALTER TABLE spans DROP COLUMN end_time;
ALTER TABLE spans RENAME COLUMN start_time_bak TO start_time;
ALTER TABLE spans RENAME COLUMN end_time_back TO end_time;
