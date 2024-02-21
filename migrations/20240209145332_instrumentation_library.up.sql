ALTER TABLE spans ADD COLUMN instrumentation_library_name TEXT DEFAULT NULL;
ALTER TABLE spans ADD COLUMN instrumentation_library_version TEXT DEFAULT NULL;
ALTER TABLE spans ADD COLUMN instrumentation_library_attributes TEXT DEFAULT NULL;
