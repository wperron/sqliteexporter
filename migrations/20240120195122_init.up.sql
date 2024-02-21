-- Copyright 2023 William Perron. All rights reserved. MIT License.
CREATE TABLE IF NOT EXISTS spans(
    "span_id" BLOB,
    "trace_id" BLOB,
    "parent_span_id" BLOB,
    "tracestate" TEXT,
    "__service_name" TEXT,
    "__duration" INTEGER,
    "name" TEXT,
    "kind" TEXT,
    "start_time" INTEGER, -- start_time is a microsecond precision unix timestamp
    "end_time" INTEGER, -- end_time is a microsecond precision unix timestamp
    "status_code" INTEGER,
    "status_description" TEXT,
    "attributes" TEXT,
    "dropped_attributes_count" INTEGER,
    "dropped_events_count" INTEGER,
    "dropped_links_count" INTEGER,
    "resource_attributes" TEXT,
    "resource_dropped_attributes_count" INTEGER,
    PRIMARY KEY ("span_id", "trace_id")
);

CREATE TABLE IF NOT EXISTS events(
    "span_id" BLOB,
    "timestamp" INTEGER, -- timestamp is a microsecond precision unix timestamp
    "name" TEXT,
    "attributes" TEXT,
    "dropped_attributes_count" INTEGER,
    FOREIGN KEY ("span_id") REFERENCES spans("span_id")
);

CREATE TABLE IF NOT EXISTS links(
    "parent_span_id" BLOB,
    "span_id" BLOB,
    "trace_id" BLOB,
    "tracestate" TEXT,
    "attributes" TEXT,
    "dropped_attributes_count" INTEGER
);
