# sqliteexporter

Sqlite exporter for the OpenTelemetry Collector and OpenTelemetry Go.

Note that this is an alpha release, tables and column names may change in the
future. [golang-migrate](https://pkg.go.dev/github.com/golang-migrate/migrate/v4)
is used to manage updates to the database schema.

## Configuration Options

* `path` [no default]: Path to the Sqlite database file. If the file does not
  exist, it will be created on startup.

## Example

```yaml
exporters:
  sqlite:
    path: local.db
```

## Tables

Currently, this exporter creates 3 tables to store trace span data:

* `spans`: Each individual spans
* `events`: Span events, with a `span_id` to JOIN with the `spans` table
* `links`: Span links, with a `parent_span_id` to JOIN with the `spans` table

The Resource and Instrumentation Library are inlined in the `spans` table. This
creates some duplication but makes the schema much easier to navigate and query.
Attributes are inlined as JSON-encoded string and can be queried using Sqlite's
[JSON functions and operators](https://www.sqlite.org/json1.html).

## Note on JSONB data type

Sqlite recently added [support for the JSONB data type](https://sqlite.org/draft/jsonb.html)
which improves performance on JSON-encoded data. Support for this feature is
planned for the future.
