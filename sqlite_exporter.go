// Copyright 2024 William Perron. All rights reserved. MIT License.
package sqliteexporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"go.wperron.io/sqliteexporter/internal/transform"
)

// The goal is to have the sqliteexporter usable both in a collector deployment
// but also embedded in an app. As such, it must satisfy the component.Component
// interface and the sdktrace.SpanExporter interface.
var _ sdktrace.SpanExporter = &sqliteExporter{}
var _ component.Component = &sqliteExporter{}

type sqliteExporter struct {
	db *sql.DB

	// Logger that the factory can use during creation and can pass to the created
	// component to be used later as well.
	logger *zap.Logger
}

// DO NOT CHANGE: any modification will not be backwards compatible and
// must never be done outside of a new major release.
// ExportSpans exports a batch of spans.
//
// This function is called synchronously, so there is no concurrency
// safety requirement. However, due to the synchronous calling pattern,
// it is critical that all timeouts and cancellations contained in the
// passed context must be honored.
//
// Any retry logic must be contained in this function. The SDK that
// calls this function will not implement any retry logic. All errors
// returned by this function are considered unrecoverable and will be
// reported to a configured error Handler.
// DO NOT CHANGE: any modification will not be backwards compatible and
// must never be done outside of a new major release.
func (e *sqliteExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	ss := transform.Spans(spans)
	return e.ConsumeTraces(ctx, ss)
}

// Start tells the component to start. Host parameter can be used for communicating
// with the host after Start() has already returned. If an error is returned by
// Start() then the collector startup will be aborted.
// If this is an exporter component it may prepare for exporting
// by connecting to the endpoint.
//
// If the component needs to perform a long-running starting operation then it is recommended
// that Start() returns quickly and the long-running operation is performed in background.
// In that case make sure that the long-running operation does not use the context passed
// to Start() function since that context will be cancelled soon and can abort the long-running
// operation. Create a new context from the context.Background() for long-running operations.
func (e *sqliteExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Shutdown is invoked during service shutdown. After Shutdown() is called, if the component
// accepted data in any way, it should not accept it anymore.
//
// This method must be safe to call:
//   - without Start() having been called
//   - if the component is in a shutdown state already
//
// If there are any background operations running by the component they must be aborted before
// this function returns. Remember that if you started any long-running background operations from
// the Start() method, those operations must be also cancelled. If there are any buffers in the
// component, they should be cleared and the data sent immediately to the next component.
//
// The component's lifecycle is completed once the Shutdown() method returns. No other
// methods of the component are called after that. If necessary a new component with
// the same or different configuration may be created and started (this may happen
// for example if we want to restart the component).
func (e *sqliteExporter) Shutdown(ctx context.Context) error {
	return e.db.Close()
}

// TODO(wperron) add instrumentation library (scope) name and version
const insertSpanQ string = `INSERT INTO spans
(
    span_id,
    trace_id,
    parent_span_id,
    tracestate,
    __service_name,
    __duration,
    name,
    kind,
    start_time,
    end_time,
    status_code,
    status_description,
    attributes,
    dropped_attributes_count,
    dropped_events_count,
    dropped_links_count,
    resource_attributes,
    resource_dropped_attributes_count,
    instrumentation_library_name,
    instrumentation_library_version,
    instrumentation_library_attributes
)
VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, json(?), ?, ?, ?, json(?), ?, ?, ?, json(?)
);
`

const insertEventQ string = `INSERT INTO events
(
    span_id,
    timestamp,
    name,
    attributes,
    dropped_attributes_count
)
VALUES (
    ?, ?, ?, json(?), ?
);
`

const insertLinkQ string = `INSERT INTO links
(
    parent_span_id,
    span_id,
    trace_id,
    tracestate,
    attributes,
    dropped_attributes_count
)
VALUES (
    ?, ?, ?, ?, json(?), ?
)`

func (e *sqliteExporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Commit()

	e.db.Begin()

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		resource := traces.ResourceSpans().At(i)

		svc := "unknown"
		resource.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "service.name" {
				svc = v.Str()
				if svc == "" { // protect against service name being another type for some reason
					svc = "unknown"
				}
				return false
			}
			return true
		})

		rattrs, err := pcommonMapAsJSON(resource.Resource().Attributes())
		if err != nil {
			return fmt.Errorf("failed to marshal resource attributes as json: %w", err)
		}

		for j := 0; j < resource.ScopeSpans().Len(); j++ {
			scope := resource.ScopeSpans().At(j)
			sattrs, err := pcommonMapAsJSON(scope.Scope().Attributes())
			if err != nil {
				return fmt.Errorf("failed to marshal instrumentation scope attributes as json: %w", err)
			}

			for k := 0; k < scope.Spans().Len(); k++ {
				span := scope.Spans().At(k)

				dur := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime())

				stmt, err := tx.PrepareContext(ctx, insertSpanQ)
				if err != nil {
					return fmt.Errorf("failed to prepare span insert stmt: %w", err)
				}

				attrs, err := pcommonMapAsJSON(span.Attributes())
				if err != nil {
					return fmt.Errorf("failed to marshal attributes as json: %w", err)
				}

				spanidraw := [8]byte(span.SpanID())
				spanidbs := spanidraw[:]
				traceidraw := [16]byte(span.TraceID())
				traceidbs := traceidraw[:]
				parentidraw := [8]byte(span.ParentSpanID())
				parentidbs := parentidraw[:]
				if span.ParentSpanID().IsEmpty() {
					parentidbs = nil
				}

				_, err = stmt.ExecContext(ctx,
					spanidbs,
					traceidbs,
					parentidbs,
					span.TraceState().AsRaw(),
					svc,
					dur.Microseconds(),
					span.Name(),
					span.Kind().String(),
					// Use microsecond precision for start and timestamps
					unixMicro(span.StartTimestamp().AsTime()),
					unixMicro(span.EndTimestamp().AsTime()),
					span.Status().Code(),
					span.Status().Message(),
					attrs,
					span.DroppedAttributesCount(),
					span.DroppedEventsCount(),
					span.DroppedLinksCount(),
					rattrs,
					resource.Resource().DroppedAttributesCount(),
					scope.Scope().Name(),
					scope.Scope().Version(),
					sattrs,
				)
				if err != nil {
					return fmt.Errorf("error occured while inserting span: %w", err)
				}

				for l := 0; l < span.Events().Len(); l++ {
					event := span.Events().At(l)

					attrs, err := pcommonMapAsJSON(event.Attributes())
					if err != nil {
						return fmt.Errorf("failed to marshal event attributes as json: %w", err)
					}

					stmt, err := tx.PrepareContext(ctx, insertEventQ)
					if err != nil {
						return fmt.Errorf("failed to prepare event insert query: %w", err)
					}

					_, err = stmt.ExecContext(ctx,
						spanidbs,
						unixMicro(event.Timestamp().AsTime()),
						event.Name(),
						attrs,
						event.DroppedAttributesCount(),
					)
					if err != nil {
						return fmt.Errorf("error occured while inserting event: %w", err)
					}
				}

				for l := 0; l < span.Links().Len(); l++ {
					link := span.Links().At(l)

					attrs, err := pcommonMapAsJSON(link.Attributes())
					if err != nil {
						return fmt.Errorf("failed to marshal link attributes as json: %w", err)
					}

					stmt, err := tx.PrepareContext(ctx, insertLinkQ)
					if err != nil {
						return fmt.Errorf("failed to prepare link insert query: %w", err)
					}

					linkidraw := [8]byte(link.SpanID())
					linkidbs := linkidraw[:]
					linktraceraw := [16]byte(link.TraceID())
					linetracebs := linktraceraw[:]

					_, err = stmt.ExecContext(ctx,
						spanidbs,
						linkidbs,
						linetracebs,
						link.TraceState().AsRaw(),
						attrs,
						link.DroppedAttributesCount(),
					)
					if err != nil {
						return fmt.Errorf("error occured while inserting link: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func pcommonMapAsJSON(m pcommon.Map) ([]byte, error) {
	return json.Marshal(m.AsRaw())
}

func unixMicro(t time.Time) int64 {
	return t.UnixNano() / 1000
}
