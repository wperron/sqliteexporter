// Copyright 2024 William Perron. All rights reserved. MIT License.
package sqliteexporter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func Test_ExporterExportSpan(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	// manually build the exporter so we can inspect the database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	err = doMigrate(db)
	require.NoError(t, err)

	ex := sqliteExporter{db: db}

	// build the trace
	testTrace := ptrace.NewTraces()
	rs := testTrace.ResourceSpans().AppendEmpty()
	r := rs.Resource()
	r.Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test-scope")
	ss.Scope().SetVersion("v0.1.0")
	ss.Scope().Attributes().PutStr("scope.stringkey", "scope.stringval")

	// define a first server span
	span1 := ss.Spans().AppendEmpty()
	span1.SetTraceID(pcommon.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	span1.SetSpanID(pcommon.SpanID{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01})
	ts := span1.TraceState()
	ts.FromRaw("rojo=00f067aa0ba902b7")
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindServer)
	span1.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-5 * time.Millisecond)))
	span1.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	span1.Status().SetCode(ptrace.StatusCodeOk)
	span1.Status().SetMessage(ptrace.StatusCodeOk.String())
	span1.Attributes().PutStr("http.method", "GET")
	span1.Attributes().PutInt("http.status_code", 200)
	span1event1 := span1.Events().AppendEmpty()
	span1event1.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-3 * time.Millisecond)))
	span1event1.SetName("this_happened")
	span1event1.Attributes().PutStr("value", "example")
	span1event2 := span1.Events().AppendEmpty()
	span1event2.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-4 * time.Millisecond)))
	span1event2.SetName("that_happened")
	span1event2.Attributes().PutStr("value", "example")

	// define an internal child span for span1
	span2 := ss.Spans().AppendEmpty()
	span2.SetTraceID(pcommon.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	span2.SetSpanID(pcommon.SpanID{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x02})
	span2.SetParentSpanID(pcommon.SpanID{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01})
	span2.SetName("span2")
	span2.SetKind(ptrace.SpanKindInternal)
	span2.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-4 * time.Millisecond)))
	span2.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(-1 * time.Millisecond)))
	span2.Status().SetCode(ptrace.StatusCodeOk)
	span2.Status().SetMessage(ptrace.StatusCodeOk.String())
	span2.Attributes().PutStr("custom.key", "custom-value")
	span2event1 := span2.Events().AppendEmpty()
	span2event1.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-3 * time.Millisecond)))
	span2event1.SetName("this_happened")
	span2event1.Attributes().PutStr("value", "example")
	span2event2 := span2.Events().AppendEmpty()
	span2event2.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-4 * time.Millisecond)))
	span2event2.SetName("that_happened")
	span2event2.Attributes().PutStr("value", "example")
	span2link1 := span2.Links().AppendEmpty()
	span2link1.SetSpanID(span1.SpanID())
	span2link1.SetTraceID(span1.TraceID())
	span2link1.Attributes().PutStr("relation", "follows_from")

	err = ex.ConsumeTraces(ctx, testTrace)
	require.Nil(t, err)

	row := db.QueryRow("select count(1) from spans;")
	require.NoError(t, err)

	var total int
	err = row.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 2, total)

	rows, err := db.Query(`select
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
		resource_attributes,
		resource_dropped_attributes_count,
		instrumentation_library_name,
		instrumentation_library_version,
		instrumentation_library_attributes
	from spans order by name asc;`)
	require.NoError(t, err)

	ok := rows.Next()
	require.True(t, ok)

	var rspanId, rtraceId, rparentSpanId sql.RawBytes
	var tracestate, serviceName, name, kind, statusDescription, attributes, resourceAttributes, instrumentationLibName, instrumentationLibVer, instrumentationLibAttributes string
	var duration, startTime, endTime, statusCode, droppedAttributesCount, resourceDroppedAttributesCount int
	err = rows.Scan(
		&rspanId,
		&rtraceId,
		&rparentSpanId,
		&tracestate,
		&serviceName,
		&duration,
		&name,
		&kind,
		&startTime,
		&endTime,
		&statusCode,
		&statusDescription,
		&attributes,
		&droppedAttributesCount,
		&resourceAttributes,
		&resourceDroppedAttributesCount,
		&instrumentationLibName,
		&instrumentationLibVer,
		&instrumentationLibAttributes,
	)
	require.NoError(t, err)
	assert.Equal(t, sql.RawBytes([]byte{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01}), rspanId)
	assert.Equal(t, sql.RawBytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}), rtraceId)
	assert.Nil(t, rparentSpanId)
	assert.Equal(t, "rojo=00f067aa0ba902b7", tracestate)
	assert.Equal(t, "test-service", serviceName)
	assert.Equal(t, 5000, duration)
	assert.Equal(t, "span1", name)
	assert.Equal(t, ptrace.SpanKindServer.String(), kind)
	assert.Equal(t, unixMicro(now.Add(-5*time.Millisecond)), int64(startTime))
	assert.Equal(t, unixMicro(now), int64(endTime))
	assert.Equal(t, int(ptrace.StatusCodeOk), statusCode)
	assert.Equal(t, ptrace.StatusCodeOk.String(), statusDescription)
	assert.Equal(t, "{\"http.method\":\"GET\",\"http.status_code\":200}", attributes)
	assert.Equal(t, 0, droppedAttributesCount)
	assert.Equal(t, "{\"service.name\":\"test-service\"}", resourceAttributes)
	assert.Equal(t, 0, resourceDroppedAttributesCount)
	assert.Equal(t, "test-scope", instrumentationLibName)
	assert.Equal(t, "v0.1.0", instrumentationLibVer)
	assert.Equal(t, "{\"scope.stringkey\":\"scope.stringval\"}", instrumentationLibAttributes)

	ok = rows.Next()
	require.True(t, ok)

	err = rows.Scan(
		&rspanId,
		&rtraceId,
		&rparentSpanId,
		&tracestate,
		&serviceName,
		&duration,
		&name,
		&kind,
		&startTime,
		&endTime,
		&statusCode,
		&statusDescription,
		&attributes,
		&droppedAttributesCount,
		&resourceAttributes,
		&resourceDroppedAttributesCount,
		&instrumentationLibName,
		&instrumentationLibVer,
		&instrumentationLibAttributes,
	)
	require.NoError(t, err)
	assert.Equal(t, sql.RawBytes([]byte{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x02}), rspanId)
	assert.Equal(t, sql.RawBytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}), rtraceId)
	assert.Equal(t, sql.RawBytes([]byte{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01}), rparentSpanId)
	assert.Equal(t, "", tracestate)
	assert.Equal(t, "test-service", serviceName)
	assert.Equal(t, 3000, duration)
	assert.Equal(t, "span2", name)
	assert.Equal(t, ptrace.SpanKindInternal.String(), kind)
	assert.Equal(t, unixMicro(now.Add(-4*time.Millisecond)), int64(startTime))
	assert.Equal(t, unixMicro(now.Add(-1*time.Millisecond)), int64(endTime))
	assert.Equal(t, int(ptrace.StatusCodeOk), statusCode)
	assert.Equal(t, ptrace.StatusCodeOk.String(), statusDescription)
	assert.Equal(t, "{\"custom.key\":\"custom-value\"}", attributes)
	assert.Equal(t, 0, droppedAttributesCount)
	assert.Equal(t, "{\"service.name\":\"test-service\"}", resourceAttributes)
	assert.Equal(t, 0, resourceDroppedAttributesCount)
	assert.Equal(t, "test-scope", instrumentationLibName)
	assert.Equal(t, "v0.1.0", instrumentationLibVer)
	assert.Equal(t, "{\"scope.stringkey\":\"scope.stringval\"}", instrumentationLibAttributes)

	assert.False(t, rows.Next())

	row = db.QueryRow("select count(1) from events;")
	require.NoError(t, err)

	err = row.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 4, total)

	row = db.QueryRow("select count(1) from links;")
	require.NoError(t, err)

	err = row.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
}

func Test_pcommonMapAsJSON(t *testing.T) {
	tests := []struct {
		name    string
		args    pcommon.Map
		want    []byte
		wantErr bool
	}{
		{
			name:    "empty",
			args:    pcommon.NewMap(),
			want:    []byte("{}"),
			wantErr: false,
		},
		{
			name: "simple",
			args: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("http.method", "GET")
				m.PutInt("http.status_code", 200)
				return m
			}(),
			want:    []byte("{\"http.method\":\"GET\",\"http.status_code\":200}"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pcommonMapAsJSON(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("pcommonMapAsJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("pcommonMapAsJSON() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_ExporterExportSpanFilesystem(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	f := "tmp-test.db"
	defer func() {
		if err := os.Remove(f); err != nil {
			t.Log(err)
			t.Fail()
		}
	}()

	// manually build the exporter so we can inspect the database
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&_journal_mode=wal", f))
	require.NoError(t, err)

	err = doMigrate(db)
	require.NoError(t, err)

	ex := sqliteExporter{db: db}

	// build the trace
	testTrace := ptrace.NewTraces()
	rs := testTrace.ResourceSpans().AppendEmpty()
	r := rs.Resource()
	r.Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test-scope")
	ss.Scope().SetVersion("v0.1.0")
	ss.Scope().Attributes().PutStr("scope.stringkey", "scope.stringval")

	// define a first server span
	span1 := ss.Spans().AppendEmpty()
	span1.SetTraceID(pcommon.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	span1.SetSpanID(pcommon.SpanID{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01})
	ts := span1.TraceState()
	ts.FromRaw("rojo=00f067aa0ba902b7")
	span1.SetName("span1")
	span1.SetKind(ptrace.SpanKindServer)
	span1.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-5 * time.Millisecond)))
	span1.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	span1.Status().SetCode(ptrace.StatusCodeOk)
	span1.Status().SetMessage(ptrace.StatusCodeOk.String())
	span1.Attributes().PutStr("http.method", "GET")
	span1.Attributes().PutInt("http.status_code", 200)
	span1event1 := span1.Events().AppendEmpty()
	span1event1.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-3 * time.Millisecond)))
	span1event1.SetName("this_happened")
	span1event1.Attributes().PutStr("value", "example")
	span1event2 := span1.Events().AppendEmpty()
	span1event2.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-4 * time.Millisecond)))
	span1event2.SetName("that_happened")
	span1event2.Attributes().PutStr("value", "example")

	// define an internal child span for span1
	span2 := ss.Spans().AppendEmpty()
	span2.SetTraceID(pcommon.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	span2.SetSpanID(pcommon.SpanID{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x02})
	span2.SetParentSpanID(pcommon.SpanID{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01})
	span2.SetName("span2")
	span2.SetKind(ptrace.SpanKindInternal)
	span2.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-4 * time.Millisecond)))
	span2.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(-1 * time.Millisecond)))
	span2.Status().SetCode(ptrace.StatusCodeOk)
	span2.Status().SetMessage(ptrace.StatusCodeOk.String())
	span2.Attributes().PutStr("custom.key", "custom-value")
	span2event1 := span2.Events().AppendEmpty()
	span2event1.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-3 * time.Millisecond)))
	span2event1.SetName("this_happened")
	span2event1.Attributes().PutStr("value", "example")
	span2event2 := span2.Events().AppendEmpty()
	span2event2.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-4 * time.Millisecond)))
	span2event2.SetName("that_happened")
	span2event2.Attributes().PutStr("value", "example")
	span2link1 := span2.Links().AppendEmpty()
	span2link1.SetSpanID(span1.SpanID())
	span2link1.SetTraceID(span1.TraceID())
	span2link1.Attributes().PutStr("relation", "follows_from")

	err = ex.ConsumeTraces(ctx, testTrace)
	require.Nil(t, err)

	row := db.QueryRow("select count(1) from spans;")
	require.NoError(t, err)

	var total int
	err = row.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 2, total)

	rows, err := db.Query(`select
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
		resource_attributes,
		resource_dropped_attributes_count,
		instrumentation_library_name,
		instrumentation_library_version,
		instrumentation_library_attributes
	from spans order by name asc;`)
	require.NoError(t, err)

	ok := rows.Next()
	require.True(t, ok)

	var rspanId, rtraceId, rparentSpanId sql.RawBytes
	var tracestate, serviceName, name, kind, statusDescription, attributes, resourceAttributes, instrumentationLibName, instrumentationLibVer, instrumentationLibAttributes string
	var duration, startTime, endTime, statusCode, droppedAttributesCount, resourceDroppedAttributesCount int
	err = rows.Scan(
		&rspanId,
		&rtraceId,
		&rparentSpanId,
		&tracestate,
		&serviceName,
		&duration,
		&name,
		&kind,
		&startTime,
		&endTime,
		&statusCode,
		&statusDescription,
		&attributes,
		&droppedAttributesCount,
		&resourceAttributes,
		&resourceDroppedAttributesCount,
		&instrumentationLibName,
		&instrumentationLibVer,
		&instrumentationLibAttributes,
	)
	require.NoError(t, err)
	assert.Equal(t, sql.RawBytes([]byte{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01}), rspanId)
	assert.Equal(t, sql.RawBytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}), rtraceId)
	assert.Nil(t, rparentSpanId)
	assert.Equal(t, "rojo=00f067aa0ba902b7", tracestate)
	assert.Equal(t, "test-service", serviceName)
	assert.Equal(t, 5000, duration)
	assert.Equal(t, "span1", name)
	assert.Equal(t, ptrace.SpanKindServer.String(), kind)
	assert.Equal(t, unixMicro(now.Add(-5*time.Millisecond)), int64(startTime))
	assert.Equal(t, unixMicro(now), int64(endTime))
	assert.Equal(t, int(ptrace.StatusCodeOk), statusCode)
	assert.Equal(t, ptrace.StatusCodeOk.String(), statusDescription)
	assert.Equal(t, "{\"http.method\":\"GET\",\"http.status_code\":200}", attributes)
	assert.Equal(t, 0, droppedAttributesCount)
	assert.Equal(t, "{\"service.name\":\"test-service\"}", resourceAttributes)
	assert.Equal(t, 0, resourceDroppedAttributesCount)
	assert.Equal(t, "test-scope", instrumentationLibName)
	assert.Equal(t, "v0.1.0", instrumentationLibVer)
	assert.Equal(t, "{\"scope.stringkey\":\"scope.stringval\"}", instrumentationLibAttributes)

	ok = rows.Next()
	require.True(t, ok)

	err = rows.Scan(
		&rspanId,
		&rtraceId,
		&rparentSpanId,
		&tracestate,
		&serviceName,
		&duration,
		&name,
		&kind,
		&startTime,
		&endTime,
		&statusCode,
		&statusDescription,
		&attributes,
		&droppedAttributesCount,
		&resourceAttributes,
		&resourceDroppedAttributesCount,
		&instrumentationLibName,
		&instrumentationLibVer,
		&instrumentationLibAttributes,
	)
	require.NoError(t, err)
	assert.Equal(t, sql.RawBytes([]byte{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x02}), rspanId)
	assert.Equal(t, sql.RawBytes([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}), rtraceId)
	assert.Equal(t, sql.RawBytes([]byte{0xee, 0xbc, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01}), rparentSpanId)
	assert.Equal(t, "", tracestate)
	assert.Equal(t, "test-service", serviceName)
	assert.Equal(t, 3000, duration)
	assert.Equal(t, "span2", name)
	assert.Equal(t, ptrace.SpanKindInternal.String(), kind)
	assert.Equal(t, unixMicro(now.Add(-4*time.Millisecond)), int64(startTime))
	assert.Equal(t, unixMicro(now.Add(-1*time.Millisecond)), int64(endTime))
	assert.Equal(t, int(ptrace.StatusCodeOk), statusCode)
	assert.Equal(t, ptrace.StatusCodeOk.String(), statusDescription)
	assert.Equal(t, "{\"custom.key\":\"custom-value\"}", attributes)
	assert.Equal(t, 0, droppedAttributesCount)
	assert.Equal(t, "{\"service.name\":\"test-service\"}", resourceAttributes)
	assert.Equal(t, 0, resourceDroppedAttributesCount)
	assert.Equal(t, "test-scope", instrumentationLibName)
	assert.Equal(t, "v0.1.0", instrumentationLibVer)
	assert.Equal(t, "{\"scope.stringkey\":\"scope.stringval\"}", instrumentationLibAttributes)

	assert.False(t, rows.Next())

	row = db.QueryRow("select count(1) from events;")
	require.NoError(t, err)

	err = row.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 4, total)

	row = db.QueryRow("select count(1) from links;")
	require.NoError(t, err)

	err = row.Scan(&total)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
}
