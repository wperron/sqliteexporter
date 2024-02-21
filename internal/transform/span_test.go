// Copyright 2024 William Perron. All rights reserved. MIT license
package transform

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTransformSpan(t *testing.T) {
	sdkspanslice := make([]sdktrace.ReadOnlySpan, 1)
	start := time.Unix(1000, 0)
	end := start.Add(5 * time.Second)
	evtts := time.Unix(1500, 0)

	stringVals := []string{"first", "second"}
	intVals := []int{1, 2, 3}
	floatVals := []float64{1.1, 2.2, 3.3}
	boolVals := []bool{true, false}

	sdkspanslice[0] = tracetest.SpanStub{
		Name: "span-stub",
		SpanContext: trace.SpanContext{}.
			WithSpanID(trace.SpanID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}).
			WithTraceID(trace.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}).
			WithTraceState(must(trace.ParseTraceState("foo=bar"))),
		Parent: trace.SpanContext{}.
			WithSpanID(trace.SpanID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11}).
			WithTraceID(trace.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}).
			WithTraceState(must(trace.ParseTraceState("foo=bar"))),
		SpanKind:  trace.SpanKindServer,
		StartTime: start,
		EndTime:   end,
		Attributes: []attribute.KeyValue{
			{
				Key:   "stringkey",
				Value: attribute.StringValue("stringval"),
			},
			{
				Key:   "intkey",
				Value: attribute.IntValue(123),
			},
			{
				Key:   "floatkey",
				Value: attribute.Float64Value(111.2),
			},
			{
				Key:   "boolkey",
				Value: attribute.BoolValue(true),
			},
			{
				Key:   "stringslicekey",
				Value: attribute.StringSliceValue(stringVals),
			},
			{
				Key:   "intslicekey",
				Value: attribute.IntSliceValue(intVals),
			},
			{
				Key:   "floatslicekey",
				Value: attribute.Float64SliceValue(floatVals),
			},
			{
				Key:   "boolslicekey",
				Value: attribute.BoolSliceValue(boolVals),
			},
		},
		Events: []sdktrace.Event{
			{
				Name: "spanevent",
				Attributes: []attribute.KeyValue{
					{
						Key:   "eventstringkey",
						Value: attribute.StringValue("eventstringval"),
					},
				},
				DroppedAttributeCount: 4,
				Time:                  evtts,
			},
		},
		Links: []sdktrace.Link{
			{
				SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
					SpanID:     [8]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
					TraceFlags: 1,
					TraceState: must(trace.ParseTraceState("foo=bar")),
					Remote:     false,
				}),
				Attributes: []attribute.KeyValue{
					{
						Key:   "linkstringkey",
						Value: attribute.StringValue("linkstringval"),
					},
				},
				DroppedAttributeCount: 5,
			},
		},
		Status: sdktrace.Status{
			Code:        codes.Ok,
			Description: "OK",
		},
		DroppedAttributes: 1,
		DroppedEvents:     2,
		DroppedLinks:      3,
		ChildSpanCount:    0,
		Resource: resource.NewWithAttributes("https://opentelemetry.io/schemas/1.24.0",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("test-service")},
		),
		InstrumentationLibrary: instrumentation.Scope{
			Name:      "test-tracer",
			Version:   "0.0.1",
			SchemaURL: "https://opentelemetry.io/schemas/1.24.0",
		},
	}.Snapshot()

	spans := Spans(sdkspanslice)
	assert.Equal(t, 1, spans.ResourceSpans().Len())

	for i := 0; i < spans.ResourceSpans().Len(); i++ {
		require.Less(t, i, 1)
		rs := spans.ResourceSpans().At(i)
		res := rs.Resource()
		exp := pcommon.NewMap()
		exp.PutStr("service.name", "test-service")
		assert.Equal(t, exp, res.Attributes())
		assert.Equal(t, uint32(0), res.DroppedAttributesCount())

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			require.Less(t, j, 1)
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				require.Less(t, k, 1)
				span := ss.Spans().At(k)

				ts := pcommon.NewTraceState()
				ts.FromRaw("foo=bar")
				assert.Equal(t, pcommon.SpanID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, span.SpanID())
				assert.Equal(t, pcommon.TraceID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, span.TraceID())
				assert.Equal(t, pcommon.SpanID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11}, span.ParentSpanID())
				assert.Equal(t, ts, span.TraceState())
				assert.Equal(t, "span-stub", span.Name())
				assert.Equal(t, ptrace.SpanKindServer, span.Kind())
				assert.Equal(t, ptrace.StatusCodeOk, span.Status().Code())
				assert.Equal(t, "OK", span.Status().Message())
				assert.Equal(t, pcommon.NewTimestampFromTime(start), span.StartTimestamp())
				assert.Equal(t, pcommon.NewTimestampFromTime(end), span.EndTimestamp())
				assert.Equal(t, uint32(1), span.DroppedAttributesCount())
				assert.Equal(t, uint32(2), span.DroppedEventsCount())
				assert.Equal(t, uint32(3), span.DroppedLinksCount())

				a, ok := span.Attributes().Get("stringkey")
				assert.True(t, ok)
				assert.Equal(t, "stringval", a.Str())

				a, ok = span.Attributes().Get("intkey")
				assert.True(t, ok)
				assert.Equal(t, int64(123), a.Int())

				a, ok = span.Attributes().Get("floatkey")
				assert.True(t, ok)
				assert.Equal(t, float64(111.2), a.Double())

				a, ok = span.Attributes().Get("boolkey")
				assert.True(t, ok)
				assert.Equal(t, true, a.Bool())

				a, ok = span.Attributes().Get("stringslicekey")
				assert.True(t, ok)
				for i := 0; i < a.Slice().Len(); i++ {
					v := a.Slice().At(i)
					assert.Equal(t, stringVals[i], v.Str())
				}

				a, ok = span.Attributes().Get("intslicekey")
				assert.True(t, ok)
				for i := 0; i < a.Slice().Len(); i++ {
					v := a.Slice().At(i)
					assert.Equal(t, int64(intVals[i]), v.Int())
				}

				a, ok = span.Attributes().Get("floatslicekey")
				assert.True(t, ok)
				for i := 0; i < a.Slice().Len(); i++ {
					v := a.Slice().At(i)
					assert.Equal(t, floatVals[i], v.Double())
				}

				a, ok = span.Attributes().Get("boolslicekey")
				assert.True(t, ok)
				for i := 0; i < a.Slice().Len(); i++ {
					v := a.Slice().At(i)
					assert.Equal(t, boolVals[i], v.Bool())
				}

				assert.Equal(t, 1, span.Events().Len())
				ev := span.Events().At(0)
				assert.Equal(t, evtts.UTC(), ev.Timestamp().AsTime())
				assert.Equal(t, "spanevent", ev.Name())
				assert.Equal(t, uint32(4), ev.DroppedAttributesCount())
				a, ok = ev.Attributes().Get("eventstringkey")
				assert.True(t, ok)
				assert.Equal(t, "eventstringval", a.Str())

				assert.Equal(t, 1, span.Links().Len())
				ln := span.Links().At(0)
				assert.Equal(t, pcommon.TraceID([16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}), ln.TraceID())
				assert.Equal(t, pcommon.SpanID([8]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}), ln.SpanID())
				assert.Equal(t, ts, ln.TraceState())
				assert.Equal(t, uint32(5), ln.DroppedAttributesCount())
				a, ok = ln.Attributes().Get("linkstringkey")
				assert.True(t, ok)
				assert.Equal(t, "linkstringval", a.Str())
			}
		}
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
