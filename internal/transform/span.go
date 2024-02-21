// Copyright 2024 William Perron. All rights reserved. MIT license
package transform

import (
	"hash/fnv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func Spans(sdl []sdktrace.ReadOnlySpan) ptrace.Traces {
	traces := ptrace.NewTraces()
	rss := traces.ResourceSpans()
	resMap := make(map[uint64]ptrace.ResourceSpans)
	scopeMap := make(map[uint64]ptrace.ScopeSpans)

	for _, s := range sdl {
		var rs ptrace.ResourceSpans
		if r, ok := resMap[hashResource(s.Resource())]; ok {
			rs = r
		} else {
			// create a new resource
			// append it to the traces
			// add it to the map
			rs = rss.AppendEmpty()
			resMap[hashResource(s.Resource())] = rs
		}

		res := rs.Resource()
		res.SetDroppedAttributesCount(0) // TODO(wperron) how can we get this number?
		ra := transformAttributes(s.Resource().Attributes())
		ra.CopyTo(res.Attributes())

		var ss ptrace.ScopeSpans
		if scope, ok := scopeMap[hashScope(s.InstrumentationScope())]; ok {
			ss = scope
		} else {
			// create a new scope
			// append it to the resource
			// add it to the map
			ss = rs.ScopeSpans().AppendEmpty()
			scopeMap[hashScope(s.InstrumentationScope())] = ss
		}

		// create a new span and fill it with the info from the readonly span
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(pcommon.TraceID(s.SpanContext().TraceID()))
		span.SetSpanID(pcommon.SpanID(s.SpanContext().SpanID()))
		span.SetParentSpanID(pcommon.SpanID(s.Parent().SpanID()))
		span.TraceState().FromRaw(s.SpanContext().TraceState().String())
		span.SetName(s.Name())
		span.SetKind(ptrace.SpanKind(s.SpanKind()))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(s.StartTime()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(s.EndTime()))
		var code ptrace.StatusCode
		switch s.Status().Code {
		case codes.Unset:
			code = ptrace.StatusCodeUnset
		case codes.Ok:
			code = ptrace.StatusCodeOk
		case codes.Error:
			code = ptrace.StatusCodeError
		default:
			panic("unreachable")
		}
		span.Status().SetCode(code)
		span.Status().SetMessage(s.Status().Description)
		span.SetDroppedAttributesCount(uint32(s.DroppedAttributes()))
		span.SetDroppedEventsCount(uint32(s.DroppedEvents()))
		span.SetDroppedLinksCount(uint32(s.DroppedLinks()))

		a := transformAttributes(s.Attributes())
		a.CopyTo(span.Attributes())

		for _, e := range s.Events() {
			ev := span.Events().AppendEmpty()
			ev.SetTimestamp(pcommon.NewTimestampFromTime(e.Time))
			ev.SetName(e.Name)
			ev.SetDroppedAttributesCount(uint32(e.DroppedAttributeCount))
			ea := transformAttributes(e.Attributes)
			ea.CopyTo(ev.Attributes())
		}

		for _, l := range s.Links() {
			ln := span.Links().AppendEmpty()
			ln.SetTraceID(pcommon.TraceID(l.SpanContext.TraceID()))
			ln.SetSpanID(pcommon.SpanID(l.SpanContext.SpanID()))
			ln.TraceState().FromRaw(l.SpanContext.TraceState().String())
			ln.SetDroppedAttributesCount(uint32(l.DroppedAttributeCount))
			la := transformAttributes(l.Attributes)
			la.CopyTo(ln.Attributes())
		}
	}

	return traces
}

func transformAttributes(from []attribute.KeyValue) pcommon.Map {
	to := pcommon.NewMap()
	to.EnsureCapacity(len(from))

	for _, a := range from {
		switch a.Value.Type() {
		case attribute.BOOL:
			to.PutBool(string(a.Key), a.Value.AsBool())
		case attribute.INT64:
			to.PutInt(string(a.Key), a.Value.AsInt64())
		case attribute.FLOAT64:
			to.PutDouble(string(a.Key), a.Value.AsFloat64())
		case attribute.STRING:
			to.PutStr(string(a.Key), a.Value.AsString())
		case attribute.BOOLSLICE:
			s := to.PutEmptySlice(string(a.Key))
			raw := a.Value.AsBoolSlice()
			s.EnsureCapacity(len(raw))
			for _, r := range raw {
				v := s.AppendEmpty()
				v.SetBool(r)
			}
		case attribute.INT64SLICE:
			s := to.PutEmptySlice(string(a.Key))
			raw := a.Value.AsInt64Slice()
			s.EnsureCapacity(len(raw))
			for _, r := range raw {
				v := s.AppendEmpty()
				v.SetInt(r)
			}
		case attribute.STRINGSLICE:
			s := to.PutEmptySlice(string(a.Key))
			raw := a.Value.AsStringSlice()
			s.EnsureCapacity(len(raw))
			for _, r := range raw {
				v := s.AppendEmpty()
				v.SetStr(r)
			}
		case attribute.FLOAT64SLICE:
			s := to.PutEmptySlice(string(a.Key))
			raw := a.Value.AsFloat64Slice()
			s.EnsureCapacity(len(raw))
			for _, r := range raw {
				v := s.AppendEmpty()
				v.SetDouble(r)
			}
		}
	}

	return to
}

func hashResource(res *resource.Resource) uint64 {
	h := fnv.New64a()
	h.Write([]byte(res.Encoded(attribute.DefaultEncoder())))
	return h.Sum64()
}

func hashScope(s instrumentation.Scope) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s.Name))
	h.Write([]byte(s.SchemaURL))
	h.Write([]byte(s.Version))
	return h.Sum64()
}
