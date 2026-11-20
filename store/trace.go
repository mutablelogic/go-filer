package store

import (
	"context"

	// Packages
	propagation "go.opentelemetry.io/otel/propagation"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Traceparent returns the W3C traceparent of the context's span, which work
// which is queued with it keeps, so its work can be linked to it, or empty
// when it isn't traced
func Traceparent(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Link returns the link to the span of a W3C traceparent, which work which
// is done is linked to, so it's known what queued it, or false when it's
// empty, or isn't valid
func Link(traceparent string) (trace.Link, bool) {
	if traceparent == "" {
		return trace.Link{}, false
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	span := trace.SpanContextFromContext(ctx)
	if !span.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: span}, true
}
