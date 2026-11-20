package indexer

import (
	"context"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// span starts a span of the index's work, of its own trace, as work is done
// apart from what queued it, linked to the span which queued it, when it's
// known, and returns a function which ends it, with an error, or nil, so the
// store's spans of the work are of it, rather than traces of their own
func (self *Indexer) span(ctx context.Context, name string, link string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	if self.tracer == nil {
		return ctx, func(error) {}
	}
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithAttributes(append([]attribute.KeyValue{attribute.String("volume", self.name)}, attrs...)...),
	}
	if l, ok := store.Link(link); ok {
		opts = append(opts, trace.WithLinks(l))
	}
	ctx, span := self.tracer.Start(ctx, name, opts...)
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// workSpan starts the span of work of the queue, linked to what queued it
func (self *Indexer) workSpan(ctx context.Context, w store.Work) (context.Context, func(error)) {
	return self.span(ctx, "indexer."+string(w.Kind), w.Trace,
		attribute.String("key", w.Key),
		attribute.Int("attempts", int(w.Attempts)),
		attribute.Int("outages", int(w.Outages)),
	)
}
