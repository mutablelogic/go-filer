package filer

import (
	"context"
	"path/filepath"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	attribute "go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func Test_Registry_Metrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	dir := t.TempDir()
	r, err := New(context.Background(), WithMeter(provider.Meter("test")))
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	runRegistry(t, r)

	// The changes are counted, by their volumes and types
	events := watchAll(t, r, 100)
	received := map[string]int64{}
	for _, volume := range []string{"media", "photos"} {
		u := fileURL(t, dir, volume)
		_, err := r.Add(context.Background(), volume, u)
		require.NoError(t, err)
		waitFor(t, events, filepath.FromSlash(u.Path), func(event schema.Event) bool {
			received[event.Name+"/"+event.EventType.String()]++
			return event.Name == volume && event.EventType == schema.EventTypeCreated
		})
	}
	for len(events) > 0 {
		event := <-events
		received[event.Name+"/"+event.EventType.String()]++
	}

	// Each change which is received is counted, of its volume and type,
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	counted := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != metricEvents {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.True(t, sum.IsMonotonic)
			for _, point := range sum.DataPoints {
				volume, _ := point.Attributes.Value(attribute.Key(attrVolume))
				eventType, _ := point.Attributes.Value(attribute.Key(attrEventType))
				counted[volume.AsString()+"/"+eventType.AsString()] += point.Value
			}
		}
	}
	// as a change is counted before it's sent, so one which is being sent is
	// counted, but not received
	assert.GreaterOrEqual(t, counted["media/created"], int64(1))
	assert.GreaterOrEqual(t, counted["photos/created"], int64(1))
	for key, n := range received {
		assert.GreaterOrEqual(t, counted[key], n, key)
	}
}

func Test_Registry_NoMetrics(t *testing.T) {
	// Without a meter, there are no metrics, which aren't counted
	r, err := New(context.Background())
	require.NoError(t, err)
	defer r.Close()
	assert.Nil(t, r.metrics)
	r.metrics.event(context.Background(), schema.Event{})
}
