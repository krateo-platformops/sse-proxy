// OTLP metrics for the SSE proxy.
//
// Three instruments, all on the global MeterProvider (which is a no-op unless
// OTEL_METRICS_ENABLED is truthy and setupTelemetry installed a real one):
//
//   - sse_proxy_connected_clients (observable gauge) — the live count of
//     connected SSE clients, read on each collection from hub.count().
//   - sse_proxy_events_delivered_total (counter) — SSE messages broadcast to
//     clients (incremented per event the poller fans out).
//   - sse_proxy_poll_errors_total (counter) — ClickHouse poll failures.
//
// initMetrics is safe to call unconditionally: against the no-op global meter
// the instruments are cheap no-ops, so the metrics path adds no behaviour when
// the feature is off.
package main

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/krateo-platformops/sse-proxy"

type proxyMetrics struct {
	eventsDelivered metric.Int64Counter
	pollErrors      metric.Int64Counter
}

// initMetrics registers the instruments on the global MeterProvider. The
// connected-clients gauge is registered as an async observable backed by
// hub.count(); the returned proxyMetrics carries the two counters.
func initMetrics(h *hub) (*proxyMetrics, error) {
	meter := otel.GetMeterProvider().Meter(meterName)

	eventsDelivered, err := meter.Int64Counter(
		"sse_proxy_events_delivered_total",
		metric.WithDescription("Total SSE events broadcast to connected clients."),
	)
	if err != nil {
		return nil, err
	}

	pollErrors, err := meter.Int64Counter(
		"sse_proxy_poll_errors_total",
		metric.WithDescription("Total ClickHouse poll errors."),
	)
	if err != nil {
		return nil, err
	}

	connectedClients, err := meter.Int64ObservableGauge(
		"sse_proxy_connected_clients",
		metric.WithDescription("Number of currently connected SSE clients."),
	)
	if err != nil {
		return nil, err
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(connectedClients, int64(h.count()))
			return nil
		},
		connectedClients,
	)
	if err != nil {
		return nil, err
	}

	return &proxyMetrics{
		eventsDelivered: eventsDelivered,
		pollErrors:      pollErrors,
	}, nil
}

// addEventsDelivered records n delivered SSE events. Nil-safe.
func (m *proxyMetrics) addEventsDelivered(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.eventsDelivered.Add(ctx, n)
}

// incPollError records a single poll error. Nil-safe.
func (m *proxyMetrics) incPollError(ctx context.Context) {
	if m == nil {
		return
	}
	m.pollErrors.Add(ctx, 1)
}
