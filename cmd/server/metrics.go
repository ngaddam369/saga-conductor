package main

import (
	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/prometheus/client_golang/prometheus"
)

// prometheusRecorder implements engine.Recorder using prometheus counters and
// histograms registered against the provided Registerer.
type prometheusRecorder struct {
	sagaTotal    *prometheus.CounterVec
	sagaDuration prometheus.Histogram
	stepTotal    *prometheus.CounterVec
	stepDuration prometheus.Histogram
}

// newPrometheusRecorder creates and registers the saga-conductor metrics
// against reg. Pass prometheus.DefaultRegisterer for production.
func newPrometheusRecorder(reg prometheus.Registerer) *prometheusRecorder {
	sagaTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_conductor_saga_total",
		Help: "Total number of sagas that reached a terminal state, by status.",
	}, []string{"status"})

	sagaDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "saga_conductor_saga_duration_seconds",
		Help:    "Wall-clock duration of sagas from RUNNING to terminal state.",
		Buckets: prometheus.DefBuckets,
	})

	stepTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "saga_conductor_step_total",
		Help: "Total number of steps that reached a terminal state, by status.",
	}, []string{"status"})

	stepDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "saga_conductor_step_duration_seconds",
		Help:    "Wall-clock duration of steps from RUNNING to terminal state.",
		Buckets: prometheus.DefBuckets,
	})

	reg.MustRegister(sagaTotal, sagaDuration, stepTotal, stepDuration)

	return &prometheusRecorder{
		sagaTotal:    sagaTotal,
		sagaDuration: sagaDuration,
		stepTotal:    stepTotal,
		stepDuration: stepDuration,
	}
}

func (r *prometheusRecorder) RecordSaga(status saga.SagaStatus, durationSecs float64) {
	r.sagaTotal.WithLabelValues(string(status)).Inc()
	if durationSecs > 0 {
		r.sagaDuration.Observe(durationSecs)
	}
}

func (r *prometheusRecorder) RecordStep(status saga.StepStatus, durationSecs float64) {
	r.stepTotal.WithLabelValues(string(status)).Inc()
	if durationSecs > 0 {
		r.stepDuration.Observe(durationSecs)
	}
}
