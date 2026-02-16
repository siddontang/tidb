// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics labels for service framework.
const (
	LabelService = "service"
	LabelMethod  = "method"
	LabelResult  = "result"
	LabelMode    = "mode"
)

// Metrics values for service results.
const (
	ResultSuccess = "success"
	ResultFailure = "failure"
)

// Metrics values for service modes.
const (
	ModeMonolithic  = "monolithic"
	ModeDistributed = "distributed"
)

// Metrics for the service framework.
var (
	// ServiceHealthGauge tracks the health status of each service.
	// Labels: service
	// Values: 1 = healthy, 0 = unhealthy, -1 = stopped
	ServiceHealthGauge *prometheus.GaugeVec

	// ServiceStartTotal counts service starts.
	// Labels: service, result
	ServiceStartTotal *prometheus.CounterVec

	// ServiceStopTotal counts service stops.
	// Labels: service, result
	ServiceStopTotal *prometheus.CounterVec

	// ServiceInitDuration measures service initialization duration.
	// Labels: service
	ServiceInitDuration *prometheus.HistogramVec

	// ServiceOperationDuration measures service operation duration.
	// Labels: service, method
	ServiceOperationDuration *prometheus.HistogramVec

	// ServiceGRPCRequestTotal counts gRPC requests to services.
	// Labels: service, method, result
	ServiceGRPCRequestTotal *prometheus.CounterVec

	// ServiceGRPCRequestDuration measures gRPC request duration.
	// Labels: service, method
	ServiceGRPCRequestDuration *prometheus.HistogramVec

	// ServiceRegistryGauge tracks registered services.
	// Labels: mode (monolithic/distributed)
	ServiceRegistryGauge *prometheus.GaugeVec

	// ServiceDependencyResolutionDuration measures dependency resolution time.
	ServiceDependencyResolutionDuration prometheus.Histogram
)

// InitServiceMetrics initializes service framework metrics.
func InitServiceMetrics() {
	ServiceHealthGauge = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "health_status",
			Help:      "Health status of services (1=healthy, 0=unhealthy, -1=stopped).",
		},
		[]string{LabelService},
	)

	ServiceStartTotal = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "start_total",
			Help:      "Total number of service starts.",
		},
		[]string{LabelService, LabelResult},
	)

	ServiceStopTotal = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "stop_total",
			Help:      "Total number of service stops.",
		},
		[]string{LabelService, LabelResult},
	)

	ServiceInitDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "init_duration_seconds",
			Help:      "Duration of service initialization in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		},
		[]string{LabelService},
	)

	ServiceOperationDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "operation_duration_seconds",
			Help:      "Duration of service operations in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms to ~52s
		},
		[]string{LabelService, LabelMethod},
	)

	ServiceGRPCRequestTotal = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "grpc_request_total",
			Help:      "Total number of gRPC requests to services.",
		},
		[]string{LabelService, LabelMethod, LabelResult},
	)

	ServiceGRPCRequestDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "grpc_request_duration_seconds",
			Help:      "Duration of gRPC requests to services in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20), // 0.1ms to ~52s
		},
		[]string{LabelService, LabelMethod},
	)

	ServiceRegistryGauge = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "registry_count",
			Help:      "Number of services registered in the service registry.",
		},
		[]string{LabelMode},
	)

	ServiceDependencyResolutionDuration = metricscommon.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "service",
			Name:      "dependency_resolution_duration_seconds",
			Help:      "Duration of service dependency resolution in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~1.6s
		},
	)
}
