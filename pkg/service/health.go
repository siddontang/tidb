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

package service

import (
	"time"
)

// HealthState represents the current state of a service.
type HealthState string

const (
	// StateUnknown indicates the service state is unknown.
	StateUnknown HealthState = "unknown"
	// StateStarting indicates the service is starting up.
	StateStarting HealthState = "starting"
	// StateHealthy indicates the service is running normally.
	StateHealthy HealthState = "healthy"
	// StateDegraded indicates the service is running but with reduced functionality.
	StateDegraded HealthState = "degraded"
	// StateUnhealthy indicates the service is not functioning properly.
	StateUnhealthy HealthState = "unhealthy"
	// StateStopping indicates the service is shutting down.
	StateStopping HealthState = "stopping"
	// StateStopped indicates the service has stopped.
	StateStopped HealthState = "stopped"
)

// HealthStatus contains detailed health information for a service.
type HealthStatus struct {
	// State is the current health state.
	State HealthState `json:"state"`

	// Message provides additional context about the health state.
	Message string `json:"message,omitempty"`

	// LastCheck is the timestamp of the last health check.
	LastCheck time.Time `json:"last_check,omitempty"`

	// Details contains service-specific health details.
	Details map[string]any `json:"details,omitempty"`

	// Dependencies contains the health status of dependent services.
	Dependencies map[string]HealthState `json:"dependencies,omitempty"`
}

// IsHealthy returns true if the service is in a healthy state.
func (h HealthStatus) IsHealthy() bool {
	return h.State == StateHealthy
}

// IsReady returns true if the service is ready to handle requests.
func (h HealthStatus) IsReady() bool {
	return h.State == StateHealthy || h.State == StateDegraded
}

// HealthChecker provides health checking functionality.
type HealthChecker interface {
	// Check performs a health check and returns the current status.
	Check(ctx any) HealthStatus
}

// HealthAggregator aggregates health status from multiple services.
type HealthAggregator struct {
	services map[string]Service
}

// NewHealthAggregator creates a new health aggregator.
func NewHealthAggregator() *HealthAggregator {
	return &HealthAggregator{
		services: make(map[string]Service),
	}
}

// Register adds a service to be monitored.
func (a *HealthAggregator) Register(svc Service) {
	a.services[svc.Name()] = svc
}

// Unregister removes a service from monitoring.
func (a *HealthAggregator) Unregister(name string) {
	delete(a.services, name)
}

// GetHealth returns the health status of a specific service.
func (a *HealthAggregator) GetHealth(name string) (HealthStatus, bool) {
	svc, ok := a.services[name]
	if !ok {
		return HealthStatus{}, false
	}
	return svc.Health(), true
}

// GetAllHealth returns the health status of all registered services.
func (a *HealthAggregator) GetAllHealth() map[string]HealthStatus {
	result := make(map[string]HealthStatus, len(a.services))
	for name, svc := range a.services {
		result[name] = svc.Health()
	}
	return result
}

// GetOverallHealth returns an aggregated health status.
// The overall state is the worst state among all services.
func (a *HealthAggregator) GetOverallHealth() HealthStatus {
	if len(a.services) == 0 {
		return HealthStatus{State: StateUnknown, Message: "no services registered"}
	}

	var worstState HealthState = StateHealthy
	deps := make(map[string]HealthState, len(a.services))
	var unhealthyServices []string

	for name, svc := range a.services {
		status := svc.Health()
		deps[name] = status.State

		if stateOrder(status.State) > stateOrder(worstState) {
			worstState = status.State
		}
		if status.State == StateUnhealthy {
			unhealthyServices = append(unhealthyServices, name)
		}
	}

	msg := ""
	if len(unhealthyServices) > 0 {
		msg = "unhealthy services: " + joinStrings(unhealthyServices)
	}

	return HealthStatus{
		State:        worstState,
		Message:      msg,
		LastCheck:    time.Now(),
		Dependencies: deps,
	}
}

// stateOrder returns a numeric order for health states (higher = worse).
func stateOrder(state HealthState) int {
	switch state {
	case StateHealthy:
		return 0
	case StateDegraded:
		return 1
	case StateStarting:
		return 2
	case StateStopping:
		return 3
	case StateUnknown:
		return 4
	case StateUnhealthy:
		return 5
	case StateStopped:
		return 6
	default:
		return 99
	}
}

func joinStrings(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	result := ss[0]
	for i := 1; i < len(ss); i++ {
		result += ", " + ss[i]
	}
	return result
}
