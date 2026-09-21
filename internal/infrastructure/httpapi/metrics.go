package httpapi

import "github.com/d-dionisio/backend-challenge/internal/observability"

type Metrics = observability.Metrics

func NewMetrics() *Metrics { return observability.NewMetrics() }
