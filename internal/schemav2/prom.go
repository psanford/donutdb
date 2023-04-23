package schemav2

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var promNamespace = "donutdb_v2"

var buckets = []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10}

var batchGetItemHist = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: promNamespace,
		Name:      "batch_get_item_latency_seconds",
		Buckets:   buckets,
	},
)

var batchGetItemCount = promauto.NewCounter(
	prometheus.CounterOpts{
		Namespace: promNamespace,
		Name:      "batch_get_item_count",
	},
)

var GetItemHist = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: promNamespace,
		Name:      "get_item_latency_seconds",
		Buckets:   buckets,
	},
)

var UpdateItemHist = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: promNamespace,
		Name:      "update_item_latency_seconds",
		Buckets:   buckets,
	},
)

var BatchWriteItemHist = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: promNamespace,
		Name:      "batch_write_item_latency_seconds",
		Buckets:   buckets,
	},
)

var BatchWriteItemCount = promauto.NewCounter(
	prometheus.CounterOpts{
		Namespace: promNamespace,
		Name:      "batch_write_item_count",
	},
)
