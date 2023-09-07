package config

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

// PrometheusExporterConfig defines setting for prometheus exporter
type PrometheusExporterConfig struct {
	HTTPServer HTTPServerSettings `yaml:"http-server"`

	// Namespace if set, exports metrics under the provided value.
	Namespace string `yaml:"namespace"`

	// ConstLabels are values that are applied for every exported metric.
	ConstLabels prometheus.Labels `yaml:"const-labels"`

	// SendTimestamps will send the underlying scrape timestamp with the export
	SendTimestamps bool `yaml:"send-timestamps"`

	// MetricExpiration defines how long metrics are kept without updates
	MetricExpiration time.Duration `yaml:"metric-expiration"`

	// EnableOpenMetrics enables the use of the OpenMetrics encoding option for the prometheus exporter.
	EnableOpenMetrics bool `yaml:"enable_open-metrics"`

	// AddMetricSuffixes controls whether suffixes are added to metric names. Defaults to true.
	AddMetricSuffixes bool `yaml:"add_metric-suffixes"`
}

// HTTPServerSettings defines settings for creating an prometheus exporter HTTP server.
type HTTPServerSettings struct {
	// Endpoint configures the listening address for the server.
	Endpoint string `yaml:"endpoint"`

	// MaxRequestBodySize sets the maximum request body size in bytes
	MaxRequestBodySize int64 `yaml:"max_request_body_size"`

	// IncludeMetadata propagates the client metadata from the incoming requests to the downstream consumers
	// Experimental: *NOTE* this option is subject to change or removal in the future.
	IncludeMetadata bool `yaml:"include_metadata"`

	// Additional headers attached to each HTTP response sent to the client.
	// Header values are opaque since they may be sensitive.
	ResponseHeaders map[string]string `yaml:"response_headers"`
}
