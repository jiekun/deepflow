package config

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

// PrometheusExporterConfig defines setting for prometheus exporter
type PrometheusExporterConfig struct {
	HTTPServerSettings `yaml:"http-server,inline"`

	// Namespace if set, exports metrics under the provided value.
	Namespace string `yaml:"namespace"`

	// ConstLabels are values that are applied for every exported metric.
	ConstLabels prometheus.Labels `yaml:"const-labels"`

	// MetricExpiration defines how long metrics are kept without updates
	MetricExpiration time.Duration `yaml:"metric-expiration"`

	// ProtocolFilter defines L7 Protocols that could be export
	ProtocolFilter []string `yaml:"protocol-filter"`

	// ServiceFilter defines Service Name that could be export
	ServiceFilter []string `yaml:"service-filter"`

	QueueCount int `yaml:"queue-count"`
	QueueSize  int `yaml:"queue-size"`

	OverridableCfg `yaml:",inline"`
}

// HTTPServerSettings defines settings for creating an prometheus exporter HTTP server.
type HTTPServerSettings struct {
	// Endpoint configures the listening address for the server.
	// You need to config the Kubernetes service as well to expose the related port.
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
