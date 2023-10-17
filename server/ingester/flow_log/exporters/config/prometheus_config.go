package config

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

const (
	DefaultPrometheusExportQueueCount = 4
	DefaultPrometheusExportQueueSize  = 100000
	DefaultGranularity                = "summary"
)

var (
	DefaultPrometheusExporterProtocolFilter = []string{"http", "https", "http2", "https2", "mysql", "redis", "kafka", "mqtt", "grpc", "protobuf_rpc"}
	DefaultPrometheusExporterServiceFilter  = []string{"*"}
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

	// Granularity can be either "detail" or "summary", which control the export data to reduce cardinality of metric.
	// e.g.:
	// MySQL - detail: endpoint="SELECT user_tab_01"; summary: endpoint="SELECT"
	// HTTP - detail: endpoint="my-host.com/api/v1/edit_user"; summary: endpoint="my-host.com"
	// Redis - detail: endpoint="hmset"; summary: endpoint="write"
	Granularity string `yaml:"granularity"`

	QueueCount int `yaml:"queue-count"`
	QueueSize  int `yaml:"queue-size"`

	// loki config
	LokiURL      string `yaml:"loki-url"`
	LokiTenantID string `yaml:"loki-tenant-id"`
	// MaxMessageWaitSecond maximum wait period before sending batch of message
	MaxMessageWaitSecond int64 `yaml:"max-message-wait-second"`
	// MaxMessageBytes maximum batch size of message to accrue before sending
	MaxMessageBytes int64 `yaml:"max-message-bytes"`
	// TimeoutSecond maximum time to wait for server to respond
	TimeoutSecond int64 `yaml:"timeout-second"`
	// MinBackoffSecond minimum backoff time between retries
	MinBackoffSecond int64 `yaml:"min-backoff-second"`
	// MaxBackoffSecond maximum backoff time between retries
	MaxBackoffSecond int64 `yaml:"max-backoff-second"`
	// MaxRetries maximum number of retries when sending batches
	MaxRetries int64 `yaml:"max-retries"`

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

func (cfg *PrometheusExporterConfig) Validate(overridableCfg OverridableCfg) error {
	if cfg.Endpoint == "" {
		return fmt.Errorf("invalid prometheus exporter endpoint")
	}

	if cfg.QueueCount == 0 {
		cfg.QueueCount = DefaultPrometheusExportQueueCount
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = DefaultPrometheusExportQueueSize
	}

	if len(cfg.ProtocolFilter) == 0 {
		cfg.ProtocolFilter = DefaultPrometheusExporterProtocolFilter
	}

	if len(cfg.ServiceFilter) == 0 {
		cfg.ServiceFilter = DefaultPrometheusExporterServiceFilter
	}

	if cfg.Granularity != "summary" && cfg.Granularity != "detail" {
		cfg.Granularity = DefaultGranularity
	}

	// overwritten params
	if cfg.ExportCustomK8sLabelsRegexp == "" {
		cfg.ExportCustomK8sLabelsRegexp = overridableCfg.ExportCustomK8sLabelsRegexp
	}

	if len(cfg.ExportDatas) == 0 {
		cfg.ExportDatas = overridableCfg.ExportDatas
	}

	if len(cfg.ExportDataTypes) == 0 {
		cfg.ExportDataTypes = overridableCfg.ExportDataTypes
	}

	if cfg.ExportOnlyWithTraceID != nil {
		cfg.ExportOnlyWithTraceID = overridableCfg.ExportOnlyWithTraceID
	}

	return nil
}
