package config

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"

	yaml "gopkg.in/yaml.v2"
)

type baseConfig struct {
	Config ingesterConfig `yaml:"ingester"`
}

type ingesterConfig struct {
	ExportersCfg ExportersCfg `yaml:"exporters"`
}

func TestConfig(t *testing.T) {
	ingesterCfg := baseConfig{}
	configBytes, _ := ioutil.ReadFile("./config_test.yaml")
	err := yaml.Unmarshal(configBytes, &ingesterCfg)
	if err != nil {
		t.Fatalf("yaml unmarshal failed: %v", err)
	}
	expect := baseConfig{
		Config: ingesterConfig{
			ExportersCfg: ExportersCfg{
				OverridableCfg: OverridableCfg{
					ExportDatas:                 []string{"cbpf-net-span"},
					ExportDataTypes:             []string{"service_info"},
					ExportCustomK8sLabelsRegexp: "",
					ExportOnlyWithTraceID:       nil,
				},
				Enabled: false,

				OtlpExporterCfgs: []OtlpExporterConfig{
					{
						Enabled:          true,
						Addr:             "127.0.0.1:4317",
						QueueCount:       4,
						QueueSize:        100000,
						ExportBatchCount: 32,
						GrpcHeaders: map[string]string{
							"key1": "value1",
							"key2": "value2",
						},
						OverridableCfg: OverridableCfg{
							ExportDatas:                 []string{"ebpf-sys-span"},
							ExportDataTypes:             []string{"tracing_info", "network_layer", "flow_info", "transport_layer", "application_layer", "metrics"},
							ExportCustomK8sLabelsRegexp: "",
							ExportOnlyWithTraceID:       proto.Bool(true),
						},
					},
				},
				PrometheusExporterCfg: []PrometheusExporterConfig{
					{
						Namespace:            "test",
						ProtocolFilter:       []string{"http", "https", "http2", "https2", "mysql", "redis", "kafka", "mqtt", "grpc", "protobuf_rpc"},
						ServiceFilter:        []string{"*"},
						QueueCount:           4,
						QueueSize:            100000,
						LokiURL:              "127.0.0.1:1234",
						LokiTenantID:         "test-tenant-id",
						MaxMessageWaitSecond: 2,
						MaxMessageBytes:      1024 * 1024,
						TimeoutSecond:        5,
						MinBackoffSecond:     1,
						MaxBackoffSecond:     60,
						MaxRetries:           3,

						HTTPServerSettings: HTTPServerSettings{
							Endpoint: "127.0.0.1:4317",
						},
						OverridableCfg: OverridableCfg{
							ExportDatas:                 []string{"ebpf-sys-span"},
							ExportDataTypes:             []string{"tracing_info", "network_layer", "flow_info", "transport_layer", "application_layer", "metrics"},
							ExportCustomK8sLabelsRegexp: "",
							ExportOnlyWithTraceID:       proto.Bool(true),
						},
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(expect, ingesterCfg) {
		t.Fatalf("yaml unmarshal not equal, expect: %v, got: %v", expect, ingesterCfg)
	}
}
