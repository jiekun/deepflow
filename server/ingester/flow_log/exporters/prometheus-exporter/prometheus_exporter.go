package prometheus_exporter

import (
	"fmt"
	"github.com/deepflowio/deepflow/server/ingester/common"
	exporter_common "github.com/deepflowio/deepflow/server/ingester/flow_log/exporters/common"
	exporters_cfg "github.com/deepflowio/deepflow/server/ingester/flow_log/exporters/config"
	utag "github.com/deepflowio/deepflow/server/ingester/flow_log/exporters/universal_tag"
	"github.com/deepflowio/deepflow/server/ingester/flow_log/log_data"
	"github.com/deepflowio/deepflow/server/ingester/ingesterctl"
	"github.com/deepflowio/deepflow/server/libs/datatype"
	"github.com/deepflowio/deepflow/server/libs/debug"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"github.com/deepflowio/deepflow/server/libs/stats"
	"github.com/deepflowio/deepflow/server/libs/utils"
	"github.com/op/go-logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

var (
	log              = logging.MustGetLogger("prometheus_exporter")
	serviceNameRegex *regexp.Regexp
	protocolMap      = map[string]datatype.L7Protocol{
		"http":         datatype.L7_PROTOCOL_HTTP_1,
		"https":        datatype.L7_PROTOCOL_HTTP_1_TLS,
		"http/2":       datatype.L7_PROTOCOL_HTTP_2,
		"https/2":      datatype.L7_PROTOCOL_HTTP_2_TLS,
		"dubbo":        datatype.L7_PROTOCOL_DUBBO,
		"grpc":         datatype.L7_PROTOCOL_GRPC,
		"protobuf_rpc": datatype.L7_PROTOCOL_PROTOBUF_RPC,
		"sofarpc":      datatype.L7_PROTOCOL_SOFARPC,
		"fastcgi":      datatype.L7_PROTOCOL_FASTCGI,
		"mysql":        datatype.L7_PROTOCOL_MYSQL,
		"postgre":      datatype.L7_PROTOCOL_POSTGRE,
		"redis":        datatype.L7_PROTOCOL_REDIS,
		"kafka":        datatype.L7_PROTOCOL_KAFKA,
		"mqtt":         datatype.L7_PROTOCOL_MQTT,
		"mongodb":      datatype.L7_PROTOCOL_MONGODB,
	}
)

const (
	QUEUE_BATCH_COUNT = 1024
)

type Counter struct {
	RecvCounter          int64 `statsd:"recv-count"`
	SendCounter          int64 `statsd:"send-count"`
	SendBatchCounter     int64 `statsd:"send-batch-count"`
	ExportUsedTimeNs     int64 `statsd:"export-used-time-ns"`
	DropCounter          int64 `statsd:"drop-count"`
	DropBatchCounter     int64 `statsd:"drop-batch-count"`
	DropNoTraceIDCounter int64 `statsd:"drop-no-traceid-count"`
}

func init() {
	serviceNameRegex, _ = regexp.Compile(`^[a-zA-Z].+`)
	shardingRegex, _ = regexp.Compile(`_\d+$`)
}

func (e *PrometheusExporter) GetCounter() interface{} {
	var counter Counter
	counter, *e.counter = *e.counter, Counter{}
	e.lastCounter = counter
	return &counter
}

type PrometheusExporter struct {
	cfg         *exporters_cfg.PrometheusExporterConfig
	constLabels prometheus.Labels

	index             int
	protocolFilterMap map[datatype.L7Protocol]bool
	serviceFilterMap  map[string]bool
	dataQueues        queue.FixedMultiQueue
	queueCount        int
	counter           *Counter
	lastCounter       Counter
	running           bool

	deepFlowRemoteRequestHist   *prometheus.HistogramVec
	deepFlowDatabaseRequestHist *prometheus.HistogramVec
	deepFlowCacheRequestHist    *prometheus.HistogramVec
	deepFlowMQRequestHist       *prometheus.HistogramVec

	universalTagsManager *utag.UniversalTagsManager
	utils.Closable
}

func NewPrometheusExporter(index int, config *exporters_cfg.ExportersCfg, universalTagsManager *utag.UniversalTagsManager) *PrometheusExporter {
	promExporterCfg := config.PrometheusExporterCfg[index]

	dataQueues := queue.NewOverwriteQueues(
		fmt.Sprintf("prometheus_exporter_%d", index), queue.HashKey(promExporterCfg.QueueCount), promExporterCfg.QueueSize,
		queue.OptionFlushIndicator(time.Second),
		queue.OptionRelease(func(p interface{}) { p.(exporter_common.ExportItem).Release() }),
		common.QUEUE_STATS_MODULE_INGESTER)

	protocolFilter := make(map[datatype.L7Protocol]bool)
	for i := range promExporterCfg.ProtocolFilter {
		if v, ok := protocolMap[promExporterCfg.ProtocolFilter[i]]; ok {
			protocolFilter[v] = true
		}
	}

	serviceFilter := make(map[string]bool)
	for i := range promExporterCfg.ServiceFilter {
		serviceFilter[promExporterCfg.ServiceFilter[i]] = true
	}

	exporter := &PrometheusExporter{
		cfg: &promExporterCfg,

		index:                index,
		protocolFilterMap:    protocolFilter,
		serviceFilterMap:     serviceFilter,
		dataQueues:           dataQueues,
		queueCount:           promExporterCfg.QueueCount,
		universalTagsManager: universalTagsManager,
		counter:              &Counter{},
	}

	exporter.deepFlowRemoteRequestHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "deepflow_remote_request_duration",
		Namespace:                       promExporterCfg.Namespace,
		ConstLabels:                     promExporterCfg.ConstLabels,
		Buckets:                         []float64{3, 7, 15, 30, 60, 120, 250, 500, 1000, 2000, 5000},
		NativeHistogramBucketFactor:     1.2,
		NativeHistogramMaxBucketNumber:  40,
		NativeHistogramZeroThreshold:    1,   // 3ms
		NativeHistogramMaxZeroThreshold: 100, // 100ms
		NativeHistogramMinResetDuration: 30 * time.Minute,
	}, []string{
		"request_status",
		"service_name",
		"service_namespace",
		"service_cluster",
		"request_endpoint",
		"request_protocol",
	})

	exporter.deepFlowDatabaseRequestHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "deepflow_database_request_duration",
		Namespace:                       promExporterCfg.Namespace,
		ConstLabels:                     promExporterCfg.ConstLabels,
		Buckets:                         []float64{3, 7, 15, 30, 60, 120, 250, 500, 1000, 2000, 5000},
		NativeHistogramBucketFactor:     1.2,
		NativeHistogramMaxBucketNumber:  40,
		NativeHistogramZeroThreshold:    1,   // 1ms
		NativeHistogramMaxZeroThreshold: 100, // 100ms
		NativeHistogramMinResetDuration: 30 * time.Minute,
	}, []string{
		"request_status",
		"service_name",
		"service_namespace",
		"service_cluster",
		"request_endpoint",
		"request_protocol",
	})

	exporter.deepFlowCacheRequestHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "deepflow_cache_request_duration",
		Namespace:                       promExporterCfg.Namespace,
		ConstLabels:                     promExporterCfg.ConstLabels,
		Buckets:                         []float64{.2, 0.5, 1, 2, 5, 10, 20, 40, 80, 160, 400, 1000},
		NativeHistogramBucketFactor:     1.2,
		NativeHistogramMaxBucketNumber:  40,
		NativeHistogramZeroThreshold:    0.1, // 0.1ms
		NativeHistogramMaxZeroThreshold: 10,  // 100ms
		NativeHistogramMinResetDuration: 30 * time.Minute,
	}, []string{
		"request_status",
		"service_name",
		"service_namespace",
		"service_cluster",
		"request_endpoint",
		"request_protocol",
	})

	exporter.deepFlowMQRequestHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "deepflow_mq_request_duration",
		Namespace:                       promExporterCfg.Namespace,
		ConstLabels:                     promExporterCfg.ConstLabels,
		Buckets:                         []float64{5, 10, 20, 50, 120, 250, 500, 1000, 1500, 2500, 5000},
		NativeHistogramBucketFactor:     1.2,
		NativeHistogramMaxBucketNumber:  40,
		NativeHistogramZeroThreshold:    5,   // 0.1ms
		NativeHistogramMaxZeroThreshold: 100, // 100ms
		NativeHistogramMinResetDuration: 30 * time.Minute,
	}, []string{
		"request_status",
		"service_name",
		"service_namespace",
		"service_cluster",
		"request_endpoint",
		"request_protocol",
	})

	prometheus.MustRegister(exporter.deepFlowRemoteRequestHist)
	prometheus.MustRegister(exporter.deepFlowDatabaseRequestHist)
	prometheus.MustRegister(exporter.deepFlowCacheRequestHist)
	prometheus.MustRegister(exporter.deepFlowMQRequestHist)

	debug.ServerRegisterSimple(ingesterctl.CMD_PROMETHEUS_EXPORTER, exporter)
	common.RegisterCountableForIngester("exporter", exporter, stats.OptionStatTags{
		"type": "prometheus", "index": strconv.Itoa(index)})
	log.Infof("prometheus exporter %d created", index)
	return exporter
}

func (e *PrometheusExporter) Start() {
	if e.running {
		log.Warningf("prometheus exporter %d already running", e.index)
		return
	}
	e.running = true
	for i := 0; i < e.queueCount; i++ {
		go e.queueProcess(int(i))
	}

	go e.startMetricsServer()
}

func (e *PrometheusExporter) Close() {
	e.running = false
	log.Infof("prometheus exporter %d stopping", e.index)
}

func (e *PrometheusExporter) Put(items ...interface{}) {
	e.counter.RecvCounter++
	e.dataQueues.Put(queue.HashKey(int(e.counter.RecvCounter)%e.queueCount), items...)
}

func (e *PrometheusExporter) IsExportData(l *log_data.L7FlowLog) bool {
	// always not export data from OTel
	if l.SignalSource != uint16(datatype.SIGNAL_SOURCE_PACKET) {
		e.counter.DropCounter++
		return false
	}

	if !e.protocolFilterMap[datatype.L7Protocol(l.L7Protocol)] {
		e.counter.DropCounter++
		return false
	}
	return true
}

func (e *PrometheusExporter) queueProcess(queueID int) {
	defer log.Warningf("prometheus exporter queue worker %d exit", queueID)
	flows := make([]interface{}, QUEUE_BATCH_COUNT)

	for e.running {
		n := e.dataQueues.Gets(queue.HashKey(queueID), flows)
		for _, flow := range flows[:n] {
			switch f := flow.(type) {
			case (*log_data.L7FlowLog):
				tags0, tags1 := e.universalTagsManager.QueryUniversalTags(f)
				var serviceName, side, status, namespace, cluster string

				endpoint := e.getEndpoint(f)
				if endpoint == "" {
					f.Release()
					continue
				}
				if exporter_common.IsClientSide(f.TapSide) {
					side = "client"
					serviceName = tags0.AutoService
					namespace = tags0.PodNS
					cluster = tags0.PodCluster
				} else {
					side = "server"
					serviceName = tags1.AutoService
					namespace = tags1.PodNS
					cluster = tags1.PodCluster
				}

				// not export if:
				// 1. service not legal (not starts with a-zA-Z, e.g. an IP address).
				// 2. service name filter is working (not containing "*" which means export all) and service name not in filter whitelist.
				// 3. server side flow_log.
				if !serviceNameRegex.MatchString(serviceName) || (!e.serviceFilterMap["*"] && !e.serviceFilterMap[serviceName]) || side == "server" {
					f.Release()
					continue
				}

				switch datatype.LogMessageStatus(f.ResponseStatus) {
				case datatype.STATUS_OK:
					status = "0"
				case datatype.STATUS_CLIENT_ERROR, datatype.STATUS_SERVER_ERROR, datatype.STATUS_ERROR:
					status = "1"
				default:
					status = "-1"
				}

				label := prometheus.Labels{
					"request_status":    status,
					"service_name":      serviceName,
					"service_namespace": namespace,
					"service_cluster":   cluster,
					"request_endpoint":  endpoint,
					"request_protocol":  datatype.L7Protocol(f.L7Protocol).String(),
				}
				switch datatype.L7Protocol(f.L7Protocol) {
				case datatype.L7_PROTOCOL_HTTP_1, datatype.L7_PROTOCOL_HTTP_2, datatype.L7_PROTOCOL_HTTP_1_TLS, datatype.L7_PROTOCOL_HTTP_2_TLS, datatype.L7_PROTOCOL_GRPC:
					e.deepFlowRemoteRequestHist.With(label).Observe(float64((f.EndTime() - f.StartTime()).Milliseconds()))
				case datatype.L7_PROTOCOL_MYSQL, datatype.L7_PROTOCOL_POSTGRE, datatype.L7_PROTOCOL_MONGODB:
					e.deepFlowDatabaseRequestHist.With(label).Observe(float64((f.EndTime() - f.StartTime()).Milliseconds()))
				case datatype.L7_PROTOCOL_KAFKA, datatype.L7_PROTOCOL_MQTT:
					e.deepFlowMQRequestHist.With(label).Observe(float64((f.EndTime() - f.StartTime()).Milliseconds()))
				case datatype.L7_PROTOCOL_REDIS:
					e.deepFlowCacheRequestHist.With(label).Observe(float64((f.EndTime() - f.StartTime()).Milliseconds()))
				}
				f.Release()
			default:
				continue
			}
		}
	}
}

func (e *PrometheusExporter) startMetricsServer() {
	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(e.cfg.Endpoint, nil); err != nil {
		fmt.Printf("Start prometheus exporter on http %s failed: %v", e.cfg.Endpoint, err)
		os.Exit(1)
	}
}

// getEndpoint return a customized endpoint string.
// Endpoints for different protocol are different. getEndpoint try to compose a format:
// Host/Path for RPC request and Host for middleware request.
func (e *PrometheusExporter) getEndpoint(l7 *log_data.L7FlowLog) string {
	var summaryEndpoint, detailEndpoint string
	switch datatype.L7Protocol(l7.L7Protocol) {
	case datatype.L7_PROTOCOL_MYSQL, datatype.L7_PROTOCOL_POSTGRE:
		// e.g.: SELECT / SELECT user_tab
		summaryEndpoint, _ = GetMySQLEndpoint(utils.IpFromUint32(l7.IP41), l7.RequestResource)
	case datatype.L7_PROTOCOL_REDIS:
		// e.g.: read / command
		summaryEndpoint, _ = GetRedisEndpoint(utils.IpFromUint32(l7.IP41), l7.RequestType)
	case datatype.L7_PROTOCOL_KAFKA:
		// e.g.: TODO
		summaryEndpoint, _ = GetKafkaEndpoint(utils.IpFromUint32(l7.IP41), l7.RequestDomain)
	case datatype.L7_PROTOCOL_MQTT:
		// e.g.: TODO
		summaryEndpoint, _ = GetMQTTEndpoint(utils.IpFromUint32(l7.IP41), l7.RequestDomain)
	case datatype.L7_PROTOCOL_GRPC:
		// e.g.: service / service+command
		summaryEndpoint, _ = GetGRPCEndpoint(l7.Endpoint)
	case datatype.L7_PROTOCOL_HTTP_1, datatype.L7_PROTOCOL_HTTP_2, datatype.L7_PROTOCOL_HTTP_1_TLS, datatype.L7_PROTOCOL_HTTP_2_TLS:
		// e.g.: host / host+path
		summaryEndpoint, _ = GetHTTPEndpoint(l7.RequestDomain, l7.RequestResource)
	case datatype.L7_PROTOCOL_MONGODB:
		summaryEndpoint, _ = GetMongoEndpoint(utils.IpFromUint32(l7.IP41), l7.RequestResource)
	}
	log.Debugf("getEndpoint, protocol: %s, summary: %s, detail: %s", l7.L7ProtocolStr, summaryEndpoint, detailEndpoint)
	if e.cfg.Granularity == "detail" {
		return detailEndpoint
	}
	return summaryEndpoint
}

func (e *PrometheusExporter) HandleSimpleCommand(op uint16, arg string) string {
	return fmt.Sprintf("prometheus exporter %d last 10s counter: %+v", e.index, e.lastCounter)
}
