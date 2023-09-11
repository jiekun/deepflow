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
	"strings"
	"time"
)

var (
	log                          = logging.MustGetLogger("prometheus_exporter")
	deepFlowRemoteRequestSummary *prometheus.SummaryVec
	serviceNameRegex             *regexp.Regexp
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
	deepFlowRemoteRequestSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "deepflow_remote_request_duration",
	}, []string{
		"side",
		"status",
		"service_name",
		"endpoint",
		"protocol",
	})
	prometheus.MustRegister(deepFlowRemoteRequestSummary)

	serviceNameRegex, _ = regexp.Compile("^[a-zA-Z].+")
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

	index       int
	dataQueues  queue.FixedMultiQueue
	queueCount  int
	counter     *Counter
	lastCounter Counter
	running     bool

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

	exporter := &PrometheusExporter{
		cfg: &promExporterCfg,

		index:                index,
		dataQueues:           dataQueues,
		queueCount:           promExporterCfg.QueueCount,
		universalTagsManager: universalTagsManager,
		counter:              &Counter{},
	}
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

	go func() {
		for {
			resp, _ := http.Get(e.cfg.Endpoint + "/metrics")
			result := []byte{}
			resp.Body.Read(result)
			log.Info(result)
			time.Sleep(10 * time.Second)

		}
	}()
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
	if l.SignalSource != uint16(datatype.SIGNAL_SOURCE_EBPF) {
		e.counter.DropCounter++
		return false
	}
	return true
}

func (e *PrometheusExporter) queueProcess(queueID int) {
	flows := make([]interface{}, QUEUE_BATCH_COUNT)

	for e.running {
		n := e.dataQueues.Gets(queue.HashKey(queueID), flows)
		for _, flow := range flows[:n] {
			switch t := flow.(type) {
			case (*log_data.L7FlowLog):
				f := flow.(*log_data.L7FlowLog)
				tags0, tags1 := e.universalTagsManager.QueryUniversalTags(f)
				var serviceName string
				var side string
				var status string

				endpoint := e.getEndpoint(f)
				if endpoint == "" {
					f.Release()
					continue
				}
				if exporter_common.IsClientSide(f.TapSide) {
					side = "client"
					serviceName = tags0.AutoService
				} else {
					side = "server"
					serviceName = tags1.AutoService
				}

				if !serviceNameRegex.MatchString(serviceName) || side == "server" {
					f.Release()
					return
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
					"side":         side,
					"status":       status,
					"service_name": serviceName,
					"endpoint":     endpoint,
					"protocol":     datatype.L7Protocol(f.L7Protocol).String(),
				}
				deepFlowRemoteRequestSummary.With(label).Observe(float64((f.EndTime() - f.StartTime()).Milliseconds()))
				f.Release()
			default:
				log.Warningf("flow type(%T) unsupport", t)
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
	var endpoint string
	switch datatype.L7Protocol(l7.L7Protocol) {
	case datatype.L7_PROTOCOL_MYSQL, datatype.L7_PROTOCOL_POSTGRE:
		// e.g.: SELECT my_tab / UPDATE user_info_table
		stmtType, tableName := GetStmtTypeAndTableName(l7.RequestResource)
		endpoint = stmtType + " " + tableName
	case datatype.L7_PROTOCOL_REDIS:
		// e.g.: GET
		endpoint = l7.RequestType
	case datatype.L7_PROTOCOL_KAFKA:
		// e.g.: TODO
		endpoint = l7.RequestDomain
	case datatype.L7_PROTOCOL_MQTT:
		// e.g.: TODO
		endpoint = l7.RequestDomain
	case datatype.L7_PROTOCOL_GRPC:
		// e.g.: /oteldemo.CheckoutService/PlaceOrder
		endpoint = l7.Endpoint
	case datatype.L7_PROTOCOL_HTTP_1, datatype.L7_PROTOCOL_HTTP_2, datatype.L7_PROTOCOL_HTTP_1_TLS, datatype.L7_PROTOCOL_HTTP_2_TLS:
		if strings.HasPrefix(strings.ToLower(l7.RequestResource), "http") {
			endpoint = l7.RequestResource
		} else {
			endpoint = l7.RequestDomain + l7.RequestResource
		}
	}
	return endpoint
}

func (e *PrometheusExporter) HandleSimpleCommand(op uint16, arg string) string {
	return fmt.Sprintf("prometheus exporter %d last 10s counter: %+v", e.index, e.lastCounter)
}
