package prometheus_exporter

import (
	"fmt"
	exporter_common "github.com/deepflowio/deepflow/server/ingester/flow_log/exporters/common"
	exporters_cfg "github.com/deepflowio/deepflow/server/ingester/flow_log/exporters/config"
	utag "github.com/deepflowio/deepflow/server/ingester/flow_log/exporters/universal_tag"
	"github.com/deepflowio/deepflow/server/ingester/flow_log/log_data"
	"github.com/deepflowio/deepflow/server/libs/datatype"
	"github.com/deepflowio/deepflow/server/libs/queue"
	"github.com/deepflowio/deepflow/server/libs/utils"
	"github.com/op/go-logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
)

var (
	log                          = logging.MustGetLogger("otlp_exporter")
	deepFlowRemoteRequestSummary *prometheus.SummaryVec
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

func (e *PrometheusExporter) GetCounter() interface{} {
	var counter Counter
	counter, *e.counter = *e.counter, Counter{}
	e.lastCounter = counter
	return &counter
}

type PrometheusExporter struct {
	cfg         exporters_cfg.PrometheusExporterConfig
	name        string
	constLabels prometheus.Labels

	dataQueues         queue.FixedMultiQueue
	queueCount         int
	counter            *Counter
	lastCounter        Counter
	exportDataTypeBits uint32
	running            bool

	universalTagsManager *utag.UniversalTagsManager
	utils.Closable
}

func (e *PrometheusExporter) Start() {
	deepFlowRemoteRequestSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "deepflow_remote_request_duration",
	}, []string{})
	prometheus.MustRegister(deepFlowRemoteRequestSummary)
	go e.startMetricsServer()
}

func (e *PrometheusExporter) Close() {

}

func (e *PrometheusExporter) Put(items ...interface{}) {

}

func (e *PrometheusExporter) IsExportData(l *log_data.L7FlowLog) bool {
	return false
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
				if exporter_common.IsClientSide(f.TapSide) {
					side = "client"
					serviceName = tags0.AutoService
				} else {
					side = "server"
					serviceName = tags1.AutoService
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
					"endpoint":     e.getEndpoint(f),
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
func (e *PrometheusExporter) getEndpoint(l *log_data.L7FlowLog) string {
	return ""
}
