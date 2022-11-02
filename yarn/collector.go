package yarn

import (
	"github.com/prometheus/client_golang/prometheus"
)

const metricsNamespace = "yarn"

func newFuncMetric(metricName string, docString string, variableLabels []string, constLabels prometheus.Labels) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "", metricName), docString, variableLabels, constLabels)
}
