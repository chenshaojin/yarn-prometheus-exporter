package yarn

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"log"
	"net/http"
	"net/url"
	log2 "yarn-prometheus-exporter/logger"
)

/**
定义 response body
*/

type applicationList struct {
	Apps applications `json:"apps"`
}
type applications struct {
	App []*application `json:"app"`
}
type application struct {
	ElapsedTime            int     `json:"elapsedTime"`
	AllocatedMB            int     `json:"allocatedMB"`
	AllocatedVCores        int     `json:"allocatedVCores"`
	RunningContainers      int     `json:"runningContainers"`
	MemorySeconds          int     `json:"memorySeconds"`
	VCoreSeconds           int     `json:"vcoreSeconds"`
	QueueUsagePercentage   float64 `json:"queueUsagePercentage"`
	ClusterUsagePercentage float64 `json:"clusterUsagePercentage"`
	// 标签
	Id              string `json:"id"`
	User            string `json:"user"`
	Name            string `json:"name"`
	Queue           string `json:"queue"`
	State           string `json:"state"`
	FinalStatus     string `json:"finalStatus"`
	ApplicationType string `json:"applicationType"`
	ApplicationTags string `json:"applicationTags"`
}

/**
 * 这个标签方法，需要和application中的标签对应，并且在Collect方式中，将标签值按照此顺序传入label中
 */

func (ac *ApplicationCollector) labels() []string {
	var labels []string
	return append(labels, "id", "user", "name", "queue", "state", "finalStatus", "applicationType", "applicationTags")
}

func (ac *ApplicationCollector) metrics() []string {
	var labels []string
	return append(labels,
		"elapsedTime",
		"allocatedMB",
		"allocatedVCores",
		"runningContainers",
		"memorySeconds",
		"vcoreSeconds",
		"queueUsagePercentage",
		"clusterUsagePercentage")
}

type ApplicationCollector struct {
	ApplicationEndpoint    *url.URL
	ElapsedTime            *prometheus.Desc
	AllocatedMB            *prometheus.Desc
	AllocatedVCores        *prometheus.Desc
	RunningContainers      *prometheus.Desc
	MemorySeconds          *prometheus.Desc
	VCoreSeconds           *prometheus.Desc
	QueueUsagePercentage   *prometheus.Desc
	ClusterUsagePercentage *prometheus.Desc
}

/**
收集指标
*/

func (ac *ApplicationCollector) Collect(ch chan<- prometheus.Metric) {
	metrics, err := ac.fetch(ac.ApplicationEndpoint)
	if err != nil {
		log.Println("Error while collecting data from YARN: " + err.Error())
		return
	}
	for _, a := range metrics {
		ac.metrics2File(a)
		labelValues := make([]string, 0, len(ac.labels()))
		labelValues = append(labelValues, a.Id, a.User, a.Name, a.Queue, a.State, a.FinalStatus, a.ApplicationType, a.ApplicationTags)
		ch <- prometheus.MustNewConstMetric(ac.ElapsedTime, prometheus.GaugeValue, float64(a.ElapsedTime), labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.AllocatedMB, prometheus.GaugeValue, float64(a.AllocatedMB), labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.AllocatedVCores, prometheus.GaugeValue, float64(a.AllocatedVCores), labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.RunningContainers, prometheus.GaugeValue, float64(a.RunningContainers), labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.MemorySeconds, prometheus.GaugeValue, float64(a.MemorySeconds), labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.VCoreSeconds, prometheus.GaugeValue, float64(a.VCoreSeconds), labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.QueueUsagePercentage, prometheus.GaugeValue, a.QueueUsagePercentage, labelValues...)
		ch <- prometheus.MustNewConstMetric(ac.ClusterUsagePercentage, prometheus.GaugeValue, a.ClusterUsagePercentage, labelValues...)
	}

}

func (ac *ApplicationCollector) metrics2File(a *application) {
	jsonMetrics, err := json.Marshal(a)
	if err != nil {
		log.Println("json 解析失败！！")
	}
	log2.Info(string(jsonMetrics))
}

/**
定义指标
*/

func (ac *ApplicationCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- ac.ElapsedTime
	ch <- ac.AllocatedMB
	ch <- ac.AllocatedVCores
	ch <- ac.RunningContainers
	ch <- ac.MemorySeconds
	ch <- ac.VCoreSeconds
	ch <- ac.QueueUsagePercentage
	ch <- ac.ClusterUsagePercentage
}

/*
*
请求数据源
*/
func (ac *ApplicationCollector) fetch(u *url.URL) ([]*application, error) {
	req := http.Request{
		Method:     "GET",
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}
	resp, err := http.DefaultClient.Do(&req)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatal("response body error: " + err.Error())
		}
	}(resp.Body)

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("unexpected HTTP status: %v", resp.StatusCode))
	}

	var c applicationList
	err = json.NewDecoder(resp.Body).Decode(&c)
	if err != nil {
		return nil, err
	}

	return c.Apps.App, nil
}

func NewAppsCollector(endpoint *url.URL) *ApplicationCollector {
	labels := new(ApplicationCollector).labels()
	return &ApplicationCollector{
		// application
		ApplicationEndpoint:    endpoint,
		ElapsedTime:            newFuncMetric("elapsed_time", "elapsed time", labels, nil),
		AllocatedMB:            newFuncMetric("allocated_MB", "allocated memory :MB", labels, nil),
		AllocatedVCores:        newFuncMetric("allocated_v_cores", "allocated core", labels, nil),
		RunningContainers:      newFuncMetric("running_containers", "running containers", labels, nil),
		MemorySeconds:          newFuncMetric("memory_seconds", "memory seconds", labels, nil),
		VCoreSeconds:           newFuncMetric("v_core_seconds", "core seconds", labels, nil),
		QueueUsagePercentage:   newFuncMetric("queue_usage_percentage", "queue usage percentage", labels, nil),
		ClusterUsagePercentage: newFuncMetric("cluster_usage_percentage", "cluster_usage_percentage", labels, nil),
	}
}
