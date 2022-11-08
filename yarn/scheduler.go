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

type queueMetrics struct {
	Scheduler scheduler `json:"scheduler"`
}

type scheduler struct {
	SchedulerInfo schedulerInfo `json:"schedulerInfo"`
}
type schedulerInfo struct {
	Queues queues `json:"queues"`
}
type queues struct {
	Queue []*queue `json:"queue"`
}

type queue struct {
	Capacity             float64       `json:"capacity"`
	MaxCapacity          float64       `json:"maxCapacity"`
	UsedCapacity         float64       `json:"usedCapacity"`
	AbsoluteCapacity     float64       `json:"absoluteCapacity"`
	AbsoluteMaxCapacity  float64       `json:"absoluteMaxCapacity"`
	AbsoluteUsedCapacity float64       `json:"absoluteUsedCapacity"`
	NumApplications      int           `json:"numApplications"`
	ResourcesUsed        resourcesUsed `json:"resourcesUsed"`

	// 标签
	Type      string `json:"type"`
	QueueName string `json:"queueName"`
}
type resourcesUsed struct {
	Memory int `json:"memory"`
	VCores int `json:"vCores"`
}

func (sc *SchedulerCollector) labels() []string {
	var labels []string
	labels = append(labels, "queueName", "type")
	return labels
}

type SchedulerCollector struct {
	// queue
	SchedulerEndpoint    *url.URL
	Capacity             *prometheus.Desc
	MaxCapacity          *prometheus.Desc
	UsedCapacity         *prometheus.Desc
	AbsoluteCapacity     *prometheus.Desc
	AbsoluteMaxCapacity  *prometheus.Desc
	AbsoluteUsedCapacity *prometheus.Desc
	NumApplications      *prometheus.Desc
	ResourcesUsedMemory  *prometheus.Desc
	ResourcesUsedVCores  *prometheus.Desc
}

func (sc *SchedulerCollector) metrics2File(q *queue) {
	jsonMetrics, err := json.Marshal(q)
	if err != nil {
		log.Println("json 解析失败！！")
	}
	log2.Info(string(jsonMetrics))
}

func (sc *SchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	// 访问请求接口
	metrics, err := sc.fetch(sc.SchedulerEndpoint)
	if err != nil {
		log.Println("Error while collecting data from YARN: " + err.Error())
		return
	}
	for _, a := range metrics {
		sc.metrics2File(a)
		labelValues := make([]string, 0, len(sc.labels()))
		labelValues = append(labelValues, a.QueueName, a.Type)
		ch <- prometheus.MustNewConstMetric(sc.Capacity, prometheus.GaugeValue, a.Capacity, labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.MaxCapacity, prometheus.GaugeValue, a.MaxCapacity, labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.UsedCapacity, prometheus.GaugeValue, a.UsedCapacity, labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.NumApplications, prometheus.GaugeValue, float64(a.NumApplications), labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.AbsoluteCapacity, prometheus.GaugeValue, a.AbsoluteCapacity, labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.AbsoluteUsedCapacity, prometheus.GaugeValue, a.AbsoluteUsedCapacity, labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.AbsoluteMaxCapacity, prometheus.GaugeValue, a.AbsoluteMaxCapacity, labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.ResourcesUsedMemory, prometheus.GaugeValue, float64(a.ResourcesUsed.Memory), labelValues...)
		ch <- prometheus.MustNewConstMetric(sc.ResourcesUsedVCores, prometheus.GaugeValue, float64(a.ResourcesUsed.VCores), labelValues...)

	}

}

func (sc *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- sc.Capacity
	ch <- sc.MaxCapacity
	ch <- sc.UsedCapacity
	ch <- sc.AbsoluteCapacity
	ch <- sc.AbsoluteMaxCapacity
	ch <- sc.AbsoluteUsedCapacity
	ch <- sc.NumApplications
	ch <- sc.ResourcesUsedMemory
	ch <- sc.ResourcesUsedVCores
}

func (sc *SchedulerCollector) fetch(u *url.URL) ([]*queue, error) {
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

	var c queueMetrics
	err = json.NewDecoder(resp.Body).Decode(&c)
	if err != nil {
		return nil, err
	}

	return c.Scheduler.SchedulerInfo.Queues.Queue, nil
}

func NewSchedulerCollector(endpoint *url.URL) *SchedulerCollector {
	labels := new(SchedulerCollector).labels()
	return &SchedulerCollector{
		// queue
		SchedulerEndpoint:    endpoint,
		Capacity:             newFuncMetric("capacity", "capacity percentage", labels, nil),
		MaxCapacity:          newFuncMetric("max_capacity", "max capacity", labels, nil),
		UsedCapacity:         newFuncMetric("used_capacity", "used capacity", labels, nil),
		AbsoluteCapacity:     newFuncMetric("absolute_capacity", "used capacity", labels, nil),
		AbsoluteMaxCapacity:  newFuncMetric("absolute_max_capacity", "used capacity", labels, nil),
		AbsoluteUsedCapacity: newFuncMetric("absolute_used_capacity", "used capacity", labels, nil),
		NumApplications:      newFuncMetric("num_applications", "queue running number applications", labels, nil),
		ResourcesUsedMemory:  newFuncMetric("resources_used_memory", "used memory", labels, nil),
		ResourcesUsedVCores:  newFuncMetric("resources_used_v_cores", "used cores", labels, nil),
	}
}
