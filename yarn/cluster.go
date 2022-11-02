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
)

func (cc *ClusterCollector) labels() []string {
	var labels []string
	// 给指标添加公共标签
	return append(labels)
}

type Cluster struct {
	ClusterMetrics metrics `json:"clusterMetrics"`
}

type metrics struct {
	AppsSubmitted         int `json:"appsSubmitted"`
	AppsCompleted         int `json:"appsCompleted"`
	AppsPending           int `json:"appsPending"`
	AppsRunning           int `json:"appsRunning"`
	AppsFailed            int `json:"appsFailed"`
	AppsKilled            int `json:"appsKilled"`
	ReservedMB            int `json:"reservedMB"`
	AvailableMB           int `json:"availableMB"`
	AllocatedMB           int `json:"allocatedMB"`
	ReservedVirtualCores  int `json:"reservedVirtualCores"`
	AvailableVirtualCores int `json:"availableVirtualCores"`
	AllocatedVirtualCores int `json:"allocatedVirtualCores"`
	ContainersAllocated   int `json:"containersAllocated"`
	ContainersReserved    int `json:"containersReserved"`
	ContainersPending     int `json:"containersPending"`
	TotalMB               int `json:"totalMB"`
	TotalVirtualCores     int `json:"totalVirtualCores"`
	TotalNodes            int `json:"totalNodes"`
	LostNodes             int `json:"lostNodes"`
	UnhealthyNodes        int `json:"unhealthyNodes"`
	DecommissioningNodes  int `json:"decommissioningNodes"`
	DecommissionedNodes   int `json:"decommissionedNodes"`
	RebootedNodes         int `json:"rebootedNodes"`
	ActiveNodes           int `json:"activeNodes"`
	ShutdownNodes         int `json:"shutdownNodes"`
}

type ClusterCollector struct {
	ClusterEndpoint *url.URL
	Up              *prometheus.Desc
	// cluster info metrics
	ApplicationsSubmitted *prometheus.Desc
	ApplicationsCompleted *prometheus.Desc
	ApplicationsPending   *prometheus.Desc
	ApplicationsRunning   *prometheus.Desc
	ApplicationsFailed    *prometheus.Desc
	ApplicationsKilled    *prometheus.Desc
	MemoryReserved        *prometheus.Desc
	MemoryAvailable       *prometheus.Desc
	MemoryAllocated       *prometheus.Desc
	MemoryTotal           *prometheus.Desc
	VirtualCoresReserved  *prometheus.Desc
	VirtualCoresAvailable *prometheus.Desc
	VirtualCoresAllocated *prometheus.Desc
	VirtualCoresTotal     *prometheus.Desc
	ContainersAllocated   *prometheus.Desc
	ContainersReserved    *prometheus.Desc
	ContainersPending     *prometheus.Desc
	NodesTotal            *prometheus.Desc
	NodesLost             *prometheus.Desc
	NodesUnhealthy        *prometheus.Desc
	NodesDecommissioned   *prometheus.Desc
	NodesDecommissioning  *prometheus.Desc
	NodesRebooted         *prometheus.Desc
	NodesActive           *prometheus.Desc
	NodesShutdown         *prometheus.Desc
	ScrapeFailures        *prometheus.Desc
	FailureCount          int
}

func (cc *ClusterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.Up
	ch <- cc.ApplicationsSubmitted
	ch <- cc.ApplicationsCompleted
	ch <- cc.ApplicationsPending
	ch <- cc.ApplicationsRunning
	ch <- cc.ApplicationsFailed
	ch <- cc.ApplicationsKilled
	ch <- cc.MemoryReserved
	ch <- cc.MemoryAvailable
	ch <- cc.MemoryAllocated
	ch <- cc.MemoryTotal
	ch <- cc.VirtualCoresReserved
	ch <- cc.VirtualCoresAvailable
	ch <- cc.VirtualCoresAllocated
	ch <- cc.VirtualCoresTotal
	ch <- cc.ContainersAllocated
	ch <- cc.ContainersReserved
	ch <- cc.ContainersPending
	ch <- cc.NodesTotal
	ch <- cc.NodesLost
	ch <- cc.NodesUnhealthy
	ch <- cc.NodesDecommissioned
	ch <- cc.NodesDecommissioning
	ch <- cc.NodesRebooted
	ch <- cc.NodesActive
	ch <- cc.NodesShutdown
	ch <- cc.ScrapeFailures
}

func (cc *ClusterCollector) Collect(ch chan<- prometheus.Metric) {
	up := 1.0
	metrics, err := cc.fetch(cc.ClusterEndpoint)
	labelValues := make([]string, 0, len(cc.labels()))
	labelValues = append(labelValues)
	if err != nil {
		up = 0.0
		cc.FailureCount++
		log.Println("Error while collecting data from YARN: " + err.Error())
	}
	ch <- prometheus.MustNewConstMetric(cc.Up, prometheus.GaugeValue, up, labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ScrapeFailures, prometheus.CounterValue, float64(cc.FailureCount), labelValues...)

	if up == 0.0 {
		return
	}

	ch <- prometheus.MustNewConstMetric(cc.ApplicationsSubmitted, prometheus.CounterValue, float64(metrics.AppsSubmitted), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ApplicationsCompleted, prometheus.CounterValue, float64(metrics.AppsCompleted), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ApplicationsPending, prometheus.GaugeValue, float64(metrics.AppsPending), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ApplicationsRunning, prometheus.GaugeValue, float64(metrics.AppsRunning), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ApplicationsFailed, prometheus.CounterValue, float64(metrics.AppsFailed), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ApplicationsKilled, prometheus.CounterValue, float64(metrics.AppsKilled), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.MemoryReserved, prometheus.GaugeValue, float64(metrics.ReservedMB), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.MemoryAvailable, prometheus.GaugeValue, float64(metrics.AvailableMB), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.MemoryAllocated, prometheus.GaugeValue, float64(metrics.AllocatedMB), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.MemoryTotal, prometheus.GaugeValue, float64(metrics.TotalMB), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.VirtualCoresReserved, prometheus.GaugeValue, float64(metrics.ReservedVirtualCores), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.VirtualCoresAvailable, prometheus.GaugeValue, float64(metrics.AvailableVirtualCores), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.VirtualCoresAllocated, prometheus.GaugeValue, float64(metrics.AllocatedVirtualCores), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.VirtualCoresTotal, prometheus.GaugeValue, float64(metrics.TotalVirtualCores), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ContainersAllocated, prometheus.GaugeValue, float64(metrics.ContainersAllocated), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ContainersReserved, prometheus.GaugeValue, float64(metrics.ContainersReserved), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.ContainersPending, prometheus.GaugeValue, float64(metrics.ContainersPending), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesTotal, prometheus.GaugeValue, float64(metrics.TotalNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesLost, prometheus.GaugeValue, float64(metrics.LostNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesUnhealthy, prometheus.GaugeValue, float64(metrics.UnhealthyNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesDecommissioned, prometheus.GaugeValue, float64(metrics.DecommissionedNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesDecommissioning, prometheus.GaugeValue, float64(metrics.DecommissioningNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesRebooted, prometheus.GaugeValue, float64(metrics.RebootedNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesActive, prometheus.GaugeValue, float64(metrics.ActiveNodes), labelValues...)
	ch <- prometheus.MustNewConstMetric(cc.NodesShutdown, prometheus.GaugeValue, float64(metrics.ShutdownNodes), labelValues...)

}

func NewClusterCollector(clusterEP *url.URL) *ClusterCollector {
	labels := new(ClusterCollector).labels()
	return &ClusterCollector{
		ClusterEndpoint: clusterEP,
		Up:              newFuncMetric("up", "Able to contact YARN", labels, nil),
		// cluster info metrics
		ApplicationsSubmitted: newFuncMetric("applications_submitted", "Total applications submitted", labels, nil),
		ApplicationsCompleted: newFuncMetric("applications_completed", "Total applications completed", labels, nil),
		ApplicationsPending:   newFuncMetric("applications_pending", "Applications pending", labels, nil),
		ApplicationsRunning:   newFuncMetric("applications_running", "Applications running", labels, nil),
		ApplicationsFailed:    newFuncMetric("applications_failed", "Total application failed", labels, nil),
		ApplicationsKilled:    newFuncMetric("applications_killed", "Total application killed", labels, nil),
		MemoryReserved:        newFuncMetric("memory_reserved", "Memory reserved", labels, nil),
		MemoryAvailable:       newFuncMetric("memory_available", "Memory available", labels, nil),
		MemoryAllocated:       newFuncMetric("memory_allocated", "Memory allocated", labels, nil),
		MemoryTotal:           newFuncMetric("memory_total", "Total memory", labels, nil),
		VirtualCoresReserved:  newFuncMetric("virtual_cores_reserved", "Virtual cores reserved", labels, nil),
		VirtualCoresAvailable: newFuncMetric("virtual_cores_available", "Virtual cores available", labels, nil),
		VirtualCoresAllocated: newFuncMetric("virtual_cores_allocated", "Virtual cores allocated", labels, nil),
		VirtualCoresTotal:     newFuncMetric("virtual_cores_total", "Total virtual cores", labels, nil),
		ContainersAllocated:   newFuncMetric("containers_allocated", "Containers allocated", labels, nil),
		ContainersReserved:    newFuncMetric("containers_reserved", "Containers reserved", labels, nil),
		ContainersPending:     newFuncMetric("containers_pending", "Containers pending", labels, nil),
		NodesTotal:            newFuncMetric("nodes_total", "Nodes total", labels, nil),
		NodesLost:             newFuncMetric("nodes_lost", "Nodes lost", labels, nil),
		NodesUnhealthy:        newFuncMetric("nodes_unhealthy", "Nodes unhealthy", labels, nil),
		NodesDecommissioned:   newFuncMetric("nodes_decommissioned", "Nodes decommissioned", labels, nil),
		NodesDecommissioning:  newFuncMetric("nodes_decommissioning", "Nodes decommissioning", labels, nil),
		NodesRebooted:         newFuncMetric("nodes_rebooted", "Nodes rebooted", labels, nil),
		NodesActive:           newFuncMetric("nodes_active", "Nodes active", labels, nil),
		NodesShutdown:         newFuncMetric("nodes_shutdown", "Nodes shutdown", labels, nil),
		ScrapeFailures:        newFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics", labels, nil),
	}
}

func (cc *ClusterCollector) fetch(u *url.URL) (*metrics, error) {
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

	var c Cluster
	err = json.NewDecoder(resp.Body).Decode(&c)
	if err != nil {
		return nil, err
	}

	return &c.ClusterMetrics, nil
}
