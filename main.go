package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"yarn-prometheus-exporter/yarn"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr string
	cep  *url.URL
	aep  *url.URL
	sep  *url.URL
)

func main() {
	loadEnv()
	c := yarn.NewClusterCollector(cep)
	s := yarn.NewSchedulerCollector(sep)
	a := yarn.NewAppsCollector(aep)

	registry := prometheus.NewRegistry()
	registry.MustRegister(c, s, a)
	log.Println("监控服务已启动...")
	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	log.Fatal(http.ListenAndServe(addr, nil))
}

func loadEnv() {
	log.Println("env 加载中...")
	addr = getEnvOr("YARN_PROMETHEUS_LISTEN_ADDR", ":9113")

	scheme := getEnvOr("YARN_PROMETHEUS_ENDPOINT_SCHEME", "http")
	host := getEnvOr("YARN_PROMETHEUS_ENDPOINT_HOST", "localhost")
	port := getEnvOr("YARN_PROMETHEUS_ENDPOINT_PORT", "8088")
	clusterPath := getEnvOr("YARN_CLUSTER_PROMETHEUS_ENDPOINT_PATH", "ws/v1/cluster/metrics")
	appsPath := getEnvOr("YARN_APPS_PROMETHEUS_ENDPOINT_PATH", "ws/v1/cluster/apps")
	schedulerPath := getEnvOr("YARN_SCHEDULER_PROMETHEUS_ENDPOINT_PATH", "ws/v1/cluster/scheduler")

	clusterUrl := scheme + "://" + host + ":" + port + "/" + clusterPath
	appsUrl := scheme + "://" + host + ":" + port + "/" + appsPath
	schedulerUrl := scheme + "://" + host + ":" + port + "/" + schedulerPath

	clusterEP, err := url.Parse(clusterUrl)
	appsEP, err := url.Parse(appsUrl)
	schedulerEP, err := url.Parse(schedulerUrl)
	if err != nil {
		log.Fatal(err)
	}

	cep = clusterEP
	aep = appsEP
	sep = schedulerEP
	log.Println("env 加载完成...")
}

func getEnvOr(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}
