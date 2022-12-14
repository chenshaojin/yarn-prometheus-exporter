# YARN prometheus exporter

Export YARN metrics in [Prometheus](https://prometheus.io/) format.

# Build

Requires [Go](https://golang.org/doc/install). Tested with Go 1.9+.


    # CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build/install main.go
    # CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build/install main.go
    # CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build/install main.go
    go get
    go build -o yarn-prometheus-exporter .

# Run

The exporter can be configured using environment variables. These are the defaults:

    YARN_PROMETHEUS_LISTEN_ADDR=:9113
    YARN_PROMETHEUS_ENDPOINT_SCHEME=http
    YARN_PROMETHEUS_ENDPOINT_HOST=localhost
    YARN_PROMETHEUS_ENDPOINT_PORT=8088
    YARN_APPS_PROMETHEUS_ENDPOINT_PATH=ws/v1/cluster/apps
    YARN_CLUSTER_PROMETHEUS_ENDPOINT_PATH=ws/v1/cluster/metrics
    YARN_SCHEDULER_PROMETHEUS_ENDPOINT_PATH=ws/v1/cluster/scheduler

Run the exporter:

    ./yarn-prometheus-exporter

The metrics can be scraped from:

    http://localhost:9113/metrics

# Run using docker

Run using docker:

    docker run -p 9113:9113 pbweb/yarn-prometheus-exporter

Or using docker-compose:

    services:
        image: pbweb/yarn-prometheus-exporter
        restart: always
        environment:
            - "YARN_PROMETHEUS_ENDPOINT_HOST=yarn.hadoop.lan"
        ports:
            - "9113:9113"

# License

See [LICENSE.md](LICENSE.md)
