#!/bin/sh
# Connect Prometheus and json-exporter to each big-data stack's Docker network
# that exists right now, so they can reach namenode:9870, hbase-master:16010,
# kafka-exporter:9308 ... by name. Safe to run any number of times; called by
# `make monitor-up`. Run it again (make monitor-attach) after starting a
# stack that was not running when monitoring came up.
set -eu
PROJECT=${MONITOR_PROJECT:-urbanflow-monitoring}
NETWORKS=${MONITOR_NETWORKS:-"urbanflow-hadoop urbanflow-hbase-net urbanflow-streaming"}

for service in prometheus json-exporter; do
    cid=$(docker compose -p "$PROJECT" -f docker-compose.monitoring.yml ps -q "$service")
    [ -n "$cid" ] || { echo "monitor: $service is not running (make monitor-up)"; exit 1; }
    for net in $NETWORKS; do
        if ! docker network inspect "$net" >/dev/null 2>&1; then
            [ "$service" = prometheus ] && echo "monitor: $net not found (stack not started) - skipped"
            continue
        fi
        if docker network inspect -f '{{range .Containers}}{{.Name}} {{end}}' "$net" | grep -q "$PROJECT-$service-"; then
            continue
        fi
        docker network connect "$net" "$cid"
        echo "monitor: attached $service to $net"
    done
done
