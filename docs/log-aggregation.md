# Log Aggregation Setup

Soroban Pulse emits structured JSON logs when `RUST_LOG_FORMAT=json`. This document covers integration with Loki/Grafana, ELK Stack, Datadog, and AWS CloudWatch.

## Enabling JSON Logs

```bash
RUST_LOG_FORMAT=json RUST_LOG=info ./soroban-pulse
```

Example log line:

```json
{
  "timestamp": "2026-06-27T03:00:00.000Z",
  "level": "INFO",
  "message": "Event indexed",
  "contract_id": "CABC...XYZ",
  "ledger": 54321,
  "target": "soroban_pulse::indexer"
}
```

---

## Loki + Grafana Integration

The repository ships a ready-to-use Loki stack in `docker-compose.observability.yml`
(Loki + Promtail + Grafana with provisioned datasources and dashboards).

### Starting the Full Stack

```bash
# Start the application together with Loki, Promtail and Grafana
docker-compose -f docker-compose.yml -f docker-compose.observability.yml up -d

# Follow logs of the observability services
docker-compose -f docker-compose.yml -f docker-compose.observability.yml logs -f loki promtail grafana
```

Make sure the application emits JSON logs so Promtail can parse them:

```yaml
# docker-compose.yml (soroban-pulse service)
environment:
  - RUST_LOG_FORMAT=json
  - RUST_LOG=info
```

Services exposed by the observability overlay:

| Service | Port | Purpose |
|---------|------|---------|
| `loki` | `3100` | Log storage / query API |
| `promtail` | `9080` | Log shipper (scrapes container logs) |
| `grafana` | `3000` | Dashboards and Explore UI |

### How Promtail Scrapes the Application

Promtail runs with access to the Docker container log directory and the Docker
socket, so it discovers the `soroban-pulse` container and tails its stdout/stderr:

```yaml
# promtail/config.yml (excerpt)
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
    relabel_configs:
      - source_labels: ['__meta_docker_container_name']
        regex: '/(.*)'
        target_label: container
      - source_labels: ['__meta_docker_container_label_com_docker_compose_service']
        target_label: service
    pipeline_stages:
      - json:
          expressions:
            level: level
            message: message
            target: target
            contract_id: contract_id
            ledger: ledger
            correlation_id: correlation_id
      - labels:
          level:
          target:
      - timestamp:
          source: timestamp
          format: RFC3339Nano
```

Each log line is stored in Loki with the labels `container`, `service`,
`level` and `target`, while the remaining JSON fields stay in the log body.

### Querying Logs in Grafana Explore

1. Open Grafana at <http://localhost:3000> (default credentials `admin` / `admin`).
2. Go to **Explore** and select the **Loki** datasource
   (provisioned from `docs/grafana/provisioning/datasources/loki.yml`).
3. Use LogQL queries such as:

```logql
# All logs from the application container
{container="soroban-pulse"}

# Only errors
{container="soroban-pulse", level="ERROR"}

# Filter by contract
{container="soroban-pulse"} | json | contract_id="CABC...XYZ"

# Ledger range
{container="soroban-pulse"} | json | ledger >= 50000 and ledger <= 60000

# Error rate over time
sum(count_over_time({container="soroban-pulse", level="ERROR"} [5m]))
```

### Logs Panel on the Grafana Dashboard

The provisioned dashboard (`docs/grafana/provisioning/dashboards/`) includes a
**Logs** panel backed by the Loki datasource. It shows the most recent
application logs and supports filtering by `level` and `contract_id` via the
dashboard variables. To add your own panel:

1. **Add** → **Visualization** → choose **Logs**.
2. Select the **Loki** datasource.
3. Enter a query, e.g. `{container="soroban-pulse"} |= ""`.
4. Save the dashboard.

---

## ELK Stack Integration

### Filebeat Configuration

```yaml
# filebeat.yml
filebeat.inputs:
  - type: container
    paths:
      - /var/lib/docker/containers/*/*.log
    processors:
      - decode_json_fields:
          fields: ["message"]
          target: ""
          overwrite_keys: true
      - add_fields:
          target: service
          fields:
            name: soroban-pulse

output.elasticsearch:
  hosts: ["http://elasticsearch:9200"]
  index: "soroban-pulse-%{+yyyy.MM.dd}"

setup.template.settings:
  index.number_of_shards: 1
```

### Logstash Pipeline

```ruby
# logstash/pipeline/soroban-pulse.conf
input {
  beats { port => 5044 }
}

filter {
  if [service][name] == "soroban-pulse" {
    json { source => "message" }
    date { match => ["timestamp", "ISO8601"] target => "@timestamp" }
    mutate {
      rename => { "level" => "log.level" }
      rename => { "target" => "log.logger" }
    }
  }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "soroban-pulse-%{+YYYY.MM.dd}"
  }
}
```

### Key Fields for Kibana

| Field | Type | Description |
|-------|------|-------------|
| `log.level` | keyword | INFO, WARN, ERROR, DEBUG |
| `contract_id` | keyword | Soroban contract identifier |
| `ledger` | long | Ledger sequence number |
| `correlation_id` | keyword | Request trace ID |
| `error` | text | Error message (when present) |

---

## Datadog Log Forwarding

### datadog-agent.yaml

```yaml
# /etc/datadog-agent/conf.d/soroban_pulse.d/conf.yaml
logs:
  - type: docker
    service: soroban-pulse
    source: rust
    tags:
      - env:production
      - team:indexer
    processing_rules:
      - type: multi_line
        name: new_log_start
        pattern: '^\{"timestamp"'
```

### Datadog Log Pipelines

Create a pipeline in Datadog with these processors:

1. **JSON parser** — parse the full log line as JSON
2. **Date remapper** — map `timestamp` → `@timestamp`
3. **Status remapper** — map `level` → log status (`INFO`→info, `WARN`→warning, `ERROR`→error)
4. **Attribute remapper** — map `target` → `logger.name`

### Useful Datadog Queries

```
service:soroban-pulse level:ERROR                        # All errors
service:soroban-pulse @contract_id:CABC*                 # Contract-specific logs
service:soroban-pulse @ledger:[50000 TO 60000]           # Ledger range
service:soroban-pulse @correlation_id:<id>               # Request trace
```

---

## AWS CloudWatch Integration

### CloudWatch Agent Configuration

```json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/soroban-pulse/*.log",
            "log_group_name": "/soroban-pulse/application",
            "log_stream_name": "{instance_id}",
            "timestamp_format": "%Y-%m-%dT%H:%M:%S",
            "timezone": "UTC",
            "multi_line_start_pattern": "^\\{\"timestamp\""
          }
        ]
      }
    }
  }
}
```

### Kubernetes: Fluent Bit → CloudWatch

```yaml
# fluent-bit-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
data:
  fluent-bit.conf: |
    [SERVICE]
        Flush        5
        Log_Level    info

    [INPUT]
        Name             tail
        Path             /var/log/containers/soroban-pulse*.log
        Parser           docker
        Tag              soroban.pulse.*
        Refresh_Interval 5

    [FILTER]
        Name    parser
        Match   soroban.pulse.*
        Key_Name log
        Parser  json

    [OUTPUT]
        Name              cloudwatch_logs
        Match             soroban.pulse.*
        region            us-east-1
        log_group_name    /soroban-pulse/application
        log_stream_prefix soroban-pulse-
        auto_create_group true
```

### CloudWatch Insights Queries

```sql
-- Error rate over time
fields @timestamp, level, message, error
| filter level = "ERROR"
| stats count() as error_count by bin(5m)
| sort @timestamp desc

-- Top error messages
fields message, error
| filter level = "ERROR"
| stats count() as occurrences by message
| sort occurrences desc
| limit 20

-- Indexer lag events
fields @timestamp, ledger, message
| filter message like /indexer lag/
| sort @timestamp desc
```

---

## Structured Log Parsing Reference

All Soroban Pulse log fields follow the conventions in `docs/logging.md`.

| Field | Present When | Example |
|-------|-------------|---------|
| `timestamp` | Always | `2026-06-27T03:00:00Z` |
| `level` | Always | `INFO` |
| `message` | Always | `Event indexed` |
| `target` | Always | `soroban_pulse::indexer` |
| `contract_id` | Event context | `CABC...XYZ` |
| `tx_hash` | Event context | `a1b2c3...` |
| `ledger` | Event context | `54321` |
| `error` | On errors/warnings | `connection timeout` |
| `correlation_id` | HTTP requests | `550e8400-e29b-41d4-a716` |
| `attempt` | Retries | `2` |
