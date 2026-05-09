# Kafka HTTP Proxy

Synchronous HTTP to Kafka proxy with request-reply pattern. Forwards HTTP requests through Apache Kafka and waits for synchronous responses.

## Installation

```bash
pip install git+https://github.com/fedorovvl/kafka-http-proxy.git
```
Or from PyPI:
```bash

pip install kafka-http-proxy
```
## Quick Start
Proxy (Server A)

Receives HTTP requests and forwards them to Kafka, waiting for synchronous responses.
```bash

# Environment variables
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export WARMUP_TOPICS=common-responses,orders-responses
export PROXY_PORT=8080

kafka-proxy
```
Or with config file:
```bash

kafka-proxy --config proxy_config.yaml
```
Processor (Server B)

Consumes messages from Kafka and forwards them to backend services.
```bash

# Environment variables
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export KAFKA_TOPICS=orders:orders-requests,orders-responses,http://backend:8080

kafka-processor
```
Or with config file:
```bash

kafka-processor --config processor_config.yaml
```
## Single binary mode

Use MODE environment variable to switch between proxy and processor:
bash

# Run as proxy
MODE=proxy kafka-http-proxy

# Run as processor
MODE=processor kafka-http-proxy

Architecture
```text

Client → Nginx → Proxy → Kafka → Processor → Backend
                    ↑________________________↓
                    (synchronous response via Kafka)
```
    Client sends HTTP request to Nginx
    Nginx forwards to Proxy with topic headers
    Proxy publishes request to Kafka request topic
    Processor consumes request, forwards to backend
    Backend responds to processor
    Processor publishes response to Kafka response topic
    Proxy receives response and returns to client

## Configuration
#### Proxy config (proxy_config.yaml)
```yaml

kafka:
  bootstrap_servers: kafka:9092

port: 8080
default_timeout: 30
warmup_topics:
  - orders-responses
  - users-responses
  - common-responses
```

| Variable |Env |Default |Description |
| - | - | - | - |
| kafka.bootstrap_servers | KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | Kafka broker address |
| port | PROXY_PORT | 8080 | HTTP listen port |
| default_timeout | DEFAULT_TIMEOUT | 30 | Request timeout in seconds |
| warmup_topics |	WARMUP_TOPICS |	common-responses |	Topics to subscribe at startup
			
#### Processor config (processor_config.yaml)
```yaml

kafka:
  bootstrap_servers: kafka:9092

topics:
  - request_topic: orders-requests
    response_topic: orders-responses
    backend_url: http://orders-backend:8080
    consumer_group: orders-processors
    timeout: 30
    max_retries: 3
    rate_limit: 100

  - request_topic: users-requests
    response_topic: users-responses
    backend_url: http://users-backend:8080
    consumer_group: users-processors
    timeout: 25
    max_retries: 2

max_concurrent_requests: 200
health_check_interval: 30
dead_letter_topic: failed-requests
management_port: 8081
```
| Variable |Env |Default |Description |
| - | - | - | - |
| kafka.bootstrap_servers | KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | Kafka broker address | 
| topics[].request_topic | KAFKA_TOPICS | - | Kafka topic for requests |
| topics[].response_topic | - | {request_topic}-responses | Kafka topic for responses |
| topics[].backend_url | - | - | Backend service URL |
| topics[].consumer_group | - | processor-{request_topic} | Consumer group ID |
| topics[].timeout | - | 30 | Backend request timeout |
| topics[].max_retries | - | 3 | Max retry attempts |
| topics[].rate_limit | - | - | Messages per second limit |
| max_concurrent_requests | MAX_CONCURRENT_REQUESTS | 100 | Concurrent request limit |
| health_check_interval | - | 30 | Backend health check interval |
| dead_letter_topic	| DEAD_LETTER_TOPIC | - | Failed messages topic |
| management_port | MANAGEMENT_PORT | 0 | Management API port (0 = disabled) |

### Environment variables for topics

Use semicolons to separate topics, colons for name, commas for config:
```bash

KAFKA_TOPICS=orders:orders-requests,orders-responses,http://orders:8080,30,3;users:users-requests,users-responses,http://users:8080,25,2
```
Format: **name:request_topic,response_topic,backend_url[,timeout[,max_retries[,rate_limit]]]**

## Nginx Configuration
```nginx

upstream proxy_backend {
    server proxy:8080;
    keepalive 100;
}

server {
    listen 80;
    
    location /api/orders/ {
        proxy_pass http://proxy_backend;
        proxy_set_header X-Kafka-Topic "orders-requests";
        proxy_set_header X-Kafka-Reply-Topic "orders-responses";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 35s;
    }
    
    location /api/users/ {
        proxy_pass http://proxy_backend;
        proxy_set_header X-Kafka-Topic "users-requests";
        proxy_set_header X-Kafka-Reply-Topic "users-responses";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 35s;
    }
    
    location / {
        proxy_pass http://proxy_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 35s;
    }
}
```
Proxy determines Kafka topics from HTTP headers:
-
    X-Kafka-Topic — request topic (default: common-requests)
    X-Kafka-Reply-Topic — response topic (default: common-responses)

## Docker
```yaml

services:
  proxy:
    build: .
    environment:
      - MODE=proxy
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
      - WARMUP_TOPICS=common-responses,orders-responses
    ports:
      - "8080:8080"
  
  processor:
    build: .
    environment:
      - MODE=processor
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
      - KAFKA_TOPICS=orders:orders-requests,orders-responses,http://orders:8080
```
# Scaling
### Processor

Kafka distributes partitions across consumers in the same group. Create topics with enough partitions:
```bash

kafka-topics --create \
  --topic orders-requests \
  --partitions 10 \
  --bootstrap-server kafka:9092
```
Rule: partitions >= max processor instances
### Proxy

Run multiple proxy instances behind load balancer. Each proxy uses unique consumer group to receive all responses.
Auto-created topics

Configure Kafka to create topics automatically:
```yaml

KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
KAFKA_NUM_PARTITIONS: 10
KAFKA_DEFAULT_REPLICATION_FACTOR: 1
```
# Metrics

Prometheus metrics available at /metrics:

Proxy metrics
-
    proxy_requests_total — request count by method, topic, status
    proxy_request_latency_seconds — request latency histogram
    proxy_kafka_latency_seconds — Kafka round-trip latency
    proxy_pending_requests — pending requests gauge
    proxy_timeouts_total — timeout counter

Processor metrics
-
    processor_messages_total — processed messages by topic, status
    processor_latency_seconds — processing latency histogram
    processor_backend_latency_seconds — backend request latency
    processor_active_consumers — active consumers gauge
    processor_messages_in_flight — messages being processed
    processor_backend_errors_total — backend error counter

## Management API

Enable with MANAGEMENT_PORT=8081:
```bash

# Get status
curl http://localhost:8081/status
```

## Supported HTTP Methods

All standard HTTP methods are forwarded:
-
    GET, POST, PUT, DELETE, PATCH
    OPTIONS, HEAD
    Custom methods

Request body, headers, and query parameters are preserved.

Error Handling
-
    504 Gateway Timeout — backend request timed out
    500 Internal Server Error — processing failed
    Dead Letter Queue — failed messages sent to dead_letter_topic
    Retry with backoff — automatic retries for transient failures

Requirements
-
    Python 3.9+
    Apache Kafka 2.8+
    aiohttp (for processor)