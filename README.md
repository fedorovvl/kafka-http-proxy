# Kafka HTTP Proxy

Синхронный прокси для передачи HTTP запросов через Apache Kafka с паттерном request-reply.

## Установка

```bash
pip install git+https://github.com/fedorovvl/kafka-http-proxy.git
```

## Быстрый старт
Прокси (сервер А)

```bash
# Переменные окружения
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export WARMUP_TOPICS=common-responses,orders-responses
export PROXY_PORT=8080

kafka-proxy
```
Или с конфиг файлом:
```bash
kafka-proxy --config proxy_config.yaml
```

Обработчик (сервер Б)
```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export KAFKA_TOPICS=orders:orders-requests,orders-responses,http://backend:8080
kafka-processor
```

## Архитектура
```text

Клиент → Nginx → Прокси → Kafka → Обработчик → Бекенд
                        ↑________________________↓
                        (синхронный ответ через Kafka)
```
## Docker
```bash
cd examples
docker-compose up -d
```
## Метрики

Prometheus метрики доступны на /metrics:
- 
    proxy_requests_total - количество запросов
    proxy_request_latency_seconds - латентность запросов
    proxy_pending_requests - ожидающие запросы
    processor_messages_total - обработанные сообщения
    processor_latency_seconds - латентность обработки