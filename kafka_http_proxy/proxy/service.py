import asyncio
import json
import os
import uuid
import yaml
import logging
import uvicorn
import time as time_module
from concurrent.futures import TimeoutError
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from .config import ConfigLoader, ProxyConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REQUEST_COUNT = Counter('proxy_requests_total', 'Total requests', ['method', 'topic', 'status'])
REQUEST_LATENCY = Histogram('proxy_request_latency_seconds', 'Request latency', ['method', 'topic'])
KAFKA_LATENCY = Histogram('proxy_kafka_latency_seconds', 'Kafka round-trip latency', ['topic'])
PENDING_REQUESTS = Gauge('proxy_pending_requests', 'Pending requests waiting for response')
ACTIVE_CONSUMERS = Gauge('proxy_active_consumers', 'Active response topic subscriptions')
TIMEOUT_COUNT = Counter('proxy_timeouts_total', 'Total timeouts', ['topic'])

class KafkaRequestReply:
    def __init__(self, bootstrap_servers, request_timeout_ms=30000):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None
        self.pending_requests = {}
        self.request_timeout_ms = request_timeout_ms
        self.responses = {}
        self.subscribed_topics = set()
        
    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            enable_idempotence=True,
            compression_type='gzip',
            request_timeout_ms=self.request_timeout_ms
        )
        await self.producer.start()

        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=f"proxy-{uuid.uuid4()}",
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        await self.consumer.start()
        asyncio.create_task(self._process_responses())

    async def _ensure_topic_subscription(self, response_topic):
        current_subscription = self.consumer.subscription()
        if current_subscription and response_topic in current_subscription:
            return
        new_topics = list(current_subscription) + [response_topic] if current_subscription else [response_topic]
        self.consumer.subscribe(topics=new_topics)
        self.subscribed_topics.add(response_topic)
        ACTIVE_CONSUMERS.set(len(self.subscribed_topics))
        logger.info(f"Subscribed to response topic: {response_topic}")

    async def _process_responses(self):
        async for msg in self.consumer:
            correlation_id = msg.value.get('correlation_id')
            if correlation_id and correlation_id in self.pending_requests:
                self.responses[correlation_id] = msg.value
                self.pending_requests[correlation_id].set()
                logger.info(f"Received response for {correlation_id}")

    async def request(self, request_data, request_topic, response_topic, timeout=30):
        correlation_id = str(uuid.uuid4())
        request_data['correlation_id'] = correlation_id

        await self._ensure_topic_subscription(response_topic)

        event = asyncio.Event()
        self.pending_requests[correlation_id] = event
        PENDING_REQUESTS.set(len(self.pending_requests))

        start_time = time_module.time()
        try:
            logger.info(f"Sent request {correlation_id} to {request_topic}")
            await self.producer.send_and_wait(
                request_topic,
                request_data,
                headers=[('correlation_id', correlation_id.encode())]
            )

            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                TIMEOUT_COUNT.labels(topic=request_topic).inc()
                raise TimeoutError(f"Request {correlation_id} timed out after {timeout}s")
            
            KAFKA_LATENCY.labels(topic=request_topic).observe(time_module.time() - start_time)
            
            response = self.responses.pop(correlation_id)
            return response

        finally:
            self.pending_requests.pop(correlation_id, None)
            self.responses.pop(correlation_id, None)
            PENDING_REQUESTS.set(len(self.pending_requests))

    async def close(self):
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()

kafka_client = None
config = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_client, config
    config_source = os.getenv('CONFIG_SOURCE', 'env')
    config_path = os.getenv('CONFIG_PATH', 'config.yaml')
    
    if config_source == 'yaml':
        config = ConfigLoader.from_yaml(config_path)
    elif config_source == 'json':
        config = ConfigLoader.from_json(config_path)
    else:
        config = ConfigLoader.from_env()
    kafka_client = KafkaRequestReply(bootstrap_servers=config.bootstrap_servers,request_timeout_ms=config.default_timeout * 1000)
    await kafka_client.start()
    if config.warmup_topics:
        for topic in config.warmup_topics:
            await kafka_client._ensure_topic_subscription(topic)
        logger.info(f"Warmed up with topics: {config.warmup_topics}")
    yield
    if kafka_client:
        await kafka_client.close()

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_request(request: Request, path: str):
    start_time = time_module.time()
    request_topic = request.headers.get('X-Kafka-Topic', 'common-requests')
    response_topic = request.headers.get('X-Kafka-Reply-Topic', 'common-responses')

    body = None
    if request.method in ["POST", "PUT", "PATCH", "DELETE"]:
        try:
            body_bytes = await request.body()
            if body_bytes:
                try:
                    body = body_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    import base64
                    body = base64.b64encode(body_bytes).decode('utf-8')
        except Exception as e:
            logger.error(f"Error reading body: {e}")

    kafka_message = {
        "method": request.method,
        "path": f"/{path}" if path else "/",
        "query_params": dict(request.query_params),
        "headers": dict(request.headers),
        "body": body,
        "client_ip": request.client.host,
        "timestamp": asyncio.get_event_loop().time(),
    }

    logger.info(f"Proxying {request.method} {kafka_message.path} to {request_topic}")

    try:
        response = await kafka_client.request(kafka_message, request_topic, response_topic, timeout=config.default_timeout)
        REQUEST_COUNT.labels(method=request.method,topic=request_topic,status='success').inc()
        REQUEST_LATENCY.labels(method=request.method, topic=request_topic).observe(time_module.time() - start_time)
        
        return Response(
            content=response.get('body'),
            status_code=response.get('status_code', 200),
            headers={k: v for k, v in response.get('headers', {}).items()
                     if k.lower() not in ['content-length', 'transfer-encoding']}
        )

    except TimeoutError:
        REQUEST_COUNT.labels(method=request.method,topic=request_topic,status='timeout').inc()
        raise HTTPException(status_code=504, detail="Backend timeout")
    except Exception as e:
        REQUEST_COUNT.labels(method=request.method,topic=request_topic,status='error').inc()
        logger.error(f"Error proxying request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    
def main():
    uvicorn.run(app, host="0.0.0.0", port=config.port if config else 8080)

if __name__ == "__main__":
    main()