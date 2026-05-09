import asyncio
import json
import os
import yaml
import aiohttp
import logging
import time as time_module
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from pathlib import Path
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi.responses import Response
from .config import ConfigLoader, ProcessorConfig, TopicConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MESSAGES_PROCESSED = Counter('processor_messages_total', 'Total messages processed', ['topic', 'status'])
PROCESSING_LATENCY = Histogram('processor_latency_seconds', 'Message processing latency', ['topic'])
BACKEND_LATENCY = Histogram('processor_backend_latency_seconds', 'Backend request latency', ['topic', 'backend'])
ACTIVE_CONSUMERS_GAUGE = Gauge('processor_active_consumers', 'Number of active consumers')
MESSAGES_IN_FLIGHT = Gauge('processor_messages_in_flight', 'Messages currently being processed')
BACKEND_ERRORS = Counter('processor_backend_errors_total', 'Backend errors', ['topic', 'backend'])
    
class DynamicTopicProcessor:
    def __init__(self, config: ProcessorConfig):
        self.config = config
        self.producer = None
        self.consumers = []
        self.semaphore = None
        self.running = False
        self.session = None
		self.topic_configs: Dict[str, TopicConfig] = {}
        self.active_tasks = 0
        self.task_lock = asyncio.Lock()
        
        
        for topic_cfg in config.topics:
            self.topic_configs[topic_cfg.request_topic] = topic_cfg
        
    async def start(self):
        self.running = True
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            compression_type='gzip',
            max_request_size=1048576  # 1MB
        )
        await self.producer.start()
        
        self.session = aiohttp.ClientSession()
        
        await self._start_consumers()
        
        if self.config.health_check_interval > 0:
            asyncio.create_task(self._health_check())
        
        logger.info(f"Processor started with {len(self.config.topics)} topic configurations")
        for topic_cfg in self.config.topics:
            logger.info(f"  Listening: {topic_cfg.request_topic} -> {topic_cfg.backend_url}")
    
    async def _start_consumers(self):
        group_topics: Dict[str, List[str]] = {}
        for topic_cfg in self.config.topics:
            if topic_cfg.consumer_group not in group_topics:
                group_topics[topic_cfg.consumer_group] = []
            group_topics[topic_cfg.consumer_group].append(topic_cfg.request_topic)
        
        for group_id, topics in group_topics.items():
            consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.config.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=10,  # Берем не больше 10 сообщений за раз
                max_poll_interval_ms=300000  # 5 минут на обработку
            )
            
            await consumer.start()
            self.consumers.append(consumer)
            
            asyncio.create_task(self._process_consumer(consumer))
            
            logger.info(f"Started consumer group '{group_id}' for topics: {topics}")
        ACTIVE_CONSUMERS_GAUGE.set(len(self.consumers))
        
    async def _process_consumer(self, consumer: AIOKafkaConsumer):
        try:
            async for msg in consumer:
                async with self.semaphore:
                    async with self.task_lock:
                        self.active_tasks += 1
                        MESSAGES_IN_FLIGHT.set(self.active_tasks)
                    
                    try:
                        start_time = time_module.time()
                        await self._process_message(msg)
                        
                        MESSAGES_PROCESSED.labels(topic=msg.topic, status='success').inc()
                        PROCESSING_LATENCY.labels(topic=msg.topic).observe(time_module.time() - start_time)
                        
                        await consumer.commit()
                        
                    except Exception as e:
                        MESSAGES_PROCESSED.labels(topic=msg.topic, status='error').inc()
                        logger.error(f"Error processing message from {msg.topic}: {e}")
                        await self._handle_failed_message(msg, e)
                        await consumer.commit()
                        
                    finally:
                        async with self.task_lock:
                            self.active_tasks -= 1
                            MESSAGES_IN_FLIGHT.set(self.active_tasks)
                            
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            if self.running:
                await asyncio.sleep(5)
                asyncio.create_task(self._process_consumer(consumer))
    
    async def _process_message(self, msg):
        correlation_id = msg.value.get('correlation_id')
        
        if not correlation_id:
            logger.warning(f"Message without correlation_id from {msg.topic}, skipping")
            return
        
        topic_config = self.topic_configs.get(msg.topic)
        if not topic_config:
            backend_url = msg.value.get('backend_url', 'http://localhost:8080')
            topic_config = TopicConfig(
                request_topic=msg.topic,
                response_topic=msg.value.get('response_topic', f"{msg.topic}-responses"),
                backend_url=backend_url
            )
            logger.warning(f"Using dynamic config for unknown topic {msg.topic}")
        
        response_topic = msg.value.get('response_topic') or topic_config.response_topic
        
        for attempt in range(topic_config.max_retries):
            try:
                logger.info(f"Processing {correlation_id} from {msg.topic} (attempt {attempt + 1})")
                
                if topic_config.rate_limit:
                    await asyncio.sleep(1.0 / topic_config.rate_limit)
                
                response_data = await self._forward_to_backend(
                    msg.value, topic_config.backend_url, topic_config.timeout
                )
                
                await self._send_response(correlation_id, response_topic, response_data)
                
                logger.info(f"Successfully processed {correlation_id}")
                return
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {correlation_id}: {e}")
                if attempt == topic_config.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
    
    async def _forward_to_backend(self, request_data: dict, backend_url: str, timeout: int) -> dict:
        start_time = time_module.time()
        method = request_data.get('method', 'GET').upper()
        path = request_data.get('path', '/')
        headers = request_data.get('headers', {})
        body = request_data.get('body')
        params = request_data.get('query_params', {})
        correlation_id = request_data.get('correlation_id', '')
        
        logger.debug(f"Forwarding {method} {path} to {backend_url} [corr_id: {correlation_id}]")
        
        forbidden_headers = [
            'Host', 'Content-Length', 'Transfer-Encoding', 'Connection',
            'X-Kafka-Topic', 'X-Kafka-Reply-Topic', 'X-Forwarded-For',
            'Content-Encoding'
        ]
        clean_headers = {k: v for k, v in headers.items() if k not in forbidden_headers}
        
        clean_headers['X-Request-ID'] = correlation_id
        clean_headers['X-Forwarded-For'] = request_data.get('client_ip', 'unknown')
        clean_headers['X-Original-Method'] = method
        
        url = f"{backend_url.rstrip('/')}{path}"
        
        methods_with_body = ['POST', 'PUT', 'PATCH', 'DELETE']
        should_send_body = method in methods_with_body and body is not None
        
        try:
            request_kwargs = {
                'method': method,
                'url': url,
                'headers': clean_headers,
                'params': params,
                'timeout': aiohttp.ClientTimeout(total=timeout - 5)
            }
            
            if should_send_body:
                request_kwargs['data'] = body
                
                if isinstance(body, str):
                    try:
                        json.loads(body)
                        if 'Content-Type' not in clean_headers:
                            request_kwargs['headers']['Content-Type'] = 'application/json'
                    except (json.JSONDecodeError, ValueError):
                        pass
            
            async with self.session.request(**request_kwargs) as response:
                response_body = await response.text()
                
                try:
                    response_body = json.loads(response_body)
                except (json.JSONDecodeError, ValueError):
                    pass
                
                response_data = {
                    'status_code': response.status,
                    'headers': dict(response.headers),
                    'body': response_body
                }
                
                logger.info(
                    f"Backend response: {method} {path} -> "
                    f"{response.status} [corr_id: {correlation_id}]"
                )
                BACKEND_LATENCY.labels(topic=request_data.get('request_topic', 'unknown'),		    backend=backend_url).observe(time_module.time() - start_time)
                return response_data
                
        except aiohttp.ClientError as e:
            BACKEND_ERRORS.labels(topic=request_data.get('request_topic', 'unknown'), backend=backend_url).inc()
            logger.error(f"HTTP error forwarding {method} {path}: {e}")
            raise
        except Exception as e:
            BACKEND_ERRORS.labels(
                topic=request_data.get('request_topic', 'unknown'),
                backend=backend_url
            ).inc()
            logger.error(f"Unexpected error forwarding {method} {path}: {e}")
            raise
    
    async def _send_response(self, correlation_id: str, response_topic: str, response_data: dict):
        response_message = {
            'correlation_id': correlation_id,
            'timestamp': asyncio.get_event_loop().time(),
            **response_data
        }
        
        await self.producer.send_and_wait(
            response_topic,
            response_message,
            headers=[('correlation_id', correlation_id.encode())]
        )
        logger.debug(f"Sent response for {correlation_id} to {response_topic}")
    
    async def _handle_failed_message(self, msg, error: Exception):
        if self.config.dead_letter_topic:
            failed_message = {
                'original_message': msg.value,
                'error': str(error),
                'topic': msg.topic,
                'partition': msg.partition,
                'offset': msg.offset,
                'timestamp': asyncio.get_event_loop().time()
            }
            
            await self.producer.send_and_wait(self.config.dead_letter_topic,failed_message)
            logger.info(f"Sent failed message to dead letter topic: {self.config.dead_letter_topic}")
    
    async def _health_check(self):
        while self.running:
            await asyncio.sleep(self.config.health_check_interval)
            
            for topic_cfg in self.config.topics:
                try:
                    async with self.session.get(
                        f"{topic_cfg.backend_url}/health",
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as response:
                        if response.status != 200:
                            logger.warning(f"Health check failed for {topic_cfg.backend_url}: {response.status}")
                except Exception as e:
                    logger.error(f"Health check error for {topic_cfg.backend_url}: {e}")
    
    async def close(self):
        self.running = False
        
        if self.session:
            await self.session.close()
        
        for consumer in self.consumers:
            await consumer.stop()
        
        if self.producer:
            await self.producer.stop()
        
        logger.info("Processor stopped")

def create_management_api(processor: DynamicTopicProcessor):
    app = FastAPI(title="Kafka Processor Manager")

    @app.get("/metrics")
    async def metrics():
        return Response(content=generate_latest(), media_type="text/plain")
    
    @app.get("/status")
    async def status():
        return {
            'running': processor.running,
            'configured_topics': len(processor.topic_configs),
            'active_consumers': len(processor.consumers),
            'topics': {
                topic: {
                    'backend_url': cfg.backend_url,
                    'response_topic': cfg.response_topic
                }
                for topic, cfg in processor.topic_configs.items()
            }
        }
    return app

async def main():
    
    config_source = os.getenv('CONFIG_SOURCE', 'env')
    config_path = os.getenv('CONFIG_PATH', 'config.yaml')
    
    if config_source == 'yaml':
        config = ConfigLoader.from_yaml(config_path)
    elif config_source == 'json':
        config = ConfigLoader.from_json(config_path)
    else:
        config = ConfigLoader.from_env()
    if not config.topics:
        logger.error("No topics configured!")
        sys.exit(1)
    
    processor = DynamicTopicProcessor(config)
    
    async def run():
        await processor.start()
        
        if config.management_port > 0:
            import uvicorn
            app = create_management_api(processor)
            uvicorn_config = uvicorn.Config(app, host="0.0.0.0", port=config.management_port, log_level="info")
            server = uvicorn.Server(uvicorn_config)
            await server.serve()
        else:
            while processor.running:
                await asyncio.sleep(1)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        asyncio.run(processor.close())

if __name__ == "__main__":
    main()