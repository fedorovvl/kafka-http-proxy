import os
import json
import yaml
from typing import List, Optional
from dataclasses import dataclass, field

@dataclass
class TopicConfig:
    request_topic: str
    response_topic: str
    backend_url: str
    consumer_group: str = "default-group"
    timeout: int = 30
    max_retries: int = 3
    rate_limit: Optional[int] = None

@dataclass
class ProcessorConfig:
    bootstrap_servers: str
    topics: List[TopicConfig] = field(default_factory=list)
    max_concurrent_requests: int = 100
    health_check_interval: int = 30
    dead_letter_topic: Optional[str] = None
    management_port: int = 0

class ConfigLoader:
    @staticmethod
    def from_env() -> ProcessorConfig:
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        
        topics_str = os.getenv('KAFKA_TOPICS', '')
        topics = []
        
        if topics_str:
            for topic_def in topics_str.split(';'):
                parts = topic_def.split(':')
                if len(parts) >= 2:
                    name = parts[0]
                    config_parts = parts[1].split(',')
                    if len(config_parts) >= 3:
                        topics.append(TopicConfig(
                            request_topic=config_parts[0],
                            response_topic=config_parts[1],
                            backend_url=config_parts[2],
                            consumer_group=os.getenv(f'CONSUMER_GROUP_{name.upper()}', f'processor-{name}'),
                            timeout=int(config_parts[3]) if len(config_parts) > 3 else 30,
                            max_retries=int(config_parts[4]) if len(config_parts) > 4 else 3,
                            rate_limit=int(config_parts[5]) if len(config_parts) > 5 else None
                        ))
        
        return ProcessorConfig(
            bootstrap_servers=bootstrap_servers,
            topics=topics,
            max_concurrent_requests=int(os.getenv('MAX_CONCURRENT_REQUESTS', '100')),
            dead_letter_topic=os.getenv('DEAD_LETTER_TOPIC'),
            management_port=int(os.getenv('MANAGEMENT_PORT', '0'))
        )
    
    @staticmethod
    def from_yaml(file_path: str) -> ProcessorConfig:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        
        kafka_config = data.get('kafka', {})
        topics_config = data.get('topics', [])
        
        topics = []
        for topic_def in topics_config:
            topics.append(TopicConfig(
                request_topic=topic_def['request_topic'],
                response_topic=topic_def.get('response_topic', f"{topic_def['request_topic']}-responses"),
                backend_url=topic_def['backend_url'],
                consumer_group=topic_def.get('consumer_group', f"processor-{topic_def['request_topic']}"),
                timeout=topic_def.get('timeout', 30),
                max_retries=topic_def.get('max_retries', 3),
                rate_limit=topic_def.get('rate_limit')
            ))
        
        return ProcessorConfig(
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'kafka:9092'),
            topics=topics,
            max_concurrent_requests=data.get('max_concurrent_requests', 100),
            health_check_interval=data.get('health_check_interval', 30),
            dead_letter_topic=data.get('dead_letter_topic'),
            management_port=data.get('management_port', 0)
        )
    
    @staticmethod
    def from_json(file_path: str) -> ProcessorConfig:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        kafka_config = data.get('kafka', {})
        topics_config = data.get('topics', [])
        
        topics = []
        for topic_def in topics_config:
            topics.append(TopicConfig(
                request_topic=topic_def['request_topic'],
                response_topic=topic_def.get('response_topic', f"{topic_def['request_topic']}-responses"),
                backend_url=topic_def['backend_url'],
                consumer_group=topic_def.get('consumer_group', f"processor-{topic_def['request_topic']}"),
                timeout=topic_def.get('timeout', 30),
                max_retries=topic_def.get('max_retries', 3),
                rate_limit=topic_def.get('rate_limit')
            ))
        
        return ProcessorConfig(
            bootstrap_servers=kafka_config.get('bootstrap_servers', 'kafka:9092'),
            topics=topics,
            max_concurrent_requests=data.get('max_concurrent_requests', 100),
            health_check_interval=data.get('health_check_interval', 30),
            dead_letter_topic=data.get('dead_letter_topic'),
            management_port=data.get('management_port', 0)
        )