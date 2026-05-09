import os
import json
import yaml
from typing import List
from dataclasses import dataclass, field

@dataclass
class ProxyConfig:
    bootstrap_servers: str = "kafka:9092"
    port: int = 8080
    default_timeout: int = 30
    warmup_topics: List[str] = field(default_factory=lambda: ["common-responses"])

class ConfigLoader:
    @staticmethod
    def from_env() -> ProxyConfig:
        warmup_str = os.getenv('WARMUP_TOPICS', 'common-responses')
        return ProxyConfig(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            port=int(os.getenv('PROXY_PORT', '8080')),
            default_timeout=int(os.getenv('DEFAULT_TIMEOUT', '30')),
            warmup_topics=warmup_str.split(',')
        )
    
    @staticmethod
    def from_yaml(file_path: str) -> ProxyConfig:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        
        return ProxyConfig(
            bootstrap_servers=data.get('kafka', {}).get('bootstrap_servers', 'kafka:9092'),
            port=data.get('port', 8080),
            default_timeout=data.get('default_timeout', 30),
            warmup_topics=data.get('warmup_topics', ['common-responses'])
        )
    
    @staticmethod
    def from_json(file_path: str) -> ProxyConfig:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        return ProxyConfig(
            bootstrap_servers=data.get('kafka', {}).get('bootstrap_servers', 'kafka:9092'),
            port=data.get('port', 8080),
            default_timeout=data.get('default_timeout', 30),
            warmup_topics=data.get('warmup_topics', ['common-responses'])
        )