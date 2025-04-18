from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class ExchangeConfig:
    exchange_name: str
    config_data: Dict[str, Any]
    
    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.config_data["kafka"]["bootstrap_servers"]
    
    @property
    def database_config(self) -> Dict[str, Any]:
        return self.config_data["database"]
    
    def get_topic(self, topic_type: str) -> str:
        topics = self.config_data.get("topics", {})
        return topics.get(topic_type, f"{self.exchange_name}.trades.{topic_type}")
    
    def get_url(self, endpoint: str) -> str:
        urls = self.config_data.get("urls", {})
        return urls[endpoint]
    
    @property
    def highload_pairs(self) -> list:
        return self.config_data.get("highload_pairs", [])