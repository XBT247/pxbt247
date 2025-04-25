# infrastructure/config/settings.py
import json

import json
from pathlib import Path
from typing import Dict, Any

class ConfigLoader:
    _configs: Dict[str, Any] = {}
    
    @classmethod
    def load(cls, exchange: str):
        if exchange not in cls._configs:
            base_path = Path(__file__).parent
            # Load base config
            with open(base_path / "base.json") as f:
                base = json.load(f)
            
            # Load exchange-specific config
            try:
                with open(base_path / f"{exchange}.json") as f:
                    exchange_cfg = json.load(f)
            except FileNotFoundError:
                exchange_cfg = {}
                
            cls._configs[exchange] = {**base, **exchange_cfg}
            
        return cls._configs[exchange]

def load_config(exchange: str) -> Dict[str, Any]:
    return ConfigLoader.load(exchange)

class ExchangeConfig:
    def __init__(self, exchange_name: str, config_data: dict):
        self.exchange_name = exchange_name
        self.config = config_data
        
    def get_url(self, endpoint: str) -> str:
        return self.config['urls'].get(endpoint)
        
    def get_topic(self, topic_type: str) -> str:
        return self.config['topics'].get(topic_type)
        
    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.config['kafka']['bootstrap_servers']