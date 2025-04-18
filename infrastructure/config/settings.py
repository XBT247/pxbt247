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
    def __init__(self, exchange_name: str):
        self.exchange = exchange_name
        with open(f"config/{exchange_name}.json") as f:
            self.config = json.load(f)
        
    @property
    def kafka_config(self):
        return {
            'bootstrap_servers': self.config['kafka']['bootstrap_servers'],
            'raw_topic': f"{self.exchange}.trades.raw",
            'cache_topic': f"cache.{self.exchange}.trendaware"
        }