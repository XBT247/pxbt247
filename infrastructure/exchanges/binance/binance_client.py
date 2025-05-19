import aiohttp
import logging
from typing import Dict, Any
from core.interfaces.exchanges.ibinance_client import IBinanceClient

logger = logging.getLogger(__name__)

class BinanceClient(IBinanceClient):
    def __init__(self, api_key: str, secret_key: str, base_url: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url
        logger.info(f"BinanceClient initialized with base URL: {self.base_url}")

    async def get_exchange_info(self) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v3/exchangeInfo"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.json()

    async def get_account_info(self) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v3/account"
        headers = {"X-MBX-APIKEY": self.api_key}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()