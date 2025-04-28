import json
import asyncio
import logging
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError, KafkaConnectionError
from core.domain.trade import RawTrade, AggregatedTrade
from core.interfaces.iproducer import ITradeProducer

class KafkaProducerClient(ITradeProducer):
    def __init__(self, bootstrap_servers: str, raw_topic: str, agg_topic: str, **kwargs):
        self.logger = kwargs.get('logger') or logging.getLogger(__name__)  # Initialize logger
        self.producer = AIOKafkaProducer(            
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=kwargs.get('linger_ms', 200),  # Increased linger_ms to 200ms
            request_timeout_ms=kwargs.get('request_timeout_ms', 30000),  # Increased timeout to 30 seconds
            max_request_size=kwargs.get('max_request_size', 1048576)
        )
        self.raw_topic = raw_topic
        self.agg_topic = agg_topic

    async def check_kafka_readiness(self):
        """Check if Kafka server is running and the topic exists."""
        try:
            metadata = await self.producer.client._client.fetch_all_metadata()
            if self.raw_topic not in metadata.topics:
                self.logger.error(f"Kafka topic '{self.raw_topic}' does not exist.")
                raise RuntimeError(f"Kafka topic '{self.raw_topic}' does not exist.")
            if self.agg_topic not in metadata.topics:
                self.logger.error(f"Kafka topic '{self.agg_topic}' does not exist.")
                raise RuntimeError(f"Kafka topic '{self.agg_topic}' does not exist.")
            self.logger.info("Kafka server is running and topics are available.")
        except KafkaConnectionError as e:
            self.logger.error(f"Failed to connect to Kafka server: {e}")
            raise RuntimeError("Kafka server is not running.") from e

    async def initialize(self):
        try:
            self.logger.info("Initializing Kafka producer...")
            await self.producer.start()
            await self.check_kafka_readiness()  # Ensure Kafka readiness
            self.logger.info("Kafka producer initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    async def produce(self, trade: RawTrade):
        if not self.producer._closed:  # Ensure producer is started
            self.logger.debug("Kafka producer is ready to send messages")
        else:
            self.logger.error("Kafka producer is not initialized or has been closed")
            raise RuntimeError("Kafka producer is not initialized or has been closed")

        retries = 3  # Increased retries
        last_exception = None  # Track the last exception
        for attempt in range(retries):
            try:
                message = {
                    'price': trade.price,
                    'quantity': trade.quantity,
                    'timestamp': trade.timestamp.isoformat(),
                    'is_buyer_maker': trade.is_buyer_maker,
                    'trade_id': trade.trade_id
                }
                self.logger.debug(f"Sending message to topic {self.raw_topic}: {message} (attempt {attempt + 1})")
                await asyncio.wait_for(
                    self.producer.send(
                        topic=self.raw_topic,
                        key=trade.symbol.encode(),
                        value=message
                    ),
                    timeout=15  # Increased timeout from 10 to 15 seconds
                )
                self.logger.info(f"Message sent successfully to topic {self.raw_topic}")
                return  # Exit on success
            except asyncio.TimeoutError as e:
                self.logger.error(f"Kafka timeout while sending message to topic {self.raw_topic} (attempt {attempt + 1})")
                last_exception = e
            except Exception as e:
                self.logger.error(f"Failed to send message to Kafka: {e}", exc_info=True)
                last_exception = e
        self.logger.error(f"Exhausted retries for Kafka message: {trade}")
        if last_exception:
            raise last_exception  # Raise the last exception

    async def produce_aggregated(self, trade: AggregatedTrade):
        await self.producer.send(
            topic=self.agg_topic,
            key=trade.symbol.encode(),
            value={
                'open': trade.open_price,
                'close': trade.close_price,
                'high': trade.high_price,
                'low': trade.low_price,
                'total_quantity': trade.total_quantity,
                'total_trades': trade.total_trades,
                'buy_quantity': trade.buy_quantity,
                'sell_quantity': trade.sell_quantity,
                'start_time': trade.start_time.isoformat(),
                'end_time': trade.end_time.isoformat()
            }
        )

    async def stop(self):
        if self.producer:
            try:
                await self.producer.stop()
                self.logger.info("Kafka producer stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping Kafka producer: {e}")
            finally:
                self.producer = None  # Ensure producer is dereferenced