import json
import logging
import aiomysql
import asyncio
from aiokafka.errors import KafkaError
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Configure the logger once
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class KafkaBase:
    def __init__(self):
        with open("config_binance.json") as config_file:
            self.config = json.load(config_file)
        self.awareShort = 25
        self.awareMed = 45
        self.awareLong = 75
        self.quote_currencies = {} # Dictionary to store quote currency symbols and their prices
        self.trading_pairs = {}  # Dictionary to store trading pairs
        self.producer = None  # Kafka producer instance
        self.logger = logger  # Use the shared logger
        self.highload_pairs = ['btcusdt', 'ethusdt', 'xrpusdt', 'bnbusdt', 'usdcusdt', 'btcfdusd']

        self.config_kafka = self.config["kafka"]
        self.config_db = self.config["database"]
        self.config_binance = self.config["binance"]
        self.config_binance_urls = self.config_binance["urls"]

        self.topicTradesRaw = "binance.trades.raw"
        self.topicTrades = "binance.trades"
        self.cacheTrendaware = "cache.binance.trendaware"
        self.topicTradingPairsCache = "cache.binance.tradingpairs"  # New Kafka cache topic
        self.topicQuoteUSDCache = "cache.binance.quoteusd"  # New Kafka cache topic
            
    async def create_kafka_producer(self):
        """Create and return an AIOKafkaProducer."""
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.config_kafka['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize messages to JSON
                # Optional: Configure other parameters like request_timeout_ms, retry_backoff_ms, etc.
                request_timeout_ms=30000,  # Increase timeout for better reliability
                retry_backoff_ms=1000,     # Delay between retries in milliseconds
                #batch_size=32 * 1024,  # Set batch size to 32 KB
                #linger_ms=10,  # Wait up to 10ms to accumulate messages before sending
                #acks='all',  # Wait for all replicas to acknowledge the message
            )
            await producer.start()  # Start the producer
            self.logger.info("Kafka Producer created successfully.")
            return producer
        except KafkaError as e:
            self.logger.error(f"Kafka Producer creation failed: {e}")
            raise

    async def create_kafka_consumer(self, topic, group_id):
        """Create and return an AIOKafkaConsumer."""
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.config_kafka['bootstrap_servers'],
                auto_offset_reset='earliest',
                group_id=group_id,  # Consumer group for parallel processing
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                request_timeout_ms=120000,  # Increase to 120 seconds
                session_timeout_ms=100000    # Increase to 10 seconds
            )
            self.logger.info(f"Kafka Consumer {topic} created successfully.")
            return consumer
        except KafkaError as e:
            self.logger.error(f"Kafka Consumer creation failed: {e}")
            raise
    
    async def create_db_connection(self):
        try:
            return await aiomysql.connect(
                host=self.config_db['host'],
                port=self.config_db['port'],
                user=self.config_db['user'],
                password=self.config_db['password'],
                db=self.config_db['database'],
                cursorclass=aiomysql.DictCursor
            )
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    async def fetch_trading_pairs(self):
        """Fetch trading pairs from the Kafka compact cache."""
        self.logger.info(f"Base fetch_trading_pairs into self.trading_pairs.")
        consumer = None
        try:
            consumer = await self.create_kafka_consumer(self.topicTradingPairsCache, "trading-pairs-fetcher")
            await consumer.start()
            i = 0
            try:
                async for message in consumer:
                    key = message.key.decode('utf-8')
                    value = message.value
                    self.trading_pairs[key] = value
                    self.logger.info(f"{i}. Received trading pair {key}")
                    i += 1
            except Exception as e:
                self.logger.error(f"Error processing Kafka message: {e}")
            self.logger.info(f"Received trading pairs from Kafka into self.trading_pairs.")
            self.logger.info(f"Received {i} trading pairs from Kafka into self.trading_pairs.")
            asyncio.sleep(2)
        except KafkaError as e:
            self.logger.error(f"Failed to fetch trading pairs: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to fetch trading pairs: {e}")
            raise
        finally:
            if consumer:
                try:
                    self.logger.info(f"Stopping consumer {self.topicTradingPairsCache}")
                    await consumer.stop()
                except Exception as e:
                    self.logger.warning(f"Error while stopping Kafka consumer: {e}")

    async def wait_for_kafka(self, max_retries=30, retry_delay=5):
        """Wait for Kafka to be ready."""
        retries = 0
        consumer = None
        while retries < max_retries:
            try:
                # Create a temporary Kafka consumer to check if Kafka is ready
                self.logger.info("base starting wait_for_kafka")
                consumer = AIOKafkaConsumer(
                    bootstrap_servers=self.config_kafka['bootstrap_servers'],
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    request_timeout_ms=120000,  # Increase to 120 seconds
                    session_timeout_ms=100000    # Increase to 10 seconds
                )
                await consumer.start()
                await asyncio.sleep(1)
                self.logger.info("Kafka is ready.")
                return True
            except KafkaError as e:
                self.logger.warning(f"Kafka is not ready yet. Retrying in {retry_delay} seconds... (Attempt {retries + 1}/{max_retries})")
                retries += 1
                await asyncio.sleep(retry_delay)
            except Exception as e:
                self.logger.error(f"Unexpected error while checking Kafka: {e}")
                retries += 1
                await asyncio.sleep(retry_delay)
            finally:
                if consumer:
                    try:
                        await consumer.stop()
                    except Exception as e:
                        self.logger.warning(f"Error while stopping Kafka consumer: {e}")
        self.logger.error("Failed to connect to Kafka after multiple retries.")
        return False