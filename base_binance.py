import json
import logging
import aiomysql
from kafka.errors import KafkaError
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
        self.highload_pairs = ['btcusdt', 'ethusdt', 'xrpusdt', 'bnbusdt']

        self.config_kafka = self.config["kafka"]
        self.config_db = self.config["database"]
        self.config_binance = self.config["binance"]
        self.config_binance_urls = self.config_binance["urls"]

        self.topicTradesRaw = "binance.trades.raw"
        self.topicTrades = "binance.trades"
        self.cacheTrendaware = "binance.cache.trendaware"
        self.topicTradingPairsCache = "binance.cache.tradingpairs"  # New Kafka cache topic
        self.topicQuoteUSDCache = "binance.cache.quoteusd"  # New Kafka cache topic
            
    async def create_kafka_producer(self):
        """Create and return an AIOKafkaProducer."""
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.config_kafka['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize messages to JSON
                # Optional: Configure other parameters like request_timeout_ms, retry_backoff_ms, etc.
                request_timeout_ms=30000,  # Increase timeout for better reliability
                retry_backoff_ms=1000,     # Delay between retries in milliseconds
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
            self.logger.info("Kafka Consumer created successfully.")
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