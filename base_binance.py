import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import aiomysql

class KafkaBase:
    def __init__(self):
        with open("config_binance.json") as config_file:
            self.config = json.load(config_file)
        self.kafka_config = self.config["kafka"]
        self.db_config = self.config["database"]
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def create_kafka_producer(self):
        try:
            return KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
        except KafkaError as e:
            self.logger.error(f"Kafka Producer creation failed: {e}")
            raise

    def create_kafka_consumer(self, topic):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except KafkaError as e:
            self.logger.error(f"Kafka Consumer creation failed: {e}")
            raise

    async def create_db_connection(self):
        try:
            return await aiomysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                db=self.db_config['database'],
                cursorclass=aiomysql.DictCursor
            )
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise