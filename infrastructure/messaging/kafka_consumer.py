import json
import asyncio
from aiokafka import AIOKafkaConsumer
from core.interfaces.imessaging import IMessageConsumer

class KafkaConsumerClient(IMessageConsumer):
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, **kwargs):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=kwargs.get('auto_offset_reset', 'earliest'),
            enable_auto_commit=kwargs.get('enable_auto_commit', False),
            session_timeout_ms=kwargs.get('session_timeout_ms', 10000),
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
    async def consume(self):
        try:
            await self.consumer.start()
            async for msg in self.consumer:
                try:
                    yield {
                        'key': msg.key.decode('utf-8') if msg.key else None,
                        'value': msg.value,
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset
                    }
                except Exception as e:
                    logger.error(f"Message processing failed: {e}")
                    continue
        except Exception as e:
            logger.error(f"Consumer failed: {e}")
            raise
        finally:
            await self.consumer.stop()

    async def commit(self):
        await self.consumer.commit()

    async def stop(self):
        await self.consumer.stop()

