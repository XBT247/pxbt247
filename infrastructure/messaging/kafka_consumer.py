import json
import asyncio
from aiokafka import AIOKafkaConsumer
from core.interfaces.imessaging import IMessageConsumer

class KafkaConsumerClient(IMessageConsumer):
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
    async def consume(self):
        await self.consumer.start()
        try:
            async for msg in self.consumer:
                yield {
                    'key': msg.key,
                    'value': msg.value,
                    'topic': msg.topic,
                    'partition': msg.partition,
                    'offset': msg.offset
                }
        finally:
            await self.consumer.stop()

    async def commit(self):
        await self.consumer.commit()

    async def stop(self):
        await self.consumer.stop()

