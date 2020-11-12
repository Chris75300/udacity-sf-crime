#from kafka import KafkaConsumer
from confluent_kafka import Consumer
import asyncio


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "sf.crime"





async def consume(topic_name):
    """Consumes data from the Kafka Topic"""

    c = Consumer({'bootstrap.servers':BROKER_URL, 'group.id':'bla-group-id'})

    c.subscribe([topic_name])

    while True:

        message = c.poll(1.0)

        if message is None:
            print("No message received")
        elif message.error() is not None:
            print(f"error: {message.error()}")
        else:
            print(f"receiverd message key: {message.key()}; value: {message.value()}")

        await asyncio.sleep(0.1)

        
async def eat():
    t1 = asyncio.create_task(consume(TOPIC_NAME))
    await t1


if __name__ == "__main__":
    asyncio.run(eat())
    
