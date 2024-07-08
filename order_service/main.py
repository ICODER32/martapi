from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
from contextlib import asynccontextmanager


from fastapi import Depends
import asyncio
import order_pb2
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def consume():
    consumer = AIOKafkaConsumer(
        'orders',  # topic name
        bootstrap_servers='broker:19092',  # kafka broker
        group_id='orders-group'
    )
    while True:
        try:
            await consumer.start()
            break
        except KafkaConnectionError as e:
            logger.error(f"Kafka connection error: {e}")
            await asyncio.sleep(5)
    try:
        async for msg in consumer:
            order = order_pb2.Order()
            order.ParseFromString(msg.value)
            logger.info(f"Received message: {order}")
    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
        


async def get_kafka_producer():
    producer=AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    loop=asyncio.get_event_loop()
    loop.create_task(consume())
    yield
    loop.stop()
app = FastAPI(
    lifespan=lifespan
)

class Order(BaseModel):
    product_id: int
    quantity: int
    price: float




@app.post("/order/")
async def create_order(order: Order, producer: AIOKafkaProducer = Depends(get_kafka_producer)):
    order_message = order_pb2.Order()
    order_message.product_id = order.product_id
    order_message.quantity = order.quantity
    order_message.price = order.price   
    order_message.operation = order_pb2.OperationType.CREATE
    await producer.send_and_wait("orders", order_message.SerializeToString())
    return {"message": "Order created successfully"}
