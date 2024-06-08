import time
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from product_db_service.db import create_db_and_tables, get_session, Product, engine
from product_db_service.utils.addProduct import add_product
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from product_db_service import product_pb2
import asyncio

MAX_RETRIES = 3
RETRY_INTERVAL = 10  # seconds

async def consume():
    consumer = AIOKafkaConsumer(
        "products",
        bootstrap_servers="broker:19092",
        group_id="product_service",
    )
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            print("Attempting to connect to Kafka broker...")
            await consumer.start()
            print("Connected to Kafka broker")
            async for msg in consumer:
                if msg.value is not None:
                    print(f"Consumed message: {msg}")
            break
        except KafkaConnectionError:
            retries += 1
            print(f"Kafka connection failed, retrying {retries}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_INTERVAL)
        finally:
            await consumer.stop()
    
    if retries == MAX_RETRIES:
        raise Exception("Failed to connect to Kafka broker after several retries")

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    loop = asyncio.get_event_loop()
    consume_task = loop.create_task(consume())
    yield
    consume_task.cancel()
    await consume_task

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"service": "product db service"}

@app.post("/addProduct")
async def add_product_endpoint(product: Product, sess=Depends(get_session)):
    sess.add(product)
    sess.commit()
    sess.refresh(product)
    return product
