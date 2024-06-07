import time
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from product_db_service.db import create_db_and_tables, get_session, Product, engine
from product_db_service.utils.addProduct import add_product
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from product_db_service import product_pb2
import asyncio


async def consume():
    consumer = AIOKafkaConsumer(
        "products",
        bootstrap_servers="broker:19092",
        group_id="product_service",
    )
    try:
        await consumer.start()
        async for msg in consumer:
            if msg.value is not None:
                product = product_pb2.Product()
                product.ParseFromString(msg.value)
                print(product)
    finally:
        await consumer.stop()
    

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    loop=asyncio.get_event_loop()
    consume_task=loop.create_task(consume())
    yield
    consume_task.cancel()
    await consume_task
    
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"service": "product db service"}

@app.post("/addProduct")
async def addProduct(product: Product, sess=Depends(get_session)):
    sess.add(product)
    sess.commit()
    sess.refresh(product)
    return product
