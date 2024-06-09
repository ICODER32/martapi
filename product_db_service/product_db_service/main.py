import time
import logging
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from product_db_service.db import create_db_and_tables, get_session, Product, engine, Session
from product_db_service.utils.addProduct import add_product
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from product_db_service import product_pb2
import asyncio

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def consume():
    consumer = AIOKafkaConsumer(
        "products",
        bootstrap_servers="broker:19092",
        group_id="product_db_service",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                product = product_pb2.Product()
                product.ParseFromString(msg.value)
                with Session(engine) as sess:
                    sess.add(Product(
                        name=product.name,
                        description=product.description,
                        category=product.category,
                        price=product.price
                    ))
                    sess.commit()
                    sess.refresh(product)
                    logger.info(f"Product added to database: {product}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await consumer.stop()

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
