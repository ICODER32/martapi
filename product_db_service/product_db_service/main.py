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
        'products',
        bootstrap_servers='broker:19092',
        group_id='product-group'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            product = product_pb2.Product()
            product.ParseFromString(msg.value)
            logger.info(f"Received message: {product}")
            with Session(engine) as sess:
                if product.operation == product_pb2.OperationType.CREATE:
                    new_product = Product(
                        id=product.id,
                        name=product.name,
                        description=product.description,
                        category=product.category,
                        price=product.price
                    )
                    sess.add(new_product)
                    sess.commit()
                    sess.refresh(new_product)
                    logger.info(f"Product added to database: {new_product}")

                elif product.operation == product_pb2.OperationType.UPDATE:
                    existing_product = sess.query(Product).filter_by(id=product.id).first()
                    if existing_product:
                        existing_product.name = product.name
                        existing_product.description = product.description
                        existing_product.category = product.category
                        existing_product.price = product.price
                        sess.commit()
                        sess.refresh(existing_product)
                        logger.info(f"Product updated in database: {existing_product}")
                    else:
                        logger.warning(f"Product with ID {product.id} not found for update")

                elif product.operation == product_pb2.OperationType.DELETE:
                    existing_product = sess.query(Product).filter_by(id=product.id).first()
                    if existing_product:
                        sess.delete(existing_product)
                        sess.commit()
                        logger.info(f"Product deleted from database: {existing_product}")
                    else:
                        logger.warning(f"Product with ID {product.id} not found for deletion")
                else:
                    logger.warning(f"Unknown operation type: {product.operation}")
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
