from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
import asyncio
import logging
from db import Inventory, create_db_and_tables,engine
import inventory_pb2
from sqlmodel import Session,select

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)






async def consume():
    consumer = AIOKafkaConsumer(
        'inventory',  # topic name
        bootstrap_servers='broker:19092',  # kafka broker
        group_id="inventory-group",
        auto_offset_reset="earliest",
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
            inventory = inventory_pb2.Inventory()
            inventory.ParseFromString(msg.value)
            logger.info(f"Consumed inventory: {inventory}")
            if inventory.operation == inventory_pb2.InventoryOpType.InvCREATE:
                with Session(engine) as session:
                    product = session.exec(select(Inventory).where(Inventory.product_id == inventory.product_id)).first()
                    if product:
                        product.quantity += 1
                        session.add(product)
                        session.commit()
                        logger.info("Product already exists")
                    else:
                        new_product = Inventory(
                            product_id=inventory.product_id,
                            name=inventory.name,
                            description=inventory.description,
                            price=inventory.price,
                            category=inventory.category,
                            quantity=1
                        )
                        session.add(new_product)
                        session.commit()
                        logger.info("Product added to inventory")
            
        

    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        await asyncio.sleep(5)



    
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    loop = asyncio.get_event_loop()
    loop.create_task(consume())
    yield
    

app = FastAPI(
    lifespan=lifespan
)


@app.get("/")

def read_root():
    return {"Hello": "World"}


@app.get('/inventory/{product_id}')
def get_inventory(product_id:str):
    with Session(engine) as session:
        product = session.exec(select(Inventory).where(Inventory.product_id == product_id)).first()
        if product:
            return {
                "product_id": product.product_id,
                "name": product.name,
                "description": product.description,
                "price": product.price,
                "quantity": product.quantity,
                "category": product.category
            }
        else:
            return {"error":"Product not found"}