from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
import asyncio
import logging
from db import Inventory, create_db_and_tables,engine
import inventory_pb2
from sqlmodel import Session,select
import order_pb2

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
            order=order_pb2.Order()
            order.ParseFromString(msg.value)
            inventory.ParseFromString(msg.value)
            logger.info(f"Consumed inventory: {inventory}")
            logger.info(f"Consumed order: {order}")
            if inventory and not order.quantity:
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
                
                if inventory.operation == inventory_pb2.InventoryOpType.InvDELETE:
                    with Session(engine) as session:
                        product = session.exec(select(Inventory).where(Inventory.product_id == inventory.product_id)).first()
                        if product:
                            #  DELETE THE PRODUCT   
                            session.delete(product)
                            session.commit()
                            logger.info("Product deleted from inventory")
                            
                        else:
                            logger.info("Product not found in inventory")

                if inventory.operation == inventory_pb2.InventoryOpType.InvUPDATE:
                    with Session(engine) as session:
                        product = session.exec(select(Inventory).where(Inventory.product_id == inventory.product_id)).first()
                        if product:
                            #  UPDATE THE PRODUCT   
                            product.name = inventory.name
                            product.description = inventory.description
                            product.price = inventory.price
                            product.category = inventory.category
                            session.add(product)
                            session.commit()
                            logger.info("Product updated in inventory")
                        else:
                            logger.info("Product not found in inventory")
            if order and not inventory.description:
                if order.operation == order_pb2.OrderOp.OrdCREATE:
                    logger.info("Order created")
                    with Session(engine) as session:
                        product = session.exec(select(Inventory).where(Inventory.product_id == order.product_id)).first()
                        if product:
                            if product.quantity > 0:
                                product.quantity -= 1
                                session.add(product)
                                session.commit()
                                logger.info("Product quantity updated")
                            else:
                                logger.info("Product out of stock")
                        else:
                            logger.info("Product not found")
                
                if order.operation == order_pb2.OrderOp.OrdDELETE:
                    logger.info("Order deleted")
                    with Session(engine) as session:
                        product = session.exec(select(Inventory).where(Inventory.product_id == order.product_id)).first()
                        if product:
                            product.quantity += 1
                            session.add(product)
                            session.commit()
                            logger.info("Product quantity updated")
                        else:
                            logger.info("Product not found")

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

@app.get('/inventory/')
def get_inventory():
    with Session(engine) as session:
        products = session.exec(select(Inventory)).all()
        return {
            "products": products
        
        }
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