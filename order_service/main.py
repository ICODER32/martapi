from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
from contextlib import asynccontextmanager
from sqlmodel import Session,select
from jose import JWTError, jwt
from fastapi import Depends
import asyncio
import order_pb2
from aiokafka import AIOKafkaConsumer
import notification_pb2
from aiokafka.errors import KafkaConnectionError, KafkaError
from db import create_db_and_tables,engine,Order,OrderCreate,Product
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
        

async def get_session():
    with Session(engine) as session:
        yield session


async def get_kafka_producer():
    producer=AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    loop=asyncio.get_event_loop()
    loop.create_task(consume())
    yield
    loop.stop()
app = FastAPI(
    lifespan=lifespan
)




@app.get("/order/")
async def get_orders(token:str,session=Depends(get_session)):
    isAuthorized=jwt.decode(token,"secret",algorithms=["HS256"])
    if isAuthorized["role"]!="admin":
        return {"message": "Unauthorized"}
    orders=session.exec(select(Order)).all()
    return {
        "orders": orders
    }

@app.post("/order/")
async def create_order(token:str,order: OrderCreate,session=Depends(get_session), producer: AIOKafkaProducer = Depends(get_kafka_producer)):
    try:
        isAuthorized=jwt.decode(token,"secret",algorithms=["HS256"])
    except JWTError as e:
        return {"message": "Unauthorized"}
    
    if isAuthorized["role"]!="user":
        return {"message": "Unauthorized"}
    email=isAuthorized["email"]
    product=session.exec(select(Product).where(Product.product_id==order.product_id)).first()
    if not product:
        return {"message": "Product not found"}
    notification=notification_pb2.Notification()
    notification.email=email
    notification.message=f"Congratulations! You have successfully ordered {order.quantity} {product.name} at the price of {order.price} each."
    

    

    order=Order(
        product_id=order.product_id,
        user_id=isAuthorized["id"],
        quantity=order.quantity,
        price=order.price
    )
    session.add(order)
    session.commit()

    try:
        order_message = order_pb2.Order()
        order_message.product_id = order.product_id
        order_message.quantity = order.quantity
        order_message.price = order.price
        order_message.operation=order_pb2.OrderOp.OrdCREATE
        await producer.send_and_wait("orders", order_message.SerializeToString())
        await producer.send_and_wait("inventory", order_message.SerializeToString())
        await producer.send_and_wait("notification", notification.SerializeToString())
        return {"message": "Order created successfully"}
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        return {"message": "Error while creating order"}


@app.delete("/order/{order_id}")
async def delete_order(token:str,order_id: int,session=Depends(get_session), producer: AIOKafkaProducer = Depends(get_kafka_producer)):
    try:
        isAuthorized=jwt.decode(token,"secret",algorithms=["HS256"])
    except JWTError as e:
        return {"message": "Unauthorized"}
    if isAuthorized["role"]!="admin":
        return {"message": "Unauthorized"}  
    email=isAuthorized["email"]
    order=session.exec(select(Order).where(Order.id==order_id)).first()
    logger.info(f"Order: {order}")
    if not order:
        return {"message": "Order not found"}
    
    product=session.exec(select(Product).where(Product.product_id==order.product_id)).first()

    if not product:
        return {"message": "Product not found"}
    notification=notification_pb2.Notification()
    notification.email=email
    notification.message=f"We are sorry to inform you that your order for {order.quantity} {product.name} at the price of {order.price} each has been cancelled."
    order=session.exec(select(Order).where(Order.id==order_id)).first()
    if order:
        session.delete(order)
        session.commit()
        order_message = order_pb2.Order()
        order_message.product_id = order.product_id
        order_message.operation=order_pb2.OrderOp.OrdDELETE
        await producer.send_and_wait("orders", order_message.SerializeToString())
        await producer.send_and_wait("inventory", order_message.SerializeToString())
        await producer.send_and_wait("notification", notification.SerializeToString())
        return {"message": "Order deleted successfully"}
    return {"message": "Order not found"}