import asyncio
from fastapi import FastAPI,Depends,HTTPException
from contextlib import asynccontextmanager
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka import AIOKafkaProducer,AIOKafkaConsumer
import product_pb2
import inventory_pb2
import asyncio
from typing import Annotated
from aiokafka.errors import TopicAlreadyExistsError, KafkaConnectionError,KafkaError
from sqlmodel import Session
MAX_RETRIES = 5
import uuid
RETRY_INTERVAL = 10  # seconds
from product_service.db import Product,create_db_and_tables,engine,ProductSchema
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from jose import JWTError, jwt

from sqlmodel import select

async def create_kafka_topic():
    """ Function to create kafka topic """
    admin_client = AIOKafkaAdminClient(bootstrap_servers="broker:19092")
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            # start the admin client
            await admin_client.start()
            topic_list = [NewTopic(name="products", num_partitions=1, replication_factor=1)]
            
            try:
                # create the topic
                await admin_client.create_topics(new_topics=topic_list, validate_only=False)
                print("Topic created successfully")
            except TopicAlreadyExistsError:
                print("Topic already exists")
            finally:
                await admin_client.close()
            return
        except KafkaConnectionError:
            retries += 1
            print(f"Kafka connection failed, retrying {retries}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_INTERVAL)
    
    raise Exception("Failed to connect to Kafka broker after several retries")

async def get_kafka_producer():
        producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
        await producer.start()
        try:
            yield producer
        finally:
            await producer.stop()
async def consume():
    consumer = AIOKafkaConsumer(
        'products',  # topic name
        bootstrap_servers='broker:19092',  # kafka broker
        group_id='products-group'
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
            new_product = product_pb2.Product()
            new_product.ParseFromString(msg.value)
            logger.info(f"Consumed message: {new_product}")
            if new_product.operation == product_pb2.OperationType.CREATE:
                product_id = str(uuid.uuid4())
                with Session(engine) as session:
                    isAlreadyExists = session.exec(select(Product).where(Product.name == new_product.name
                                                                         , Product.description == new_product.description
                                                                            , Product.price == new_product.price
                                                                            , Product.category == new_product.category
                                                                         )).first()
                    if isAlreadyExists:
                        logger.info("Product already exists")
                        inventory_message=inventory_pb2.Inventory()
                        inventory_message.product_id = isAlreadyExists.product_id
                        logger.info(isAlreadyExists.product_id)
                        inventory_message.name = isAlreadyExists.name
                        inventory_message.description = isAlreadyExists.description
                        inventory_message.price = isAlreadyExists.price
                        inventory_message.category = isAlreadyExists.category
                        inventory_message.operation = inventory_pb2.InventoryOpType.InvCREATE 
                    else:
                        product = Product(
                            product_id=product_id,
                            name=new_product.name,
                            description=new_product.description,
                            price=new_product.price,
                            category=new_product.category
                        )
                        session.add(product)
                        session.commit()
                        inventory_message=inventory_pb2.Inventory()
                        inventory_message.product_id = product_id
                        inventory_message.name = new_product.name
                        inventory_message.description = new_product.description
                        inventory_message.price = new_product.price
                        inventory_message.category = new_product.category
                        inventory_message.operation = inventory_pb2.InventoryOpType.InvCREATE


                    try:
                        producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
                        await producer.start()
                        await producer.send_and_wait("inventory", inventory_message.SerializeToString())
                    finally:
                        await producer.stop()
                   



            elif new_product.operation == product_pb2.OperationType.DELETE:
                with Session(engine) as session:
                    product = session.exec(select(Product).where(Product.id == new_product.id)).first()
                    session.delete(product)
                    session.commit()
                    inventory_message = inventory_pb2.Inventory()
                    inventory_message.product_id = product.product_id
                    inventory_message.operation = inventory_pb2.InventoryOpType.InvDELETE
                    try:
                        producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
                        await producer.start()
                        await producer.send_and_wait("inventory", inventory_message.SerializeToString())
                    finally:
                        await producer.stop()
            elif new_product.operation == product_pb2.OperationType.UPDATE:
                with Session(engine) as session:
                    product = session.exec(select(Product).where(Product.id == new_product.id)).first()
                    product.name = new_product.name
                    product.description = new_product.description
                    product.price = new_product.price
                    product.category = new_product.category
                    session.commit()
                    inventory_message = inventory_pb2.Inventory()
                    inventory_message.product_id = product.product_id
                    inventory_message.name = product.name
                    inventory_message.description = product.description
                    inventory_message.price = product.price
                    inventory_message.category = product.category
                    inventory_message.operation = inventory_pb2.InventoryOpType.InvUPDATE
                    try:
                        producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
                        await producer.start()
                        await producer.send_and_wait("inventory", inventory_message.SerializeToString())
                    finally:
                        await producer.stop()
                        


    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
async def authorize(token: str ):
    try:
        payload = jwt.decode(token, "secret", algorithms=["HS256"])
        logger.info(f"Authorized user {payload}")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    await create_kafka_topic()
    loop = asyncio.get_event_loop()
    loop.create_task(consume())
    yield


def get_session():
    with Session(engine) as session:
        yield session



app = FastAPI(lifespan=lifespan)


class ProductUpdate(Product):
    id: int
@app.get("/")
async def read_root(token: str, session: Session = Depends(get_session)):
    logger.info(f"Token: {token}")
    try:
        isAuthorized = jwt.decode(token, "secret2", algorithms=["HS256"])
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token")

    if isAuthorized.get("role") == "admin":
        products = session.exec(select(Product)).all()
        return {
            "products": products
        }
    else:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.post("/products/")
async def create_product(token:str,product:ProductSchema
                         ,producer: Annotated[AIOKafkaProducer,Depends(get_kafka_producer)]):
    isAuthorized= jwt.decode(token, "secret", algorithms=["HS256"])
    if isAuthorized.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")
    product_message = product_pb2.Product()
    product_message.name = product.name
    product_message.description = product.description
    product_message.price = product.price
    product_message.category = product.category
    product_message.operation = product_pb2.OperationType.CREATE

    await producer.send_and_wait(
        "products", # topic name
         product_message.SerializeToString()
          )
    return {"product": "created"}

@app.delete("/products/")
async def delete_product(token:str,product_id: str, producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    isAuthorized = jwt.decode(token, "secret", algorithms=["HS256"])
    if isAuthorized.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    product_message = product_pb2.Product()
    # convert the product_id to int
    product_message.id = int(product_id)
    product_message.operation = product_pb2.OperationType.DELETE
    await producer.send_and_wait("products", product_message.SerializeToString())
    return {"product": "deleted"}


@app.put("/products/")
async def update_product(token:str,product_id:int, product: ProductSchema, producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    isAuthorized= jwt.decode(token, "secret", algorithms=["HS256"])
    if isAuthorized.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")
    product_message = product_pb2.Product()
    product_message.id = product_id
    product_message.name = product.name
    product_message.description = product.description
    product_message.price = product.price
    product_message.category = product.category
    product_message.operation = product_pb2.OperationType.UPDATE

    await producer.send_and_wait("products", product_message.SerializeToString())
    return {"product": "updated"}

