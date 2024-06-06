import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka import AIOKafkaProducer
from product_service.consumer import consume
import product_pb2
from pydantic import BaseModel
import asyncio
from aiokafka.errors import TopicAlreadyExistsError, KafkaConnectionError

MAX_RETRIES = 3
RETRY_INTERVAL = 10  # seconds

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_kafka_topic()
    # run consume in infinite loop
    loop=asyncio.get_event_loop()
    consume_task=loop.create_task(consume())
    yield
    consume_task.cancel()
    await consume_task


app = FastAPI(lifespan=lifespan)


class Product(BaseModel):
    name: str
    description: str
    price: float
    quantity: int
@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/products/")
async def create_product(product:Product):
    product_message = product_pb2.Product()
    product_message.name = product.name
    product_message.description = product.description
    product_message.price = product.price
    product_message.stock = product.quantity

    product_message = product_message.SerializeToString()


    producer=AIOKafkaProducer(bootstrap_servers="broker:19092")
    await producer.start()
    await producer.send_and_wait("products", product_message)
    return {"product": "created"}

