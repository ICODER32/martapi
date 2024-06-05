from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka import AIOKafkaProducer



async def create_kafka_topic():
    """ Function to create kafka topic """
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers="broker:19092")

    # start the admin client
    await admin_client.start()
    topic_list = [NewTopic(name="products",
                           num_partitions=1, replication_factor=1)]

    try:
        # create the topic
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic created successfully")
    except TopicAlreadyExistsError as e:
        print(f"Failed to create topic : {e}")
    finally:
        await admin_client.close()

        
@asynccontextmanager
async def lifespan(app:FastAPI):
    await create_kafka_topic()
    yield
    
app = FastAPI(
    lifespan=lifespan
)



# app.on_event("startup")(start_kafka_topic)



@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/products/")
def create_product():
    return {"product": "created"}