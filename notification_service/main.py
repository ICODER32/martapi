import smtplib
from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
import asyncio
import logging
import notification_pb2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def consume():
    consumer = AIOKafkaConsumer(
        'notification',  # topic name
        bootstrap_servers='broker:19092',  # kafka broker
        group_id='notification-group'
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
            notification = notification_pb2.Notification()
            notification.ParseFromString(msg.value)
            logger.info(f"Received message: {notification}")
    except KafkaError as e:
        logger.error(f"Error while consuming message: {e}")
@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    loop.create_task(consume())
    yield
    loop.stop()


app = FastAPI(
    lifespan=lifespan
)


@app.get("/")
def read_root():
    return {"Hello": "World"}

def sendMail(sender:str,receiver:str,subject:str,message:str):
    message = f"""\
    Subject: {subject}
    To: {receiver}
    From: {sender}

    {message}"""
    try:
        with smtplib.SMTP("sandbox.smtp.mailtrap.io", 587) as server:            
            server.starttls()
            server.login("e227d3eff1c055", "9c9bed77bbef51")
            server.sendmail(sender, receiver, message)
            print("Message sent",server)
    except Exception as e:
        print("Error",e)

   

sendMail("abc@gmail.com","123@gmail.com","Test","This is a test message")