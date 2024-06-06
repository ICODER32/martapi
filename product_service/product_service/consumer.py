from aiokafka import AIOKafkaConsumer
import product_pb2

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
    





