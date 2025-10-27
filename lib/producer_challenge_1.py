# publish_orders.py

import random
import time
from confluent_kafka import Producer
import socket

# This small program publishes JSON 
# orders with random amounts and customer IDs
# from a given list into a Kafka topic 'orders'.
 
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

# Simple sequential numeric ID
id = 0 
customer_ids = list(range(1, 20))

while True:
    id += 1

    amount = random.randint(2, 30)
    customer_id = customer_ids[random.randint(0, len(customer_ids) - 1)]

    template = '{{"order_id": {order_id}, "amount": {amount}, "customer_id": {customer_id}}}'
    json = template.format(order_id=id, amount=amount, customer_id=customer_id)

    producer.produce('orders', key="order-" + str(id), value=bytes(json, encoding="utf-8"))

    print(" -> sent order ID {id}".format(id=id))

    producer.flush()

    # Sleep a bit
    seconds = random.randint(1, 3)
    time.sleep(seconds)
