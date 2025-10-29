from confluent_kafka import Producer
import socket

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

producer.produce('orders', key="hello", value="Hello!")

producer.flush()
