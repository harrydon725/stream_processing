from confluent_kafka import Producer
import socket

# Configuration to connect to Kafka: the bootstrap server, and a unique client ID
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

# Sending to topic 'orders' with the key 'hello' and a value.
producer.produce('orders', key="hello", value="Hello!")

# We need to explicitly 'flush' messages, as they normally get sent in batches
# only periodically.
producer.flush()
