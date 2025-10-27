from confluent_kafka import Consumer
import socket
conf = {
    'bootstrap.servers':'localhost:9092',
    'group.id': 'this_group',
    'auto.offset.reset': 'latest' 
}
consumer = Consumer(conf)
consumer.subscribe(['orders'])
try:
    while True:
        msg = consumer.poll(1.0)  
        if msg is None:
            continue
        if msg.error():
            print(f"{msg.error()}")
            continue
            
        print(f"Message: key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
