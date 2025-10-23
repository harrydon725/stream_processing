from confluent_kafka import Consumer
import socket
import ast

conf = {
    'bootstrap.servers':'localhost:9092',
    'group.id': 'this_group',
    'auto.offset.reset': 'latest' 
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])
total_price = 0
total_orders = 0

try:
    while True:
        msg = consumer.poll(1.0)  
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        key = msg.key().decode('utf-8')
        values = ast.literal_eval(msg.value().decode('utf-8'))
        total_orders +=1
        total_price += values.get("amount")
        print(f"New Order:\nCurrent orders: {total_orders}, Current Values: {total_price}, Avg Price: {total_price/total_orders}")
except KeyboardInterrupt:
    pass
finally:

    consumer.close()
