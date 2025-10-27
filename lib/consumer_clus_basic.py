from confluent_kafka import Consumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import socket 
def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

consumer = Consumer({
    "debug": "all",
    'bootstrap.servers': "b-2-public.harrydoncluster.gbkxxy.c3.kafka.eu-west-2.amazonaws.com:9198,b-1-public.harrydoncluster.gbkxxy.c3.kafka.eu-west-2.amazonaws.com:9198",
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

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
