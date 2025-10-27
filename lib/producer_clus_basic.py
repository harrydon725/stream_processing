from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer
import socket

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

conf = {
    'bootstrap.servers': 'b-2-public.harrydoncluster.gbkxxy.c3.kafka.eu-west-2.amazonaws.com:9198,b-1-public.harrydoncluster.gbkxxy.c3.kafka.eu-west-2.amazonaws.com:9198',
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
}

## Rest of your code below

producer = Producer(conf)

# Sending to topic 'orders' with the key 'hello' and a value.
producer.produce('orders', key="hello", value="Hello!")

# We need to explicitly 'flush' messages, as they normally get sent in batches
# only periodically.
producer.flush()