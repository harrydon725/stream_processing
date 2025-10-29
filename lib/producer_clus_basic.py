from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer
import socket

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    return auth_token, expiry_ms/1000

conf = {
    'bootstrap.servers': 'b-2-public.harrydoncluster.gbkxxy.c3.kafka.eu-west-2.amazonaws.com:9198,b-1-public.harrydoncluster.gbkxxy.c3.kafka.eu-west-2.amazonaws.com:9198',
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
}

producer = Producer(conf)

producer.produce('orders', key="hello", value="Hello!")

producer.flush()