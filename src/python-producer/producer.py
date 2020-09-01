from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from lxml import etree

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'], 
    value_serializer=lambda m: json.dumps(m).encode('ascii'))

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('Error producing record', exc_info=excp)
    # handle exception

for i in range(3000):
    site = 'xplan-site-1'
    producer.send(site, {'site': site, 'consent': 'Okies'}).add_callback(on_send_success).add_errback(on_send_error)

    site = 'xplan-site-2'
    producer.send(site, {'site': site, 'consent': 'Nope!'}).add_callback(on_send_success).add_errback(on_send_error)

    site = 'xplan-site-3'
    producer.send(site, {'site': site, 'consent': 'No Way dude!'}).add_callback(on_send_success).add_errback(on_send_error) 

producer.flush()

