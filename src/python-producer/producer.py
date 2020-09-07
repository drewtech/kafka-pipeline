from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from lxml import etree
import logging
import os
import sys

file_handler = logging.FileHandler(filename='logs/python-producer.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers
)

log = logging.getLogger('python-producer')

kafka_broker = os.environ.get('KAFKA_BROKER', "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=[kafka_broker], 
    value_serializer=lambda m: json.dumps(m).encode('ascii'))

def on_send_success(record_metadata):
    log.info(record_metadata.topic)
    log.info(record_metadata.partition)
    log.info(record_metadata.offset)

def on_send_error(excp):
    log.error('Error producing record', exc_info=excp)
    # handle exception


# this producer is just a way to get stuff on the pipeline for testing
# populate messages.txt with test messages
# examples are already in there.   
# site is topic for testing

f = open('messages.txt', "r")
line = f.readline()
while line:
    log.info("Sending {}".format(line))
    data = json.loads(line)
    producer.send(data['site'], data).add_callback(on_send_success).add_errback(on_send_error)
    log.info("Sent {}".format(line))
    line = f.readline()

f.close()
producer.flush()

for handler in log.handlers:
    handler.close()
    log.removeFilter(handler)
