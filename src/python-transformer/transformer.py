from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
from lxml import etree
import logging
import os
import sys

file_handler = logging.FileHandler(filename="logs/python-transformer.log")
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    level=logging.INFO, 
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    handlers=handlers
)

log = logging.getLogger('python-transformer')

kafka_broker = os.environ.get('KAFKA_BROKER', "localhost:9092")
kafka_topic_pattern = os.environ.get("KAFKA_TOPIC_PATTERN", "^my-site.*")
kafka_group = os.environ.get("KAFKA_GROUP", "xml-transformer-group")
kafka_topic = os.environ.get("KAFKA_TOPIC", "xml-topic")

consumer = KafkaConsumer(
    bootstrap_servers=[kafka_broker], 
    group_id=kafka_group,
    auto_offset_reset='earliest')
consumer.subscribe(pattern=kafka_topic_pattern)

producer = KafkaProducer(
    bootstrap_servers=[kafka_broker])

def _generate_xml(data):
    root = etree.Element("XmlRequest")
    etree.SubElement(root, "Site").text = data['site']
    etree.SubElement(root, "DoYouConsent").text = data['consent']
    etree.SubElement(root, "File").text = data["file"]
    return etree.tostring(root)

    
def on_send_success(record_metadata):
    log.info("Transformed and sent!")
    log.info(record_metadata.topic)
    log.info(record_metadata.partition)
    log.info(record_metadata.offset)


def on_send_error(excp):
    log.exception('Error producing record', exc_info=excp)
    # handle exception

for message in consumer:
    log.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value))
    data = json.loads(message.value)
    xml = _generate_xml(data)

    log.info("Sending payload: %s" % xml)
    producer.send(kafka_topic, xml).add_callback(on_send_success).add_errback(on_send_error)
    log.info("Sent payload: %s" % xml)

