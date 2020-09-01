from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
from lxml import etree

consumer = KafkaConsumer(
    bootstrap_servers=["127.0.0.1:9092"], 
    group_id="epi-transformer", 
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('ascii')))
consumer.subscribe(pattern='^xplan-site.*')

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'])

def _generate_epi_xml(message):
    root = etree.Element("EPIDataRequest")
    etree.SubElement(root, "Site").text = message['site']
    etree.SubElement(root, "DoYouConsent").text = message['consent']
    return etree.tostring(root)

    
def on_send_success(record_metadata):
    print("Transformed and sent!")
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('Error producing record', exc_info=excp)
    # handle exception

for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value))
    epiXml = _generate_epi_xml(message.value)

    print ("Transformed Epi: %s" % epiXml)

    producer.send('epi-fee-consent', epiXml).add_callback(on_send_success).add_errback(on_send_error)

