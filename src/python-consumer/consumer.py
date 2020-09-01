from kafka import KafkaConsumer
import json
from lxml import etree

consumer = KafkaConsumer(
    'epi-fee-consent',
    group_id='epi-fee_consent_group',
    bootstrap_servers=["127.0.0.1:9092"])

for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode('utf8')))
                                          