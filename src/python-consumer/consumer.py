from kafka import KafkaConsumer
import json
from lxml import etree
import os
import boto3
import botocore
import logging
import sys

file_handler = logging.FileHandler(filename='logs/python-consumer.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers
)

log = logging.getLogger('python-consumer')

aws_region = os.environ.get('AWS_REGION', 'ap-southeast-2')
aws_s3_endpoint = os.environ.get('AWS_S3_ENDPOINT', "http://localhost:4566")
aws_access_key = os.environ.get('AWS_ACCESS_KEY', "ABC")
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', "DFG")
kafka_topic = os.environ.get("KAFKA_TOPIC","xml-topic")
kafka_group = os.environ.get("KAFKA_GROUP", "xml-group")
kafka_broker = os.environ.get('KAFKA_BROKER', "localhost:9092")

aws_client = boto3.client(service_name="s3", region_name=aws_region,
                          endpoint_url=aws_s3_endpoint,
                          verify=False,
                          aws_access_key_id=aws_access_key,
                          aws_secret_access_key=aws_secret_access_key)


consumer = KafkaConsumer(
    kafka_topic,
    group_id=kafka_group,
    bootstrap_servers=[kafka_broker])

for message in consumer:
    data = etree.fromstring(message.value)
    log.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode('utf8')))
