from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType
import logging

# Get logs from the pykafka.broker
logging.getLogger("pykafka.broker").setLevel('ERROR')

client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b'service-calls']

# Create a Consumer
consumer = topic.get_balanced_consumer(
    consumer_group=b'sf-crime',
    auto_commit_enable=False,
    auto_offset_reset=OffsetType.EARLIEST,
    zookeeper_connect='localhost:2181'
)

# Print Messages
for message in consumer:
    if message is not None:
        print(message.offset, message.value)