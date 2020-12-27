"""Producer base-class providing common utilites and functionality"""
import logging
import time
import socket

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from confluent_kafka.admin import AdminClient, NewTopic

SCHEMA_REGISTRY_URL = "http://localhost:8081"

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Done: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Done: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.producer = AvroProducer(self.broker_properties, schema_registry=schema_registry, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # Done: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        admin_client = AdminClient(self.broker_properties)
        topic_list = []
        topic_list.append(NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor= self.num_replicas ))
        admin_client.create_topics(topic_list)

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # Done: Write cleanup code for the Producer here
        #
        #
        self.producer.close()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
