# conversation_manager_service/util/kafka_client.py
from kafka import KafkaProducer, KafkaConsumer

class KafkaClient:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

    def create_producer(self):
        return KafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def create_consumer(self, topic, group_id):
        return KafkaConsumer(topic, group_id=group_id, bootstrap_servers=self.bootstrap_servers)
