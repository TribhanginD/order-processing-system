import json
from kafka import KafkaProducer
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA_CERT = os.getenv("KAFKA_CA_CERT")

class Producer:
    def __init__(self):
        kafka_config = {
            "bootstrap_servers": [KAFKA_BROKER],
            "value_serializer": lambda v: json.dumps(v).encode('utf-8')
        }
        if KAFKA_USERNAME and KAFKA_PASSWORD:
            kafka_config.update({
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": KAFKA_USERNAME,
                "sasl_plain_password": KAFKA_PASSWORD
            })
            
            if KAFKA_CA_CERT:
                ca_path = "/tmp/ca.pem"
                with open(ca_path, "w") as f:
                    f.write(KAFKA_CA_CERT)
                kafka_config["ssl_cafile"] = ca_path
                
        self.producer = KafkaProducer(**kafka_config)

    def send_event(self, topic, data):
        self.producer.send(topic, data)
        self.producer.flush()

producer = Producer()
