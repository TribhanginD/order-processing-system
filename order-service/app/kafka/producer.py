import json
from kafka import KafkaProducer
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA_CERT = os.getenv("KAFKA_CA_CERT")
KAFKA_SERVICE_CERT = os.getenv("KAFKA_SERVICE_CERT")
KAFKA_SERVICE_KEY = os.getenv("KAFKA_SERVICE_KEY")

class Producer:
    def __init__(self):
        kafka_config = {
            "bootstrap_servers": [KAFKA_BROKER],
            "value_serializer": lambda v: json.dumps(v).encode('utf-8')
        }
        if KAFKA_SERVICE_CERT and KAFKA_SERVICE_KEY and KAFKA_CA_CERT:
            # Aiven mTLS (SSL)
            ca_path = "/tmp/ca.pem"
            cert_path = "/tmp/service.cert"
            key_path = "/tmp/service.key"
            
            with open(ca_path, "w") as f: f.write(KAFKA_CA_CERT)
            with open(cert_path, "w") as f: f.write(KAFKA_SERVICE_CERT)
            with open(key_path, "w") as f: f.write(KAFKA_SERVICE_KEY)
            
            kafka_config.update({
                "security_protocol": "SSL",
                "ssl_cafile": ca_path,
                "ssl_certfile": cert_path,
                "ssl_keyfile": key_path
            })
        elif KAFKA_USERNAME and KAFKA_PASSWORD:
            kafka_config.update({
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": KAFKA_USERNAME,
                "sasl_plain_password": KAFKA_PASSWORD
            })
            
            if KAFKA_CA_CERT:
                ca_path = "/tmp/ca.pem"
                with open(ca_path, "w") as f: f.write(KAFKA_CA_CERT)
                kafka_config["ssl_cafile"] = ca_path
                
        self.producer = KafkaProducer(**kafka_config)

    def send_event(self, topic, data):
        self.producer.send(topic, data)
        self.producer.flush()

producer = Producer()
