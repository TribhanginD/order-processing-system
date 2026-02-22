import json
import random
import time
from kafka import KafkaConsumer, KafkaProducer
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA_CERT = os.getenv("KAFKA_CA_CERT")
KAFKA_SERVICE_CERT = os.getenv("KAFKA_SERVICE_CERT")
KAFKA_SERVICE_KEY = os.getenv("KAFKA_SERVICE_KEY")

def process_payment(order_id):
    # Simulate payment processing failure (10% chance)
    if random.random() < 0.1:
        raise Exception("Payment gateway timeout")
    return True

def start_consumer():
    kafka_config_consumer = {
        "bootstrap_servers": [KAFKA_BROKER],
        "value_deserializer": lambda m: json.loads(m.decode('utf-8'))
    }
    kafka_config_producer = {
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
        
        for config in [kafka_config_consumer, kafka_config_producer]:
            config.update({
                "security_protocol": "SSL",
                "ssl_cafile": ca_path,
                "ssl_certfile": cert_path,
                "ssl_keyfile": key_path
            })
    elif KAFKA_USERNAME and KAFKA_PASSWORD:
        if KAFKA_CA_CERT:
            ca_path = "/tmp/ca.pem"
            with open(ca_path, "w") as f:
                f.write(KAFKA_CA_CERT)

        for config in [kafka_config_consumer, kafka_config_producer]:
            config.update({
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_plain_username": KAFKA_USERNAME,
                "sasl_plain_password": KAFKA_PASSWORD
            })
            if KAFKA_CA_CERT:
                config["ssl_cafile"] = "/tmp/ca.pem"

    consumer = KafkaConsumer("inventory.reserved", **kafka_config_consumer)
    producer = KafkaProducer(**kafka_config_producer)

    print("Payment consumer started...")
    for message in consumer:
        order = message.value
        try:
            # Simple retry logic (3 attempts)
            for attempt in range(3):
                try:
                    process_payment(order["order_id"])
                    producer.send("payment.succeeded", order)
                    print(f"Payment succeeded for {order['order_id']}")
                    break
                except Exception as e:
                    print(f"Attempt {attempt+1} failed for {order['order_id']}: {e}")
                    if attempt == 2:
                        raise e
                    time.sleep(2**attempt) # Exponential backoff
        except Exception:
            # Move to DLQ
            producer.send("order.dlq", {"order_id": order["order_id"], "stage": "payment", "error": "Gateway failure after retries"})
            producer.send("payment.failed", order)
            print(f"Payment permanently failed for {order['order_id']}")

if __name__ == "__main__":
    start_consumer()
