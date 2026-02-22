from kafka import KafkaConsumer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA_CERT = os.getenv("KAFKA_CA_CERT")
KAFKA_SERVICE_CERT = os.getenv("KAFKA_SERVICE_CERT")
KAFKA_SERVICE_KEY = os.getenv("KAFKA_SERVICE_KEY")

def start_consumer():
    kafka_config = {
        "bootstrap_servers": [KAFKA_BROKER],
        "value_deserializer": lambda m: json.loads(m.decode('utf-8'))
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
            with open(ca_path, "w") as f:
                f.write(KAFKA_CA_CERT)
            kafka_config["ssl_cafile"] = ca_path

    consumer = KafkaConsumer(
        "payment.succeeded", "payment.failed", "order.dlq",
        **kafka_config
    )

    print("Notification service started (watching payment events)...")
    for message in consumer:
        topic = message.topic
        data = message.value
        if topic == "payment.succeeded":
            print(f"✅ [NOTIFICATION] Order {data['order_id']} confirmed! Sending email...")
        elif topic == "payment.failed":
            print(f"❌ [NOTIFICATION] Order {data['order_id']} failed at payment. Notifying user...")
        elif topic == "order.dlq":
            print(f"⚠️ [NOTIFICATION] Order {data['order_id']} moved to DLQ. Reason: {data['error']}")

if __name__ == "__main__":
    start_consumer()
