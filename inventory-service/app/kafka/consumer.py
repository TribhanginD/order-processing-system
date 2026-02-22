import json
import time
from kafka import KafkaConsumer, KafkaProducer
import os
from .. import models

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA_CERT = os.getenv("KAFKA_CA_CERT")
KAFKA_SERVICE_CERT = os.getenv("KAFKA_SERVICE_CERT")
KAFKA_SERVICE_KEY = os.getenv("KAFKA_SERVICE_KEY")

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

    consumer = KafkaConsumer("order.created", **kafka_config_consumer)
    producer = KafkaProducer(**kafka_config_producer)

    print("Inventory consumer started...")
    for message in consumer:
        order = message.value
        db = models.SessionLocal()
        try:
            item = db.query(models.Inventory).filter(models.Inventory.product_id == order["product_id"]).first()
            if item and item.stock >= order["quantity"]:
                # Optimistic locking update
                item.stock -= order["quantity"]
                item.version += 1
                db.commit()
                producer.send("inventory.reserved", order)
                print(f"Reserved inventory for order {order['order_id']}")
            else:
                producer.send("inventory.failed", {"order_id": order["order_id"], "reason": "Out of stock"})
                print(f"Inventory failed for order {order['order_id']}")
        except Exception as e:
            db.rollback()
            print(f"Error processing order: {e}")
        finally:
            db.close()

# Note: This is primarily a consumer-based service. 
# A separate FastAPI health check endpoint could be added if needed, 
# but for this demo, we'll monitor the process health.
