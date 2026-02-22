from kafka import KafkaConsumer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def start_consumer():
    consumer = KafkaConsumer(
        "payment.succeeded", "payment.failed", "order.dlq",
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
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
