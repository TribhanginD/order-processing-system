import json
import random
import time
from kafka import KafkaConsumer, KafkaProducer
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def process_payment(order_id):
    # Simulate payment processing failure (10% chance)
    if random.random() < 0.1:
        raise Exception("Payment gateway timeout")
    return True

def start_consumer():
    consumer = KafkaConsumer(
        "inventory.reserved",
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

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
