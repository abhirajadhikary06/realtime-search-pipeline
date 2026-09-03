import json
import random
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "product-events"

# Sample catalog for event generation
PRODUCT_CATALOG = [
    {"product_id": 101, "name": "Wireless Headphones", "category": "Electronics", "price": 2499, "stock": 25},
    {"product_id": 102, "name": "Mechanical Keyboard", "category": "Electronics", "price": 4999, "stock": 15},
    {"product_id": 103, "name": "Ergonomic Chair", "category": "Furniture", "price": 12999, "stock": 10},
    {"product_id": 104, "name": "Running Shoes", "category": "Footwear", "price": 3499, "stock": 50},
]

EVENT_TYPES = ["UPSERT", "UPDATE_PRICE", "UPDATE_STOCK"]

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Successfully connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka connection... ({e})")
            time.sleep(3)

def generate_event():
    product = random.choice(PRODUCT_CATALOG).copy()
    event_type = random.choice(EVENT_TYPES)
    
    product["event_type"] = event_type

    if event_type == "UPDATE_PRICE":
        # Modify price by +/- 10%
        product["price"] = max(100, int(product["price"] * random.uniform(0.9, 1.1)))
    elif event_type == "UPDATE_STOCK":
        # Modify stock count randomly
        product["stock"] = max(0, product["stock"] + random.randint(-5, 5))

    return product

def main():
    producer = create_producer()
    print(f"Starting event stream to topic '{TOPIC_NAME}'...")

    try:
        while True:
            event = generate_event()
            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event}")
            time.sleep(2)  # Stream event every 2 seconds
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()