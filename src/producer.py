"""Fake e-commerce clickstream producer.

Generates realistic-looking page view / add-to-cart / checkout events and
pushes them to Kafka as JSON. I use this for local dev and load testing —
it also works against MSK, just point KAFKA_BOOTSTRAP_SERVERS at the cluster.

Run:  python src/producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# A small catalog to make events feel real. Prices are in USD.
PRODUCTS = [
    ("headphones-pro", 199.99),
    ("mechanical-keyboard", 129.50),
    ("running-shoes", 89.99),
    ("coffee-maker", 149.00),
    ("yoga-mat", 35.00),
    ("desk-lamp", 49.99),
    ("water-bottle", 24.95),
    ("backpack", 79.00),
]

PAGES = ["/", "/products", "/cart", "/checkout", "/search", "/deals"]

# Weighted event types — page views dominate, checkouts are rare, like real life.
EVENT_TYPES = ["page_view", "page_view", "page_view", "add_to_cart", "remove_from_cart", "checkout"]


def make_event() -> dict:
    """Build one random clickstream event.

    The schema is intentionally small but covers what a real pipeline needs:
    an id for dedup, timestamps for windowing, and enough context to enrich.
    """
    product, price = random.choice(PRODUCTS)
    event_type = random.choice(EVENT_TYPES)
    now = datetime.now(timezone.utc)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": now.isoformat(),
        "user_id": f"user_{random.randint(1, 5000)}",
        "session_id": f"session_{random.randint(1, 20000)}",
        "page": random.choice(PAGES),
        "product_id": product,
        "price": price,
        "quantity": random.randint(1, 3) if event_type in ("add_to_cart", "checkout") else 0,
        "user_agent": random.choice(["mobile", "desktop", "tablet"]),
        "country": random.choice(["US", "US", "US", "CA", "UK", "DE", "IN"]),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # JSON-encode each event; the streaming job expects UTF-8 JSON.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Acks=all is slower but you don't lose events if a broker restarts.
        acks="all",
        retries=5,
    )
    print(f"Producing to {KAFKA_BOOTSTRAP_SERVERS}, topic '{KAFKA_TOPIC}' (Ctrl+C to stop)")

    try:
        while True:
            event = make_event()
            # Use user_id as the key so one user's events land on the same
            # partition — keeps ordering per user, which the session logic likes.
            producer.send(KAFKA_TOPIC, key=event["user_id"].encode("utf-8"), value=event)
            # Small random sleep so the stream looks bursty, not metronomic.
            time.sleep(random.uniform(0.05, 0.4))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
