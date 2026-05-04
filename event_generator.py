"""
ingestion/event_generator.py

Simulates a real-time e-commerce event stream.
In production this would be replaced by an actual Kafka consumer
reading from Azure Event Hubs or a Kafka topic.

Events generated:
  - page_view
  - add_to_cart
  - purchase
  - refund
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from kafka_simulator import EventProducer

# ── Config ─────────────────────────────────────────────────────────────────────

RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "refund"]

# Realistic price ranges per category (min, max)
PRICE_RANGES = {
    "Electronics":   (29.99, 1499.99),
    "Clothing":      (9.99,  249.99),
    "Home & Garden": (14.99, 599.99),
    "Sports":        (19.99, 399.99),
    "Books":         (5.99,  49.99),
}

# Event type weights — purchases are rarer than page views
EVENT_WEIGHTS = [0.50, 0.25, 0.18, 0.07]


# ── Helpers ────────────────────────────────────────────────────────────────────

def random_timestamp(days_back: int = 7) -> str:
    """Return a random ISO timestamp within the last N days."""
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    # return (datetime.utcnow() - delta).isoformat() + "Z"
    return (datetime.utcnow() - delta).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_event() -> dict:
    """Generate a single synthetic e-commerce event."""
    category = random.choice(CATEGORIES)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    price_min, price_max = PRICE_RANGES[category]

    event = {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "user_id":    f"user_{random.randint(1000, 9999)}",
        "session_id": str(uuid.uuid4()),
        "product_id": f"prod_{random.randint(100, 999)}",
        "category":   category,
        "timestamp":  random_timestamp(),
        "amount":     None,  # Only purchases and refunds have amounts
        "metadata": {
            "device":   random.choice(["mobile", "desktop", "tablet"]),
            "country":  random.choice(["IN", "US", "GB", "DE", "SG"]),
        },
    }

    # Purchases and refunds carry monetary values
    if event_type in ("purchase", "refund"):
        event["amount"] = round(random.uniform(price_min, price_max), 2)

    # Refunds are always negative
    if event_type == "refund" and event["amount"]:
        event["amount"] = -abs(event["amount"])

    return event


# ── Main ───────────────────────────────────────────────────────────────────────

def generate_batch(num_events: int = 5000) -> Path:
    """
    Generate a batch of events, write to a dated JSONL file,
    and also push through the Kafka simulator to demonstrate
    producer/consumer streaming pattern.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    output_file = RAW_DATA_DIR / f"events_{today}.jsonl"

    producer = EventProducer(topic="ecommerce-events")

    print(f"Generating {num_events} events → {output_file}")
    events = []
    for i in range(num_events):
        event = generate_event()
        producer.send(event)          # push to in-memory queue
        events.append(event)

        if (i + 1) % 1000 == 0:
            print(f"  ✓ {i + 1} events produced")

    # Write to landing zone (simulates writing to ADLS / S3)
    with open(output_file, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"\nProduced {len(events)} events to:")
    print(f"  File  : {output_file}")
    print(f"  Topic : ecommerce-events ({producer.queue_size()} messages queued)")
    print("\nEvent type breakdown:")
    for etype in EVENT_TYPES:
        count = sum(1 for e in events if e["event_type"] == etype)
        pct = count / len(events) * 100
        print(f"  {etype:<15} {count:>5}  ({pct:.1f}%)")

    return output_file


if __name__ == "__main__":
    generate_batch(num_events=5000)
