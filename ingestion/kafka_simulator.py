"""
ingestion/kafka_simulator.py

A lightweight in-process pub/sub message queue that mirrors the
Kafka producer/consumer API surface.

Why this exists:
  Real Kafka requires a running broker + ZooKeeper (or KRaft), which adds
  significant infrastructure overhead for a demo project.  This module
  demonstrates the *concepts* — topics, producers, consumers, offsets,
  consumer groups — without that dependency.

  In a real Azure deployment this would be replaced by:
    - azure-eventhub SDK  (Azure Event Hubs, which is Kafka-compatible)
    - confluent-kafka     (Confluent Cloud or self-hosted Kafka on AKS)
"""

import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


# ── Data structures ─────────────────────────────────────────────────────────────

@dataclass
class Message:
    """Represents a single message on a topic (mirrors Kafka ConsumerRecord)."""
    topic: str
    value: Any
    partition: int = 0
    offset: int = 0
    timestamp: float = field(default_factory=time.time)
    key: Optional[str] = None


class MessageBroker:
    """
    Singleton in-memory broker.
    Maintains a deque per topic — O(1) append and popleft.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._topics: Dict[str, deque] = defaultdict(deque)
            cls._instance._offsets: Dict[str, int] = defaultdict(int)
        return cls._instance

    def publish(self, topic: str, message: Message) -> int:
        """Append a message to a topic and return its offset."""
        offset = self._offsets[topic]
        message.offset = offset
        self._topics[topic].append(message)
        self._offsets[topic] += 1
        return offset

    def consume(self, topic: str, max_messages: int = 100) -> list[Message]:
        """Consume up to max_messages from the front of a topic queue."""
        messages = []
        for _ in range(min(max_messages, len(self._topics[topic]))):
            messages.append(self._topics[topic].popleft())
        return messages

    def queue_size(self, topic: str) -> int:
        return len(self._topics[topic])

    def list_topics(self) -> list[str]:
        return list(self._topics.keys())


# ── Producer ────────────────────────────────────────────────────────────────────

class EventProducer:
    """
    Mirrors the confluent-kafka / azure-eventhub Producer API.

    Usage:
        producer = EventProducer(topic="ecommerce-events")
        producer.send({"event_type": "purchase", "amount": 49.99})
    """

    def __init__(self, topic: str, serialize: bool = True):
        self.topic = topic
        self.serialize = serialize
        self._broker = MessageBroker()
        self._sent_count = 0

    def send(self, value: Any, key: Optional[str] = None) -> int:
        """Serialize and publish a message. Returns the assigned offset."""
        payload = json.dumps(value) if self.serialize else value
        msg = Message(topic=self.topic, value=payload, key=key)
        offset = self._broker.publish(self.topic, msg)
        self._sent_count += 1
        return offset

    def queue_size(self) -> int:
        return self._broker.queue_size(self.topic)

    def flush(self):
        """No-op: synchronous in-memory broker always flushes immediately."""
        pass

    @property
    def sent_count(self) -> int:
        return self._sent_count


# ── Consumer ────────────────────────────────────────────────────────────────────

class EventConsumer:
    """
    Mirrors the confluent-kafka / azure-eventhub Consumer API.

    Usage:
        consumer = EventConsumer(
            topics=["ecommerce-events"],
            group_id="spark-transform-group",
        )
        for message in consumer.poll(max_messages=500):
            process(message.value)
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str = "default-group",
        deserialize: bool = True,
        on_error: Optional[Callable] = None,
    ):
        self.topics = topics
        self.group_id = group_id
        self.deserialize = deserialize
        self.on_error = on_error
        self._broker = MessageBroker()
        self._consumed_count = 0

    def poll(self, max_messages: int = 100) -> list[Message]:
        """
        Pull up to max_messages from each subscribed topic.
        Deserializes JSON payloads and returns Message objects.
        """
        all_messages = []
        for topic in self.topics:
            raw = self._broker.consume(topic, max_messages)
            for msg in raw:
                try:
                    if self.deserialize:
                        msg.value = json.loads(msg.value)
                    all_messages.append(msg)
                    self._consumed_count += 1
                except (json.JSONDecodeError, TypeError) as e:
                    if self.on_error:
                        self.on_error(msg, e)
        return all_messages

    def commit(self):
        """Acknowledge processing — no-op for in-memory broker."""
        pass

    @property
    def consumed_count(self) -> int:
        return self._consumed_count


# ── Demo ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Kafka Simulator Demo ===\n")

    # Producer
    producer = EventProducer(topic="orders")
    for i in range(5):
        offset = producer.send({"order_id": i, "amount": round((i + 1) * 12.5, 2)})
        print(f"[Producer] Sent order {i} → offset {offset}")

    print(f"\nBroker queue size: {producer.queue_size()} messages\n")

    # Consumer
    consumer = EventConsumer(topics=["orders"], group_id="analytics-group")
    messages = consumer.poll(max_messages=10)
    for msg in messages:
        print(f"[Consumer] Offset {msg.offset} → {msg.value}")

    print(f"\nConsumed {consumer.consumed_count} messages")
    print(f"Remaining in queue: {producer.queue_size()}")
