import json
import threading
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, Producer, KafkaException

from .config import KAFKA_BOOTSTRAP_SERVERS, SHOP_TOPIC, KAFKA_GROUP_ID
from .store import store


@dataclass
class Operation:
    bo: str
    action: str
    object: Any = None

    @staticmethod
    def from_bytes(raw: bytes) -> "Operation":
        d = json.loads(raw.decode("utf-8"))
        return Operation(d.get("bo"), d.get("action"), d.get("object"))

    def to_bytes(self) -> bytes:
        return json.dumps(
            {"bo": self.bo, "action": self.action, "object": self.object}
        ).encode("utf-8")


class ShopProducer:
    """Sendet Operation-Events auf 'shop' (confluent-kafka). Verbindung wird
    verzoegert aufgebaut, damit die App auch ohne laufendes Kafka startet."""

    def __init__(self):
        self._producer = None

    def _get(self):
        if self._producer is None:
            self._producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        return self._producer

    def send(self, op: "Operation"):
        errors = []

        def _cb(err, msg):
            if err is not None:
                errors.append(err)

        p = self._get()
        p.produce(SHOP_TOPIC, value=op.to_bytes(), on_delivery=_cb)
        p.flush(10)
        if errors:
            raise KafkaException(errors[0])


class ShopListener:
    def __init__(self):
        self.store = store

    def handle(self, op: Operation):
        obj = op.object or {}
        if op.bo == "stock":
            if op.action in ("create", "update", "upsert"):
                self.store.save(obj)
            elif op.action == "delete":
                self.store.delete(obj.get("uuid"))
        elif op.bo == "basket" and op.action == "upsert":
            for item in (obj.get("items") or []):
                self.store.decrement(item.get("articleId"), item.get("quantity", 0))

    def start(self):
        threading.Thread(target=self._consume, daemon=True).start()

    def _consume(self):
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([SHOP_TOPIC])
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[stock] Consumer-Fehler: {msg.error()}")
                continue
            try:
                self.handle(Operation.from_bytes(msg.value()))
            except Exception as e:
                print(f"[stock] Fehler beim Verarbeiten: {e}")
