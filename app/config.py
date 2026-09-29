"""Konfiguration (Defaults = Java-Original application.yml)."""
import os
from uuid import uuid4

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SHOP_TOPIC = os.getenv("SHOP_TOPIC", "shop")
# Java: stock-${random.uuid} -> jede Instanz baut ihren In-Memory-Store neu auf
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", f"stock-{uuid4()}")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8081"))
