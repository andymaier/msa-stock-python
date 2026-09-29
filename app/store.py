"""In-Memory-Store fuer Stocks (Pendant zur ConcurrentHashMap im Original).
Vollstaendig vorgegeben - nicht Teil der Uebung."""
import threading

from .models import Stock


class StockStore:
    def __init__(self):
        self._stocks = {}
        self._lock = threading.Lock()

    def find_all(self):
        with self._lock:
            return [s.to_dict() for s in self._stocks.values()]

    def count(self):
        with self._lock:
            return len(self._stocks)

    def save(self, data: dict):
        with self._lock:
            uuid = data["uuid"]
            s = self._stocks.get(uuid)
            if s is None:
                s = Stock(uuid=uuid)
                self._stocks[uuid] = s
            if data.get("quantity") is not None:
                s.quantity = data["quantity"]

    def delete(self, uuid):
        with self._lock:
            self._stocks.pop(uuid, None)

    def decrement(self, article_id, qty):
        with self._lock:
            s = self._stocks.get(article_id)
            if s is not None:
                s.quantity -= qty


# gemeinsame Instanz fuer API und Listener
store = StockStore()
