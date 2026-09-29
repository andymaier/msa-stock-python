"""Datenmodelle (dataclasses)."""
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Stock:
    uuid: str
    quantity: int = 0

    def to_dict(self):
        return {"uuid": self.uuid, "quantity": self.quantity}


@dataclass
class Item:
    articleId: str
    quantity: int = 0


@dataclass
class Basket:
    uuid: Optional[str] = None
    items: List[Item] = field(default_factory=list)
