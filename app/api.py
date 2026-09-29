"""REST-API (Flask).

TODO (Uebung): Endpunkte implementieren.
Original (Java): GET /stocks -> alle Bestaende (dort noch `return null`),
GET /stocks/count -> Anzahl.
"""
from flask import Blueprint, jsonify

from .store import store

bp = Blueprint("stocks", __name__)


@bp.get("/stocks")
def index():
    # TODO: alle Bestaende aus dem Store als JSON zurueckgeben.
    raise NotImplementedError("GET /stocks noch nicht implementiert")


@bp.get("/stocks/count")
def count():
    # TODO: Anzahl der Bestaende zurueckgeben.
    raise NotImplementedError("GET /stocks/count noch nicht implementiert")
