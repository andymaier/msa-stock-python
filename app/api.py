"""REST-API (Flask) - Loesung."""
from flask import Blueprint, jsonify

from .store import store

bp = Blueprint("stocks", __name__)


@bp.get("/stocks")
def index():
    return jsonify(store.find_all())


@bp.get("/stocks/count")
def count():
    return jsonify(store.count())
