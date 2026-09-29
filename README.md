# msa-stock (Python)

Python-Portierung des Java/Spring-Boot-Service `stock` (predic8-MSA-Shop).
Flask (REST) + confluent-kafka (Event-Anbindung), In-Memory-Store.

## Architektur
- REST `GET /stocks`, `GET /stocks/count`.
- Kafka-Topic `shop`, Nachricht `Operation {bo, action, object}`.
  Der Service **hoert** auf `shop`:
  - `bo="stock"`  -> Bestand anlegen/aktualisieren/loeschen
  - `bo="basket"` (action `upsert`) -> Bestaende der enthaltenen Items reduzieren

## Branches
- `main`     – Skelett; **API (`app/api.py`) und Kafka-Listener
  (`app/events.py`) sind als TODO offen** (im Java-Original liefert
  `GET /stocks` sogar `null` und ein Listener fehlt komplett).
- `solution` – fertige Loesung.

## Start
    python3 -m venv .venv && . .venv/bin/activate
    pip install -r requirements.txt
    python -m app.main   # Kafka muss laufen (Default localhost:9092)
