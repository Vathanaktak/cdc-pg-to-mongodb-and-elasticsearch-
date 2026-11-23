# Project: PostgreSQL -> Debezium -> Kafka -> Elasticsearch

## Run
```bash
docker compose up -d
pip install -r requirements.txt
until curl -s localhost:8083/connectors >/dev/null; do sleep 3; done
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/postgres-source.json localhost:8083/connectors
curl -s localhost:8083/connectors/pg-es-source/status        # RUNNING
python es_sink.py
```

## Test INSERT / UPDATE / DELETE
```bash
docker compose exec postgres psql -U app shop <<'SQL'
INSERT INTO orders (customer_id,status,total_amount) VALUES (2,'NEW',30);
INSERT INTO order_items (order_id,sku,product_name,quantity,unit_price) VALUES (2,'SKU-3','Monitor',1,30);
UPDATE order_items SET quantity=2 WHERE id=1;
UPDATE orders SET status='PAID' WHERE id=1;
UPDATE customers SET name='Alice Smith' WHERE id=1;
DELETE FROM order_items WHERE id=2;
DELETE FROM orders WHERE id=2;
SQL
curl -s 'localhost:9200/orders/_search?pretty'
curl -s 'localhost:9200/orders/_search?pretty' -H 'Content-Type: application/json' \
  -d '{"query":{"nested":{"path":"items","query":{"term":{"items.sku":"SKU-1"}}}}}'
```

## Design
- Same Debezium source config as the Mongo project (own slot/publication names).
- Index `orders`: strict mapping, `nested` items, `scaled_float` money, `date` timestamps; `customers` lookup index for enrichment.
- `helpers.bulk` per polled batch; item changes are scripted upserts/removals; deletes tolerate 404.
- Customer updates run `update_by_query` to refresh denormalized names.
- Manual offset commits + idempotent writes; failures go to `pg.dlq`.
- Lag: `kafka-consumer-groups.sh --describe --group es-sink`.

## Verify synchronization
```bash
python verify.py      # compares every order, its items and customer name; exit 0 = in sync
```

## Requirement checklist
| Requirement | Where |
|---|---|
| Configure Debezium PostgreSQL connector | `connectors/postgres-source.json` |
| Capture changes and publish to Kafka | pgoutput; topics `pg.public.*` |
| Consume CDC events from Kafka | `common.py` (consumer group `es-sink`) |
| Transform relational data into ES documents | `es_sink.py` (`actions_for`) |
| Configure index mappings | `ORDERS_MAPPING`, `CUSTOMERS_MAPPING`, `ensure_indices()` |
| Implement bulk indexing | `helpers.streaming_bulk` in `flush()` |
| Handle INSERT, UPDATE, DELETE | upsert / scripted item update / bulk delete |
| Maintain synchronization | idempotent actions, manual commits, `update_by_query` for customers, `verify.py` |
