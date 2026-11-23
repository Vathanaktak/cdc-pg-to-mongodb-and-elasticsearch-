# Project: PostgreSQL -> Debezium -> Kafka -> MongoDB

## Run
```bash
docker compose up -d
pip install -r requirements.txt
until curl -s localhost:8083/connectors >/dev/null; do sleep 3; done
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/postgres-source.json localhost:8083/connectors
curl -s localhost:8083/connectors/pg-mongo-source/status     # RUNNING
python mongo_sink.py
```

## Test INSERT / UPDATE / DELETE and one-to-many
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
docker compose exec mongo mongosh shop --eval 'db.orders.find().pretty()'
```

## Design
- Debezium pgoutput, `REPLICA IDENTITY FULL`, `ExtractNewRecordState` SMT (adds `__op`, `__deleted`, `__source_ts_ms`).
- Orders become one document: `{_id, status, total_amount, created_at, customer:{...}, items:[...]}`.
- Item events: `$set items.$` or `$push`; deletes use `$pull`. Customer updates fan out to embedded snapshots.
- Manual offset commits + idempotent writes; failures go to `pg.dlq`.
- Lag: `kafka-consumer-groups.sh --describe --group mongo-sink`.

## Verify consistency
```bash
python verify.py      # compares every order, its items and customer name; exit 0 = in sync
```

## Requirement checklist
| Requirement | Where |
|---|---|
| Configure Debezium PostgreSQL connector | `connectors/postgres-source.json` |
| Capture PostgreSQL changes | pgoutput logical replication, `sql/init.sql` (`REPLICA IDENTITY FULL`) |
| Publish CDC events to Kafka topics | `pg.public.customers`, `pg.public.orders`, `pg.public.order_items` |
| Transform relational records into MongoDB documents | `mongo_sink.py` (`apply`) |
| Handle INSERT, UPDATE, DELETE | upsert / `$set` / `$pull` / `delete_one` in `apply` |
| Handle one-to-many | `order_items` embedded as `orders.items[]` |
| Maintain consistency | idempotent writes, manual commits, customer fan-out, `verify.py` |
