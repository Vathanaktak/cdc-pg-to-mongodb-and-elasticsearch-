"""PostgreSQL -> Kafka -> MongoDB.
Documents:
  customers: {_id, name, email, updated_at}
  orders:    {_id, status, total_amount, created_at, customer:{id,name,email}, items:[{id,sku,...}]}
Every write is an idempotent upsert / $pull, so redelivery is safe."""
import os
from datetime import datetime
from bson.decimal128 import Decimal128
from pymongo import MongoClient
from common import run, is_delete

db = MongoClient(os.getenv("MONGO", "mongodb://localhost:27017"))["shop"]


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def apply(t, v):
    deleted = is_delete(v)

    if t == "customers":
        if deleted:
            db.customers.delete_one({"_id": v["id"]}); return
        db.customers.replace_one({"_id": v["id"]}, {
            "name": v["name"], "email": v["email"], "updated_at": ts(v["updated_at"])}, upsert=True)
        # keep embedded snapshots consistent
        db.orders.update_many({"customer.id": v["id"]}, {"$set": {
            "customer": {"id": v["id"], "name": v["name"], "email": v["email"]}}})

    elif t == "orders":
        if deleted:
            db.orders.delete_one({"_id": v["id"]}); return
        c = db.customers.find_one({"_id": v["customer_id"]}) or {}
        db.orders.update_one({"_id": v["id"]}, {
            "$set": {"status": v["status"],
                     "total_amount": Decimal128(v["total_amount"]),
                     "created_at": ts(v["created_at"]),
                     "customer": {"id": v["customer_id"], "name": c.get("name"), "email": c.get("email")}},
            "$setOnInsert": {"items": []}}, upsert=True)

    elif t == "order_items":
        oid = v["order_id"]
        if deleted:
            db.orders.update_one({"_id": oid}, {"$pull": {"items": {"id": v["id"]}}}); return
        item = {"id": v["id"], "sku": v["sku"], "product_name": v["product_name"],
                "quantity": v["quantity"], "unit_price": Decimal128(v["unit_price"])}
        # update in place if present, otherwise append (idempotent on replay)
        r = db.orders.update_one({"_id": oid, "items.id": v["id"]}, {"$set": {"items.$": item}})
        if r.matched_count == 0:
            db.orders.update_one({"_id": oid}, {"$push": {"items": item}}, upsert=True)


def handle_batch(events):
    failed = []
    for ev in events:
        try:
            apply(ev["table"], ev["v"])
            op = "DELETE" if is_delete(ev["v"]) else "UPSERT"
            print(f"[SYNCED] {op} {ev['table']} (id={ev['v'].get('id')})")
        except Exception as e:
            failed.append((ev, e))
    return failed


if __name__ == "__main__":
    run("mongo-sink", handle_batch)
