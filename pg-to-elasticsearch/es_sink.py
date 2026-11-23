"""PostgreSQL -> Kafka -> Elasticsearch (bulk indexing).
Index `orders`: one document per order, customer object + nested items array.
Index `customers`: lookup index used to enrich orders."""
import os
from elasticsearch import Elasticsearch, NotFoundError, helpers
from common import run, is_delete

es = Elasticsearch(os.getenv("ES", "http://localhost:9200"))

ORDERS_MAPPING = {
    "dynamic": "strict",
    "properties": {
        "id": {"type": "long"},
        "customer_id": {"type": "long"},
        "status": {"type": "keyword"},
        "total_amount": {"type": "scaled_float", "scaling_factor": 100},
        "created_at": {"type": "date"},
        "customer": {"properties": {
            "id": {"type": "long"},
            "name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "email": {"type": "keyword"}}},
        "items": {"type": "nested", "properties": {
            "id": {"type": "long"},
            "sku": {"type": "keyword"},
            "product_name": {"type": "text"},
            "quantity": {"type": "integer"},
            "unit_price": {"type": "scaled_float", "scaling_factor": 100}}},
    },
}
CUSTOMERS_MAPPING = {"dynamic": "strict", "properties": {
    "id": {"type": "long"}, "name": {"type": "text"}, "email": {"type": "keyword"}}}

UPSERT_ITEM = ("if (ctx._source.items == null) { ctx._source.items = []; } "
               "ctx._source.items.removeIf(i -> i.id == params.item.id); "
               "ctx._source.items.add(params.item);")
REMOVE_ITEM = ("if (ctx._source.items != null) "
               "{ ctx._source.items.removeIf(i -> i.id == params.id); }")


def ensure_indices():
    for name, mapping in (("orders", ORDERS_MAPPING), ("customers", CUSTOMERS_MAPPING)):
        if not es.indices.exists(index=name):
            es.indices.create(index=name, mappings=mapping, settings={"number_of_shards": 1})


def customer(cid):
    try:
        return es.get(index="customers", id=cid)["_source"]
    except NotFoundError:
        return {}


def actions_for(t, v):
    """Yield bulk actions for one CDC event."""
    deleted = is_delete(v)
    if t == "orders":
        if deleted:
            yield {"_op_type": "delete", "_index": "orders", "_id": v["id"]}; return
        c = customer(v["customer_id"])
        yield {"_op_type": "update", "_index": "orders", "_id": v["id"], "retry_on_conflict": 3,
               "doc_as_upsert": True,
               "doc": {"id": v["id"], "customer_id": v["customer_id"], "status": v["status"],
                       "total_amount": float(v["total_amount"]), "created_at": v["created_at"],
                       "customer": {"id": v["customer_id"], "name": c.get("name"), "email": c.get("email")}}}
    elif t == "order_items":
        oid = v["order_id"]
        if deleted:
            yield {"_op_type": "update", "_index": "orders", "_id": oid, "retry_on_conflict": 3,
                   "script": {"source": REMOVE_ITEM, "params": {"id": v["id"]}}}
            return
        item = {"id": v["id"], "sku": v["sku"], "product_name": v["product_name"],
                "quantity": v["quantity"], "unit_price": float(v["unit_price"])}
        yield {"_op_type": "update", "_index": "orders", "_id": oid, "retry_on_conflict": 3,
               "script": {"source": UPSERT_ITEM, "params": {"item": item}},
               "upsert": {"id": oid, "items": [item]}}


def flush(actions, pending, failed):
    """Bulk-index the accumulated actions; results come back in action order."""
    if not actions:
        return
    results = helpers.streaming_bulk(es, actions, chunk_size=500,
                                     raise_on_error=False, raise_on_exception=False)
    for (ok, res), ev in zip(results, pending):
        if ok:
            continue
        op = next(iter(res.values()))
        if op.get("status") == 404:      # delete/remove on an already-deleted order is fine
            continue
        failed.append((ev, op.get("error")))
    actions.clear(); pending.clear()


def handle_batch(events):
    actions, pending, failed = [], [], []
    for ev in events:
        t, v = ev["table"], ev["v"]
        try:
            if t == "customers":
                flush(actions, pending, failed)          # keep ordering before the side effects
                if is_delete(v):
                    es.options(ignore_status=404).delete(index="customers", id=v["id"])
                else:
                    es.index(index="customers", id=v["id"],
                             document={"id": v["id"], "name": v["name"], "email": v["email"]})
                    es.indices.refresh(index="orders")
                    es.update_by_query(index="orders", refresh=True, conflicts="proceed",
                        query={"term": {"customer.id": v["id"]}},
                        script={"source": "ctx._source.customer.name=params.n; ctx._source.customer.email=params.e",
                                "params": {"n": v["name"], "e": v["email"]}})
            else:
                for a in actions_for(t, v):
                    actions.append(a); pending.append(ev)
        except Exception as e:
            failed.append((ev, e))
    flush(actions, pending, failed)
    return failed


if __name__ == "__main__":
    ensure_indices()
    run("es-sink", handle_batch)
