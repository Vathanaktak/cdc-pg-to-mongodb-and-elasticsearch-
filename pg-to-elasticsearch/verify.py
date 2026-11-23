"""Consistency check: compare PostgreSQL with Elasticsearch. Exit code 0 = in sync."""
import os, sys
import psycopg2
from elasticsearch import Elasticsearch

def connect_pg():
    if "PG" in os.environ:
        return psycopg2.connect(os.environ["PG"])
    for port in (5433, 5432):
        try:
            return psycopg2.connect(f"host=localhost port={port} dbname=shop user=app password=app", connect_timeout=3)
        except Exception:
            continue
    return psycopg2.connect("host=localhost port=5433 dbname=shop user=app password=app")

pg = connect_pg()
es = Elasticsearch(os.getenv("ES", "http://localhost:9200"))
cur = pg.cursor()

cur.execute("""SELECT o.id,o.status,o.total_amount,c.name FROM orders o JOIN customers c ON c.id=o.customer_id""")
expected = {r[0]: {"status": r[1], "total": float(r[2]), "customer": r[3], "items": []} for r in cur.fetchall()}
cur.execute("SELECT order_id,id,sku,quantity,unit_price FROM order_items ORDER BY id")
for oid, iid, sku, q, price in cur.fetchall():
    expected[oid]["items"].append((iid, sku, q, float(price)))

es.indices.refresh(index="orders")
hits = es.search(index="orders", size=10000, query={"match_all": {}})["hits"]["hits"]
actual = {}
for h in hits:
    d = h["_source"]
    actual[int(h["_id"])] = {"status": d["status"], "total": float(d["total_amount"]),
                             "customer": (d.get("customer") or {}).get("name"),
                             "items": sorted((i["id"], i["sku"], i["quantity"], float(i["unit_price"]))
                                             for i in d.get("items", []))}
problems = []
for oid in expected.keys() | actual.keys():
    if oid not in actual:     problems.append(f"order {oid}: missing in Elasticsearch")
    elif oid not in expected: problems.append(f"order {oid}: extra in Elasticsearch (should be deleted)")
    elif expected[oid] != actual[oid]:
        problems.append(f"order {oid}: PG={expected[oid]} ES={actual[oid]}")
print(f"PG orders={len(expected)}  ES orders={len(actual)}")
print("IN SYNC" if not problems else "\n".join(problems))
sys.exit(1 if problems else 0)
