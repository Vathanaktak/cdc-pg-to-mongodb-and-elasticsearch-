"""Consistency check: compare PostgreSQL with MongoDB. Exit code 0 = in sync."""
import os, subprocess, sys
from pymongo import MongoClient

def get_pg_data():
    # 1. Try standard TCP connection via psycopg2
    try:
        import psycopg2
        pg = psycopg2.connect(os.getenv("PG", "host=127.0.0.1 port=5432 dbname=shop user=app password=app"))
        cur = pg.cursor()
        cur.execute("""SELECT o.id,o.status,o.total_amount,c.name FROM orders o JOIN customers c ON c.id=o.customer_id""")
        expected = {r[0]: {"status": r[1], "total": float(r[2]), "customer": r[3], "items": []} for r in cur.fetchall()}
        cur.execute("SELECT order_id,id,sku,quantity,unit_price FROM order_items ORDER BY id")
        for oid, iid, sku, q, price in cur.fetchall():
            expected[oid]["items"].append((iid, sku, q, float(price)))
        cur.close()
        pg.close()
        return expected
    except Exception:
        pass

    # 2. Fallback: Query directly inside Docker container (avoids host port 5432 / password conflicts)
    try:
        cmd_orders = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "app", "-d", "shop", "-t", "-A", "-F", "|",
                      "-c", "SELECT o.id,o.status,o.total_amount,c.name FROM orders o JOIN customers c ON c.id=o.customer_id;"]
        out1 = subprocess.check_output(cmd_orders, text=True).strip()
        expected = {}
        if out1:
            for line in out1.splitlines():
                if not line.strip(): continue
                parts = line.split("|")
                if len(parts) >= 4:
                    expected[int(parts[0])] = {"status": parts[1], "total": float(parts[2]), "customer": parts[3], "items": []}

        cmd_items = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "app", "-d", "shop", "-t", "-A", "-F", "|",
                     "-c", "SELECT order_id,id,sku,quantity,unit_price FROM order_items ORDER BY id;"]
        out2 = subprocess.check_output(cmd_items, text=True).strip()
        if out2:
            for line in out2.splitlines():
                if not line.strip(): continue
                parts = line.split("|")
                if len(parts) >= 5:
                    oid = int(parts[0])
                    if oid in expected:
                        expected[oid]["items"].append((int(parts[1]), parts[2], int(parts[3]), float(parts[4])))
        return expected
    except Exception as e:
        print(f"Error reading from PostgreSQL: {e}")
        sys.exit(1)

def get_mongo_data():
    mongo = MongoClient(os.getenv("MONGO", "mongodb://127.0.0.1:27017"))["shop"]
    actual = {}
    for d in mongo.orders.find():
        actual[d["_id"]] = {
            "status": d.get("status"),
            "total": float(d["total_amount"].to_decimal()) if "total_amount" in d and hasattr(d["total_amount"], "to_decimal") else float(d.get("total_amount", 0)),
            "customer": d.get("customer", {}).get("name") if isinstance(d.get("customer"), dict) else None,
            "items": sorted((i["id"], i["sku"], i["quantity"], float(i["unit_price"].to_decimal()) if hasattr(i.get("unit_price"), "to_decimal") else float(i.get("unit_price", 0)))
                            for i in d.get("items", []))
        }
    return actual

expected = get_pg_data()
actual = get_mongo_data()

problems = []
for oid in expected.keys() | actual.keys():
    if oid not in actual:
        problems.append(f"order {oid}: missing in MongoDB")
    elif oid not in expected:
        problems.append(f"order {oid}: extra in MongoDB (should be deleted)")
    elif expected[oid] != actual[oid]:
        problems.append(f"order {oid}: PG={expected[oid]} MONGO={actual[oid]}")

print(f"PG orders={len(expected)}  Mongo orders={len(actual)}")
print("IN SYNC" if not problems else "\n".join(problems))
sys.exit(1 if problems else 0)
