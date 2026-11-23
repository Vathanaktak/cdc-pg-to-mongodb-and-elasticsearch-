"""Shared Kafka consumer loop: manual commits (at-least-once), batching, dead-letter queue."""
import json, logging, os, signal
from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv("KAFKA", "localhost:29092")
TOPICS = ["pg.public.customers", "pg.public.orders", "pg.public.order_items"]
DLQ = "pg.dlq"
log = logging.getLogger("sink")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")


def is_delete(v):
    return v.get("__deleted") == "true" or v.get("__op") == "d"


def run(group, handle_batch, batch_size=500, timeout=1.0):
    """handle_batch(events) -> list of (event, error). events keep Kafka order.
    event = {"table": str, "v": dict, "raw": bytes}"""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP, "group.id": group,
        "enable.auto.commit": False, "auto.offset.reset": "earliest",
    })
    dlq = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe(TOPICS)
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop); signal.signal(signal.SIGTERM, stop)

    while running:
        msgs = consumer.consume(batch_size, timeout)
        events = []
        for m in msgs:
            if m.error():
                log.error("kafka error: %s", m.error()); continue
            if m.value() is None:          # tombstone: nothing to do
                continue
            events.append({"table": m.topic().rsplit(".", 1)[-1],
                           "v": json.loads(m.value()), "raw": m.value()})
        if not msgs:
            continue
        for ev, err in handle_batch(events):
            log.error("-> DLQ (%s): %s", err, ev["v"])
            dlq.produce(DLQ, ev["raw"], headers={"error": str(err)[:500], "table": ev["table"]})
        dlq.flush()
        consumer.commit(asynchronous=False)   # commit only after the batch is applied
    consumer.close()
