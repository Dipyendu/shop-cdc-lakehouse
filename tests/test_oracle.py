"""The merge-rule oracle: a worked example with known results, and order independence."""
import datetime as dt
import json
import random
from decimal import Decimal

from shop.oracle import fold


def line(op, key, lsn, price=None):
    after = None if op == "d" else {"product_id": key, "name": "p", "category": "c", "price": price,
                                    "updated_at": "2026-11-16T14:20:31.018481Z"}
    payload = {"op": op, "after": after, "source": {"lsn": lsn}}
    return json.dumps({"kafka_key": json.dumps({"product_id": key}), "payload": json.dumps(payload)})


TS = dt.datetime(2026, 11, 16, 14, 20, 31, 18481, tzinfo=dt.UTC)
EVENTS = [
    line("c", 1, 100, "10.00"), line("u", 1, 200, "12.50"),       # key 1: last update wins
    line("c", 2, 110, "5.00"), line("d", 2, 300),                  # key 2: deleted
    line("u", 2, 250, "6.00"),                                      # late, older than the delete
    line("c", 3, 120, "1.00"),                                      # key 3: single insert
]


def test_worked_example():
    s = fold(EVENTS, "products")
    assert s[1] == (1, "p", "c", Decimal("12.50"), TS, 200, False)
    assert s[2] == (2, None, None, None, None, 300, True)
    assert s[3] == (3, "p", "c", Decimal("1.00"), TS, 120, False)


def test_order_and_duplicates_do_not_matter():
    expected = fold(EVENTS, "products")
    r = random.Random(7)
    for _ in range(50):
        ev = EVENTS + r.sample(EVENTS, 3)  # duplicates
        r.shuffle(ev)
        assert fold(ev, "products") == expected


def test_without_the_newer_rule_order_would_matter():
    """Control: applying events in arrival order (last one wins) gives a different state."""
    late_last = EVENTS  # the late update for key 2 arrives after the delete
    state = {}
    for x in late_last:
        rec = json.loads(x)
        ev = json.loads(rec["payload"])
        state[json.loads(rec["kafka_key"])["product_id"]] = ev["op"]
    assert state[2] == "u" and fold(EVENTS, "products")[2][-1] is True
