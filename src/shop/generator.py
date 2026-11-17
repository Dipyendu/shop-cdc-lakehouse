"""Shop activity generator.

Two parts:
- ``plan(seed, n_events)`` is pure: it returns exactly ``n_events`` row-level
  changes from a fixed random seed. No database, no clock. Unit-tested.
- ``apply(changes, dsn)`` executes a plan against PostgreSQL. Each change is one
  SQL statement that touches exactly one row, so one change = one CDC event.

``updated_at`` (and ``created_at``/``ordered_at``) are set by the database.

Usage: python -m shop.generator --events 5000 --seed 42 [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

TABLES = ("customers", "products", "orders", "order_items", "payments")
PK = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}

FIRST = ["Asha", "Ravi", "Meera", "Arjun", "Priya", "Kabir", "Nina", "Omar", "Lena", "Tom", "Sara", "Ivan"]
LAST = ["Das", "Rao", "Iyer", "Khan", "Mehta", "Smith", "Garcia", "Muller", "Rossi", "Sato", "Kim", "Silva"]
CITIES = [
    ("Kolkata", "IN"), ("Mumbai", "IN"), ("Bengaluru", "IN"), ("Delhi", "IN"), ("Pune", "IN"),
    ("London", "GB"), ("Berlin", "DE"), ("Paris", "FR"), ("Austin", "US"), ("Toronto", "CA"),
]
CATEGORIES = ["books", "electronics", "kitchen", "garden", "toys", "clothing", "sports", "beauty"]
METHODS = ["card", "upi", "wallet", "cod"]
NEXT_STATUS = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}


@dataclass(frozen=True)
class Change:
    table: str
    op: str  # insert | update | delete
    key: int
    values: tuple = ()  # (column, value) pairs: full row for insert, changed columns for update

    def as_dict(self) -> dict:
        return dict(self.values)


class _IndexedSet:
    """Set with O(1) add, remove and seeded random choice (deterministic order)."""

    def __init__(self):
        self.items: list[int] = []
        self.pos: dict[int, int] = {}

    def __len__(self):
        return len(self.items)

    def __contains__(self, x):
        return x in self.pos

    def add(self, x):
        if x not in self.pos:
            self.pos[x] = len(self.items)
            self.items.append(x)

    def discard(self, x):
        i = self.pos.pop(x, None)
        if i is None:
            return
        last = self.items.pop()
        if i < len(self.items):
            self.items[i] = last
            self.pos[last] = i

    def choice(self, r: random.Random) -> int:
        return self.items[r.randrange(len(self.items))]


STATUSES = ("placed", "paid", "shipped", "delivered", "cancelled")


@dataclass
class _State:
    customers: dict = field(default_factory=dict)  # id -> {email, city, country}
    products: dict = field(default_factory=dict)  # id -> price
    orders: dict = field(default_factory=dict)  # id -> {customer_id, status}
    items: dict = field(default_factory=dict)  # order_id -> {item_id: amount}
    payments: dict = field(default_factory=dict)  # order_id -> {payment_id: status}
    customer_orders: Counter = field(default_factory=Counter)
    customer_ids: _IndexedSet = field(default_factory=_IndexedSet)
    lonely: _IndexedSet = field(default_factory=_IndexedSet)  # customers with no orders
    product_ids: _IndexedSet = field(default_factory=_IndexedSet)
    by_status: dict = field(default_factory=lambda: {s: _IndexedSet() for s in STATUSES})
    multi_placed: _IndexedSet = field(default_factory=_IndexedSet)  # placed orders with >1 item
    next_id: Counter = field(default_factory=Counter)

    def new_id(self, table: str) -> int:
        self.next_id[table] += 1
        return self.next_id[table]


class _Planner:
    def __init__(self, seed: int, n_events: int):
        self.r = random.Random(seed)
        self.n = n_events
        self.s = _State()
        self.out: list[Change] = []

    def set_status(self, oid, status):
        old = self.s.orders[oid]["status"]
        self.s.by_status[old].discard(oid)
        self.s.by_status[status].add(oid)
        self.s.orders[oid]["status"] = status
        if status != "placed":
            self.s.multi_placed.discard(oid)

    def left(self) -> int:
        return self.n - len(self.out)

    def emit(self, table, op, key, **values):
        self.out.append(Change(table, op, key, tuple(values.items())))

    # --- single-event actions -------------------------------------------
    def new_customer(self):
        cid = self.s.new_id("customers")
        city, country = self.r.choice(CITIES)
        name = f"{self.r.choice(FIRST)} {self.r.choice(LAST)}"
        email = f"customer{cid}@example.com"
        self.s.customers[cid] = {"email": email, "city": city, "country": country}
        self.s.customer_ids.add(cid)
        self.s.lonely.add(cid)
        self.emit("customers", "insert", cid, customer_id=cid, name=name, email=email, city=city, country=country)

    def new_product(self):
        pid = self.s.new_id("products")
        price = (Decimal(self.r.randint(199, 99_999)) / 100).quantize(Decimal("0.01"))
        self.s.products[pid] = price
        self.s.product_ids.add(pid)
        self.emit(
            "products", "insert", pid,
            product_id=pid, name=f"{self.r.choice(CATEGORIES)} item {pid}",
            category=self.r.choice(CATEGORIES), price=price,
        )

    def change_email(self):
        cid = self.s.customer_ids.choice(self.r)
        n = self.r.randint(1, 999)
        email = f"customer{cid}.{n}@example.com"
        self.s.customers[cid]["email"] = email
        self.emit("customers", "update", cid, email=email)

    def change_address(self):
        cid = self.s.customer_ids.choice(self.r)
        city, country = self.r.choice(CITIES)
        self.s.customers[cid].update(city=city, country=country)
        self.emit("customers", "update", cid, city=city, country=country)

    def change_price(self):
        pid = self.s.product_ids.choice(self.r)
        factor = Decimal(self.r.randint(80, 125)) / 100
        price = max(Decimal("1.00"), (self.s.products[pid] * factor).quantize(Decimal("0.01")))
        self.s.products[pid] = price
        self.emit("products", "update", pid, price=price)

    def advance_order(self) -> bool:
        # placed -> paid goes through pay_order (it also captures the payment)
        pools = [self.s.by_status[s] for s in ("paid", "shipped")]
        total = sum(len(p) for p in pools)
        if total == 0:
            return False
        i = self.r.randrange(total)
        pool = pools[0] if i < len(pools[0]) else pools[1]
        oid = pool.choice(self.r)
        status = NEXT_STATUS[self.s.orders[oid]["status"]]
        self.set_status(oid, status)
        self.emit("orders", "update", oid, status=status)
        return True

    def delete_lonely_customer(self) -> bool:
        if len(self.s.customers) <= 10 or not len(self.s.lonely):
            return False
        cid = self.s.lonely.choice(self.r)
        del self.s.customers[cid]
        self.s.customer_ids.discard(cid)
        self.s.lonely.discard(cid)
        self.emit("customers", "delete", cid)
        return True

    # --- multi-event actions (only when enough events are left) ---------
    def new_order(self) -> bool:
        n_items = self.r.randint(1, 4)
        if self.left() < n_items + 2:
            return False
        oid = self.s.new_id("orders")
        cid = self.s.customer_ids.choice(self.r)
        self.s.orders[oid] = {"customer_id": cid, "status": "placed"}
        self.s.by_status["placed"].add(oid)
        if n_items > 1:
            self.s.multi_placed.add(oid)
        self.s.customer_orders[cid] += 1
        self.s.lonely.discard(cid)
        self.emit("orders", "insert", oid, order_id=oid, customer_id=cid, status="placed")
        self.s.items[oid] = {}
        total = Decimal("0.00")
        pids = set()
        while len(pids) < min(n_items, len(self.s.product_ids)):
            pids.add(self.s.product_ids.choice(self.r))
        for pid in sorted(pids):
            iid = self.s.new_id("order_items")
            qty = self.r.randint(1, 3)
            price = self.s.products[pid]
            self.s.items[oid][iid] = price * qty
            total += price * qty
            self.emit(
                "order_items", "insert", iid,
                order_item_id=iid, order_id=oid, product_id=pid, quantity=qty, unit_price=price,
            )
        pay_id = self.s.new_id("payments")
        self.s.payments[oid] = {pay_id: "pending"}
        self.emit(
            "payments", "insert", pay_id,
            payment_id=pay_id, order_id=oid, amount=total, method=self.r.choice(METHODS), status="pending",
        )
        return True

    def pay_order(self) -> bool:
        if not len(self.s.by_status["placed"]) or self.left() < 2:
            return False
        oid = self.s.by_status["placed"].choice(self.r)
        self.set_status(oid, "paid")
        self.emit("orders", "update", oid, status="paid")
        for pay_id in sorted(self.s.payments[oid]):
            self.s.payments[oid][pay_id] = "captured"
            self.emit("payments", "update", pay_id, status="captured")
        return True

    def cancel_order(self) -> bool:
        pools = [self.s.by_status["placed"], self.s.by_status["paid"]]
        total = sum(len(p) for p in pools)
        if total == 0 or self.left() < 2:
            return False
        i = self.r.randrange(total)
        oid = (pools[0] if i < len(pools[0]) else pools[1]).choice(self.r)
        self.set_status(oid, "cancelled")
        self.emit("orders", "update", oid, status="cancelled")
        for pay_id in sorted(self.s.payments[oid]):
            self.s.payments[oid][pay_id] = "refunded"
            self.emit("payments", "update", pay_id, status="refunded")
        return True

    def remove_item(self) -> bool:
        if not len(self.s.multi_placed) or self.left() < 2:
            return False
        oid = self.s.multi_placed.choice(self.r)
        iid = self.r.choice(sorted(self.s.items[oid]))
        del self.s.items[oid][iid]
        if len(self.s.items[oid]) < 2:
            self.s.multi_placed.discard(oid)
        self.emit("order_items", "delete", iid)
        total = sum(self.s.items[oid].values(), Decimal("0.00"))
        for pay_id in sorted(self.s.payments[oid]):
            self.emit("payments", "update", pay_id, amount=total)
        return True

    def purge_cancelled_order(self) -> bool:
        if not len(self.s.by_status["cancelled"]):
            return False
        oid = self.s.by_status["cancelled"].choice(self.r)
        size = len(self.s.items[oid]) + len(self.s.payments[oid]) + 1
        if self.left() < size:
            return False
        for iid in sorted(self.s.items.pop(oid)):
            self.emit("order_items", "delete", iid)
        for pay_id in sorted(self.s.payments.pop(oid)):
            self.emit("payments", "delete", pay_id)
        self.s.by_status["cancelled"].discard(oid)
        cid = self.s.orders.pop(oid)["customer_id"]
        self.s.customer_orders[cid] -= 1
        if self.s.customer_orders[cid] == 0 and cid in self.s.customers:
            self.s.lonely.add(cid)
        self.emit("orders", "delete", oid)
        return True

    # ---------------------------------------------------------------------
    def run(self) -> list[Change]:
        n_products = max(10, self.n // 500)
        n_customers = max(20, self.n // 100)
        for _ in range(min(n_products, self.left())):
            self.new_product()
        for _ in range(min(n_customers, self.left())):
            self.new_customer()
        actions = [
            (self.new_order, 22), (self.pay_order, 14), (self.advance_order, 20),
            (self.cancel_order, 4), (self.remove_item, 3), (self.purge_cancelled_order, 3),
            (self.change_email, 6), (self.change_address, 6), (self.change_price, 8),
            (self.new_customer, 4), (self.new_product, 1), (self.delete_lonely_customer, 1),
        ]
        funcs = [a for a, _ in actions]
        weights = [w for _, w in actions]
        while self.left() > 0:
            action = self.r.choices(funcs, weights)[0]
            if action() is False:
                continue
        assert len(self.out) == self.n
        return self.out


def plan(seed: int, n_events: int) -> list[Change]:
    """Return exactly ``n_events`` deterministic changes for ``seed``."""
    if n_events < 0:
        raise ValueError("n_events must be >= 0")
    return _Planner(seed, n_events).run()


def counts(changes) -> dict:
    """Counts per table and operation: {table: {insert, update, delete}}."""
    c = {t: {"insert": 0, "update": 0, "delete": 0} for t in TABLES}
    for ch in changes:
        c[ch.table][ch.op] += 1
    return c


def apply(changes, dsn: str, batch: int = 500) -> None:
    """Execute changes in order; each statement must touch exactly one row."""
    import psycopg
    from psycopg import sql

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for i, ch in enumerate(changes, 1):
                if ch.table not in PK:
                    raise ValueError(f"unknown table {ch.table}")
                t, pk = sql.Identifier(ch.table), sql.Identifier(PK[ch.table])
                cols = [c for c, _ in ch.values]
                vals = [v for _, v in ch.values]
                if ch.op == "insert":
                    q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        t, sql.SQL(", ").join(map(sql.Identifier, cols)),
                        sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                    )
                    cur.execute(q, vals)
                elif ch.op == "update":
                    q = sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
                        t, sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in cols), pk,
                    )
                    cur.execute(q, [*vals, ch.key])
                elif ch.op == "delete":
                    cur.execute(sql.SQL("DELETE FROM {} WHERE {} = %s").format(t, pk), [ch.key])
                else:
                    raise ValueError(f"unknown op {ch.op}")
                if cur.rowcount != 1:
                    raise RuntimeError(f"change {i} {ch.table} {ch.op} {ch.key} touched {cur.rowcount} rows")
                if i % batch == 0:
                    conn.commit()
        conn.commit()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--events", type=int, required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dsn", default=os.environ.get("SHOP_PG_DSN", "postgresql://shop:shop@127.0.0.1:5433/shop"))
    p.add_argument("--dry-run", action="store_true", help="plan and count only")
    p.add_argument("--report", help="write the counts JSON to this file")
    a = p.parse_args(argv)
    changes = plan(a.seed, a.events)
    if not a.dry_run:
        apply(changes, a.dsn)
    c = counts(changes)
    total = Counter()
    for per in c.values():
        total.update(per)
    report = {"seed": a.seed, "events": a.events, "applied": not a.dry_run, "by_table": c, "by_op": dict(total)}
    text = json.dumps(report, indent=2)
    if a.report:
        with open(a.report, "w") as f:
            f.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())


TRACKED = {"customers": ("name", "email", "city", "country"), "products": ("name", "category", "price")}


def expected_versions(changes) -> dict:
    """History versions each key should have: its insert, plus each update that
    really changes a tracked column. Used to check the SCD2 tables."""
    out = {t: {} for t in TRACKED}
    state = {t: {} for t in TRACKED}
    for ch in changes:
        if ch.table not in TRACKED:
            continue
        cols = TRACKED[ch.table]
        vals = ch.as_dict()
        if ch.op == "insert":
            state[ch.table][ch.key] = {c: vals[c] for c in cols}
            out[ch.table][ch.key] = 1
        elif ch.op == "update":
            row = state[ch.table][ch.key]
            new = {**row, **{c: vals[c] for c in cols if c in vals}}
            if new != row:
                out[ch.table][ch.key] += 1
            state[ch.table][ch.key] = new
    return out
