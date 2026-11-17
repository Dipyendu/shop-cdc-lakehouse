from decimal import Decimal

import pytest

from shop.generator import PK, TABLES, counts, plan


def test_same_seed_same_plan():
    assert plan(7, 3000) == plan(7, 3000)


def test_different_seed_different_plan():
    assert plan(7, 3000) != plan(8, 3000)


@pytest.mark.parametrize("n", [0, 1, 5, 37, 5000])
def test_exact_event_count(n):
    changes = plan(3, n)
    assert len(changes) == n
    assert sum(sum(per.values()) for per in counts(changes).values()) == n


def test_counts_by_table_and_op():
    changes = plan(1, 2000)
    c = counts(changes)
    for t in TABLES:
        for op in ("insert", "update", "delete"):
            assert c[t][op] == sum(1 for ch in changes if ch.table == t and ch.op == op)


def test_mix_meets_plan_minimums():
    c = counts(plan(42, 20000))
    total = sum(sum(per.values()) for per in c.values())
    updates = sum(per["update"] for per in c.values())
    deletes = sum(per["delete"] for per in c.values())
    assert updates / total >= 0.10
    assert deletes / total >= 0.01
    # every table sees every kind of change it should
    assert c["orders"]["update"] and c["customers"]["update"] and c["products"]["update"]
    assert c["order_items"]["delete"] and c["orders"]["delete"] and c["payments"]["delete"]


def test_plan_is_consistent_with_keys_and_foreign_keys():
    """Replay the plan on an in-memory model: no duplicate inserts, no change to a
    missing row, no dangling foreign key, and price/amount values are 2-dp Decimals."""
    rows = {t: {} for t in TABLES}
    fks = {"orders": [("customer_id", "customers")],
           "order_items": [("order_id", "orders"), ("product_id", "products")],
           "payments": [("order_id", "orders")]}
    for ch in plan(5, 20000):
        tbl = rows[ch.table]
        vals = ch.as_dict()
        if ch.op == "insert":
            assert ch.key not in tbl and vals[PK[ch.table]] == ch.key
            for col, parent in fks.get(ch.table, []):
                assert vals[col] in rows[parent]
            tbl[ch.key] = vals
        elif ch.op == "update":
            assert ch.key in tbl
            tbl[ch.key].update(vals)
        else:
            assert ch.key in tbl
            # nothing may still reference the deleted row
            for child, refs in fks.items():
                for col, parent in refs:
                    if parent == ch.table:
                        assert all(r[col] != ch.key for r in rows[child].values())
            del tbl[ch.key]
        for col in ("price", "unit_price", "amount"):
            if col in vals:
                assert isinstance(vals[col], Decimal) and vals[col].as_tuple().exponent == -2


def test_negative_count_rejected():
    with pytest.raises(ValueError):
        plan(1, -1)


def test_expected_versions_counts_only_real_changes():
    from shop.generator import Change, expected_versions

    ch = [
        Change("customers", "insert", 1, (("customer_id", 1), ("name", "A"), ("email", "a@x"),
                                          ("city", "Pune"), ("country", "IN"))),
        Change("customers", "update", 1, (("email", "b@x"),)),
        Change("customers", "update", 1, (("city", "Pune"), ("country", "IN"))),  # no real change
        Change("customers", "delete", 1),
        Change("products", "insert", 9, (("product_id", 9), ("name", "p"), ("category", "c"),
                                         ("price", Decimal("1.00")))),
    ]
    assert expected_versions(ch) == {"customers": {1: 2}, "products": {9: 1}}
