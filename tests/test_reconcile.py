import datetime as dt
from decimal import Decimal

from shop.reconcile import canon, compare, row_hash, table_hash

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def test_canonical_forms():
    assert canon(None) is None
    assert canon(42) == "42"
    assert canon(True) == "true"
    assert canon(Decimal("1234.50")) == "1234.50"
    assert canon(Decimal("10.00")) == "10.00"
    # same instant: aware IST, aware UTC, naive (read in a UTC session)
    a = dt.datetime(2026, 11, 16, 19, 50, 31, 18481, tzinfo=IST)
    b = dt.datetime(2026, 11, 16, 14, 20, 31, 18481, tzinfo=dt.UTC)
    c = dt.datetime(2026, 11, 16, 14, 20, 31, 18481)
    assert canon(a) == canon(b) == canon(c) == "2026-11-16T14:20:31.018481Z"
    assert canon(dt.datetime(2026, 1, 1, tzinfo=dt.UTC)) == "2026-01-01T00:00:00.000000Z"


def test_decimal_scale_matters():
    assert row_hash((1, Decimal("10.0"))) != row_hash((1, Decimal("10.00")))


def test_null_differs_from_text():
    assert row_hash((1, None)) != row_hash((1, "None")) != row_hash((1, ""))


def test_compare_passes_on_equal_tables():
    rows = [(1, "a", Decimal("1.00")), (2, "b", None)]
    r = compare(rows, list(reversed(rows)))
    assert r["pass"] and r["differing_rows"] == 0 and r["source_rows"] == r["lake_rows"] == 2


def test_compare_reports_exactly_the_changed_row():
    src = [(i, f"name{i}", Decimal("1.00")) for i in range(100)]
    lake = list(src)
    lake[37] = (37, "name37", Decimal("1.01"))
    r = compare(src, lake)
    assert not r["pass"]
    assert r["differing_rows"] == 1 and r["examples"]["differing"] == [37]
    assert r["missing_in_lake"] == r["extra_in_lake"] == 0


def test_compare_reports_missing_and_extra():
    r = compare([(1, "a"), (2, "b")], [(2, "b"), (3, "c")])
    assert r["examples"]["missing"] == [1] and r["examples"]["extra"] == [3] and not r["pass"]


def test_table_hash_ignores_row_order_but_not_content():
    rows = [(1, "a"), (2, "b")]
    assert table_hash(rows) == table_hash(rows[::-1])
    assert table_hash(rows) != table_hash([(1, "a"), (2, "c")])


def test_empty_comparison_does_not_pass():
    r = compare([], [])
    assert not r["pass"] and r["source_rows"] == 0
