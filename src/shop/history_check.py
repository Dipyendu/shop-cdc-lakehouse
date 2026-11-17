"""The four history (SCD2) checks for customers and products.

1. Intervals of one key do not overlap: ordered by valid_from_lsn, each version
   ends exactly where the next begins (valid_to_lsn = next valid_from_lsn).
2. A key that is not deleted in silver has exactly one current row.
3. A deleted key has no current row.
4. Versions per key = 1 (insert) + updates that really changed a tracked column,
   counted independently from the generator plan (same seed and event count).
5. For each live key, the current history row equals the silver row on every tracked column.

Usage: python -m shop.history_check --silver S --seed 42 --events N
"""

from __future__ import annotations

import argparse
import json
import sys

from shop.dbx import CATALOG, query, sql_conn
from shop.generator import TRACKED, expected_versions, plan

PK = {"customers": "customer_id", "products": "product_id"}


def checks(conn, silver: str, expected: dict) -> dict:
    out = {}
    for t, pk in PK.items():
        h, s = f"{CATALOG}.{silver}.{t}_history", f"{CATALOG}.{silver}.{t}"
        overlaps = query(conn, f"""
            SELECT count(*) FROM (
              SELECT valid_from_lsn, valid_to_lsn,
                     lead(valid_from_lsn) OVER (PARTITION BY {pk} ORDER BY valid_from_lsn) AS nxt
              FROM {h})
            WHERE (nxt IS NOT NULL AND (valid_to_lsn IS NULL OR valid_to_lsn <> nxt))
               OR (valid_to_lsn IS NOT NULL AND valid_to_lsn <= valid_from_lsn)""")[0][0]
        live_bad = query(conn, f"""
            SELECT count(*) FROM {s} k
            LEFT JOIN (SELECT {pk}, count_if(is_current) AS n FROM {h} GROUP BY {pk}) c USING ({pk})
            WHERE NOT k._deleted AND coalesce(c.n, 0) <> 1""")[0][0]
        deleted_bad = query(conn, f"""
            SELECT count(*) FROM {s} k JOIN {h} USING ({pk}) WHERE k._deleted AND is_current""")[0][0]
        same = " AND ".join(f"h.{c} <=> k.{c}" for c in TRACKED[t])
        value_bad = query(conn, f"""
            SELECT count(*) FROM {s} k JOIN {h} h ON h.{pk} = k.{pk} AND h.is_current
            WHERE NOT k._deleted AND NOT ({same})""")[0][0]
        got = dict(query(conn, f"SELECT {pk}, count(*) FROM {h} GROUP BY {pk}"))
        exp = expected[t]
        mismatched = sorted(k for k in set(exp) | set(got) if exp.get(k) != got.get(k))
        out[t] = {
            "history_rows": sum(got.values()), "expected_versions": sum(exp.values()), "keys": len(exp),
            "1_overlapping_or_gapped_intervals": overlaps,
            "2_live_keys_without_exactly_one_current": live_bad,
            "3_deleted_keys_with_a_current_row": deleted_bad,
            "4_keys_with_wrong_version_count": len(mismatched), "examples": mismatched[:10],
            "5_current_rows_differing_from_silver": value_bad,
        }
        out[t]["pass"] = (overlaps == 0 and live_bad == 0 and deleted_bad == 0 and not mismatched and value_bad == 0
                          and sum(got.values()) > 0)
    out["ALL_PASS"] = all(v["pass"] for v in out.values())
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--silver", default="shop_silver")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--events", type=int, required=True)
    p.add_argument("--extra", action="append", default=[],
                   help="table:key, one extra version for a change made outside the plan (e.g. the latency test)")
    a = p.parse_args(argv)
    expected = expected_versions(plan(a.seed, a.events))
    for x in a.extra:
        t, k = x.split(":")
        expected[t][int(k)] += 1
    assert set(expected) == set(TRACKED)
    r = checks(sql_conn(), a.silver, expected)
    print(json.dumps(r, indent=2))
    return 0 if r["ALL_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
