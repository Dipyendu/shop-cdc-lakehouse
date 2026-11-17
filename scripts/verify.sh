#!/usr/bin/env bash
# Re-run the local and capture acceptance checks; each check's result is its exit code.
# Usage: scripts/verify.sh <evidence dir> <summary file>
# Needs the shopcdc stack (PostgreSQL, Kafka, Connect) running and conf/local.env set.
set -u
cd "$(dirname "$0")/.."
E="${1:?evidence dir}"; S="${2:?summary file}"; mkdir -p "$E"
W="scripts/with-secrets.sh uv run python"
res(){ echo "$1 | $([ "$2" = 0 ] && echo PASS || echo "FAIL (exit $2)") | $(date '+%H:%M:%S') | $3" | tee -a "$S"; }
echo "# Verification — started $(date '+%Y-%m-%d %H:%M IST')" > "$S"
echo "check | result | time IST | evidence" >> "$S"

make ci > "$E/1-make-ci.txt" 2>&1; rc=$?; res "1 make ci ($(grep -Eo '[0-9]+ passed' "$E/1-make-ci.txt"))" $rc "$E/1-make-ci.txt"

# 10. secret scan: a planted fake token must be found (exit 1), then a clean index passes (exit 0)
python3 -c "print('t=dapi'+'0123456789abcdef'*2)" > planted_verify.txt; git add -f planted_verify.txt
scripts/secret_scan.sh > "$E/10-secret-scan.txt" 2>&1; planted=$?
git rm -q --cached planted_verify.txt; /usr/bin/trash planted_verify.txt
scripts/secret_scan.sh >> "$E/10-secret-scan.txt" 2>&1; clean=$?
echo "planted exit $planted (must be 1), clean exit $clean (must be 0)" >> "$E/10-secret-scan.txt"
res "10 secret scan + planted proof" "$([ $planted = 1 ] && [ $clean = 0 ] && echo 0 || echo 1)" "$E/10-secret-scan.txt"

# 3. lander re-run over the same offsets: fresh folder and group, run, reset the group, run again
L=$(mktemp -d "${TMPDIR:-/tmp}/lander-verify.XXXX"); G="shop-lander-verify-$(date +%s)"
PYTHONPATH=src uv run python -m shop.lander --landing "$L" --group "$G" > "$E/3-lander-run1.json" 2>&1
(cd "$L" && find . -type f -name '*.jsonl' | sort | xargs shasum -a 256) > "$E/3-before.sha"
docker compose -p shopcdc exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group "$G" --reset-offsets --to-earliest --all-topics --execute > "$E/3-reset.txt" 2>&1
PYTHONPATH=src uv run python -m shop.lander --landing "$L" --group "$G" > "$E/3-lander-rerun.json" 2>&1
(cd "$L" && find . -type f -name '*.jsonl' | sort | xargs shasum -a 256) > "$E/3-after.sha"
cmp -s "$E/3-before.sha" "$E/3-after.sha" && [ -s "$E/3-after.sha" ] && ! grep -q '"files_written"' "$E/3-lander-rerun.json"; rc=$?
res "3 lander re-run ($(wc -l < "$E/3-after.sha" | tr -d ' ') files, $(grep -o '"files_unchanged": [0-9]*' "$E/3-lander-rerun.json"))" $rc "$E/3-before.sha, $E/3-after.sha, $E/3-lander-rerun.json"
/usr/bin/trash "$L"

# 8. dbt build on both runs' tables
SHOP_MARTS_SCHEMA=shop_marts scripts/with-secrets.sh uv run dbt build --project-dir dbt --profiles-dir dbt > "$E/8-dbt-build-100k.txt" 2>&1; rc=$?
res "8 dbt build, 100,000-change tables ($(sed 's/\x1b\[[0-9;]*m//g' "$E/8-dbt-build-100k.txt" | grep -o 'PASS=[0-9]* WARN=[0-9]* ERROR=[0-9]*'))" $rc "$E/8-dbt-build-100k.txt"
SHOP_MARTS_SCHEMA=shop_1m_marts scripts/with-secrets.sh uv run dbt build --project-dir dbt --profiles-dir dbt --vars '{silver_schema: shop_1m_silver, bronze_schema: shop_1m_bronze}' > "$E/8-dbt-build-1m.txt" 2>&1; rc=$?
res "8 dbt build, 1,000,000-change tables ($(sed 's/\x1b\[[0-9;]*m//g' "$E/8-dbt-build-1m.txt" | grep -o 'PASS=[0-9]* WARN=[0-9]* ERROR=[0-9]*'))" $rc "$E/8-dbt-build-1m.txt"

# 2. capture: insert, update, delete on a new key, read back from Kafka (changes the source, so it runs last)
make connector > "$E/2-connector-register.txt" 2>&1 && make connector >> "$E/2-connector-register.txt" 2>&1; rc=$?
res "15 make connector twice (idempotent, curl -f)" $rc "$E/2-connector-register.txt"
P(){ docker compose -p shopcdc exec -T postgres psql -U shop -d shop -Atc "$1"; }
K=$(( 990000 + $(date +%s) % 9000 ))
{ P "insert into customers(customer_id,name,email,city,country) values ($K,'Verify User','v1@example.com','Pune','IN')"; P "update customers set email='v2@example.com' where customer_id=$K"; P "delete from customers where customer_id=$K"; sleep 5
  end=$(docker compose -p shopcdc exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic shop.public.customers 2>/dev/null | awk -F: '{print $3}')
  echo "topic end offset: $end"
  docker compose -p shopcdc exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic shop.public.customers --partition 0 --offset $(( end - 10 )) --max-messages 10 --timeout-ms 20000 2>/dev/null | grep "\"customer_id\":$K" | grep -o '"op":"[cud]"'; } > "$E/2-capture.txt" 2>&1
[ "$(sort -u "$E/2-capture.txt" | grep -c '"op":"[cud]"')" = 3 ]; rc=$?
res "2 capture c/u/d (key $K)" $rc "$E/2-capture.txt"

# 9. every README evidence link resolves; EXPLAIN has its 12 questions
miss=$(grep -o '](docs/[^)]*)' README.md | tr -d '()]' | sort -u | while read -r f; do [ -e "$f" ] || echo "$f"; done)
q=$(grep -cE '^[0-9]+\. ' docs/EXPLAIN.md)
[ -z "$miss" ] && [ "$q" -ge 12 ]; rc=$?
res "9 README links resolve ($([ -z "$miss" ] && echo all || echo "missing: $miss")); EXPLAIN questions: $q" $rc "README.md, docs/EXPLAIN.md"
echo "finished $(date '+%Y-%m-%d %H:%M IST')" >> "$S"
