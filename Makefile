.PHONY: secret-scan lint test dbt-parse ci up down generate connector connector-status land upload deploy
secret-scan:
	scripts/secret_scan.sh
lint:
	uv run ruff check --output-format concise src tests jobs scripts
test:
	uv run pytest -q
dbt-parse:
	DATABRICKS_HOST=ci.invalid DATABRICKS_TOKEN="not-a-secret" SHOP_WAREHOUSE_HTTP_PATH=/ci uv run dbt parse --project-dir dbt --profiles-dir dbt
ci: lint test dbt-parse
up:
	@command -v memory_pressure >/dev/null && memory_pressure | tail -1 || true
	docker compose -p shopcdc up -d
down:
	docker compose -p shopcdc stop
EVENTS ?= 5000
SEED ?= 42
generate:
	PYTHONPATH=src uv run python -m shop.generator --events $(EVENTS) --seed $(SEED)
connector:  # waits for Connect; PUT .../config creates or updates (safe to repeat); -f fails on an HTTP error
	scripts/wait_for_connect.sh
	python3 -c "import json; print(json.dumps(json.load(open('connect/shop-postgres.json'))['config']))" | \
	  curl -fsS -X PUT -H 'Content-Type: application/json' --data @- 127.0.0.1:8083/connectors/shop-postgres/config
	@echo
connector-status:
	curl -fsS 127.0.0.1:8083/connectors/shop-postgres/status
	@echo
land:
	PYTHONPATH=src uv run python -m shop.lander --landing landing
upload:
	scripts/with-secrets.sh uv run python -m shop.upload --landing landing
conf = $(shell sed -n 's/^$(1) = "\(.*\)"/\1/p' conf/shop.toml)
deploy:
	scripts/with-secrets.sh databricks bundle deploy --var landing_root=$(call conf,landing_root) \
	  --var catalog=$(call conf,catalog) --var checkpoints=$(call conf,checkpoints)
