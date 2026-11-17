#!/usr/bin/env bash
# Run a command with the Databricks settings loaded. Never prints them.
# Settings: conf/local.env (see conf/local.env.example), then the secrets file it names
# (default: .env in the repository root, which is gitignored).
set -euo pipefail
set +x
root="$(cd "$(dirname "$0")/.." && pwd)"
[ -f "$root/conf/local.env" ] && { set -a; . "$root/conf/local.env"; set +a; }
secrets="${SHOP_SECRETS_FILE:-$root/.env}"
[ -r "$secrets" ] || { echo "secrets file not found: set SHOP_SECRETS_FILE in conf/local.env" >&2; exit 2; }
set -a; . "$secrets"; set +a
export DATABRICKS_HOST="${DATABRICKS_HOST%%\?*}"; export DATABRICKS_HOST="${DATABRICKS_HOST%/}"
: "${SHOP_WAREHOUSE_HTTP_PATH:?set SHOP_WAREHOUSE_HTTP_PATH in conf/local.env}"
export SHOP_WAREHOUSE_HTTP_PATH SHOP_RUN_STOP_LOG="${SHOP_RUN_STOP_LOG:-}"
export SHOP_CATALOG="$(sed -n 's/^catalog = "\(.*\)"/\1/p' "$root/conf/shop.toml")"
export PYTHONPATH="${PYTHONPATH:-src}"
exec "$@"
