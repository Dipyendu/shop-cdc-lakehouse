#!/usr/bin/env bash
# Scan the STAGED diff (added lines only) for secrets. Prints pattern names and
# file names only, never the matched text. Exit 0 = clean, 1 = finding, 2 = error.
set -u
diff=$(git diff --cached --no-color -U0 --diff-filter=ACMR) || exit 2
added=$(printf '%s\n' "$diff" | grep -E '^\+' | grep -vE '^\+\+\+ ')
found=0
check() {  # name, extended regex
  local n; n=$(printf '%s\n' "$added" | grep -cE -- "$2")
  if [ "$n" -gt 0 ]; then echo "FINDING: $1 ($n line(s))"; found=1; fi
}
check "databricks-pat"        'dapi[0-9a-f]{32}'
check "databricks-oauth"      'dose[0-9a-f]{32}'
check "aws-access-key-id"     '(AKIA|ASIA)[0-9A-Z]{16}'
check "aws-secret-key-assign" 'aws_secret_access_key[[:space:]]*[=:][[:space:]]*[A-Za-z0-9/+]{40}'
check "token-assignment"      'DATABRICKS_TOKEN[[:space:]]*=[[:space:]]*[^[:space:]$"{]'
check "private-key-block"     '-----BEGIN [A-Z ]*PRIVATE KEY-----'
# The real token value, if the secrets file is filled (value never printed).
root="$(git rev-parse --show-toplevel)"
[ -f "$root/conf/local.env" ] && SHOP_SECRETS_FILE="$(set -a; . "$root/conf/local.env"; printf '%s' "${SHOP_SECRETS_FILE:-}")"
sf="${SHOP_SECRETS_FILE:-$root/.env}"
if [ -r "$sf" ]; then
  tok=$(set -a; . "$sf"; printf '%s' "${DATABRICKS_TOKEN:-}")
  if [ -n "$tok" ] && printf '%s\n' "$added" | grep -qF -- "$tok"; then echo "FINDING: live DATABRICKS_TOKEN value"; found=1; fi
fi
files=$(git diff --cached --name-only --diff-filter=ACMR | grep -E '(^|/)(\.env[^/]*|[^/]*\.env|\.databrickscfg|[^/]*\.tfstate[^/]*)$')
[ -n "$files" ] && { echo "FINDING: secret-bearing file name staged:"; printf '  %s\n' $files; found=1; }
[ "$found" -eq 0 ] && echo "secret scan: clean ($(git diff --cached --name-only | wc -l | tr -d ' ') staged file(s))"
exit $found
