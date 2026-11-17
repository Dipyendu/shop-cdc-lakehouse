"""One-time Databricks setup from conf/shop.toml: the schema and the two managed volumes
named by landing_root and checkpoints (/Volumes/<catalog>/<schema>/<volume>). The catalog
must already exist. Safe to repeat.

Usage: scripts/with-secrets.sh uv run python scripts/setup_databricks.py
"""
from databricks.sdk.service.catalog import VolumeType

from shop import config
from shop.dbx import client

w = client()
for path in (config.landing_root(), config.checkpoints()):
    _, _, cat, schema, vol = path.split("/")[:5]
    if cat != config.catalog():
        raise SystemExit(f"{path} is not in catalog {config.catalog()}")
    if schema not in {s.name for s in w.schemas.list(catalog_name=cat)}:
        w.schemas.create(name=schema, catalog_name=cat)
        print(f"created schema {cat}.{schema}")
    if vol not in {v.name for v in w.volumes.list(catalog_name=cat, schema_name=schema)}:
        w.volumes.create(catalog_name=cat, schema_name=schema, name=vol, volume_type=VolumeType.MANAGED)
        print(f"created volume {path}")
    print(f"ok: {path}")
