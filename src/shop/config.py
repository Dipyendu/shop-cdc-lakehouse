"""Project configuration from conf/shop.toml (catalog, LANDING_ROOT, checkpoint location)."""

import tomllib
from pathlib import Path

CONF = Path(__file__).resolve().parents[2] / "conf" / "shop.toml"


def _conf() -> dict:
    with open(CONF, "rb") as f:
        return tomllib.load(f)


def landing_root() -> str:
    return _conf()["landing_root"].rstrip("/")


def catalog() -> str:
    return _conf()["catalog"]


def checkpoints() -> str:
    return _conf()["checkpoints"].rstrip("/")
