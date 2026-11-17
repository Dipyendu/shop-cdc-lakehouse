"""Volume landing check: create the landing schema and a managed volume, upload a small file, list it and read it back."""
import io
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

from shop import config

w = WorkspaceClient()
cat, schema, vol = config.catalog(), "shop_landing", "landing"
if schema not in {s.name for s in w.schemas.list(catalog_name=cat)}:
    w.schemas.create(name=schema, catalog_name=cat)
if vol not in {v.name for v in w.volumes.list(catalog_name=cat, schema_name=schema)}:
    w.volumes.create(catalog_name=cat, schema_name=schema, name=vol, volume_type=VolumeType.MANAGED)
root = f"/Volumes/{cat}/{schema}/{vol}"
t0 = time.time()
w.files.upload(f"{root}/_t3/hello.txt", io.BytesIO(b"t3 shop\n"), overwrite=True)
print("upload seconds", round(time.time() - t0, 2))
print("listed:", [e.path for e in w.files.list_directory_contents(f"{root}/_t3")])
print("read back:", w.files.download(f"{root}/_t3/hello.txt").contents.read())
