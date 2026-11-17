from types import SimpleNamespace

from shop.upload import upload


class FakeFiles:
    """In-memory stand-in for databricks.sdk FilesAPI."""

    def __init__(self):
        self.store = {}
        self.dirs = set()

    def list_directory_contents(self, d):
        from databricks.sdk.errors import NotFound

        if d not in self.dirs:
            raise NotFound("no dir")
        return [SimpleNamespace(name=k.rsplit("/", 1)[1], is_directory=False)
                for k in self.store if k.rsplit("/", 1)[0] == d]

    def create_directory(self, d):
        self.dirs.add(d)

    def upload(self, path, fh, overwrite=None):
        assert overwrite is False and path not in self.store
        self.store[path] = fh.read()


def test_upload_is_safe_to_repeat(tmp_path):
    (tmp_path / "orders").mkdir()
    (tmp_path / "orders" / "a.jsonl").write_bytes(b"1\n")
    (tmp_path / "customers").mkdir()
    (tmp_path / "customers" / "b.jsonl").write_bytes(b"2\n")
    fake = FakeFiles()
    assert upload(tmp_path, "/Volumes/c/s/v", fake) == {"uploaded": 2, "skipped_existing": 0}
    assert upload(tmp_path, "/Volumes/c/s/v", fake) == {"uploaded": 0, "skipped_existing": 2}
    (tmp_path / "orders" / "c.jsonl").write_bytes(b"3\n")
    assert upload(tmp_path, "/Volumes/c/s/v", fake) == {"uploaded": 1, "skipped_existing": 2}
    assert fake.store["/Volumes/c/s/v/shop/orders/a.jsonl"] == b"1\n"
    assert len(fake.store) == 3


def test_failed_first_attempt_is_retried_with_overwrite(tmp_path, monkeypatch):
    import shop.upload as up

    monkeypatch.setattr(up.time, "sleep", lambda s: None)

    class Flaky(FakeFiles):
        def __init__(self):
            super().__init__()
            self.calls = []

        def upload(self, path, fh, overwrite=None):
            self.calls.append(overwrite)
            if len(self.calls) == 1:
                self.store[path] = fh.read()[:1]  # a partial file left behind, then a timeout
                raise TimeoutError("simulated")
            assert overwrite is True
            self.store[path] = fh.read()

    f = tmp_path / "a.jsonl"
    f.write_bytes(b"full content\n")
    fake = Flaky()
    up.put_one(fake, f, "/Volumes/c/s/v/shop/orders/a.jsonl")
    assert fake.calls == [False, True]
    assert fake.store["/Volumes/c/s/v/shop/orders/a.jsonl"] == b"full content\n"


def test_upload_gives_up_after_bounded_attempts(tmp_path, monkeypatch):
    import pytest

    import shop.upload as up

    monkeypatch.setattr(up.time, "sleep", lambda s: None)

    class Down(FakeFiles):
        n = 0

        def upload(self, path, fh, overwrite=None):
            Down.n += 1
            raise ConnectionError("down")

    f = tmp_path / "a.jsonl"
    f.write_bytes(b"x\n")
    with pytest.raises(ConnectionError):
        up.put_one(Down(), f, "/r/a.jsonl")
    assert Down.n == 3
