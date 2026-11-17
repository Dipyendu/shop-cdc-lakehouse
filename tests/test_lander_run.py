"""lander.run against a fake Kafka consumer: offsets are committed only after every record
below the committed offset is in a file on disk, and a crash at a commit loses nothing."""
import json
from types import SimpleNamespace

import pytest

from shop.lander import run

T = "shop.public.orders"


class Msg:
    def __init__(self, offset, value):
        self._o, self._v = offset, value

    def error(self):
        return None

    def topic(self):
        return T

    def partition(self):
        return 0

    def offset(self):
        return self._o

    def key(self):
        return b'{"order_id":%d}' % self._o

    def value(self):
        return self._v


class FakeConsumer:
    """One topic, one partition, records 0..n-1; tombstones at the given offsets."""

    def __init__(self, landing, n=12, tombstones=(3, 11), committed=-1001, crash_at_commit=None):
        self.landing, self.n = landing, n
        self.records = [Msg(o, None if o in tombstones else b'{"op":"c","n":%d}' % o) for o in range(n)]
        self.committed_offset = committed
        self.crash_at_commit = crash_at_commit
        self.commits = []
        self.queue = []

    def list_topics(self, topic=None, timeout=None):
        return SimpleNamespace(topics={T: SimpleNamespace(partitions={0: None})})

    def committed(self, parts, timeout=None):
        return [SimpleNamespace(topic=T, partition=0, offset=self.committed_offset)]

    def get_watermark_offsets(self, tp, timeout=None):
        return 0, self.n

    def assign(self, parts):
        self.queue = [r for r in self.records if r.offset() >= parts[0].offset]

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None

    def offsets_on_disk(self):
        out = set()
        for f in self.landing.rglob("*.jsonl"):
            out |= {json.loads(x)["kafka_offset"] for x in f.read_text().splitlines()}
        return out

    def commit(self, offsets, asynchronous=True):
        assert asynchronous is False
        new = offsets[0].offset
        if self.crash_at_commit is not None and len(self.commits) == self.crash_at_commit:
            raise RuntimeError("simulated crash before the commit reached Kafka")
        needed = {r.offset() for r in self.records if r.offset() < new and r.value() is not None}
        assert needed <= self.offsets_on_disk(), f"commit {new} before records {needed - self.offsets_on_disk()} were on disk"
        self.commits.append(new)
        self.committed_offset = new

    def incremental_unassign(self, parts):
        pass

    def close(self):
        pass


def test_commits_follow_file_writes_and_reach_the_end(tmp_path):
    c = FakeConsumer(tmp_path)
    stats = run("unused", tmp_path, 5, "g", consumer=c)
    assert c.commits == [5, 10, 12]  # after each completed block, then the partition end
    assert c.offsets_on_disk() == set(range(12)) - {3, 11}
    assert stats["tombstones_skipped"] == 2 and stats["files_written"] == 3


def test_crash_at_a_commit_then_rerun_loses_nothing_and_adds_no_file(tmp_path):
    c = FakeConsumer(tmp_path, crash_at_commit=1)  # the second commit fails
    with pytest.raises(RuntimeError):
        run("unused", tmp_path, 5, "g", consumer=c)
    files_after_crash = {f.name: f.read_bytes() for f in tmp_path.rglob("*.jsonl")}
    resumed = FakeConsumer(tmp_path, committed=c.committed_offset)
    stats = run("unused", tmp_path, 5, "g", consumer=resumed)
    files = {f.name: f.read_bytes() for f in tmp_path.rglob("*.jsonl")}
    assert all(files[k] == v for k, v in files_after_crash.items())  # nothing rewritten
    assert len(files) == 3 and resumed.offsets_on_disk() == set(range(12)) - {3, 11}
    assert stats["files_unchanged"] == 1  # the block written before the crash, seen again


def test_deadline_stops_a_run_that_cannot_reach_the_end(tmp_path):
    c = FakeConsumer(tmp_path)
    c.assign = lambda parts: None  # nothing ever arrives
    with pytest.raises(TimeoutError):
        run("unused", tmp_path, 5, "g", consumer=c, max_seconds=0)
