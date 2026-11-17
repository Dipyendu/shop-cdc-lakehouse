"""Lander: Kafka change events -> JSON Lines files in a local landing folder.

- Files are cut at fixed offset blocks (``block`` offsets per file) and at the
  partition end seen at the start of the run, so a re-run over the same offsets
  produces the same file names and the same bytes.
- File name: ``<topic>__p<partition>__<first offset>-<last offset>.jsonl``
  (offsets zero-padded to 12 digits), under ``<landing>/<table>/``.
- Each line keeps the raw Kafka value unchanged in ``payload`` plus the Kafka
  topic, partition, offset and key.
- Tombstones (null value) are skipped and counted.
- A file is written to a temporary name, fsynced and renamed; only then is the
  offset committed. An existing file with the same name must have identical
  bytes (then it is left alone); different bytes stop the run.

Usage: python -m shop.lander [--landing landing] [--block 2000]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

TOPIC_PREFIX = "shop.public."


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


def table_of(topic: str) -> str:
    if not topic.startswith(TOPIC_PREFIX):
        raise ValueError(f"unexpected topic {topic}")
    return topic[len(TOPIC_PREFIX):]


def file_name(topic: str, partition: int, first: int, last: int) -> str:
    return f"{topic}__p{partition}__{first:012d}-{last:012d}.jsonl"


def block_of(offset: int, block: int) -> int:
    return offset // block


def render(records: list[Record]) -> bytes:
    """One JSON object per line; payload is the raw Kafka value as text."""
    lines = []
    for r in records:
        lines.append(json.dumps({
            "kafka_topic": r.topic,
            "kafka_partition": r.partition,
            "kafka_offset": r.offset,
            "kafka_key": r.key.decode("utf-8") if r.key is not None else None,
            "payload": r.value.decode("utf-8"),
        }, separators=(",", ":"), ensure_ascii=False))
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_atomic(path: Path, data: bytes) -> str:
    """Write ``data`` to ``path`` safely. Returns 'written' or 'unchanged'."""
    if path.exists():
        if path.read_bytes() == data:
            return "unchanged"
        raise RuntimeError(f"{path} exists with different content; refusing to overwrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    dfd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return "written"


class FileCutter:
    """Groups one partition's records into files at block boundaries."""

    def __init__(self, landing: Path, block: int):
        self.landing = landing
        self.block = block
        self.buf: list[Record] = []
        self.stats = Counter()

    def add(self, r: Record) -> list[Path]:
        """Add a record; returns files completed (flushed) by this record."""
        done = []
        if self.buf and block_of(r.offset, self.block) != block_of(self.buf[-1].offset, self.block):
            done += self.flush()
        if r.value is None:
            self.stats["tombstones_skipped"] += 1
            return done
        self.buf.append(r)
        return done

    def flush(self) -> list[Path]:
        if not self.buf:
            return []
        first, last = self.buf[0], self.buf[-1]
        path = self.landing / table_of(first.topic) / file_name(first.topic, first.partition, first.offset, last.offset)
        result = write_atomic(path, render(self.buf))
        self.stats[f"files_{result}"] += 1
        self.stats["events_in_files"] += len(self.buf)
        self.buf = []
        return [path]


def run(bootstrap: str, landing: Path, block: int, group: str, consumer=None, max_seconds: float = 900) -> dict:
    """Consume every shop topic from the committed offset to the end seen at start.
    ``consumer`` lets tests pass a fake; the run fails after ``max_seconds``."""
    from confluent_kafka import TopicPartition

    if consumer is None:
        from confluent_kafka import Consumer

        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
    c = consumer
    deadline = time.monotonic() + max_seconds
    try:
        topics = sorted(t for t in c.list_topics(timeout=10).topics if t.startswith(TOPIC_PREFIX))
        parts = []
        for t in topics:
            for p in sorted(c.list_topics(t, timeout=10).topics[t].partitions):
                parts.append(TopicPartition(t, p))
        committed = {(tp.topic, tp.partition): tp.offset for tp in c.committed(parts, timeout=10)}
        todo, start = {}, []
        for tp in parts:
            low, high = c.get_watermark_offsets(tp, timeout=10)
            pos = committed[(tp.topic, tp.partition)]
            pos = low if pos < 0 else pos
            if pos < high:
                todo[(tp.topic, tp.partition)] = high
                start.append(TopicPartition(tp.topic, tp.partition, pos))
        stats = Counter()
        cutters = {k: FileCutter(landing, block) for k in todo}
        if start:
            c.assign(start)
        while todo:
            if time.monotonic() > deadline:
                raise TimeoutError(f"lander did not reach the end within {max_seconds} s; still open: {sorted(todo)}")
            msg = c.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            k = (msg.topic(), msg.partition())
            if k not in todo or msg.offset() >= todo[k]:
                continue  # beyond the end seen at start: next run
            cut = cutters[k]
            completed = cut.add(Record(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value()))
            if completed:
                # all records before this one are in files now
                c.commit(offsets=[TopicPartition(k[0], k[1], msg.offset())], asynchronous=False)
            stats["events_read"] += 1
            if msg.offset() == todo[k] - 1:
                cut.flush()
                c.commit(offsets=[TopicPartition(k[0], k[1], todo[k])], asynchronous=False)
                del todo[k]
                c.incremental_unassign([TopicPartition(k[0], k[1])])
        for cut in cutters.values():
            stats.update(cut.stats)
        return {"partitions": len(cutters), **dict(sorted(stats.items()))}
    finally:
        c.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Kafka -> landing JSON Lines files")
    p.add_argument("--bootstrap", default=os.environ.get("SHOP_KAFKA", "127.0.0.1:29092"))
    p.add_argument("--landing", default="landing")
    p.add_argument("--block", type=int, default=2000)
    p.add_argument("--group", default="shop-lander")
    a = p.parse_args(argv)
    print(json.dumps(run(a.bootstrap, Path(a.landing), a.block, a.group), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
