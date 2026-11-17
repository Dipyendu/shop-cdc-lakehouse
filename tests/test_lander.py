import json

import pytest

from shop.lander import FileCutter, Record, file_name, render, write_atomic

T = "shop.public.orders"


def recs(offsets, tombstones=()):
    return [Record(T, 0, o, b'{"order_id":%d}' % o, None if o in tombstones else b'{"op":"c","n":%d}' % o)
            for o in offsets]


def cut_all(tmp_path, records, block=5):
    c = FileCutter(tmp_path, block)
    files = []
    for r in records:
        files += c.add(r)
    files += c.flush()
    return c, files


def test_file_name_is_zero_padded_and_ordered():
    assert file_name(T, 0, 7, 42) == "shop.public.orders__p0__000000000007-000000000042.jsonl"


def test_files_cut_at_block_boundaries(tmp_path):
    c, files = cut_all(tmp_path, recs(range(12)))
    assert [f.name for f in files] == [file_name(T, 0, 0, 4), file_name(T, 0, 5, 9), file_name(T, 0, 10, 11)]
    assert all(f.parent.name == "orders" for f in files)
    assert c.stats["events_in_files"] == 12


def test_resume_mid_block_does_not_overlap(tmp_path):
    _, a = cut_all(tmp_path, recs(range(7)))
    _, b = cut_all(tmp_path, recs(range(7, 12)))
    names = [f.name for f in a + b]
    assert names == [file_name(T, 0, 0, 4), file_name(T, 0, 5, 6), file_name(T, 0, 7, 9), file_name(T, 0, 10, 11)]


def test_tombstones_skipped_and_counted(tmp_path):
    c, files = cut_all(tmp_path, recs(range(5), tombstones={2, 4}))
    lines = files[0].read_text().splitlines()
    assert [json.loads(x)["kafka_offset"] for x in lines] == [0, 1, 3]
    assert c.stats["tombstones_skipped"] == 2
    assert files[0].name == file_name(T, 0, 0, 3)


def test_payload_kept_raw(tmp_path):
    _, files = cut_all(tmp_path, recs([0]))
    row = json.loads(files[0].read_text())
    assert row == {"kafka_topic": T, "kafka_partition": 0, "kafka_offset": 0,
                   "kafka_key": '{"order_id":0}', "payload": '{"op":"c","n":0}'}


def test_rerun_same_offsets_same_files_no_extra(tmp_path):
    _, _first = cut_all(tmp_path, recs(range(12)))
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    c, _second = cut_all(tmp_path, recs(range(12)))
    after = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert before == after
    assert c.stats["files_unchanged"] == 3 and c.stats["files_written"] == 0


def test_existing_file_with_other_content_is_not_overwritten(tmp_path):
    p = tmp_path / "x.jsonl"
    write_atomic(p, b"a\n")
    with pytest.raises(RuntimeError):
        write_atomic(p, b"b\n")
    assert p.read_bytes() == b"a\n"


def test_render_one_line_per_record():
    assert render(recs([0, 1])).count(b"\n") == 2
