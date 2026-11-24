from dynamokv.storage import FileStorage, MemoryStorage
from dynamokv.version import DottedVersionVectors as DVV
from dynamokv.version import Version


def test_apply_merges_and_reports_new_state():
    store = MemoryStorage(DVV)
    a = Version("n1", 1, {}, b"a")
    assert store.apply("k", [a]) == [a]
    b = Version("n1", 2, {"n1": 1}, b"b")
    assert store.apply("k", [b]) == [b]
    assert store.apply("k", [a]) == [b]          # an old version does not come back


def test_hints_are_kept_per_target_and_removed_once_delivered():
    store = MemoryStorage(DVV)
    a, b = Version("n1", 1, {}, b"a"), Version("n2", 1, {}, b"b")
    store.add_hint("n4", "k", [a])
    store.add_hint("n4", "k", [b])
    assert store.hint_targets() == ["n4"] and len(store.hinted("k")) == 2
    store.remove_hints("n4", "k", frozenset({a.dot}))
    assert [v.value for v in store.hinted("k")] == [b"b"]
    store.remove_hints("n4", "k", frozenset({b.dot}))
    assert store.hint_count() == 0


def test_file_storage_survives_restart(tmp_path):
    store = FileStorage(DVV, tmp_path)
    counter = store.next_counter()
    store.apply("k", [Version("n1", counter, {}, b"\x00binary\xff")])
    store.add_hint("n3", "h", [Version("n1", 9, {}, None)])
    epoch = store.next_epoch()
    store.close()

    again = FileStorage(DVV, tmp_path)
    assert [v.value for v in again.get("k")] == [b"\x00binary\xff"]
    assert again.next_counter() == counter + 1     # never reuses a dot
    assert again.next_epoch() == epoch + 1
    assert again.hinted("h")[0].value is None      # tombstones survive too


def test_torn_last_record_is_ignored(tmp_path):
    store = FileStorage(DVV, tmp_path)
    store.apply("k", [Version("n1", store.next_counter(), {}, b"ok")])
    store.close()
    with open(tmp_path / "wal.jsonl", "a") as wal:
        wal.write('{"k": "k", "v": [["n1", 2, {}, "trunc')
    assert [v.value for v in FileStorage(DVV, tmp_path).get("k")] == [b"ok"]
