from dynamokv.version import DottedVersionVectors as DVV
from dynamokv.version import PaperVectorClocks as VC
from dynamokv.version import Version, join_context, reconcile


def test_write_with_context_supersedes_what_it_read():
    a = Version("n1", 1, {}, b"a")
    b = Version("n2", 2, join_context([a]), b"b")
    assert reconcile(DVV, [a], [b]) == [b]


def test_concurrent_writes_are_both_kept():
    a = Version("n1", 1, {}, b"a")
    b = Version("n1", 2, {}, b"b")        # same coordinator, but the client never saw a
    assert sorted(v.value for v in reconcile(DVV, [a], [b])) == [b"a", b"b"]


def test_merge_write_resolves_siblings():
    a, b = Version("n1", 1, {}, b"a"), Version("n2", 1, {}, b"b")
    merged = Version("n3", 1, join_context([a, b]), b"a+b")
    assert reconcile(DVV, [a, b], [merged]) == [merged]


def test_reconcile_is_order_independent_and_idempotent():
    versions = [Version("n1", i, {"n1": i - 1} if i % 2 else {}, bytes([i])) for i in range(1, 8)]
    forward = reconcile(DVV, versions)
    backward = reconcile(DVV, list(reversed(versions)))
    assert {v.dot for v in forward} == {v.dot for v in backward}
    assert {v.dot for v in reconcile(DVV, forward, forward)} == {v.dot for v in forward}


def test_paper_vector_clocks_lose_a_concurrent_write():
    """Two clients write from the same context through the same coordinator. The paper's
    scheme gives both writes the clock {n1: 1}: one of them silently disappears."""
    first = Version("n1", VC.next_counter("n1", {}, None), {}, b"first")
    second = Version("n1", VC.next_counter("n1", {}, None), {}, b"second")
    assert first.clock == second.clock
    assert len(reconcile(VC, [first], [second])) == 1


def test_paper_vector_clocks_drop_a_write_with_a_stale_context():
    newer = Version("n1", 5, {"n1": 4}, b"newer")
    late = Version("n1", VC.next_counter("n1", {"n1": 2}, None), {"n1": 2}, b"late")
    assert reconcile(VC, [newer], [late]) == [newer]     # the acknowledged late write is gone


def test_join_context_covers_dots_and_contexts():
    a = Version("n1", 3, {"n2": 5}, b"a")
    b = Version("n2", 7, {"n1": 1}, None)
    assert join_context([a, b]) == {"n1": 3, "n2": 7}
