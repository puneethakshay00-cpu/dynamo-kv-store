from dynamokv import merkle
from dynamokv.version import DottedVersionVectors as DVV
from dynamokv.version import Version


def items(n, skip=None):
    return [(f"k{i}", [Version("n1", i, {}, b"v")]) for i in range(n) if i != skip]


def test_identical_data_gives_identical_root():
    assert merkle.build(items(100), DVV)[0] == merkle.build(list(reversed(items(100))), DVV)[0]


def test_a_missing_key_changes_one_leaf_and_its_path_only():
    full, partial = merkle.build(items(100), DVV), merkle.build(items(100, skip=37), DVV)
    assert full[0] != partial[0]
    differing = [i for i in range(merkle.LEAVES) if full[merkle.DEPTH][i] != partial[merkle.DEPTH][i]]
    assert differing == [merkle.leaf_of("k37")]
    for depth in range(merkle.DEPTH + 1):
        assert sum(a != b for a, b in zip(full[depth], partial[depth], strict=True)) == 1


def test_children_indices():
    assert merkle.children([0, 3]) == [0, 1, 6, 7]
