from dynamokv.ring import Ring

NODES = [f"n{i}" for i in range(1, 6)]


def test_preference_list_has_distinct_nodes_and_is_stable():
    ring = Ring(NODES, vnodes=32)
    for i in range(500):
        preference = ring.preference_list(f"key-{i}", 3)
        assert len(set(preference)) == 3
        assert preference == Ring(reversed(NODES), vnodes=32).preference_list(f"key-{i}", 3)


def test_walk_visits_every_node_once():
    ring = Ring(NODES, vnodes=8)
    assert sorted(ring.walk("anything")) == NODES


def test_virtual_nodes_spread_load_evenly():
    share = Ring(NODES, vnodes=64).ownership()
    assert abs(sum(share.values()) - 1.0) < 1e-9
    assert max(share.values()) < 0.30 and min(share.values()) > 0.12


def test_adding_a_node_moves_only_a_fraction_of_keys():
    before, after = Ring(NODES, 64), Ring([*NODES, "n6"], 64)
    keys = [f"key-{i}" for i in range(5000)]
    moved = sum(before.preference_list(k, 1) != after.preference_list(k, 1) for k in keys)
    assert moved / len(keys) < 0.25      # roughly 1/6 of keys move to the new node
