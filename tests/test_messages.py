from dynamokv.messages import decode, encode
from dynamokv.version import Version


def test_messages_round_trip_versions_and_bytes():
    message = {"t": "replicate", "versions": [Version("n1", 4, {"n2": 3}, b"\xff\x00"), Version("n2", 1, {}, None)],
               "hashes": [b"\x01\x02"], "items": [["k", [Version("n3", 2, {}, b"x")]]]}
    back = decode(encode(message))
    assert back["versions"][0] == message["versions"][0] and back["versions"][0].context == {"n2": 3}
    assert back["versions"][1].value is None
    assert back["hashes"] == [b"\x01\x02"]
    assert back["items"][0][1][0].value == b"x"
