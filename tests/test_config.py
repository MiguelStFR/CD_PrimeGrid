import json

import pytest

from primegrid.config import load_config


def test_local_config_defaults_and_peers(tmp_path):
    path = tmp_path / "nodes.json"
    path.write_text(json.dumps({"nodes": [{"id": 1, "host": "127.0.0.1", "port": 50051}]}))
    config = load_config(path)
    assert config.peer(1).endpoint == "127.0.0.1:50051"
    assert (config.heartbeat_sec, config.failure_after_sec, config.lease_sec) == (2, 6, 30)
    with pytest.raises(ValueError):
        config.peer(2)


@pytest.mark.parametrize("changes", [
    {"nodes": []},
    {"nodes": [{"id": 1, "host": "localhost", "port": 0}]},
    {"nodes": [{"id": True, "host": "localhost", "port": 50051}]},
    {"heartbeat_sec": 0}, {"heartbeat_sec": "2"}, {"failure_after_sec": 1},
    {"lease_sec": float("nan")},
])
def test_invalid_config(tmp_path, changes):
    raw = {"nodes": [{"id": 1, "host": "127.0.0.1", "port": 50051}], **changes}
    path = tmp_path / "nodes.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_config(path)


@pytest.mark.parametrize("duplicate_id", [True, False])
def test_duplicate_ids_or_endpoints(tmp_path, duplicate_id):
    path = tmp_path / "nodes.json"
    path.write_text(json.dumps({"nodes": [
        {"id": 1, "host": "localhost", "port": 50051},
        {"id": 1 if duplicate_id else 2, "host": "localhost", "port": 50052 if duplicate_id else 50051},
    ]}))
    with pytest.raises(ValueError):
        load_config(path)
