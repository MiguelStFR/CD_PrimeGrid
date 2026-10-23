"""Configuração estática compartilhada entre os nós."""

import json
import math
from dataclasses import dataclass
from pathlib import Path

from primegrid.models import require_integer


@dataclass(frozen=True)
class Peer:
    id: int
    host: str
    port: int

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class Config:
    nodes: tuple[Peer, ...]
    heartbeat_sec: float = 2
    failure_after_sec: float = 6
    lease_sec: float = 30
    data_dir: Path = Path("data")

    def peer(self, node_id: int) -> Peer:
        for peer in self.nodes:
            if peer.id == node_id:
                return peer
        raise ValueError(f"ID {node_id} não consta na configuração")


def load_config(path: str | Path) -> Config:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    nodes = []
    for item in raw["nodes"]:
        peer = Peer(**item)
        require_integer("id", peer.id)
        require_integer("port", peer.port, 1)
        if peer.port > 65535:
            raise ValueError("port deve ser <= 65535")
        if not isinstance(peer.host, str) or not peer.host.strip() or ":" in peer.host:
            raise ValueError("host deve ser um nome ou endereço IPv4, sem porta")
        nodes.append(peer)
    if not nodes or len({p.id for p in nodes}) != len(nodes):
        raise ValueError("nodes deve conter IDs únicos e ao menos um nó")
    if len({p.endpoint for p in nodes}) != len(nodes):
        raise ValueError("cada nó deve usar um endpoint diferente")
    times = {}
    for name, default in (("heartbeat_sec", 2), ("failure_after_sec", 6), ("lease_sec", 30)):
        value = raw.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} deve ser um número positivo finito")
        times[name] = value
    if times["failure_after_sec"] <= times["heartbeat_sec"]:
        raise ValueError("failure_after_sec deve ser maior que heartbeat_sec")
    return Config(tuple(nodes), **times, data_dir=Path(raw.get("data_dir", "data")))
