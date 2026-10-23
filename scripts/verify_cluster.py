"""Validação funcional reutilizável em dois processos ou em computadores LAN."""

import argparse
import asyncio
import json
import time
from pathlib import Path

import grpc

from primegrid.config import load_config
from primegrid.proto import primegrid_pb2 as pb
from primegrid.rpc import RpcClient, call_leader
from primegrid.sieve import count_range


async def verify(config_path: Path, timeout: float) -> dict:
    config = load_config(config_path)
    client = RpcClient()
    try:
        active = []
        for peer in config.nodes:
            try:
                status = await client.call(peer.endpoint, "GetStatus", pb.StatusQuery(local_only=True))
                active.append((peer, status))
            except grpc.aio.AioRpcError:
                pass
        if len(active) < 2:
            raise ValueError("validação exige ao menos dois nós acessíveis")
        expected_leader = max(peer.id for peer, _ in active)
        if any(not status.HasField("leader") or status.leader.node_id != expected_leader for _, status in active):
            raise ValueError("aguarde a eleição: todos os nós devem reconhecer o maior ID ativo")
        endpoint = active[0][0].endpoint
        job = await call_leader(client, endpoint, "StartJob", pb.StartRequest(start=1, end=1_000_000, chunk_size=5000, mode="count"))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = await call_leader(client, endpoint, "GetResult", pb.Empty())
            if result.state == "completed":
                expected = count_range(1, 1_000_000, 5000)
                if result.job_id != job.job_id or result.prime_count != expected or result.primes or result.completed_chunks != 200:
                    raise ValueError("resultado distribuído diverge da referência sequencial")
                return {
                    "validation": "passed", "leader_id": expected_leader,
                    "nodes": [{"id": peer.id, "endpoint": peer.endpoint} for peer, _ in active],
                    "job_id": result.job_id, "start": 1, "end": 1_000_000, "chunk_size": 5000,
                    "prime_count": result.prime_count, "expected_count": expected,
                    "completed_chunks": result.completed_chunks, "duration_sec": result.duration_sec,
                }
            await asyncio.sleep(0.1)
        raise ValueError("timeout aguardando conclusão do job")
    finally:
        await client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="evidência JSON da validação")
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    try:
        report = asyncio.run(verify(args.config, args.timeout))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report))
        return 0
    except (ValueError, OSError, grpc.RpcError) as exc:
        parser.exit(1, f"Validação falhou: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
