"""CLI: python -m primegrid --help."""

import argparse
import asyncio
import csv
import json
import logging
import sys
from pathlib import Path

import grpc

from primegrid.config import load_config
from primegrid.models import DEFAULT_CHUNK_SIZE, JobSpec
from primegrid.node import Node
from primegrid.proto import primegrid_pb2 as pb
from primegrid.rpc import RpcClient, call_leader


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description="PrimeGrid: crivo segmentado distribuído em LAN")
    sub = cli.add_subparsers(dest="command", required=True)
    node = sub.add_parser("node", help="inicia um nó coordenador/trabalhador")
    node.add_argument("--config", required=True, help="arquivo JSON com peers estáticos")
    node.add_argument("--id", type=int, required=True, help="ID deste computador na configuração")
    submit = sub.add_parser("submit", help="submete um job ao líder, com redirecionamento")
    submit.add_argument("--start", type=int, required=True, help="início inclusivo, >= 0")
    submit.add_argument("--end", type=int, required=True, help="fim inclusivo, <= 10**12")
    submit.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="inteiros por segmento (padrão: 250000)")
    submit.add_argument("--mode", choices=("count", "list"), default="count", help="list: até 1000000 inteiros")
    status = sub.add_parser("status", help="consulta líder, nós ativos e progresso")
    result = sub.add_parser("result", help="consulta contagem, duração e estatísticas")
    result.add_argument("--csv", type=Path, help="exporta primos ordenados de um job list concluído")
    for command in (submit, status, result):
        command.add_argument("--node", required=True, help="host:porta de qualquer nó do cluster")
    return cli


async def execute(args) -> dict | None:
    if args.command == "node":
        logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
        await Node(load_config(args.config), args.id).serve()
        return None
    client = RpcClient()
    try:
        if args.command == "submit":
            spec = JobSpec(args.start, args.end, args.chunk_size, args.mode)
            reply = await call_leader(client, args.node, "StartJob", pb.StartRequest(
                start=spec.start, end=spec.end, chunk_size=spec.chunk_size, mode=spec.mode))
            return {"job_id": reply.job_id, "leader_id": reply.leader.node_id, "leader_endpoint": reply.leader.endpoint}
        if args.command == "status":
            status = await call_leader(client, args.node, "GetStatus", pb.StatusQuery())
            return {"leader_id": status.leader.node_id, "leader_endpoint": status.leader.endpoint,
                "job_id": status.job_id, "state": status.state, "total_chunks": status.total_chunks,
                "completed_chunks": status.completed_chunks, "in_progress_chunks": status.in_progress_chunks,
                "active_nodes": list(status.active_nodes)}
        result = await call_leader(client, args.node, "GetResult", pb.Empty())
        if args.csv:
            if result.mode != "list" or result.state != "completed":
                raise ValueError("CSV exige job list concluído")
            with args.csv.open("w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(["prime"])
                writer.writerows((p,) for p in result.primes)
        payload = {"job_id": result.job_id, "state": result.state, "mode": result.mode,
            "prime_count": result.prime_count, "duration_sec": result.duration_sec,
            "total_chunks": result.total_chunks, "completed_chunks": result.completed_chunks}
        if result.mode == "list":
            payload["primes"] = list(result.primes)
        if args.csv:
            payload["csv"] = str(args.csv)
        return payload
    finally:
        await client.close()


def main() -> int:
    args = parser().parse_args()
    try:
        payload = asyncio.run(execute(args))
        if payload is not None:
            print(json.dumps(payload, ensure_ascii=False))
        return 0
    except KeyboardInterrupt:
        return 0
    except grpc.RpcError as exc:
        print(f"Erro gRPC ({exc.code().name}): {exc.details()}", file=sys.stderr)
        return 1
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
