"""Dois servidores reais e seus processos CPU, sem substituir o transporte."""

import asyncio
import csv
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest
import pytest_asyncio

from primegrid.proto import primegrid_pb2 as pb
from primegrid.rpc import RpcClient, call_leader
from primegrid.sieve import count_range

ROOT = Path(__file__).resolve().parents[1]


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


async def wait_until(callback, timeout=25):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = await callback()
            if result:
                return result
        except grpc.aio.AioRpcError as exc:
            last_error = exc
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout aguardando cluster: {last_error}")


class Cluster:
    def __init__(self, path, size=2):
        self.path = path
        self.ports = free_ports(size)
        self.endpoints = [f"127.0.0.1:{port}" for port in self.ports]
        self.config = path / "nodes.json"
        self.data = path / "data"
        self.config.write_text(json.dumps({
            "nodes": [{"id": i + 1, "host": "127.0.0.1", "port": port} for i, port in enumerate(self.ports)],
            "data_dir": str(self.data),
        }))
        self.client = RpcClient()
        self.processes = []
        self.logs = []

    def start(self, node_id):
        log = (self.path / f"node_{node_id}.jsonl").open("w", encoding="utf-8")
        self.logs.append(log)
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        process = subprocess.Popen([sys.executable, "-m", "primegrid", "node", "--config", str(self.config), "--id", str(node_id)],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env=env)
        self.processes.append(process)

    async def ready(self, leader_id, active_nodes):
        async def check():
            statuses = [await self.client.call(endpoint, "GetStatus", pb.StatusQuery(local_only=True), timeout=0.5)
                        for endpoint in self.endpoints[:active_nodes]]
            return all(s.HasField("leader") and s.leader.node_id == leader_id for s in statuses)
        await wait_until(check)

    async def completed(self):
        async def check():
            result = await call_leader(self.client, self.endpoints[0], "GetResult", pb.Empty())
            return result if result.state == "completed" else None
        return await wait_until(check)

    async def cli(self, *args):
        process = await asyncio.create_subprocess_exec(sys.executable, "-m", "primegrid", *args,
            cwd=ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    async def close(self):
        await self.client.close()
        # taskkill /T evita órfãos dos ProcessPoolExecutor nos testes Windows.
        for process in self.processes:
            if process.poll() is None:
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
                else:
                    process.send_signal(2)  # SIGINT: fecha servidor e executor.
                try:
                    await asyncio.to_thread(process.wait, timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait)
        for log in self.logs:
            log.close()


@pytest_asyncio.fixture
async def cluster(tmp_path):
    cluster = Cluster(tmp_path)
    try:
        cluster.start(1)
        cluster.start(2)
        await cluster.ready(2, 2)
        yield cluster
    finally:
        await cluster.close()


async def test_two_processes_count_cli_redirect_persistence_and_duplicate(cluster):
    status = await cluster.cli("status", "--node", cluster.endpoints[0])
    assert status["leader_id"] == 2
    assert status["active_nodes"] == [1, 2]
    start = await cluster.cli("submit", "--node", cluster.endpoints[0], "--start", "1", "--end", "1000000", "--chunk-size", "5000")
    assert start["leader_id"] == 2
    result = await cluster.completed()
    assert result.prime_count == count_range(1, 1_000_000, 5000) == 78_498
    assert not result.primes
    assert result.completed_chunks == result.total_chunks == 200
    assert result.duration_sec > 0

    # Cada máquina persiste ao menos uma tarefa, o líder contém a união exata.
    with sqlite3.connect(cluster.data / "node_1.sqlite3") as db:
        rows = db.execute("SELECT chunk_id, prime_count FROM results WHERE job_id = ? ORDER BY chunk_id", (result.job_id,)).fetchall()
        assert rows
        assert db.execute("SELECT COUNT(*) FROM results WHERE primes_blob IS NOT NULL").fetchone()[0] == 0
    with sqlite3.connect(cluster.data / "node_2.sqlite3") as db:
        ids = [r[0] for r in db.execute("SELECT chunk_id FROM results WHERE job_id = ? ORDER BY chunk_id", (result.job_id,))]
        assert ids == list(range(200))
        assert db.execute("SELECT SUM(prime_count) FROM results WHERE job_id = ?", (result.job_id,)).fetchone()[0] == 78_498

    ack = await cluster.client.call(cluster.endpoints[1], "SubmitResult", pb.ChunkResult(
        job_id=result.job_id, worker_id=1, chunk_id=rows[0][0], prime_count=rows[0][1]))
    assert ack.code == pb.OK
    assert (await cluster.completed()).prime_count == 78_498
    queried = await cluster.cli("result", "--node", cluster.endpoints[0])
    assert queried["prime_count"] == 78_498
    assert "primes" not in queried

    for node_id in (1, 2):
        events = [json.loads(line) for line in (cluster.path / f"node_{node_id}.jsonl").read_text().splitlines() if line.startswith("{")]
        assert any(e["event"] == "chunk_started" for e in events)
        assert any(e["event"] == "coordinator" and e["leader_id"] == 2 for e in events)
    leader_events = [json.loads(line) for line in (cluster.path / "node_2.jsonl").read_text().splitlines() if line.startswith("{")]
    assigned = {e["chunk_id"]: (e["start"], e["end"]) for e in leader_events if e["event"] == "task_assigned"}
    assert [assigned[i] for i in range(200)] == [(i * 5000 + 1, (i + 1) * 5000) for i in range(200)]
    assert any(e["event"] == "result_accepted" and e["duplicate"] for e in leader_events)


async def test_list_csv_and_second_job(cluster):
    for start, end, expected in ((1, 30, [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]), (0, 1, []), (2, 2, [2])):
        await call_leader(cluster.client, cluster.endpoints[0], "StartJob", pb.StartRequest(start=start, end=end, chunk_size=3, mode="list"))
        result = await cluster.completed()
        assert list(result.primes) == expected
        assert result.prime_count == len(expected)
        output = cluster.path / "primes.csv"
        payload = await cluster.cli("result", "--node", cluster.endpoints[0], "--csv", str(output))
        assert payload["primes"] == expected
        with output.open(newline="") as source:
            assert list(csv.reader(source)) == [["prime"], *[[str(p)] for p in expected]]


async def test_reusable_lan_validation_script(cluster):
    output = cluster.path / "validation.json"
    process = await asyncio.create_subprocess_exec(sys.executable, "scripts/verify_cluster.py",
        "--config", str(cluster.config), "--output", str(output), cwd=ROOT,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    assert process.returncode == 0, stderr.decode()
    report = json.loads(output.read_text())
    assert report["validation"] == "passed"
    assert report["prime_count"] == 78_498
    assert report["leader_id"] == 2
    assert len(report["nodes"]) == 2


async def test_invalid_job_and_single_active_job(cluster):
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await cluster.client.call(cluster.endpoints[1], "StartJob", pb.StartRequest(start=0, end=1_000_000, mode="list", chunk_size=100))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    await cluster.client.call(cluster.endpoints[1], "StartJob", pb.StartRequest(start=1, end=100_000_000, chunk_size=10_000, mode="count"), timeout=10)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await cluster.client.call(cluster.endpoints[1], "StartJob", pb.StartRequest(start=1, end=10, chunk_size=10, mode="count"))
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION


async def test_initial_election_with_offline_highest_node_and_start_requires_peer_ack(tmp_path):
    cluster = Cluster(tmp_path, size=2)
    try:
        cluster.start(1)
        await cluster.ready(1, 1)
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await cluster.client.call(cluster.endpoints[0], "StartJob", pb.StartRequest(start=1, end=10, chunk_size=10, mode="count"), timeout=10)
        assert error.value.code() == grpc.StatusCode.UNAVAILABLE
        # Um nó de maior ID que inicia depois deve assumir por Bully.
        cluster.start(2)
        await cluster.ready(2, 2)
        await call_leader(cluster.client, cluster.endpoints[0], "StartJob", pb.StartRequest(start=1, end=100, chunk_size=10, mode="count"))
        assert (await cluster.completed()).prime_count == 25
    finally:
        await cluster.close()
