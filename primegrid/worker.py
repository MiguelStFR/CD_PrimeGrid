"""Worker de rede; CPU executada em um único processo reaproveitado por nó."""

import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from math import isqrt

import grpc

from primegrid.events import event
from primegrid.models import JobSpec
from primegrid.proto import primegrid_pb2 as pb
from primegrid.rpc import result_message
from primegrid.sieve import base_primes, sieve_segment

_job_key: tuple | None = None
_base: list[int] = []


def compute(job_id: str, spec: JobSpec, chunk_id: int, start: int, end: int):
    """Função serializável para spawn, inclusive em Windows."""
    global _job_key, _base
    key = (job_id, spec)
    if _job_key != key:
        _base = base_primes(isqrt(spec.end))
        _job_key = key
    return sieve_segment(start, end, _base, spec.mode == "list", chunk_id=chunk_id)


async def worker_loop(node) -> None:
    # O executor vive por toda a execução do nó, não por segmento.
    with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as pool:
        while True:
            job = node.job
            if job is None or node.leader_id is None or job.status != "running":
                await asyncio.sleep(0.1)
                continue
            try:
                task = await node.client.call(node.leader_endpoint, "RequestTask",
                    pb.TaskRequest(job_id=job.job_id, worker_id=node.node_id))
                if task.code == pb.NOT_LEADER:
                    await asyncio.sleep(0.2)
                    continue
                if task.code == pb.DONE:
                    node.mark_completed()
                    continue
                if task.code != pb.TASK:
                    await asyncio.sleep(0.1)
                    continue
                node.active_chunk_id = task.chunk_id
                event(node.node_id, "chunk_started", job_id=job.job_id, chunk_id=task.chunk_id)
                result = await asyncio.get_running_loop().run_in_executor(
                    pool, compute, job.job_id, job.spec, task.chunk_id, task.start, task.end)
                node.storage.save_result(job.job_id, result)
                event(node.node_id, "result_persisted", job_id=job.job_id, chunk_id=task.chunk_id)
                delay = 0.1
                while True:
                    try:
                        ack = await node.client.call(node.leader_endpoint, "SubmitResult",
                            result_message(job.job_id, node.node_id, result))
                        if ack.code == pb.OK:
                            break
                    except grpc.aio.AioRpcError as exc:
                        if exc.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                            raise
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 2)
                node.active_chunk_id = None
                event(node.node_id, "result_ack", job_id=job.job_id, chunk_id=task.chunk_id)
            except grpc.aio.AioRpcError as exc:
                if exc.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    event(node.node_id, "worker_error", error=exc.details())
                await asyncio.sleep(0.5)
