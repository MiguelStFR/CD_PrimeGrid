"""Ciclo de vida e serviço grpc.aio da entrega funcional (Fase B)."""

import asyncio
import time
from dataclasses import replace
from uuid import uuid4

import grpc

from primegrid.config import Config
from primegrid.election import Bully
from primegrid.events import event
from primegrid.leader import LeaderQueue
from primegrid.models import JobRecord, JobSpec, SegmentResult
from primegrid.proto import primegrid_pb2 as pb
from primegrid.proto import primegrid_pb2_grpc as rpc
from primegrid.rpc import OPTIONS, RpcClient, job_message, job_record
from primegrid.storage import Storage
from primegrid.worker import worker_loop


class Node(rpc.PrimeGridServicer):
    def __init__(self, config: Config, node_id: int):
        self.config = config
        self.node_id = node_id
        self.peer = config.peer(node_id)
        self.storage = Storage(config.data_dir / f"node_{node_id}.sqlite3")
        self.client = RpcClient()
        self.job = self.storage.get_job()
        self.queue: LeaderQueue | None = None
        self.started_at = 0.0
        self.active_chunk_id: int | None = None
        self.last_seen: dict[int, float] = {node_id: time.monotonic()}
        self.start_lock = asyncio.Lock()
        self.election = Bully(node_id, [p.id for p in config.nodes if p.id != node_id],
            self.send_election, self.announce, self.set_leader, config.failure_after_sec)

    @property
    def leader_id(self) -> int | None:
        return self.election.leader_id

    @property
    def leader_endpoint(self) -> str:
        return "" if self.leader_id is None else self.config.peer(self.leader_id).endpoint

    def leader_message(self) -> pb.Leader | None:
        if self.leader_id is None:
            return None
        return pb.Leader(node_id=self.leader_id, endpoint=self.leader_endpoint)

    def set_leader(self, node_id: int) -> None:
        # Recuperação de um job em curso após troca de líder pertence à Fase C.
        self.last_seen[node_id] = time.monotonic()

    def touch(self, node_id: int) -> None:
        self.config.peer(node_id)
        self.last_seen[node_id] = time.monotonic()

    async def send_election(self, peer_id: int) -> bool:
        try:
            ack = await self.client.call(self.config.peer(peer_id).endpoint, "Election", pb.Candidate(node_id=self.node_id))
            self.touch(peer_id)
            return ack.code == pb.OK
        except grpc.aio.AioRpcError:
            return False

    async def announce(self, node_id: int) -> None:
        async def send(peer):
            try:
                await self.client.call(peer.endpoint, "Coordinator", self.leader_message())
                self.touch(peer.id)
            except grpc.aio.AioRpcError:
                pass
        await asyncio.gather(*(send(p) for p in self.config.nodes if p.id != self.node_id))

    def mark_completed(self) -> None:
        if self.job is not None and self.job.status != "completed":
            self.storage.set_status(self.job.job_id, "completed")
            self.job = self.storage.get_job(self.job.job_id)

    async def GetStatus(self, request, context):
        if self.leader_id == self.node_id and not request.local_only:
            async def probe(peer):
                try:
                    await self.client.call(peer.endpoint, "GetStatus", pb.StatusQuery(local_only=True))
                    self.touch(peer.id)
                except grpc.aio.AioRpcError:
                    self.last_seen.pop(peer.id, None)
            await asyncio.gather(*(probe(p) for p in self.config.nodes if p.id != self.node_id))
        queue = self.queue
        now = time.monotonic()
        return pb.Status(
            code=pb.OK if self.leader_id == self.node_id else pb.NOT_LEADER,
            leader=self.leader_message(), job_id=self.job.job_id if self.job else "",
            state=self.job.status if self.job else "idle",
            total_chunks=queue.total_chunks if queue else 0,
            completed_chunks=len(queue.completed) if queue else 0,
            in_progress_chunks=len(queue.assigned) if queue else 0,
            active_nodes=sorted({self.node_id} | {p for p, seen in self.last_seen.items() if now - seen <= self.config.failure_after_sec}),
        )

    async def StartJob(self, request, context):
        if self.leader_id != self.node_id:
            return pb.JobReply(code=pb.NOT_LEADER, leader=self.leader_message())
        try:
            spec = JobSpec(request.start, request.end, request.chunk_size, request.mode)
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        async with self.start_lock:
            if self.job and self.job.status in ("running", "syncing"):
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "há um job ativo no cluster")
            job = JobRecord(str(uuid4()), spec, time.time(), "syncing")
            self.storage.save_job(job)
            self.job = job
            self.queue = None
            event(self.node_id, "job_sync_started", job_id=job.job_id)

            async def sync(peer):
                try:
                    ack = await self.client.call(peer.endpoint, "SyncJob", job_message(job, self.node_id))
                    if ack.code == pb.OK:
                        self.touch(peer.id)
                        return True
                except grpc.aio.AioRpcError:
                    pass
                return False

            acknowledgements = await asyncio.gather(*(sync(p) for p in self.config.nodes if p.id != self.node_id))
            if not any(acknowledgements) or self.leader_id != self.node_id:
                self.storage.set_status(job.job_id, "failed")
                self.job = self.storage.get_job(job.job_id)
                await context.abort(grpc.StatusCode.UNAVAILABLE, "início exige ACK de ao menos outro nó e liderança estável")
            self.queue = LeaderQueue(spec)
            self.started_at = time.perf_counter()
            self.storage.set_status(job.job_id, "running")
            self.job = replace(job, status="running")
            event(self.node_id, "job_started", job_id=job.job_id, total_chunks=self.queue.total_chunks)
            return pb.JobReply(job_id=job.job_id, leader=self.leader_message())

    async def SyncJob(self, request, context):
        try:
            job = job_record(request)
            self.config.peer(request.leader_id)
            if self.leader_id != request.leader_id:
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "SyncJob deve vir do líder conhecido")
            if self.job and self.job.job_id != job.job_id and self.job.status == "running":
                # Um job anterior pode já ter terminado no líder: confirmar antes de substituí-lo.
                status = await self.client.call(self.leader_endpoint, "GetStatus", pb.StatusQuery(local_only=True))
                if status.job_id != job.job_id:
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "há um job ativo diferente")
            self.storage.save_job(job)
            self.job = self.storage.get_job(job.job_id)
            self.touch(request.leader_id)
        except (ValueError, TypeError) as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        event(self.node_id, "job_synced", job_id=job.job_id)
        return pb.Ack()

    async def RequestTask(self, request, context):
        if self.leader_id != self.node_id:
            return pb.TaskReply(code=pb.NOT_LEADER, leader=self.leader_message())
        if self.job is None or request.job_id != self.job.job_id:
            await context.abort(grpc.StatusCode.NOT_FOUND, "job desconhecido ou obsoleto")
        try:
            self.touch(request.worker_id)
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        if self.queue is None:
            return pb.TaskReply(code=pb.WAIT)
        if self.queue.done:
            return pb.TaskReply(code=pb.DONE)
        chunk = self.queue.request(request.worker_id)
        if chunk is None:
            return pb.TaskReply(code=pb.WAIT)
        event(self.node_id, "task_assigned", job_id=request.job_id, worker_id=request.worker_id, chunk_id=chunk.chunk_id, start=chunk.start, end=chunk.end)
        return pb.TaskReply(code=pb.TASK, chunk_id=chunk.chunk_id, start=chunk.start, end=chunk.end)

    async def SubmitResult(self, request, context):
        if self.leader_id != self.node_id:
            return pb.Ack(code=pb.NOT_LEADER, leader=self.leader_message())
        if self.job is None or request.job_id != self.job.job_id or self.queue is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "job desconhecido ou ainda não iniciado")
        try:
            self.touch(request.worker_id)
            if self.job.spec.mode == "count" and request.primes:
                raise ValueError("modo count não aceita listagens")
            result = SegmentResult(request.chunk_id, request.prime_count,
                list(request.primes) if self.job.spec.mode == "list" else None)
            self.queue.validate(request.worker_id, result)
            self.storage.save_result(request.job_id, result)
            inserted = self.queue.accept(request.worker_id, result)
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        event(self.node_id, "result_accepted", job_id=request.job_id, chunk_id=request.chunk_id, duplicate=not inserted)
        if self.queue.done and self.job.status != "completed":
            duration = time.perf_counter() - self.started_at
            self.storage.set_status(self.job.job_id, "completed", duration)
            self.job = replace(self.job, status="completed", duration_sec=duration)
            event(self.node_id, "job_completed", job_id=self.job.job_id, prime_count=self.queue.prime_count, duration_sec=duration)
        return pb.Ack()

    async def Election(self, request, context):
        if not self.election.receive_election(request.node_id):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "candidato deve ser peer com ID menor")
        self.touch(request.node_id)
        return pb.Ack()

    async def Coordinator(self, request, context):
        try:
            peer = self.config.peer(request.node_id)
            if peer.endpoint != request.endpoint:
                raise ValueError("endpoint do líder difere da configuração")
            if not self.election.receive_coordinator(request.node_id):
                raise ValueError("coordenador com ID menor que este nó")
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return pb.Ack()

    async def GetResult(self, request, context):
        if self.leader_id != self.node_id:
            return pb.Result(code=pb.NOT_LEADER, leader=self.leader_message())
        if self.job is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "nenhum job submetido")
        if self.queue is None:
            # Consulta de jobs já concluídos após reiniciar o processo.
            results = self.storage.results(self.job.job_id)
            count = sum(r.prime_count for r in results)
            primes = [p for r in results for p in (r.primes or [])]
            completed = len(results)
            total = LeaderQueue(self.job.spec).total_chunks
        else:
            count = self.queue.prime_count
            primes = self.queue.primes() if self.job.spec.mode == "list" else []
            completed, total = len(self.queue.completed), self.queue.total_chunks
        return pb.Result(job_id=self.job.job_id, state=self.job.status, mode=self.job.spec.mode,
            prime_count=count, primes=primes, total_chunks=total, completed_chunks=completed,
            duration_sec=self.job.duration_sec if self.job.status == "completed" else max(0, time.perf_counter() - self.started_at) if self.started_at else 0)

    # Heartbeat e GetSnapshot permanecem UNIMPLEMENTED nesta fase; serão
    # implementados e verificados junto com recuperação de falhas na Fase C.

    async def serve(self) -> None:
        server = grpc.aio.server(options=OPTIONS)
        rpc.add_PrimeGridServicer_to_server(self, server)
        if not server.add_insecure_port(f"0.0.0.0:{self.peer.port}"):
            raise ValueError(f"não foi possível abrir a porta {self.peer.port}")
        worker = None
        try:
            await server.start()
            event(self.node_id, "node_started", endpoint=self.peer.endpoint)
            self.election.trigger()
            worker = asyncio.create_task(worker_loop(self))
            # Propaga falhas inesperadas do worker em vez de manter um nó inerte.
            await worker
        finally:
            if worker is not None:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            await self.election.close()
            await server.stop(0)
            await self.client.close()
            self.storage.close()
