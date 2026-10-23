"""Transporte gRPC com deadlines e conversão de modelos puros."""

from dataclasses import asdict

import grpc

from primegrid.models import JobRecord, JobSpec, SegmentResult
from primegrid.proto import primegrid_pb2 as pb
from primegrid.proto import primegrid_pb2_grpc as rpc

RPC_TIMEOUT = 2.0
# list é limitado a 1 milhão de inteiros; inclui faixas próximas de 10**12.
OPTIONS = (("grpc.max_send_message_length", 16 * 1024 * 1024),
           ("grpc.max_receive_message_length", 16 * 1024 * 1024))


def job_message(job: JobRecord, leader_id: int) -> pb.Job:
    return pb.Job(job_id=job.job_id, spec=pb.StartRequest(**asdict(job.spec)),
                  created_at=job.created_at, leader_id=leader_id)


def job_record(message: pb.Job) -> JobRecord:
    if not message.job_id:
        raise ValueError("job_id obrigatório")
    return JobRecord(message.job_id, JobSpec(message.spec.start, message.spec.end,
                     message.spec.chunk_size, message.spec.mode), message.created_at)


def result_message(job_id: str, worker_id: int, result: SegmentResult) -> pb.ChunkResult:
    return pb.ChunkResult(job_id=job_id, worker_id=worker_id, chunk_id=result.chunk_id,
                          prime_count=result.prime_count, primes=result.primes or [])


class RpcClient:
    def __init__(self):
        self.channels: dict[str, grpc.aio.Channel] = {}
        self.stubs: dict[str, rpc.PrimeGridStub] = {}

    async def call(self, endpoint: str, method: str, request, timeout: float = RPC_TIMEOUT):
        if endpoint not in self.stubs:
            channel = grpc.aio.insecure_channel(endpoint, options=OPTIONS)
            self.channels[endpoint] = channel
            self.stubs[endpoint] = rpc.PrimeGridStub(channel)
        return await getattr(self.stubs[endpoint], method)(request, timeout=timeout)

    async def close(self) -> None:
        for channel in self.channels.values():
            await channel.close()


async def call_leader(client: RpcClient, endpoint: str, method: str, request):
    """Redireciona consultas/submissões com limite contra ciclos de líderes."""
    for _ in range(4):
        response = await client.call(endpoint, method, request, timeout=10)
        if response.code != pb.NOT_LEADER:
            return response
        status = await client.call(endpoint, "GetStatus", pb.StatusQuery())
        if not status.HasField("leader") or not status.leader.endpoint:
            raise ValueError("eleição em andamento; tente novamente em instantes")
        endpoint = status.leader.endpoint
    raise ValueError("não foi possível identificar um líder estável")
