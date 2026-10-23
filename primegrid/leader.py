"""Fila dinâmica e agregação puras, independentes de rede e SQLite."""

from primegrid.models import JobSpec, Segment, SegmentResult, require_integer


class LeaderQueue:
    def __init__(self, spec: JobSpec):
        self.spec = spec
        self.total_chunks = (spec.end - spec.start + spec.chunk_size) // spec.chunk_size
        self.next_chunk = 0
        self.assigned: dict[int, int] = {}  # worker_id -> chunk_id
        self.completed: dict[int, SegmentResult] = {}
        self.prime_count = 0

    @property
    def done(self) -> bool:
        return len(self.completed) == self.total_chunks

    def segment(self, chunk_id: int) -> Segment:
        require_integer("chunk_id", chunk_id)
        if chunk_id >= self.total_chunks:
            raise ValueError("chunk_id fora do job")
        start = self.spec.start + chunk_id * self.spec.chunk_size
        return Segment(chunk_id, start, min(start + self.spec.chunk_size - 1, self.spec.end))

    def request(self, worker_id: int) -> Segment | None:
        if worker_id in self.assigned:
            return self.segment(self.assigned[worker_id])
        while self.next_chunk in self.completed:
            self.next_chunk += 1
        if self.next_chunk >= self.total_chunks:
            return None
        chunk = self.segment(self.next_chunk)
        self.next_chunk += 1
        self.assigned[worker_id] = chunk.chunk_id
        return chunk

    def validate(self, worker_id: int, result: SegmentResult) -> None:
        segment = self.segment(result.chunk_id)
        require_integer("prime_count", result.prime_count)
        if result.prime_count > max(0, segment.end - max(segment.start, 2) + 1):
            raise ValueError("contagem excede o segmento")
        if self.spec.mode == "count":
            if result.primes is not None:
                raise ValueError("modo count não aceita listagens")
        elif result.primes is None or len(result.primes) != result.prime_count:
            raise ValueError("listagem não corresponde à contagem")
        else:
            previous = max(segment.start, 2) - 1
            for prime in result.primes:
                require_integer("primo", prime, 2)
                if prime <= previous or prime > segment.end:
                    raise ValueError("listagem deve ser ordenada, única e dentro do segmento")
                previous = prime
        if result.chunk_id in self.completed:
            if self.completed[result.chunk_id] != result:
                raise ValueError("resultado duplicado conflitante")
        elif self.assigned.get(worker_id) != result.chunk_id:
            raise ValueError("segmento não atribuído a este worker")

    def accept(self, worker_id: int, result: SegmentResult) -> bool:
        self.validate(worker_id, result)
        if result.chunk_id in self.completed:
            return False
        self.completed[result.chunk_id] = result
        self.prime_count += result.prime_count
        self.assigned.pop(worker_id, None)
        return True

    def primes(self) -> list[int]:
        return [p for chunk_id in sorted(self.completed) for p in (self.completed[chunk_id].primes or [])]
