"""Modelos independentes de comunicação e persistência."""

from dataclasses import dataclass

DEFAULT_CHUNK_SIZE = 250_000
MAX_END = 10**12
MAX_RANGE_SIZE = 10**9
MAX_LIST_SIZE = 1_000_000


def require_integer(name: str, value: int, minimum: int = 0) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} deve ser um inteiro >= {minimum}")


def validate_bounds(start: int, end: int) -> None:
    require_integer("start", start)
    require_integer("end", end)
    if end < start:
        raise ValueError("end deve ser >= start")
    if end > MAX_END:
        raise ValueError(f"end deve ser <= {MAX_END}")
    if end - start + 1 > MAX_RANGE_SIZE:
        raise ValueError(f"intervalo deve conter no máximo {MAX_RANGE_SIZE} inteiros")


@dataclass(frozen=True)
class JobSpec:
    start: int
    end: int
    chunk_size: int = DEFAULT_CHUNK_SIZE
    mode: str = "count"

    def __post_init__(self) -> None:
        validate_bounds(self.start, self.end)
        require_integer("chunk_size", self.chunk_size, 1)
        if self.mode not in ("count", "list"):
            raise ValueError("mode deve ser 'count' ou 'list'")
        if self.mode == "list" and self.end - self.start + 1 > MAX_LIST_SIZE:
            raise ValueError(f"modo list aceita no máximo {MAX_LIST_SIZE} inteiros")


@dataclass(frozen=True)
class Segment:
    chunk_id: int
    start: int
    end: int


@dataclass(frozen=True)
class SegmentResult:
    chunk_id: int
    prime_count: int
    primes: list[int] | None = None


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    spec: JobSpec
    created_at: float
    status: str = "running"
    duration_sec: float = 0.0
