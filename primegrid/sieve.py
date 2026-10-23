"""Crivo sequencial puro; não depende de rede, banco ou executores."""

from collections.abc import Iterator, Sequence
from math import isqrt

from primegrid.models import (
    DEFAULT_CHUNK_SIZE,
    JobSpec,
    Segment,
    SegmentResult,
    require_integer,
)


def base_primes(limit: int) -> list[int]:
    """Retorna os primos <= limit usando o crivo de Eratóstenes."""
    require_integer("limit", limit)
    if limit < 2:
        return []
    flags = bytearray(b"\x01") * (limit + 1)
    flags[0:2] = b"\x00\x00"
    for p in range(2, isqrt(limit) + 1):
        if flags[p]:
            first = p * p
            flags[first : limit + 1 : p] = b"\x00" * ((limit - first) // p + 1)
    return [p for p in range(2, limit + 1) if flags[p]]


def split_segments(
    start: int, end: int, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Iterator[Segment]:
    """Divide um intervalo inclusivo, com IDs a partir de zero, sem lacunas."""
    spec = JobSpec(start, end, chunk_size)
    return (
        Segment(chunk_id, left, min(left + spec.chunk_size - 1, spec.end))
        for chunk_id, left in enumerate(range(spec.start, spec.end + 1, spec.chunk_size))
    )


def sieve_segment(
    L: int,
    R: int,
    base_primes: Sequence[int],
    include_list: bool = False,
    *,
    chunk_id: int = 0,
) -> SegmentResult:
    """Calcula [L,R]; a base deve conter os primos ordenados até isqrt(R).

    A base é uma pré-condição do núcleo: o chamador a calcula uma vez por job.
    Em count, nenhuma lista de primos do segmento é construída.
    """
    require_integer("chunk_id", chunk_id)
    if not isinstance(include_list, bool):
        raise ValueError("include_list deve ser booleano")
    JobSpec(L, R, mode="list" if include_list else "count")
    flags = bytearray(b"\x01") * (R - L + 1)
    for p in base_primes:
        if p * p > R:
            break
        first = max(p * p, ((L + p - 1) // p) * p)
        if first <= R:
            flags[first - L : R - L + 1 : p] = b"\x00" * ((R - first) // p + 1)
    for value in range(L, min(R, 1) + 1):
        flags[value - L] = 0
    count = flags.count(1)
    primes = [L + i for i, flag in enumerate(flags) if flag] if include_list else None
    return SegmentResult(chunk_id, count, primes)


def count_range(start: int, end: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """Conta primos sequencialmente, reutilizando a mesma base nos segmentos."""
    spec = JobSpec(start, end, chunk_size)
    primes = base_primes(isqrt(spec.end))
    return sum(
        sieve_segment(segment.start, segment.end, primes, chunk_id=segment.chunk_id).prime_count
        for segment in split_segments(spec.start, spec.end, spec.chunk_size)
    )
