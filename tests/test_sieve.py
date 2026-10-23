from math import isqrt
from random import Random

import pytest

from primegrid.models import JobSpec, Segment
from primegrid.sieve import base_primes, count_range, sieve_segment, split_segments


@pytest.mark.parametrize("start,end,expected", [(0, 1, 0), (1, 10, 4), (1, 100, 25), (1, 1_000_000, 78_498)])
def test_known_counts(start, end, expected):
    assert count_range(start, end) == expected


@pytest.mark.parametrize("start,end,expected", [(2, 2, [2]), (14, 16, []), (1, 30, [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]), (0, 0, []), (1, 1, [])])
def test_known_lists(start, end, expected):
    result = sieve_segment(start, end, base_primes(isqrt(end)), True, chunk_id=7)
    assert result.chunk_id == 7
    assert result.prime_count == len(expected)
    assert result.primes == expected


@pytest.mark.parametrize("limit,expected", [(0, []), (1, []), (2, [2]), (3, [2, 3]), (25, [2, 3, 5, 7, 11, 13, 17, 19, 23])])
def test_base(limit, expected):
    assert base_primes(limit) == expected


@pytest.mark.parametrize("chunk_size", [1, 2, 7, 24, 25, 26, 49, 250_000])
def test_chunk_size_preserves_count_and_order(chunk_size):
    expected = [n for n in range(301) if is_prime(n)]
    assert count_range(0, 300, chunk_size) == len(expected)
    segments = list(split_segments(0, 300, chunk_size))
    results = [sieve_segment(s.start, s.end, base_primes(isqrt(300)), True, chunk_id=s.chunk_id) for s in segments]
    assert [p for result in results for p in result.primes] == expected
    assert [result.chunk_id for result in results] == list(range(len(segments)))


def test_partial_last_segment_and_inclusive_boundaries():
    assert list(split_segments(2, 12, 4)) == [Segment(0, 2, 5), Segment(1, 6, 9), Segment(2, 10, 12)]
    assert list(split_segments(2, 2, 4)) == [Segment(0, 2, 2)]
    assert list(split_segments(0, 499_999)) == [Segment(0, 0, 249_999), Segment(1, 250_000, 499_999)]


@pytest.mark.parametrize("p", [2, 3, 5, 7, 11, 97])
def test_prime_square_is_composite(p):
    result = sieve_segment(p * p - 1, p * p + 1, base_primes(p), True)
    assert p * p not in result.primes
    assert result.primes == [n for n in range(p * p - 1, p * p + 2) if is_prime(n)]


def is_prime(n):
    return n >= 2 and all(n % d for d in range(2, isqrt(n) + 1))


def test_random_intervals_against_independent_trial_division():
    random = Random(42)
    for _ in range(100):
        start = random.randrange(10_000)
        end = start + random.randrange(100)
        expected = [n for n in range(start, end + 1) if is_prime(n)]
        assert count_range(start, end, random.randrange(1, 30)) == len(expected)
        assert sieve_segment(start, end, base_primes(isqrt(end)), True).primes == expected


def test_count_mode_omits_list():
    assert sieve_segment(1, 100, base_primes(10)).primes is None


def test_interval_near_maximum_end():
    start, end = 10**12 - 10, 10**12
    expected = [n for n in range(start, end + 1) if is_prime(n)]
    result = sieve_segment(start, end, base_primes(isqrt(end)), True)
    assert result.primes == expected
    assert result.prime_count == len(expected)


def test_count_range_computes_base_once(monkeypatch):
    import primegrid.sieve as sieve

    calls = []
    original = sieve.base_primes

    def tracked_base(limit):
        calls.append(limit)
        return original(limit)

    monkeypatch.setattr(sieve, "base_primes", tracked_base)
    assert sieve.count_range(1, 100, 1) == 25
    assert calls == [10]


@pytest.mark.parametrize("start,end,include_list,chunk_id", [(-1, 10, False, 0), (10, 9, False, 0), (0, 10, False, -1), (0, 10, "list", 0)])
def test_invalid_segment(start, end, include_list, chunk_id):
    with pytest.raises(ValueError):
        sieve_segment(start, end, [], include_list, chunk_id=chunk_id)


@pytest.mark.parametrize("start,end,chunk_size", [(-1, 10, 1), (10, 9, 1), (0, 10**12 + 1, 1), (0, 10**9, 1), (0, 10, 0), (0, 10, -1), (True, 10, 1), (0, 2.5, 1), (0, 10, False)])
def test_invalid_range(start, end, chunk_size):
    with pytest.raises(ValueError):
        count_range(start, end, chunk_size)
    with pytest.raises(ValueError):
        split_segments(start, end, chunk_size)


def test_mode_and_range_limits():
    JobSpec(10**12, 10**12)
    JobSpec(1, 10**9)
    JobSpec(1, 1_000_000, mode="list")
    with pytest.raises(ValueError):
        JobSpec(0, 1_000_000, mode="list")
    with pytest.raises(ValueError):
        JobSpec(1, 10, mode="unknown")
    with pytest.raises(ValueError):
        sieve_segment(0, 1_000_000, [], True)


@pytest.mark.parametrize("limit", [-1, True, 2.5])
def test_invalid_base_limit(limit):
    with pytest.raises(ValueError):
        base_primes(limit)
