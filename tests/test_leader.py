import pytest

from primegrid.leader import LeaderQueue
from primegrid.models import JobSpec, SegmentResult


def test_dynamic_queue_has_no_gaps_or_overlap_and_one_task_per_worker():
    queue = LeaderQueue(JobSpec(0, 10, 4))
    first, second = queue.request(1), queue.request(2)
    assert queue.request(1) == first
    assert (first.start, first.end, second.start, second.end) == (0, 3, 4, 7)
    queue.accept(2, SegmentResult(second.chunk_id, 2))
    last = queue.request(2)
    assert (last.chunk_id, last.start, last.end) == (2, 8, 10)
    assert queue.request(3) is None
    queue.accept(1, SegmentResult(first.chunk_id, 2))
    queue.accept(2, SegmentResult(last.chunk_id, 0))
    assert queue.done
    assert queue.prime_count == 4
    assert not queue.assigned


def test_duplicate_does_not_increase_total_or_clear_next_task():
    queue = LeaderQueue(JobSpec(1, 10, 5))
    first = queue.request(1)
    result = SegmentResult(first.chunk_id, 3)
    assert queue.accept(1, result)
    second = queue.request(1)
    assert not queue.accept(1, result)
    assert queue.assigned[1] == second.chunk_id
    assert queue.prime_count == 3
    with pytest.raises(ValueError, match="conflitante"):
        queue.accept(1, SegmentResult(first.chunk_id, 2))


def test_list_aggregation_is_in_chunk_order():
    queue = LeaderQueue(JobSpec(1, 10, 5, "list"))
    queue.request(1)
    queue.request(2)
    queue.accept(2, SegmentResult(1, 1, [7]))
    queue.accept(1, SegmentResult(0, 3, [2, 3, 5]))
    assert queue.primes() == [2, 3, 5, 7]


@pytest.mark.parametrize("result", [SegmentResult(5, 1), SegmentResult(0, 11), SegmentResult(0, 1, [2])])
def test_invalid_results_are_rejected(result):
    queue = LeaderQueue(JobSpec(1, 10))
    queue.request(1)
    with pytest.raises(ValueError):
        queue.accept(1, result)


@pytest.mark.parametrize("primes", [None, [3, 2], [2, 2], [1, 2], [2, 11]])
def test_invalid_list_results(primes):
    queue = LeaderQueue(JobSpec(1, 10, mode="list"))
    queue.request(1)
    with pytest.raises(ValueError):
        queue.accept(1, SegmentResult(0, 2, primes))


def test_unassigned_worker_cannot_submit():
    queue = LeaderQueue(JobSpec(1, 10))
    queue.request(1)
    with pytest.raises(ValueError, match="atribuído"):
        queue.accept(2, SegmentResult(0, 4))
