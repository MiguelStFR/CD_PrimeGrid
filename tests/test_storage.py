import sqlite3

import pytest

from primegrid.models import JobRecord, JobSpec, SegmentResult
from primegrid.storage import Storage


def test_committed_results_survive_reopen_and_are_idempotent(tmp_path):
    path = tmp_path / "node_1.sqlite3"
    store = Storage(path)
    job = JobRecord("a", JobSpec(1, 10), 123)
    store.save_job(job)
    result = SegmentResult(0, 4)
    assert store.save_result("a", result)
    assert not store.save_result("a", result)
    assert store.db.execute("SELECT primes_blob FROM results").fetchone()[0] is None
    store.set_status("a", "completed", 0.25)
    store.close()
    store = Storage(path)
    try:
        assert store.get_job().status == "completed"
        assert store.get_job().duration_sec == 0.25
        assert store.results("a") == [result]
        with pytest.raises(ValueError, match="conflitante"):
            store.save_result("a", SegmentResult(0, 3))
        with pytest.raises(ValueError, match="especificação"):
            store.save_job(JobRecord("a", JobSpec(1, 20), 123))
    finally:
        store.close()


def test_list_blob_and_distinct_job_keys(tmp_path):
    store = Storage(tmp_path / "node.sqlite3")
    try:
        for key in ("a", "b"):
            store.save_job(JobRecord(key, JobSpec(1, 10, mode="list"), 123))
            store.save_result(key, SegmentResult(0, 4, [2, 3, 5, 7]))
        assert store.get_result("a", 0).primes == [2, 3, 5, 7]
        assert isinstance(store.db.execute("SELECT primes_blob FROM results").fetchone()[0], bytes)
        with pytest.raises(sqlite3.IntegrityError):
            store.save_result("unknown", SegmentResult(0, 0))
    finally:
        store.close()
