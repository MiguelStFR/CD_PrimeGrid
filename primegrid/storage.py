"""SQLite local; nenhum tipo gRPC nem cálculo do crivo nesta camada."""

import json
import sqlite3
import zlib
from dataclasses import asdict
from pathlib import Path

from primegrid.models import JobRecord, JobSpec, SegmentResult


class Storage:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY, spec_json TEXT NOT NULL,
                status TEXT NOT NULL, created_at REAL NOT NULL,
                duration_sec REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS results (
                job_id TEXT NOT NULL REFERENCES jobs(job_id),
                chunk_id INTEGER NOT NULL, prime_count INTEGER NOT NULL,
                primes_blob BLOB NULL,
                PRIMARY KEY(job_id, chunk_id)
            );
        """)

    def close(self) -> None:
        self.db.close()

    def save_job(self, job: JobRecord) -> None:
        existing = self.get_job(job.job_id)
        if existing is not None:
            if existing.spec != job.spec or existing.created_at != job.created_at:
                raise ValueError("job_id já existe com outra especificação")
            return
        with self.db:
            self.db.execute(
                "INSERT INTO jobs VALUES (?, ?, ?, ?, ?)",
                (job.job_id, json.dumps(asdict(job.spec)), job.status, job.created_at, job.duration_sec),
            )

    def get_job(self, job_id: str | None = None) -> JobRecord | None:
        if job_id is None:
            row = self.db.execute("SELECT * FROM jobs ORDER BY created_at DESC, rowid DESC LIMIT 1").fetchone()
        else:
            row = self.db.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return None if row is None else JobRecord(row[0], JobSpec(**json.loads(row[1])), row[3], row[2], row[4])

    def set_status(self, job_id: str, status: str, duration_sec: float = 0) -> None:
        with self.db:
            self.db.execute("UPDATE jobs SET status = ?, duration_sec = ? WHERE job_id = ?", (status, duration_sec, job_id))

    @staticmethod
    def _result(row) -> SegmentResult:
        primes = None if row[2] is None else json.loads(zlib.decompress(row[2]))
        return SegmentResult(row[0], row[1], primes)

    def get_result(self, job_id: str, chunk_id: int) -> SegmentResult | None:
        row = self.db.execute(
            "SELECT chunk_id, prime_count, primes_blob FROM results WHERE job_id = ? AND chunk_id = ?",
            (job_id, chunk_id),
        ).fetchone()
        return None if row is None else self._result(row)

    def results(self, job_id: str) -> list[SegmentResult]:
        return [self._result(row) for row in self.db.execute(
            "SELECT chunk_id, prime_count, primes_blob FROM results WHERE job_id = ? ORDER BY chunk_id", (job_id,)
        )]

    def save_result(self, job_id: str, result: SegmentResult) -> bool:
        """Commit antes do ACK; rejeita conteúdo conflitante na mesma chave."""
        existing = self.get_result(job_id, result.chunk_id)
        if existing is not None:
            if existing != result:
                raise ValueError("resultado conflitante para job_id/chunk_id")
            return False
        blob = None if result.primes is None else zlib.compress(json.dumps(result.primes).encode())
        with self.db:
            self.db.execute("INSERT INTO results VALUES (?, ?, ?, ?)", (job_id, result.chunk_id, result.prime_count, blob))
        return True
