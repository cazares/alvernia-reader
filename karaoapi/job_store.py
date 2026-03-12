from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional


class JobSQLiteStore:
    """Small SQLite-backed store for persisted jobs + idempotency lookups."""

    def __init__(self, db_path: Path) -> None:
        self._path = Path(db_path)
        self._lock = threading.Lock()
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._path), timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

    def initialize(self) -> None:
        with self._lock:
            if self._initialized:
                return
            self._path.parent.mkdir(parents=True, exist_ok=True)
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        slug TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        finished_at REAL,
                        idempotency_key TEXT,
                        dedupe_key TEXT,
                        payload_json TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_jobs_slug_status_created ON jobs(slug, status, created_at DESC)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_jobs_dedupe_created ON jobs(dedupe_key, created_at DESC)"
                )
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_unique
                    ON jobs(idempotency_key)
                    WHERE idempotency_key IS NOT NULL AND idempotency_key != ''
                    """
                )
                conn.commit()
                self._initialized = True
            finally:
                conn.close()

    def replace_all_jobs(
        self,
        jobs_payload: list[dict[str, Any]],
        *,
        prune_missing: bool = True,
    ) -> None:
        self.initialize()
        now = float(time.time())
        rows = []
        ids: list[str] = []
        for payload in jobs_payload:
            job_id = str(payload.get("id") or "").strip()
            if not job_id:
                continue
            ids.append(job_id)
            rows.append(
                {
                    "id": job_id,
                    "status": str(payload.get("status") or "queued"),
                    "slug": str(payload.get("slug") or ""),
                    "created_at": float(payload.get("created_at") or now),
                    "finished_at": payload.get("finished_at"),
                    "idempotency_key": str(payload.get("idempotency_key") or "").strip() or None,
                    "dedupe_key": str(payload.get("dedupe_key") or "").strip() or None,
                    "payload_json": json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
                    "updated_at": now,
                }
            )

        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                conn.executemany(
                    """
                    INSERT INTO jobs (
                        id, status, slug, created_at, finished_at, idempotency_key, dedupe_key, payload_json, updated_at
                    ) VALUES (
                        :id, :status, :slug, :created_at, :finished_at, :idempotency_key, :dedupe_key, :payload_json, :updated_at
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        status=excluded.status,
                        slug=excluded.slug,
                        created_at=excluded.created_at,
                        finished_at=excluded.finished_at,
                        idempotency_key=excluded.idempotency_key,
                        dedupe_key=excluded.dedupe_key,
                        payload_json=excluded.payload_json,
                        updated_at=excluded.updated_at
                    """,
                    rows,
                )
                if prune_missing:
                    if ids:
                        placeholders = ",".join("?" for _ in ids)
                        conn.execute(f"DELETE FROM jobs WHERE id NOT IN ({placeholders})", ids)
                    else:
                        conn.execute("DELETE FROM jobs")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def load_all_jobs(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT payload_json FROM jobs ORDER BY created_at ASC, id ASC"
                ).fetchall()
            finally:
                conn.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(str(row["payload_json"] or ""))
            except Exception:
                continue
            if isinstance(payload, dict):
                out.append(payload)
        return out

    def get_by_idempotency_key(self, key: str) -> Optional[dict[str, Any]]:
        self.initialize()
        clean = str(key or "").strip()
        if not clean:
            return None
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT payload_json FROM jobs WHERE idempotency_key = ? LIMIT 1",
                    (clean,),
                ).fetchone()
            finally:
                conn.close()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload_json"] or ""))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def get_recent_by_dedupe_key(self, dedupe_key: str, *, max_age_sec: float) -> Optional[dict[str, Any]]:
        self.initialize()
        clean = str(dedupe_key or "").strip()
        if not clean:
            return None
        min_created_at = 0.0
        if max_age_sec > 0:
            min_created_at = float(time.time()) - float(max_age_sec)
        with self._lock:
            conn = self._connect()
            try:
                if min_created_at > 0:
                    row = conn.execute(
                        """
                        SELECT payload_json FROM jobs
                        WHERE dedupe_key = ? AND created_at >= ?
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (clean, float(min_created_at)),
                    ).fetchone()
                else:
                    row = conn.execute(
                        """
                        SELECT payload_json FROM jobs
                        WHERE dedupe_key = ?
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (clean,),
                    ).fetchone()
            finally:
                conn.close()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload_json"] or ""))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None
