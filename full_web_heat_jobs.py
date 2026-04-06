"""Background jobs for weekly Heat Analysis updates in Grad-Project."""

from __future__ import annotations

import subprocess
import sys
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from full_web_heat_adapter import get_mediacrawler_root, get_project_analytics_service_class


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


class HeatJobManager:
    """Small in-memory job registry for Heat Analysis update tasks."""

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start_update_job(
        self,
        *,
        platform: str,
        week_start: str,
        week_end: str,
        db_path: str = "",
    ) -> dict[str, Any]:
        job_id = uuid.uuid4().hex
        payload = {
            "platform": platform,
            "week_start": week_start,
            "week_end": week_end,
            "db_path": db_path,
        }
        record = {
            "job_id": job_id,
            "job_type": "heat_update_week",
            "status": "queued",
            "payload": payload,
            "summary": None,
            "error": "",
            "created_at": _now_iso(),
            "started_at": "",
            "finished_at": "",
        }
        with self._lock:
            self._jobs[job_id] = record
            self._trim_jobs_unlocked()

        thread = threading.Thread(target=self._run_update_job, args=(job_id, payload), daemon=True)
        thread.start()
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            return dict(job)

    def list_jobs(self, *, limit: int = 20) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit), 100))
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda item: item.get("created_at", ""), reverse=True)[:safe_limit]
        return {"items": [dict(job) for job in jobs], "total": len(self._jobs), "limit": safe_limit}

    def _run_update_job(self, job_id: str, payload: dict[str, Any]) -> None:
        self._update_job(job_id, status="running", started_at=_now_iso())
        try:
            summary = run_heat_update_week_job(**payload)
        except Exception as exc:  # pragma: no cover
            self._update_job(job_id, status="failed", error=str(exc), finished_at=_now_iso())
            return
        self._update_job(job_id, status="completed", summary=summary, finished_at=_now_iso())

    def _update_job(self, job_id: str, **updates: Any) -> None:
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id] = dict(self._jobs[job_id]) | updates

    def _trim_jobs_unlocked(self, max_jobs: int = 50) -> None:
        if len(self._jobs) <= max_jobs:
            return
        ordered_ids = sorted(self._jobs, key=lambda item: self._jobs[item].get("created_at", ""))
        for job_id in ordered_ids[:-max_jobs]:
            self._jobs.pop(job_id, None)


def run_heat_update_week_job(*, platform: str, week_start: str, week_end: str, db_path: str = "") -> dict[str, Any]:
    service_class = get_project_analytics_service_class()
    service = service_class(db_path=Path(db_path)) if db_path else service_class()
    service.ensure_schema()
    service._validate_week_window(week_start=week_start, week_end=week_end)

    current_windows = service.list_analysis_windows(platform=platform, weeks=104)["items"]
    matched = next((item for item in current_windows if item["week_start"] == week_start and item["week_end"] == week_end), None)
    if matched and matched["status"] != "to_be_updated":
        raise ValueError(f"Week {week_start} to {week_end} is already {matched['status']}.")

    command = build_update_command(platform=platform, week_start=week_start, week_end=week_end, db_path=str(service.db_path))
    completed = subprocess.run(
        command,
        cwd=get_mediacrawler_root(),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ValueError(detail[:1200] or "Update command failed.")

    windows_after = service.list_analysis_windows(platform=platform, weeks=104)["items"]
    updated = next((item for item in windows_after if item["week_start"] == week_start and item["week_end"] == week_end), None)
    return {
        "platform": platform,
        "week_start": week_start,
        "week_end": week_end,
        "db_path": str(service.db_path),
        "command": command,
        "stdout": (completed.stdout or "").strip()[-2000:],
        "status_after_update": updated["status"] if updated else "",
        "window": updated,
    }


def build_update_command(*, platform: str, week_start: str, week_end: str, db_path: str) -> list[str]:
    if platform == "fb":
        return [
            sys.executable,
            "tools/facebook_heat_pipeline.py",
            "--start-date",
            week_start,
            "--end-date",
            week_end,
            "--crawl",
            "--skip-extract",
            "--db-path",
            db_path,
        ]
    if platform == "wb":
        next_sunday_value = (datetime.fromisoformat(week_end).date() + timedelta(days=1)).isoformat()
        return [
            sys.executable,
            "tools/weibo_pipeline.py",
            "run",
            "--time-start",
            week_start,
            "--time-end",
            next_sunday_value,
            "--db-path",
            db_path,
            "--force-sync",
        ]
    raise ValueError(f"Unsupported platform for weekly update: {platform}")


heat_job_manager = HeatJobManager()
