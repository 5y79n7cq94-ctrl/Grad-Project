"""Bridge Grad-Project to the MediaCrawler heat-analysis backend.

This repo is a lightweight FastAPI panel, while the full weekly heat-analysis
pipeline lives in the sibling MediaCrawler workspace. This adapter resolves the
MediaCrawler root, wires its project analytics modules into the import path,
and exposes the initialized service and job manager for local routes.
"""

from __future__ import annotations

import os
import sys
import importlib.util
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent


def _is_mediacrawler_root(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / "api" / "services" / "project_analytics.py").is_file()
        and (path / "api" / "services" / "project_jobs.py").is_file()
    )


def _resolve_mediacrawler_root() -> Path:
    env_root = os.getenv("MEDIACRAWLER_ROOT", "").strip()
    candidates: list[Path] = []
    if env_root:
        candidates.append(Path(env_root).expanduser())

    for base in [PROJECT_ROOT, *PROJECT_ROOT.parents]:
        candidates.append(base / "MediaCrawler")

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if _is_mediacrawler_root(resolved):
            return resolved

    raise RuntimeError(
        "Could not locate the MediaCrawler workspace. Set MEDIACRAWLER_ROOT to the repo root first."
    )


@lru_cache(maxsize=1)
def get_mediacrawler_root() -> Path:
    root = _resolve_mediacrawler_root()
    os.environ.setdefault("MEDIACRAWLER_ROOT", str(root))
    default_db = root / "data" / "project" / "social_media_analytics.db"
    if default_db.is_file():
        os.environ.setdefault("PROJECT_ANALYTICS_DB_PATH", str(default_db))
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


@lru_cache(maxsize=1)
def _load_module(module_name: str, file_path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec for {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def get_project_analytics_module() -> Any:
    root = get_mediacrawler_root()
    return _load_module("mediacrawler_project_analytics", root / "api" / "services" / "project_analytics.py")


@lru_cache(maxsize=1)
def get_project_analytics_service_class() -> Any:
    module = get_project_analytics_module()
    return module.ProjectAnalyticsService


@lru_cache(maxsize=1)
def get_project_analytics_service() -> Any:
    service_class = get_project_analytics_service_class()
    service = service_class()
    service.ensure_schema()
    return service
