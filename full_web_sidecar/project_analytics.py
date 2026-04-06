"""Unified analytics database service for Weibo and Facebook heat analysis.

`ProjectAnalyticsService` is the data layer behind the publish-friendly
repository. It discovers normalized JSONL inputs, syncs them into SQLite,
derives event-ready rows, and exposes query helpers used by both the CLI
pipelines and the FastAPI dashboard endpoints.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from .helpers.build_weibo_heat_analysis import (
    build_heat_outputs,
    load_event_alias_registry,
    load_event_parent_registry,
    load_organizer_registry,
)
from .helpers.rewrite_search_ready import (
    build_analysis_content,
    build_clean_content,
    build_flags,
    extract_hashtags,
    extract_mentions,
    make_topic_seed,
)
from .helpers.time_util import parse_weibo_created_at_to_timestamp


PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent


def _find_latest_promoted_event_db(project_root: Path) -> Optional[Path]:
    event_debug_dir = project_root / "tmp" / "event_debug"
    if not event_debug_dir.exists():
        return None

    candidates = [path for path in event_debug_dir.glob("*.promoted-event.db") if path.is_file()]
    if not candidates:
        return None

    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _resolve_default_db_path(project_root: Path) -> Path:
    env_db_path = os.getenv("FULL_WEB_ANALYTICS_DB_PATH") or os.getenv("PROJECT_ANALYTICS_DB_PATH")
    if env_db_path:
        return Path(env_db_path)
    return project_root / "data" / "social_media_analytics.db"


DEFAULT_DB_PATH = _resolve_default_db_path(PROJECT_ROOT)
DEFAULT_SEARCH_ROOTS = (
    PROJECT_ROOT / "data",
    PROJECT_ROOT / "tmp",
)
DEFAULT_EVENT_ALIAS_REGISTRY_PATH = PACKAGE_ROOT / "config" / "weibo_event_aliases.json"
DEFAULT_EVENT_PARENT_REGISTRY_PATH = PACKAGE_ROOT / "config" / "weibo_event_parent_groups.json"
DEFAULT_EVENT_ORGANIZER_REGISTRY_PATH = PACKAGE_ROOT / "config" / "weibo_organizer_registry.json"
PLATFORM_LABELS = {
    "wb": "Weibo",
    "fb": "Facebook",
}
FULL_WEB_ANALYSIS_FLOOR_DATE = date(2026, 3, 1)

FB_MACAU_LOCATION_CUES = {
    "macau",
    "macao",
    "澳门",
    "澳門",
    "taipa",
    "cotai",
    "路氹",
    "氹仔",
    "凼仔",
}

FB_LOCAL_ENTITY_CUES = {
    "wynn macau",
    "wynn palace",
    "永利澳门",
    "永利澳門",
    "永利皇宫",
    "永利皇宮",
    "sands china",
    "金沙中国",
    "金沙中國",
    "the venetian macao",
    "the parisian macao",
    "the londoner macao",
    "澳门威尼斯人",
    "澳門威尼斯人",
    "巴黎人澳门",
    "澳門巴黎人",
    "伦敦人澳门",
    "澳門倫敦人",
    "city of dreams macau",
    "studio city macau",
    "melco",
    "新濠天地",
    "新濠影汇",
    "新濠影匯",
    "grand lisboa palace",
    "grand lisboa macau",
    "上葡京",
    "新葡京",
    "澳娱综合",
    "澳娛綜合",
    "galaxy macau",
    "galaxy arena",
    "broadway macau",
    "澳门银河",
    "澳門銀河",
    "澳门百老汇",
    "澳門百老匯",
    "mgm macau",
    "mgm cotai",
    "美高梅澳门",
    "澳門美高梅",
    "美狮美高梅",
    "美獅美高梅",
    "mgto",
    "macao government tourism office",
    "澳门旅游局",
    "澳門旅遊局",
    "macao cultural affairs bureau",
    "澳门文化局",
    "澳門文化局",
    "macao sports bureau",
    "澳门体育局",
    "澳門體育局",
    "ipim",
    "macao trade and investment promotion institute",
    "澳门招商投资促进局",
    "澳門招商投資促進局",
    "macao sar government",
    "macao government",
    "澳门特别行政区政府",
    "澳門特別行政區政府",
}

FB_AMBIGUOUS_SHORT_CUES = {"sjm", "mgm"}

FB_SJM_BOOKTOK_NOISE_CUES = {
    "acotar",
    "sjmbooks",
    "sarah j maas",
    "rhysand",
    "rowan",
    "hunt",
    "booktok",
}


class ProjectAnalyticsService:
    """Read/write facade for the project's analytics SQLite database."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        search_roots: Optional[Sequence[Path]] = None,
    ) -> None:
        self.db_path = Path(db_path or DEFAULT_DB_PATH)
        self.search_roots = tuple(Path(root) for root in (search_roots or DEFAULT_SEARCH_ROOTS))

    def ensure_schema(self) -> None:
        """Create or migrate the SQLite schema used by the analysis modules."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS social_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_post_id TEXT NOT NULL,
                    author_id TEXT DEFAULT '',
                    author_name TEXT DEFAULT '',
                    title TEXT DEFAULT '',
                    content TEXT DEFAULT '',
                    clean_content TEXT DEFAULT '',
                    analysis_content TEXT DEFAULT '',
                    note_url TEXT DEFAULT '',
                    published_at TEXT DEFAULT '',
                    published_ts INTEGER DEFAULT 0,
                    like_count INTEGER DEFAULT 0,
                    comment_count INTEGER DEFAULT 0,
                    share_count INTEGER DEFAULT 0,
                    collect_count INTEGER DEFAULT 0,
                    media_urls TEXT DEFAULT '[]',
                    tags TEXT DEFAULT '[]',
                    source_keyword TEXT DEFAULT '',
                    source_keywords TEXT DEFAULT '[]',
                    source_file TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    UNIQUE(platform, source_post_id)
                );

                CREATE INDEX IF NOT EXISTS idx_social_posts_platform_published
                ON social_posts(platform, published_ts DESC);

                CREATE INDEX IF NOT EXISTS idx_social_posts_author
                ON social_posts(author_name);

                CREATE INDEX IF NOT EXISTS idx_social_posts_source_type
                ON social_posts(source_type);

                CREATE TABLE IF NOT EXISTS social_post_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_post_id TEXT NOT NULL,
                    source_keyword TEXT DEFAULT '',
                    source_file TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    UNIQUE(platform, source_post_id, source_file, source_keyword)
                );

                CREATE INDEX IF NOT EXISTS idx_social_post_sources_keyword
                ON social_post_sources(platform, source_keyword);

                CREATE TABLE IF NOT EXISTS event_ready_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    source_post_id TEXT NOT NULL,
                    author_name TEXT DEFAULT '',
                    published_at TEXT DEFAULT '',
                    published_ts INTEGER DEFAULT 0,
                    note_url TEXT DEFAULT '',
                    clean_content TEXT DEFAULT '',
                    analysis_content TEXT DEFAULT '',
                    hashtags TEXT DEFAULT '[]',
                    mentions TEXT DEFAULT '[]',
                    source_keywords TEXT DEFAULT '[]',
                    topic_seed_terms TEXT DEFAULT '[]',
                    relevance_flags TEXT DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'ready',
                    source_file TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(platform, source_post_id)
                );

                CREATE INDEX IF NOT EXISTS idx_event_ready_posts_status
                ON event_ready_posts(platform, status, published_ts DESC);

                CREATE TABLE IF NOT EXISTS sync_runs (
                    source_file TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_mtime REAL NOT NULL,
                    imported_rows INTEGER DEFAULT 0,
                    source_rows INTEGER DEFAULT 0,
                    event_rows INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT DEFAULT '',
                    synced_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_extracted_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    source_post_id TEXT NOT NULL,
                    author_name TEXT DEFAULT '',
                    published_at TEXT DEFAULT '',
                    published_ts INTEGER DEFAULT 0,
                    note_url TEXT DEFAULT '',
                    clean_content TEXT DEFAULT '',
                    analysis_content TEXT DEFAULT '',
                    hashtags TEXT DEFAULT '[]',
                    mentions TEXT DEFAULT '[]',
                    source_keywords TEXT DEFAULT '[]',
                    topic_seed_terms TEXT DEFAULT '[]',
                    relevance_flags TEXT DEFAULT '{}',
                    source_file TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    post_type TEXT DEFAULT '',
                    raw_event_candidate TEXT DEFAULT '',
                    canonical_event_name TEXT DEFAULT '',
                    event_eligible INTEGER DEFAULT 0,
                    event_promoted INTEGER DEFAULT 0,
                    event_confidence REAL DEFAULT 0,
                    event_geo_score REAL DEFAULT 0,
                    event_leaf_name TEXT DEFAULT '',
                    event_parent_name TEXT DEFAULT '',
                    event_key TEXT DEFAULT '',
                    event_family_key TEXT DEFAULT '',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_confidence REAL DEFAULT 0,
                    organizer_evidence TEXT DEFAULT '[]',
                    primary_topic TEXT DEFAULT '',
                    dashboard_category TEXT DEFAULT '',
                    quality_weight REAL DEFAULT 1,
                    engagement_total INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    comment_fetch_count INTEGER DEFAULT 0,
                    comment_fetch_like_sum INTEGER DEFAULT 0,
                    comment_fetch_sub_comment_sum INTEGER DEFAULT 0,
                    comment_unique_authors INTEGER DEFAULT 0,
                    top_comments TEXT DEFAULT '[]',
                    post_heat REAL DEFAULT 0,
                    base_engagement REAL DEFAULT 0,
                    discussion_strength REAL DEFAULT 0,
                    comment_value REAL DEFAULT 0,
                    recency_factor REAL DEFAULT 0,
                    raw_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, source_post_id)
                );

                CREATE INDEX IF NOT EXISTS idx_event_extracted_posts_event
                ON event_extracted_posts(platform, event_key, published_ts DESC);

                CREATE INDEX IF NOT EXISTS idx_event_extracted_posts_type
                ON event_extracted_posts(platform, post_type, published_ts DESC);

                CREATE TABLE IF NOT EXISTS event_clusters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_type TEXT NOT NULL DEFAULT 'event_key',
                    post_count INTEGER DEFAULT 0,
                    unique_authors INTEGER DEFAULT 0,
                    post_type_breakdown TEXT DEFAULT '{}',
                    total_like_count INTEGER DEFAULT 0,
                    total_comment_count INTEGER DEFAULT 0,
                    total_share_count INTEGER DEFAULT 0,
                    total_engagement INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    keywords TEXT DEFAULT '[]',
                    top_posts TEXT DEFAULT '[]',
                    top_comments TEXT DEFAULT '[]',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_breakdown TEXT DEFAULT '{}',
                    organizer_evidence TEXT DEFAULT '[]',
                    dashboard_category TEXT DEFAULT '',
                    dashboard_category_score REAL DEFAULT 0,
                    engagement_component REAL DEFAULT 0,
                    discussion_component REAL DEFAULT 0,
                    diversity_component REAL DEFAULT 0,
                    velocity_component REAL DEFAULT 0,
                    heat_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, cluster_key)
                );

                CREATE INDEX IF NOT EXISTS idx_event_clusters_heat
                ON event_clusters(platform, heat_score DESC, cluster_key ASC);

                CREATE TABLE IF NOT EXISTS topic_clusters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_type TEXT NOT NULL DEFAULT 'primary_topic',
                    post_count INTEGER DEFAULT 0,
                    unique_authors INTEGER DEFAULT 0,
                    post_type_breakdown TEXT DEFAULT '{}',
                    total_like_count INTEGER DEFAULT 0,
                    total_comment_count INTEGER DEFAULT 0,
                    total_share_count INTEGER DEFAULT 0,
                    total_engagement INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    keywords TEXT DEFAULT '[]',
                    top_posts TEXT DEFAULT '[]',
                    top_comments TEXT DEFAULT '[]',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_breakdown TEXT DEFAULT '{}',
                    organizer_evidence TEXT DEFAULT '[]',
                    dashboard_category TEXT DEFAULT '',
                    dashboard_category_score REAL DEFAULT 0,
                    engagement_component REAL DEFAULT 0,
                    discussion_component REAL DEFAULT 0,
                    diversity_component REAL DEFAULT 0,
                    velocity_component REAL DEFAULT 0,
                    heat_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, cluster_key)
                );

                CREATE INDEX IF NOT EXISTS idx_topic_clusters_heat
                ON topic_clusters(platform, heat_score DESC, cluster_key ASC);

                CREATE TABLE IF NOT EXISTS analysis_windows (
                    platform TEXT NOT NULL,
                    week_start TEXT NOT NULL,
                    week_end TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'completed',
                    source_ready_posts INTEGER DEFAULT 0,
                    extracted_post_rows INTEGER DEFAULT 0,
                    event_cluster_rows INTEGER DEFAULT 0,
                    topic_cluster_rows INTEGER DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    note TEXT DEFAULT '',
                    UNIQUE(platform, week_start, week_end)
                );

                CREATE TABLE IF NOT EXISTS weekly_event_extracted_posts (
                    platform TEXT NOT NULL,
                    week_start TEXT NOT NULL,
                    week_end TEXT NOT NULL,
                    source_post_id TEXT NOT NULL,
                    author_name TEXT DEFAULT '',
                    published_at TEXT DEFAULT '',
                    published_ts INTEGER DEFAULT 0,
                    note_url TEXT DEFAULT '',
                    clean_content TEXT DEFAULT '',
                    analysis_content TEXT DEFAULT '',
                    hashtags TEXT DEFAULT '[]',
                    mentions TEXT DEFAULT '[]',
                    source_keywords TEXT DEFAULT '[]',
                    topic_seed_terms TEXT DEFAULT '[]',
                    relevance_flags TEXT DEFAULT '{}',
                    source_file TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    post_type TEXT DEFAULT '',
                    raw_event_candidate TEXT DEFAULT '',
                    canonical_event_name TEXT DEFAULT '',
                    event_eligible INTEGER DEFAULT 0,
                    event_promoted INTEGER DEFAULT 0,
                    event_confidence REAL DEFAULT 0,
                    event_geo_score REAL DEFAULT 0,
                    event_leaf_name TEXT DEFAULT '',
                    event_parent_name TEXT DEFAULT '',
                    event_key TEXT DEFAULT '',
                    event_family_key TEXT DEFAULT '',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_confidence REAL DEFAULT 0,
                    organizer_evidence TEXT DEFAULT '[]',
                    primary_topic TEXT DEFAULT '',
                    dashboard_category TEXT DEFAULT '',
                    quality_weight REAL DEFAULT 1,
                    engagement_total INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    comment_fetch_count INTEGER DEFAULT 0,
                    comment_fetch_like_sum INTEGER DEFAULT 0,
                    comment_fetch_sub_comment_sum INTEGER DEFAULT 0,
                    comment_unique_authors INTEGER DEFAULT 0,
                    top_comments TEXT DEFAULT '[]',
                    post_heat REAL DEFAULT 0,
                    base_engagement REAL DEFAULT 0,
                    discussion_strength REAL DEFAULT 0,
                    comment_value REAL DEFAULT 0,
                    recency_factor REAL DEFAULT 0,
                    raw_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, week_start, week_end, source_post_id)
                );

                CREATE INDEX IF NOT EXISTS idx_weekly_event_extracted_posts_window
                ON weekly_event_extracted_posts(platform, week_start, week_end, published_ts DESC);

                CREATE TABLE IF NOT EXISTS weekly_event_clusters (
                    platform TEXT NOT NULL,
                    week_start TEXT NOT NULL,
                    week_end TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_type TEXT NOT NULL DEFAULT 'event_key',
                    post_count INTEGER DEFAULT 0,
                    unique_authors INTEGER DEFAULT 0,
                    post_type_breakdown TEXT DEFAULT '{}',
                    total_like_count INTEGER DEFAULT 0,
                    total_comment_count INTEGER DEFAULT 0,
                    total_share_count INTEGER DEFAULT 0,
                    total_engagement INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    keywords TEXT DEFAULT '[]',
                    top_posts TEXT DEFAULT '[]',
                    top_comments TEXT DEFAULT '[]',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_breakdown TEXT DEFAULT '{}',
                    organizer_evidence TEXT DEFAULT '[]',
                    dashboard_category TEXT DEFAULT '',
                    dashboard_category_score REAL DEFAULT 0,
                    engagement_component REAL DEFAULT 0,
                    discussion_component REAL DEFAULT 0,
                    diversity_component REAL DEFAULT 0,
                    velocity_component REAL DEFAULT 0,
                    heat_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, week_start, week_end, cluster_key)
                );

                CREATE INDEX IF NOT EXISTS idx_weekly_event_clusters_heat
                ON weekly_event_clusters(platform, week_start, week_end, heat_score DESC, cluster_key ASC);

                CREATE TABLE IF NOT EXISTS weekly_topic_clusters (
                    platform TEXT NOT NULL,
                    week_start TEXT NOT NULL,
                    week_end TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_type TEXT NOT NULL DEFAULT 'primary_topic',
                    post_count INTEGER DEFAULT 0,
                    unique_authors INTEGER DEFAULT 0,
                    post_type_breakdown TEXT DEFAULT '{}',
                    total_like_count INTEGER DEFAULT 0,
                    total_comment_count INTEGER DEFAULT 0,
                    total_share_count INTEGER DEFAULT 0,
                    total_engagement INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    keywords TEXT DEFAULT '[]',
                    top_posts TEXT DEFAULT '[]',
                    top_comments TEXT DEFAULT '[]',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_breakdown TEXT DEFAULT '{}',
                    organizer_evidence TEXT DEFAULT '[]',
                    dashboard_category TEXT DEFAULT '',
                    dashboard_category_score REAL DEFAULT 0,
                    engagement_component REAL DEFAULT 0,
                    discussion_component REAL DEFAULT 0,
                    diversity_component REAL DEFAULT 0,
                    velocity_component REAL DEFAULT 0,
                    heat_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, week_start, week_end, cluster_key)
                );

                CREATE INDEX IF NOT EXISTS idx_weekly_topic_clusters_heat
                ON weekly_topic_clusters(platform, week_start, week_end, heat_score DESC, cluster_key ASC);

                CREATE TABLE IF NOT EXISTS analysis_months (
                    platform TEXT NOT NULL,
                    month_key TEXT NOT NULL,
                    month_start TEXT NOT NULL,
                    month_end TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'completed',
                    source_ready_posts INTEGER DEFAULT 0,
                    extracted_post_rows INTEGER DEFAULT 0,
                    event_cluster_rows INTEGER DEFAULT 0,
                    topic_cluster_rows INTEGER DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    note TEXT DEFAULT '',
                    UNIQUE(platform, month_key)
                );

                CREATE TABLE IF NOT EXISTS monthly_event_extracted_posts (
                    platform TEXT NOT NULL,
                    month_key TEXT NOT NULL,
                    month_start TEXT NOT NULL,
                    month_end TEXT NOT NULL,
                    source_post_id TEXT NOT NULL,
                    author_name TEXT DEFAULT '',
                    published_at TEXT DEFAULT '',
                    published_ts INTEGER DEFAULT 0,
                    note_url TEXT DEFAULT '',
                    clean_content TEXT DEFAULT '',
                    analysis_content TEXT DEFAULT '',
                    hashtags TEXT DEFAULT '[]',
                    mentions TEXT DEFAULT '[]',
                    source_keywords TEXT DEFAULT '[]',
                    topic_seed_terms TEXT DEFAULT '[]',
                    relevance_flags TEXT DEFAULT '{}',
                    source_file TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    post_type TEXT DEFAULT '',
                    raw_event_candidate TEXT DEFAULT '',
                    canonical_event_name TEXT DEFAULT '',
                    event_eligible INTEGER DEFAULT 0,
                    event_promoted INTEGER DEFAULT 0,
                    event_confidence REAL DEFAULT 0,
                    event_geo_score REAL DEFAULT 0,
                    event_leaf_name TEXT DEFAULT '',
                    event_parent_name TEXT DEFAULT '',
                    event_key TEXT DEFAULT '',
                    event_family_key TEXT DEFAULT '',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_confidence REAL DEFAULT 0,
                    organizer_evidence TEXT DEFAULT '[]',
                    primary_topic TEXT DEFAULT '',
                    dashboard_category TEXT DEFAULT '',
                    quality_weight REAL DEFAULT 1,
                    engagement_total INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    comment_fetch_count INTEGER DEFAULT 0,
                    comment_fetch_like_sum INTEGER DEFAULT 0,
                    comment_fetch_sub_comment_sum INTEGER DEFAULT 0,
                    comment_unique_authors INTEGER DEFAULT 0,
                    top_comments TEXT DEFAULT '[]',
                    post_heat REAL DEFAULT 0,
                    base_engagement REAL DEFAULT 0,
                    discussion_strength REAL DEFAULT 0,
                    comment_value REAL DEFAULT 0,
                    recency_factor REAL DEFAULT 0,
                    raw_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, month_key, source_post_id)
                );

                CREATE INDEX IF NOT EXISTS idx_monthly_event_extracted_posts_window
                ON monthly_event_extracted_posts(platform, month_key, published_ts DESC);

                CREATE TABLE IF NOT EXISTS monthly_event_clusters (
                    platform TEXT NOT NULL,
                    month_key TEXT NOT NULL,
                    month_start TEXT NOT NULL,
                    month_end TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_type TEXT NOT NULL DEFAULT 'event_key',
                    post_count INTEGER DEFAULT 0,
                    unique_authors INTEGER DEFAULT 0,
                    post_type_breakdown TEXT DEFAULT '{}',
                    total_like_count INTEGER DEFAULT 0,
                    total_comment_count INTEGER DEFAULT 0,
                    total_share_count INTEGER DEFAULT 0,
                    total_engagement INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    keywords TEXT DEFAULT '[]',
                    top_posts TEXT DEFAULT '[]',
                    top_comments TEXT DEFAULT '[]',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_breakdown TEXT DEFAULT '{}',
                    organizer_evidence TEXT DEFAULT '[]',
                    dashboard_category TEXT DEFAULT '',
                    dashboard_category_score REAL DEFAULT 0,
                    engagement_component REAL DEFAULT 0,
                    discussion_component REAL DEFAULT 0,
                    diversity_component REAL DEFAULT 0,
                    velocity_component REAL DEFAULT 0,
                    heat_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, month_key, cluster_key)
                );

                CREATE INDEX IF NOT EXISTS idx_monthly_event_clusters_heat
                ON monthly_event_clusters(platform, month_key, heat_score DESC, cluster_key ASC);

                CREATE TABLE IF NOT EXISTS monthly_topic_clusters (
                    platform TEXT NOT NULL,
                    month_key TEXT NOT NULL,
                    month_start TEXT NOT NULL,
                    month_end TEXT NOT NULL,
                    cluster_key TEXT NOT NULL,
                    cluster_type TEXT NOT NULL DEFAULT 'primary_topic',
                    post_count INTEGER DEFAULT 0,
                    unique_authors INTEGER DEFAULT 0,
                    post_type_breakdown TEXT DEFAULT '{}',
                    total_like_count INTEGER DEFAULT 0,
                    total_comment_count INTEGER DEFAULT 0,
                    total_share_count INTEGER DEFAULT 0,
                    total_engagement INTEGER DEFAULT 0,
                    discussion_total INTEGER DEFAULT 0,
                    keywords TEXT DEFAULT '[]',
                    top_posts TEXT DEFAULT '[]',
                    top_comments TEXT DEFAULT '[]',
                    organizer_key TEXT DEFAULT '',
                    organizer_name TEXT DEFAULT '',
                    organizer_type TEXT DEFAULT '',
                    organizer_breakdown TEXT DEFAULT '{}',
                    organizer_evidence TEXT DEFAULT '[]',
                    dashboard_category TEXT DEFAULT '',
                    dashboard_category_score REAL DEFAULT 0,
                    engagement_component REAL DEFAULT 0,
                    discussion_component REAL DEFAULT 0,
                    diversity_component REAL DEFAULT 0,
                    velocity_component REAL DEFAULT 0,
                    heat_score REAL DEFAULT 0,
                    extracted_at TEXT NOT NULL,
                    UNIQUE(platform, month_key, cluster_key)
                );

                CREATE INDEX IF NOT EXISTS idx_monthly_topic_clusters_heat
                ON monthly_topic_clusters(platform, month_key, heat_score DESC, cluster_key ASC);

                CREATE TABLE IF NOT EXISTS cluster_feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    board_type TEXT NOT NULL,
                    scope_type TEXT NOT NULL,
                    week_start TEXT DEFAULT '',
                    week_end TEXT DEFAULT '',
                    month_key TEXT DEFAULT '',
                    quarter_key TEXT DEFAULT '',
                    source_cluster_key TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_cluster_key TEXT DEFAULT '',
                    note TEXT DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_cluster_feedback_scope
                ON cluster_feedback(platform, board_type, scope_type, week_start, week_end, month_key, quarter_key, source_cluster_key, created_at DESC);
                """
            )
            self._ensure_column(conn, "social_posts", "clean_content", "TEXT DEFAULT ''")
            self._ensure_column(conn, "social_posts", "analysis_content", "TEXT DEFAULT ''")
            self._ensure_column(conn, "social_posts", "source_keywords", "TEXT DEFAULT '[]'")
            self._ensure_column(conn, "sync_runs", "source_rows", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "sync_runs", "event_rows", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "event_extracted_posts", "event_promoted", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "event_extracted_posts", "event_family_key", "TEXT DEFAULT ''")
            self._ensure_column(conn, "event_extracted_posts", "dashboard_category", "TEXT DEFAULT ''")
            self._ensure_column(conn, "event_clusters", "dashboard_category", "TEXT DEFAULT ''")
            self._ensure_column(conn, "event_clusters", "dashboard_category_score", "REAL DEFAULT 0")
            self._ensure_column(conn, "topic_clusters", "dashboard_category", "TEXT DEFAULT ''")
            self._ensure_column(conn, "topic_clusters", "dashboard_category_score", "REAL DEFAULT 0")

    def bootstrap(self) -> dict[str, Any]:
        self.ensure_schema()
        with self._connect() as conn:
            count = conn.execute("SELECT COUNT(*) AS count FROM social_posts").fetchone()["count"]
        if count > 0:
            return {"bootstrapped": False, "reason": "database_not_empty"}
        sync_result = self.sync(platform="wb")
        return {"bootstrapped": True, "sync_result": sync_result}

    def sync(self, platform: Optional[str] = None, force: bool = False) -> dict[str, Any]:
        self.ensure_schema()
        source_files = self.discover_source_files(platform=platform)
        return self._sync_source_files(source_files, force=force, platform=platform)

    def sync_files(self, source_files: Sequence[Path | str], force: bool = False) -> dict[str, Any]:
        self.ensure_schema()
        normalized_files = []
        for item in source_files:
            path = Path(item)
            if not path.is_absolute():
                path = PROJECT_ROOT / path
            if path.exists() and path.is_file():
                normalized_files.append(path)
        return self._sync_source_files(normalized_files, force=force, platform=None)

    def _sync_source_files(
        self,
        source_files: Sequence[Path],
        *,
        force: bool,
        platform: Optional[str],
    ) -> dict[str, Any]:
        normalized_platform = self._normalize_platform_filter(platform)
        summary = {
            "db_path": str(self.db_path),
            "platform": normalized_platform,
            "discovered_files": len(source_files),
            "processed_files": 0,
            "skipped_files": 0,
            "imported_posts": 0,
            "imported_sources": 0,
            "updated_event_posts": 0,
            "files": [],
        }

        with self._connect() as conn:
            if force and normalized_platform:
                conn.execute(
                    "DELETE FROM event_ready_posts WHERE platform = ?",
                    (normalized_platform,),
                )
            for source_file in source_files:
                file_stat = source_file.stat()
                existing = conn.execute(
                    """
                    SELECT file_size, file_mtime, status
                    FROM sync_runs
                    WHERE source_file = ?
                    """,
                    (str(source_file),),
                ).fetchone()

                if (
                    not force
                    and existing
                    and existing["status"] == "success"
                    and existing["file_size"] == file_stat.st_size
                    and abs(existing["file_mtime"] - file_stat.st_mtime) < 0.000001
                ):
                    summary["skipped_files"] += 1
                    summary["files"].append(
                        {
                            "path": self._relative_source_path(str(source_file)),
                            "platform": self._infer_platform(source_file),
                            "source_type": self._infer_source_type(source_file),
                            "status": "skipped",
                        }
                    )
                    continue

                try:
                    sync_result = self._sync_single_file(conn, source_file)
                    self._upsert_sync_run(
                        conn,
                        source_file=source_file,
                        sync_result=sync_result,
                        status="success",
                        error_message="",
                    )
                    summary["processed_files"] += 1
                    summary["imported_posts"] += sync_result["posts_imported"]
                    summary["imported_sources"] += sync_result["sources_imported"]
                    summary["updated_event_posts"] += sync_result["event_posts_imported"]
                    summary["files"].append(
                        {
                            "path": self._relative_source_path(str(source_file)),
                            "platform": self._infer_platform(source_file),
                            "source_type": self._infer_source_type(source_file),
                            "status": "success",
                            "imported_rows": sync_result["posts_imported"],
                            "source_rows": sync_result["sources_imported"],
                            "event_rows": sync_result["event_posts_imported"],
                        }
                    )
                except Exception as exc:
                    self._upsert_sync_run(
                        conn,
                        source_file=source_file,
                        sync_result={"posts_imported": 0, "sources_imported": 0, "event_posts_imported": 0},
                        status="failed",
                        error_message=str(exc),
                    )
                    summary["files"].append(
                        {
                            "path": self._relative_source_path(str(source_file)),
                            "platform": self._infer_platform(source_file),
                            "source_type": self._infer_source_type(source_file),
                            "status": "failed",
                            "error": str(exc),
                        }
                    )

        return summary

    def discover_source_files(self, platform: Optional[str] = None) -> list[Path]:
        normalized_platform = self._normalize_platform_filter(platform)
        discovered: list[Path] = []
        seen: set[Path] = set()

        for root in self.search_roots:
            if not root.exists():
                continue
            for candidate in root.rglob("*"):
                if not candidate.is_file():
                    continue
                if candidate.suffix.lower() not in {".json", ".jsonl"}:
                    continue
                if not self._is_content_file(candidate):
                    continue
                try:
                    candidate_platform = self._infer_platform(candidate)
                except ValueError:
                    continue
                if normalized_platform and candidate_platform != normalized_platform:
                    continue
                if candidate in seen:
                    continue
                discovered.append(candidate)
                seen.add(candidate)

        discovered.sort(key=lambda item: (self._infer_platform(item), item.name, str(item)))
        return discovered

    def list_sources(self, platform: Optional[str] = None) -> dict[str, Any]:
        self.ensure_schema()
        source_files = self.discover_source_files(platform=platform)
        with self._connect() as conn:
            sync_rows = conn.execute(
                """
                SELECT source_file, imported_rows, source_rows, event_rows, status, error_message, synced_at, file_size, file_mtime
                FROM sync_runs
                """
            ).fetchall()
        sync_lookup = {row["source_file"]: dict(row) for row in sync_rows}

        files = []
        for source_file in source_files:
            stat = source_file.stat()
            run_info = sync_lookup.get(str(source_file), {})
            files.append(
                {
                    "path": self._relative_source_path(str(source_file)),
                    "platform": self._infer_platform(source_file),
                    "platform_label": PLATFORM_LABELS.get(self._infer_platform(source_file), self._infer_platform(source_file)),
                    "source_type": self._infer_source_type(source_file),
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "sync_status": run_info.get("status", "pending"),
                    "imported_rows": run_info.get("imported_rows", 0),
                    "source_rows": run_info.get("source_rows", 0),
                    "event_rows": run_info.get("event_rows", 0),
                    "synced_at": run_info.get("synced_at", ""),
                    "error_message": run_info.get("error_message", ""),
                }
            )

        return {
            "db_path": str(self.db_path),
            "total": len(files),
            "files": files,
        }

    def get_overview(self, platform: Optional[str] = None) -> dict[str, Any]:
        self.ensure_schema()
        where_clause, params = self._build_post_where_clause(platform=platform)

        with self._connect() as conn:
            total_posts = conn.execute(
                f"SELECT COUNT(*) AS count FROM social_posts {where_clause}",
                params,
            ).fetchone()["count"]
            source_file_count = conn.execute(
                f"SELECT COUNT(DISTINCT source_file) AS count FROM social_posts {where_clause}",
                params,
            ).fetchone()["count"]
            platforms = conn.execute(
                f"""
                SELECT platform, COUNT(*) AS count
                FROM social_posts
                {where_clause}
                GROUP BY platform
                ORDER BY count DESC, platform ASC
                """,
                params,
            ).fetchall()
            source_types = conn.execute(
                f"""
                SELECT source_type, COUNT(*) AS count
                FROM social_posts
                {where_clause}
                GROUP BY source_type
                ORDER BY count DESC, source_type ASC
                """,
                params,
            ).fetchall()
            engagement = conn.execute(
                f"""
                SELECT
                    COALESCE(SUM(like_count), 0) AS like_total,
                    COALESCE(SUM(comment_count), 0) AS comment_total,
                    COALESCE(SUM(share_count), 0) AS share_total
                FROM social_posts
                {where_clause}
                """,
                params,
            ).fetchone()
            top_authors = conn.execute(
                f"""
                SELECT author_name, platform, COUNT(*) AS post_count
                FROM social_posts
                {where_clause} {"AND" if where_clause else "WHERE"} author_name != ''
                GROUP BY platform, author_name
                ORDER BY post_count DESC, author_name ASC
                LIMIT 8
                """,
                params,
            ).fetchall()
            timeline = conn.execute(
                f"""
                SELECT substr(published_at, 1, 10) AS day, COUNT(*) AS count
                FROM social_posts
                {where_clause} {"AND" if where_clause else "WHERE"} published_at != ''
                GROUP BY day
                ORDER BY day DESC
                LIMIT 14
                """,
                params,
            ).fetchall()
            recent_posts = conn.execute(
                f"""
                SELECT platform, source_type, source_post_id, author_name, title, content, published_at, note_url,
                       like_count, comment_count, share_count, collect_count, source_keyword, source_keywords, source_file
                FROM social_posts
                {where_clause}
                ORDER BY published_ts DESC, id DESC
                LIMIT 8
                """,
                params,
            ).fetchall()
            recent_syncs = conn.execute(
                """
                SELECT source_file, platform, source_type, imported_rows, source_rows, event_rows, status, synced_at
                FROM sync_runs
                WHERE (? IS NULL OR platform = ?)
                ORDER BY synced_at DESC
                LIMIT 8
                """,
                (self._normalize_platform_filter(platform), self._normalize_platform_filter(platform)),
            ).fetchall()
            event_ready_count = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM event_ready_posts
                WHERE (? IS NULL OR platform = ?)
                """,
                (self._normalize_platform_filter(platform), self._normalize_platform_filter(platform)),
            ).fetchone()["count"]
            event_cluster_count = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM event_clusters
                WHERE (? IS NULL OR platform = ?)
                """,
                (self._normalize_platform_filter(platform), self._normalize_platform_filter(platform)),
            ).fetchone()["count"]
            topic_cluster_count = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM topic_clusters
                WHERE (? IS NULL OR platform = ?)
                """,
                (self._normalize_platform_filter(platform), self._normalize_platform_filter(platform)),
            ).fetchone()["count"]
            date_window = conn.execute(
                f"""
                SELECT
                    MIN(
                        CASE
                            WHEN published_ts > 10000000000 THEN date(datetime(published_ts / 1000, 'unixepoch'))
                            WHEN published_ts > 0 THEN date(datetime(published_ts, 'unixepoch'))
                            ELSE NULL
                        END
                    ) AS start_date,
                    MAX(
                        CASE
                            WHEN published_ts > 10000000000 THEN date(datetime(published_ts / 1000, 'unixepoch'))
                            WHEN published_ts > 0 THEN date(datetime(published_ts, 'unixepoch'))
                            ELSE NULL
                        END
                    ) AS end_date
                FROM social_posts
                {where_clause}
                """,
                params,
            ).fetchone()

        start_date = date_window["start_date"] if date_window else None
        end_date = date_window["end_date"] if date_window else None
        date_window_payload: dict[str, Any] = {
            "start_date": start_date,
            "end_date": end_date,
            "days": 0,
        }
        if start_date and end_date:
            start_day = datetime.fromisoformat(start_date).date()
            end_day = datetime.fromisoformat(end_date).date()
            date_window_payload["days"] = max(0, (end_day - start_day).days + 1)

        return {
            "db_path": str(self.db_path),
            "platform": self._normalize_platform_filter(platform),
            "total_posts": total_posts,
            "source_file_count": source_file_count,
            "event_ready_count": event_ready_count,
            "event_cluster_count": event_cluster_count,
            "topic_cluster_count": topic_cluster_count,
            "engagement": dict(engagement),
            "platforms": [
                {
                    "platform": row["platform"],
                    "label": PLATFORM_LABELS.get(row["platform"], row["platform"]),
                    "count": row["count"],
                }
                for row in platforms
            ],
            "source_types": [dict(row) for row in source_types],
            "top_authors": [dict(row) for row in top_authors],
            "timeline": list(reversed([dict(row) for row in timeline])),
            "top_keywords": self.list_keyword_aggregates(platform=platform, limit=8)["items"],
            "date_window": date_window_payload,
            "recent_posts": [self._serialize_post_row(dict(row)) for row in recent_posts],
            "recent_syncs": [
                {
                    **dict(row),
                    "source_file": self._relative_source_path(row["source_file"]),
                }
                for row in recent_syncs
            ],
        }

    def list_posts(
        self,
        platform: Optional[str] = None,
        q: str = "",
        source_type: Optional[str] = None,
        author_name: str = "",
        limit: int = 30,
        offset: int = 0,
    ) -> dict[str, Any]:
        self.ensure_schema()
        safe_limit = max(1, min(limit, 100))
        safe_offset = max(offset, 0)

        where_clause, params = self._build_post_where_clause(
            platform=platform,
            q=q,
            source_type=source_type,
            author_name=author_name,
        )

        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) AS count FROM social_posts {where_clause}",
                params,
            ).fetchone()["count"]
            rows = conn.execute(
                f"""
                SELECT
                    platform,
                    source_type,
                    source_post_id,
                    author_id,
                    author_name,
                    title,
                    content,
                    clean_content,
                    analysis_content,
                    note_url,
                    published_at,
                    like_count,
                    comment_count,
                    share_count,
                    collect_count,
                    media_urls,
                    tags,
                    source_keyword,
                    source_keywords,
                    source_file
                FROM social_posts
                {where_clause}
                ORDER BY published_ts DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                [*params, safe_limit, safe_offset],
            ).fetchall()

        return {
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "items": [self._serialize_post_row(dict(row)) for row in rows],
        }

    def list_keyword_aggregates(
        self,
        platform: Optional[str] = "wb",
        limit: int = 20,
    ) -> dict[str, Any]:
        self.ensure_schema()
        safe_limit = max(1, min(limit, 100))
        where_clause, params = self._build_post_where_clause(platform=platform)

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT source_post_id, source_keywords, like_count, comment_count, share_count, published_at
                FROM social_posts
                {where_clause}
                """,
                params,
            ).fetchall()

        aggregates: dict[str, dict[str, Any]] = {}
        for row in rows:
            row_dict = dict(row)
            for keyword in self._json_to_list(row_dict.get("source_keywords")):
                keyword = self._clean_text(keyword)
                if not keyword:
                    continue
                bucket = aggregates.setdefault(
                    keyword,
                    {
                        "keyword": keyword,
                        "post_count": 0,
                        "like_total": 0,
                        "comment_total": 0,
                        "share_total": 0,
                        "latest_published_at": "",
                    },
                )
                bucket["post_count"] += 1
                bucket["like_total"] += int(row_dict.get("like_count") or 0)
                bucket["comment_total"] += int(row_dict.get("comment_count") or 0)
                bucket["share_total"] += int(row_dict.get("share_count") or 0)
                latest = str(row_dict.get("published_at") or "")
                if latest and latest > bucket["latest_published_at"]:
                    bucket["latest_published_at"] = latest

        items = sorted(
            aggregates.values(),
            key=lambda item: (-item["post_count"], -item["like_total"], item["keyword"]),
        )[:safe_limit]

        return {
            "platform": self._normalize_platform_filter(platform),
            "total": len(aggregates),
            "items": items,
        }

    def list_event_ready_posts(
        self,
        platform: Optional[str] = "wb",
        status: str = "ready",
        q: str = "",
        limit: int = 30,
        offset: int = 0,
    ) -> dict[str, Any]:
        self.ensure_schema()
        safe_limit = max(1, min(limit, 100))
        safe_offset = max(offset, 0)

        clauses = []
        params: list[Any] = []

        normalized_platform = self._normalize_platform_filter(platform)
        if normalized_platform:
            clauses.append("platform = ?")
            params.append(normalized_platform)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if q:
            clauses.append("(analysis_content LIKE ? OR clean_content LIKE ? OR author_name LIKE ?)")
            keyword = f"%{q}%"
            params.extend([keyword, keyword, keyword])

        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) AS count FROM event_ready_posts {where_clause}",
                params,
            ).fetchone()["count"]
            rows = conn.execute(
                f"""
                SELECT platform, source_post_id, author_name, published_at, note_url, clean_content, analysis_content,
                       hashtags, mentions, source_keywords, topic_seed_terms, relevance_flags, status, source_file
                FROM event_ready_posts
                {where_clause}
                ORDER BY published_ts DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                [*params, safe_limit, safe_offset],
            ).fetchall()

        items = []
        for row in rows:
            row_dict = dict(row)
            for key in ("hashtags", "mentions", "source_keywords", "topic_seed_terms"):
                row_dict[key] = self._json_to_list(row_dict.get(key))
            row_dict["relevance_flags"] = self._json_to_dict(row_dict.get("relevance_flags"))
            row_dict["platform_label"] = PLATFORM_LABELS.get(row_dict["platform"], row_dict["platform"])
            row_dict["source_file"] = self._relative_source_path(row_dict.get("source_file", ""))
            items.append(row_dict)

        return {
            "platform": normalized_platform,
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "items": items,
        }

    def extract_events(
        self,
        platform: Optional[str] = "wb",
        status: str = "ready",
        replace: bool = True,
    ) -> dict[str, Any]:
        self.ensure_schema()
        normalized_platform = self._normalize_platform_filter(platform)
        target_platform = normalized_platform or "wb"

        with self._connect() as conn:
            ready_rows = self._load_event_ready_rows(
                conn=conn,
                platform=target_platform,
                status=status,
            )
            outputs = self._build_event_extraction_outputs(ready_rows)
            extracted_at = self._now_iso()

            if replace:
                conn.execute(
                    "DELETE FROM event_extracted_posts WHERE platform = ?",
                    (target_platform,),
                )
                conn.execute(
                    "DELETE FROM event_clusters WHERE platform = ?",
                    (target_platform,),
                )
                conn.execute(
                    "DELETE FROM topic_clusters WHERE platform = ?",
                    (target_platform,),
                )

            event_post_rows = self._build_event_post_rows(outputs["posts"], extracted_at=extracted_at)
            if event_post_rows:
                conn.executemany(
                    """
                    INSERT INTO event_extracted_posts (
                        platform,
                        source_post_id,
                        author_name,
                        published_at,
                        published_ts,
                        note_url,
                        clean_content,
                        analysis_content,
                        hashtags,
                        mentions,
                        source_keywords,
                        topic_seed_terms,
                        relevance_flags,
                        source_file,
                        raw_json,
                        post_type,
                        raw_event_candidate,
                        canonical_event_name,
                        event_eligible,
                        event_promoted,
                        event_confidence,
                        event_geo_score,
                        event_leaf_name,
                        event_parent_name,
                        event_key,
                        event_family_key,
                        organizer_key,
                        organizer_name,
                        organizer_type,
                        organizer_confidence,
                        organizer_evidence,
                        primary_topic,
                        dashboard_category,
                        quality_weight,
                        engagement_total,
                        discussion_total,
                        comment_fetch_count,
                        comment_fetch_like_sum,
                        comment_fetch_sub_comment_sum,
                        comment_unique_authors,
                        top_comments,
                        post_heat,
                        base_engagement,
                        discussion_strength,
                        comment_value,
                        recency_factor,
                        raw_score,
                        extracted_at
                    ) VALUES (
                        :platform,
                        :source_post_id,
                        :author_name,
                        :published_at,
                        :published_ts,
                        :note_url,
                        :clean_content,
                        :analysis_content,
                        :hashtags,
                        :mentions,
                        :source_keywords,
                        :topic_seed_terms,
                        :relevance_flags,
                        :source_file,
                        :raw_json,
                        :post_type,
                        :raw_event_candidate,
                        :canonical_event_name,
                        :event_eligible,
                        :event_promoted,
                        :event_confidence,
                        :event_geo_score,
                        :event_leaf_name,
                        :event_parent_name,
                        :event_key,
                        :event_family_key,
                        :organizer_key,
                        :organizer_name,
                        :organizer_type,
                        :organizer_confidence,
                        :organizer_evidence,
                        :primary_topic,
                        :dashboard_category,
                        :quality_weight,
                        :engagement_total,
                        :discussion_total,
                        :comment_fetch_count,
                        :comment_fetch_like_sum,
                        :comment_fetch_sub_comment_sum,
                        :comment_unique_authors,
                        :top_comments,
                        :post_heat,
                        :base_engagement,
                        :discussion_strength,
                        :comment_value,
                        :recency_factor,
                        :raw_score,
                        :extracted_at
                    )
                    ON CONFLICT(platform, source_post_id) DO UPDATE SET
                        author_name = excluded.author_name,
                        published_at = excluded.published_at,
                        published_ts = excluded.published_ts,
                        note_url = excluded.note_url,
                        clean_content = excluded.clean_content,
                        analysis_content = excluded.analysis_content,
                        hashtags = excluded.hashtags,
                        mentions = excluded.mentions,
                        source_keywords = excluded.source_keywords,
                        topic_seed_terms = excluded.topic_seed_terms,
                        relevance_flags = excluded.relevance_flags,
                        source_file = excluded.source_file,
                        raw_json = excluded.raw_json,
                        post_type = excluded.post_type,
                        raw_event_candidate = excluded.raw_event_candidate,
                        canonical_event_name = excluded.canonical_event_name,
                        event_eligible = excluded.event_eligible,
                        event_promoted = excluded.event_promoted,
                        event_confidence = excluded.event_confidence,
                        event_geo_score = excluded.event_geo_score,
                        event_leaf_name = excluded.event_leaf_name,
                        event_parent_name = excluded.event_parent_name,
                        event_key = excluded.event_key,
                        event_family_key = excluded.event_family_key,
                        organizer_key = excluded.organizer_key,
                        organizer_name = excluded.organizer_name,
                        organizer_type = excluded.organizer_type,
                        organizer_confidence = excluded.organizer_confidence,
                        organizer_evidence = excluded.organizer_evidence,
                        primary_topic = excluded.primary_topic,
                        dashboard_category = excluded.dashboard_category,
                        quality_weight = excluded.quality_weight,
                        engagement_total = excluded.engagement_total,
                        discussion_total = excluded.discussion_total,
                        comment_fetch_count = excluded.comment_fetch_count,
                        comment_fetch_like_sum = excluded.comment_fetch_like_sum,
                        comment_fetch_sub_comment_sum = excluded.comment_fetch_sub_comment_sum,
                        comment_unique_authors = excluded.comment_unique_authors,
                        top_comments = excluded.top_comments,
                        post_heat = excluded.post_heat,
                        base_engagement = excluded.base_engagement,
                        discussion_strength = excluded.discussion_strength,
                        comment_value = excluded.comment_value,
                        recency_factor = excluded.recency_factor,
                        raw_score = excluded.raw_score,
                        extracted_at = excluded.extracted_at
                    """,
                    event_post_rows,
                )

            cluster_rows = self._build_event_cluster_rows(
                platform=target_platform,
                clusters=outputs["event_clusters"],
                extracted_at=extracted_at,
            )
            if cluster_rows:
                conn.executemany(
                    """
                    INSERT INTO event_clusters (
                        platform,
                        cluster_key,
                        cluster_type,
                        post_count,
                        unique_authors,
                        post_type_breakdown,
                        total_like_count,
                        total_comment_count,
                        total_share_count,
                        total_engagement,
                        discussion_total,
                        keywords,
                        top_posts,
                        top_comments,
                        organizer_key,
                        organizer_name,
                        organizer_type,
                        organizer_breakdown,
                        organizer_evidence,
                        dashboard_category,
                        dashboard_category_score,
                        engagement_component,
                        discussion_component,
                        diversity_component,
                        velocity_component,
                        heat_score,
                        extracted_at
                    ) VALUES (
                        :platform,
                        :cluster_key,
                        :cluster_type,
                        :post_count,
                        :unique_authors,
                        :post_type_breakdown,
                        :total_like_count,
                        :total_comment_count,
                        :total_share_count,
                        :total_engagement,
                        :discussion_total,
                        :keywords,
                        :top_posts,
                        :top_comments,
                        :organizer_key,
                        :organizer_name,
                        :organizer_type,
                        :organizer_breakdown,
                        :organizer_evidence,
                        :dashboard_category,
                        :dashboard_category_score,
                        :engagement_component,
                        :discussion_component,
                        :diversity_component,
                        :velocity_component,
                        :heat_score,
                        :extracted_at
                    )
                    ON CONFLICT(platform, cluster_key) DO UPDATE SET
                        cluster_type = excluded.cluster_type,
                        post_count = excluded.post_count,
                        unique_authors = excluded.unique_authors,
                        post_type_breakdown = excluded.post_type_breakdown,
                        total_like_count = excluded.total_like_count,
                        total_comment_count = excluded.total_comment_count,
                        total_share_count = excluded.total_share_count,
                        total_engagement = excluded.total_engagement,
                        discussion_total = excluded.discussion_total,
                        keywords = excluded.keywords,
                        top_posts = excluded.top_posts,
                        top_comments = excluded.top_comments,
                        organizer_key = excluded.organizer_key,
                        organizer_name = excluded.organizer_name,
                        organizer_type = excluded.organizer_type,
                        organizer_breakdown = excluded.organizer_breakdown,
                        organizer_evidence = excluded.organizer_evidence,
                        dashboard_category = excluded.dashboard_category,
                        dashboard_category_score = excluded.dashboard_category_score,
                        engagement_component = excluded.engagement_component,
                        discussion_component = excluded.discussion_component,
                        diversity_component = excluded.diversity_component,
                        velocity_component = excluded.velocity_component,
                        heat_score = excluded.heat_score,
                        extracted_at = excluded.extracted_at
                    """,
                    cluster_rows,
                )

            topic_cluster_rows = self._build_topic_cluster_rows(
                platform=target_platform,
                clusters=outputs["topic_clusters"],
                extracted_at=extracted_at,
            )
            if topic_cluster_rows:
                conn.executemany(
                    """
                    INSERT INTO topic_clusters (
                        platform,
                        cluster_key,
                        cluster_type,
                        post_count,
                        unique_authors,
                        post_type_breakdown,
                        total_like_count,
                        total_comment_count,
                        total_share_count,
                        total_engagement,
                        discussion_total,
                        keywords,
                        top_posts,
                        top_comments,
                        organizer_key,
                        organizer_name,
                        organizer_type,
                        organizer_breakdown,
                        organizer_evidence,
                        dashboard_category,
                        dashboard_category_score,
                        engagement_component,
                        discussion_component,
                        diversity_component,
                        velocity_component,
                        heat_score,
                        extracted_at
                    ) VALUES (
                        :platform,
                        :cluster_key,
                        :cluster_type,
                        :post_count,
                        :unique_authors,
                        :post_type_breakdown,
                        :total_like_count,
                        :total_comment_count,
                        :total_share_count,
                        :total_engagement,
                        :discussion_total,
                        :keywords,
                        :top_posts,
                        :top_comments,
                        :organizer_key,
                        :organizer_name,
                        :organizer_type,
                        :organizer_breakdown,
                        :organizer_evidence,
                        :dashboard_category,
                        :dashboard_category_score,
                        :engagement_component,
                        :discussion_component,
                        :diversity_component,
                        :velocity_component,
                        :heat_score,
                        :extracted_at
                    )
                    ON CONFLICT(platform, cluster_key) DO UPDATE SET
                        cluster_type = excluded.cluster_type,
                        post_count = excluded.post_count,
                        unique_authors = excluded.unique_authors,
                        post_type_breakdown = excluded.post_type_breakdown,
                        total_like_count = excluded.total_like_count,
                        total_comment_count = excluded.total_comment_count,
                        total_share_count = excluded.total_share_count,
                        total_engagement = excluded.total_engagement,
                        discussion_total = excluded.discussion_total,
                        keywords = excluded.keywords,
                        top_posts = excluded.top_posts,
                        top_comments = excluded.top_comments,
                        organizer_key = excluded.organizer_key,
                        organizer_name = excluded.organizer_name,
                        organizer_type = excluded.organizer_type,
                        organizer_breakdown = excluded.organizer_breakdown,
                        organizer_evidence = excluded.organizer_evidence,
                        dashboard_category = excluded.dashboard_category,
                        dashboard_category_score = excluded.dashboard_category_score,
                        engagement_component = excluded.engagement_component,
                        discussion_component = excluded.discussion_component,
                        diversity_component = excluded.diversity_component,
                        velocity_component = excluded.velocity_component,
                        heat_score = excluded.heat_score,
                        extracted_at = excluded.extracted_at
                    """,
                    topic_cluster_rows,
                )

        extracted_event_posts = [row for row in outputs["posts"] if row.get("event_promoted")]
        top_event_cluster = outputs["event_clusters"][0]["cluster_key"] if outputs["event_clusters"] else None
        top_topic_cluster = outputs["topic_clusters"][0]["cluster_key"] if outputs["topic_clusters"] else None
        return {
            "db_path": str(self.db_path),
            "platform": target_platform,
            "source_ready_posts": len(ready_rows),
            "extracted_post_rows": len(event_post_rows),
            "event_post_rows": len(extracted_event_posts),
            "event_cluster_rows": len(cluster_rows),
            "topic_cluster_rows": len(topic_cluster_rows),
            "top_event_cluster": top_event_cluster,
            "top_topic_cluster": top_topic_cluster,
            "extracted_at": extracted_at,
        }

    def extract_events_weekly(
        self,
        *,
        platform: str,
        week_start: str,
        week_end: str,
        status: str = "ready",
        replace: bool = True,
    ) -> dict[str, Any]:
        self.ensure_schema()
        normalized_platform = self._normalize_platform_filter(platform)
        if not normalized_platform:
            raise ValueError("platform is required for weekly analysis.")
        self._validate_week_window(week_start=week_start, week_end=week_end)

        with self._connect() as conn:
            ready_rows = self._load_event_ready_rows(
                conn=conn,
                platform=normalized_platform,
                status=status,
                week_start=week_start,
                week_end=week_end,
            )
            outputs = self._build_event_extraction_outputs(ready_rows)
            extracted_at = self._now_iso()

            if replace:
                conn.execute(
                    """
                    DELETE FROM weekly_event_extracted_posts
                    WHERE platform = ? AND week_start = ? AND week_end = ?
                    """,
                    (normalized_platform, week_start, week_end),
                )
                conn.execute(
                    """
                    DELETE FROM weekly_event_clusters
                    WHERE platform = ? AND week_start = ? AND week_end = ?
                    """,
                    (normalized_platform, week_start, week_end),
                )
                conn.execute(
                    """
                    DELETE FROM weekly_topic_clusters
                    WHERE platform = ? AND week_start = ? AND week_end = ?
                    """,
                    (normalized_platform, week_start, week_end),
                )

            event_post_rows = self._build_event_post_rows(outputs["posts"], extracted_at=extracted_at)
            weekly_event_post_rows = self._build_weekly_event_post_rows(
                event_post_rows,
                week_start=week_start,
                week_end=week_end,
            )
            if weekly_event_post_rows:
                conn.executemany(
                    """
                    INSERT INTO weekly_event_extracted_posts (
                        platform, week_start, week_end, source_post_id, author_name, published_at, published_ts,
                        note_url, clean_content, analysis_content, hashtags, mentions, source_keywords,
                        topic_seed_terms, relevance_flags, source_file, raw_json, post_type, raw_event_candidate,
                        canonical_event_name, event_eligible, event_promoted, event_confidence, event_geo_score,
                        event_leaf_name, event_parent_name, event_key, event_family_key, organizer_key,
                        organizer_name, organizer_type, organizer_confidence, organizer_evidence, primary_topic,
                        dashboard_category, quality_weight, engagement_total, discussion_total, comment_fetch_count,
                        comment_fetch_like_sum, comment_fetch_sub_comment_sum, comment_unique_authors, top_comments,
                        post_heat, base_engagement, discussion_strength, comment_value, recency_factor, raw_score,
                        extracted_at
                    ) VALUES (
                        :platform, :week_start, :week_end, :source_post_id, :author_name, :published_at, :published_ts,
                        :note_url, :clean_content, :analysis_content, :hashtags, :mentions, :source_keywords,
                        :topic_seed_terms, :relevance_flags, :source_file, :raw_json, :post_type, :raw_event_candidate,
                        :canonical_event_name, :event_eligible, :event_promoted, :event_confidence, :event_geo_score,
                        :event_leaf_name, :event_parent_name, :event_key, :event_family_key, :organizer_key,
                        :organizer_name, :organizer_type, :organizer_confidence, :organizer_evidence, :primary_topic,
                        :dashboard_category, :quality_weight, :engagement_total, :discussion_total, :comment_fetch_count,
                        :comment_fetch_like_sum, :comment_fetch_sub_comment_sum, :comment_unique_authors, :top_comments,
                        :post_heat, :base_engagement, :discussion_strength, :comment_value, :recency_factor, :raw_score,
                        :extracted_at
                    )
                    """,
                    weekly_event_post_rows,
                )

            event_cluster_rows = self._build_event_cluster_rows(
                platform=normalized_platform,
                clusters=outputs["event_clusters"],
                extracted_at=extracted_at,
            )
            weekly_event_cluster_rows = self._build_weekly_cluster_rows(
                event_cluster_rows,
                week_start=week_start,
                week_end=week_end,
            )
            if weekly_event_cluster_rows:
                conn.executemany(
                    """
                    INSERT INTO weekly_event_clusters (
                        platform, week_start, week_end, cluster_key, cluster_type, post_count, unique_authors,
                        post_type_breakdown, total_like_count, total_comment_count, total_share_count,
                        total_engagement, discussion_total, keywords, top_posts, top_comments, organizer_key,
                        organizer_name, organizer_type, organizer_breakdown, organizer_evidence, dashboard_category,
                        dashboard_category_score, engagement_component, discussion_component, diversity_component,
                        velocity_component, heat_score, extracted_at
                    ) VALUES (
                        :platform, :week_start, :week_end, :cluster_key, :cluster_type, :post_count, :unique_authors,
                        :post_type_breakdown, :total_like_count, :total_comment_count, :total_share_count,
                        :total_engagement, :discussion_total, :keywords, :top_posts, :top_comments, :organizer_key,
                        :organizer_name, :organizer_type, :organizer_breakdown, :organizer_evidence, :dashboard_category,
                        :dashboard_category_score, :engagement_component, :discussion_component, :diversity_component,
                        :velocity_component, :heat_score, :extracted_at
                    )
                    """,
                    weekly_event_cluster_rows,
                )

            topic_cluster_rows = self._build_topic_cluster_rows(
                platform=normalized_platform,
                clusters=outputs["topic_clusters"],
                extracted_at=extracted_at,
            )
            weekly_topic_cluster_rows = self._build_weekly_cluster_rows(
                topic_cluster_rows,
                week_start=week_start,
                week_end=week_end,
            )
            if weekly_topic_cluster_rows:
                conn.executemany(
                    """
                    INSERT INTO weekly_topic_clusters (
                        platform, week_start, week_end, cluster_key, cluster_type, post_count, unique_authors,
                        post_type_breakdown, total_like_count, total_comment_count, total_share_count,
                        total_engagement, discussion_total, keywords, top_posts, top_comments, organizer_key,
                        organizer_name, organizer_type, organizer_breakdown, organizer_evidence, dashboard_category,
                        dashboard_category_score, engagement_component, discussion_component, diversity_component,
                        velocity_component, heat_score, extracted_at
                    ) VALUES (
                        :platform, :week_start, :week_end, :cluster_key, :cluster_type, :post_count, :unique_authors,
                        :post_type_breakdown, :total_like_count, :total_comment_count, :total_share_count,
                        :total_engagement, :discussion_total, :keywords, :top_posts, :top_comments, :organizer_key,
                        :organizer_name, :organizer_type, :organizer_breakdown, :organizer_evidence, :dashboard_category,
                        :dashboard_category_score, :engagement_component, :discussion_component, :diversity_component,
                        :velocity_component, :heat_score, :extracted_at
                    )
                    """,
                    weekly_topic_cluster_rows,
                )

            conn.execute(
                """
                INSERT INTO analysis_windows (
                    platform, week_start, week_end, status, source_ready_posts, extracted_post_rows,
                    event_cluster_rows, topic_cluster_rows, extracted_at, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, week_start, week_end) DO UPDATE SET
                    status = excluded.status,
                    source_ready_posts = excluded.source_ready_posts,
                    extracted_post_rows = excluded.extracted_post_rows,
                    event_cluster_rows = excluded.event_cluster_rows,
                    topic_cluster_rows = excluded.topic_cluster_rows,
                    extracted_at = excluded.extracted_at,
                    note = excluded.note
                """,
                (
                    normalized_platform,
                    week_start,
                    week_end,
                    "completed",
                    len(ready_rows),
                    len(weekly_event_post_rows),
                    len(weekly_event_cluster_rows),
                    len(weekly_topic_cluster_rows),
                    extracted_at,
                    "Weekly analysis completed.",
                ),
            )

        return {
            "db_path": str(self.db_path),
            "platform": normalized_platform,
            "week_start": week_start,
            "week_end": week_end,
            "source_ready_posts": len(ready_rows),
            "extracted_post_rows": len(weekly_event_post_rows),
            "event_cluster_rows": len(weekly_event_cluster_rows),
            "topic_cluster_rows": len(weekly_topic_cluster_rows),
            "extracted_at": extracted_at,
        }

    def extract_events_monthly(
        self,
        *,
        platform: str,
        month_key: str,
        status: str = "ready",
        replace: bool = True,
    ) -> dict[str, Any]:
        self.ensure_schema()
        normalized_platform = self._normalize_platform_filter(platform)
        if not normalized_platform:
            raise ValueError("platform is required for monthly analysis.")
        month_start, month_end = self._resolve_month_window(month_key)

        with self._connect() as conn:
            ready_rows = self._load_event_ready_rows(
                conn=conn,
                platform=normalized_platform,
                status=status,
                week_start=month_start,
                week_end=month_end,
            )
            outputs = self._build_event_extraction_outputs(ready_rows)
            extracted_at = self._now_iso()

            if replace:
                conn.execute(
                    """
                    DELETE FROM monthly_event_extracted_posts
                    WHERE platform = ? AND month_key = ?
                    """,
                    (normalized_platform, month_key),
                )
                conn.execute(
                    """
                    DELETE FROM monthly_event_clusters
                    WHERE platform = ? AND month_key = ?
                    """,
                    (normalized_platform, month_key),
                )
                conn.execute(
                    """
                    DELETE FROM monthly_topic_clusters
                    WHERE platform = ? AND month_key = ?
                    """,
                    (normalized_platform, month_key),
                )

            event_post_rows = self._build_event_post_rows(outputs["posts"], extracted_at=extracted_at)
            monthly_event_post_rows = self._build_monthly_event_post_rows(
                event_post_rows,
                month_key=month_key,
                month_start=month_start,
                month_end=month_end,
            )
            if monthly_event_post_rows:
                conn.executemany(
                    """
                    INSERT INTO monthly_event_extracted_posts (
                        platform, month_key, month_start, month_end, source_post_id, author_name, published_at, published_ts,
                        note_url, clean_content, analysis_content, hashtags, mentions, source_keywords,
                        topic_seed_terms, relevance_flags, source_file, raw_json, post_type, raw_event_candidate,
                        canonical_event_name, event_eligible, event_promoted, event_confidence, event_geo_score,
                        event_leaf_name, event_parent_name, event_key, event_family_key, organizer_key,
                        organizer_name, organizer_type, organizer_confidence, organizer_evidence, primary_topic,
                        dashboard_category, quality_weight, engagement_total, discussion_total, comment_fetch_count,
                        comment_fetch_like_sum, comment_fetch_sub_comment_sum, comment_unique_authors, top_comments,
                        post_heat, base_engagement, discussion_strength, comment_value, recency_factor, raw_score,
                        extracted_at
                    ) VALUES (
                        :platform, :month_key, :month_start, :month_end, :source_post_id, :author_name, :published_at, :published_ts,
                        :note_url, :clean_content, :analysis_content, :hashtags, :mentions, :source_keywords,
                        :topic_seed_terms, :relevance_flags, :source_file, :raw_json, :post_type, :raw_event_candidate,
                        :canonical_event_name, :event_eligible, :event_promoted, :event_confidence, :event_geo_score,
                        :event_leaf_name, :event_parent_name, :event_key, :event_family_key, :organizer_key,
                        :organizer_name, :organizer_type, :organizer_confidence, :organizer_evidence, :primary_topic,
                        :dashboard_category, :quality_weight, :engagement_total, :discussion_total, :comment_fetch_count,
                        :comment_fetch_like_sum, :comment_fetch_sub_comment_sum, :comment_unique_authors, :top_comments,
                        :post_heat, :base_engagement, :discussion_strength, :comment_value, :recency_factor, :raw_score,
                        :extracted_at
                    )
                    """,
                    monthly_event_post_rows,
                )

            event_cluster_rows = self._build_event_cluster_rows(
                platform=normalized_platform,
                clusters=outputs["event_clusters"],
                extracted_at=extracted_at,
            )
            monthly_event_cluster_rows = self._build_monthly_cluster_rows(
                event_cluster_rows,
                month_key=month_key,
                month_start=month_start,
                month_end=month_end,
            )
            if monthly_event_cluster_rows:
                conn.executemany(
                    """
                    INSERT INTO monthly_event_clusters (
                        platform, month_key, month_start, month_end, cluster_key, cluster_type, post_count, unique_authors,
                        post_type_breakdown, total_like_count, total_comment_count, total_share_count,
                        total_engagement, discussion_total, keywords, top_posts, top_comments, organizer_key,
                        organizer_name, organizer_type, organizer_breakdown, organizer_evidence, dashboard_category,
                        dashboard_category_score, engagement_component, discussion_component, diversity_component,
                        velocity_component, heat_score, extracted_at
                    ) VALUES (
                        :platform, :month_key, :month_start, :month_end, :cluster_key, :cluster_type, :post_count, :unique_authors,
                        :post_type_breakdown, :total_like_count, :total_comment_count, :total_share_count,
                        :total_engagement, :discussion_total, :keywords, :top_posts, :top_comments, :organizer_key,
                        :organizer_name, :organizer_type, :organizer_breakdown, :organizer_evidence, :dashboard_category,
                        :dashboard_category_score, :engagement_component, :discussion_component, :diversity_component,
                        :velocity_component, :heat_score, :extracted_at
                    )
                    """,
                    monthly_event_cluster_rows,
                )

            topic_cluster_rows = self._build_topic_cluster_rows(
                platform=normalized_platform,
                clusters=outputs["topic_clusters"],
                extracted_at=extracted_at,
            )
            monthly_topic_cluster_rows = self._build_monthly_cluster_rows(
                topic_cluster_rows,
                month_key=month_key,
                month_start=month_start,
                month_end=month_end,
            )
            if monthly_topic_cluster_rows:
                conn.executemany(
                    """
                    INSERT INTO monthly_topic_clusters (
                        platform, month_key, month_start, month_end, cluster_key, cluster_type, post_count, unique_authors,
                        post_type_breakdown, total_like_count, total_comment_count, total_share_count,
                        total_engagement, discussion_total, keywords, top_posts, top_comments, organizer_key,
                        organizer_name, organizer_type, organizer_breakdown, organizer_evidence, dashboard_category,
                        dashboard_category_score, engagement_component, discussion_component, diversity_component,
                        velocity_component, heat_score, extracted_at
                    ) VALUES (
                        :platform, :month_key, :month_start, :month_end, :cluster_key, :cluster_type, :post_count, :unique_authors,
                        :post_type_breakdown, :total_like_count, :total_comment_count, :total_share_count,
                        :total_engagement, :discussion_total, :keywords, :top_posts, :top_comments, :organizer_key,
                        :organizer_name, :organizer_type, :organizer_breakdown, :organizer_evidence, :dashboard_category,
                        :dashboard_category_score, :engagement_component, :discussion_component, :diversity_component,
                        :velocity_component, :heat_score, :extracted_at
                    )
                    """,
                    monthly_topic_cluster_rows,
                )

            conn.execute(
                """
                INSERT INTO analysis_months (
                    platform, month_key, month_start, month_end, status, source_ready_posts, extracted_post_rows,
                    event_cluster_rows, topic_cluster_rows, extracted_at, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, month_key) DO UPDATE SET
                    month_start = excluded.month_start,
                    month_end = excluded.month_end,
                    status = excluded.status,
                    source_ready_posts = excluded.source_ready_posts,
                    extracted_post_rows = excluded.extracted_post_rows,
                    event_cluster_rows = excluded.event_cluster_rows,
                    topic_cluster_rows = excluded.topic_cluster_rows,
                    extracted_at = excluded.extracted_at,
                    note = excluded.note
                """,
                (
                    normalized_platform,
                    month_key,
                    month_start,
                    month_end,
                    "completed",
                    len(ready_rows),
                    len(monthly_event_post_rows),
                    len(monthly_event_cluster_rows),
                    len(monthly_topic_cluster_rows),
                    extracted_at,
                    "Monthly analysis completed.",
                ),
            )

        return {
            "db_path": str(self.db_path),
            "platform": normalized_platform,
            "month_key": month_key,
            "month_start": month_start,
            "month_end": month_end,
            "source_ready_posts": len(ready_rows),
            "extracted_post_rows": len(monthly_event_post_rows),
            "event_cluster_rows": len(monthly_event_cluster_rows),
            "topic_cluster_rows": len(monthly_topic_cluster_rows),
            "extracted_at": extracted_at,
        }

    def list_analysis_windows(self, platform: str, *, weeks: int = 12, window_mode: str = "weekly") -> dict[str, Any]:
        self.ensure_schema()
        normalized_platform = self._normalize_platform_filter(platform)
        if not normalized_platform:
            raise ValueError("platform is required.")

        normalized_window_mode = self._normalize_window_mode(window_mode)
        if normalized_window_mode == "monthly":
            return self._list_analysis_months(platform=normalized_platform, months=weeks)
        if normalized_window_mode == "quarterly":
            return self._list_analysis_quarters(platform=normalized_platform, quarters=weeks)

        safe_weeks = max(1, min(int(weeks), 52))
        with self._connect() as conn:
            max_day_row = conn.execute(
                """
                SELECT MAX(substr(published_at, 1, 10)) AS end_date
                FROM social_posts
                WHERE platform = ?
                """,
                (normalized_platform,),
            ).fetchone()
            min_day_row = conn.execute(
                """
                SELECT MIN(substr(published_at, 1, 10)) AS start_date
                FROM social_posts
                WHERE platform = ?
                """,
                (normalized_platform,),
            ).fetchone()
            recorded_rows = conn.execute(
                """
                SELECT platform, week_start, week_end, status, source_ready_posts, extracted_post_rows,
                       event_cluster_rows, topic_cluster_rows, extracted_at, note
                FROM analysis_windows
                WHERE platform = ?
                ORDER BY week_start DESC
                """,
                (normalized_platform,),
            ).fetchall()

        max_day = max_day_row["end_date"] if max_day_row else None
        min_day = min_day_row["start_date"] if min_day_row else None
        if not max_day:
            return {"platform": normalized_platform, "items": [], "weeks": safe_weeks}

        latest_day = datetime.fromisoformat(max_day).date()
        days_since_saturday = (latest_day.weekday() - 5) % 7
        latest_completed_saturday = latest_day - timedelta(days=days_since_saturday)
        current_week_start = latest_completed_saturday - timedelta(days=6)
        recorded = {
            (row["week_start"], row["week_end"]): dict(row)
            for row in recorded_rows
        }

        earliest_day = datetime.fromisoformat(min_day).date() if min_day else current_week_start
        if earliest_day < FULL_WEB_ANALYSIS_FLOOR_DATE:
            earliest_day = FULL_WEB_ANALYSIS_FLOOR_DATE
        items: list[dict[str, Any]] = []
        for offset in range(safe_weeks):
            week_start_date = current_week_start - timedelta(days=7 * offset)
            week_end_date = week_start_date + timedelta(days=6)
            if week_end_date < earliest_day:
                break
            week_start_value = week_start_date.isoformat()
            week_end_value = week_end_date.isoformat()
            row = recorded.get((week_start_value, week_end_value))
            post_count = self._count_posts_in_window(
                platform=normalized_platform,
                week_start=week_start_value,
                week_end=week_end_value,
            )
            inferred_extracted_count = self._count_extracted_posts_in_window(
                platform=normalized_platform,
                week_start=week_start_value,
                week_end=week_end_value,
            )
            inferred_event_cluster_rows = self._count_event_clusters_in_window(
                platform=normalized_platform,
                week_start=week_start_value,
                week_end=week_end_value,
            )
            inferred_topic_cluster_rows = self._count_topic_clusters_in_window(
                platform=normalized_platform,
                week_start=week_start_value,
                week_end=week_end_value,
            )
            has_completed_snapshot = bool(row) or inferred_extracted_count > 0
            status_value = "completed" if has_completed_snapshot else ("to_be_analyzed" if post_count > 0 else "to_be_updated")
            items.append(
                {
                    "platform": normalized_platform,
                    "week_start": week_start_value,
                    "week_end": week_end_value,
                    "status": status_value,
                    "post_count": post_count,
                    "source_ready_posts": int((row or {}).get("source_ready_posts", 0) or 0),
                    "extracted_post_rows": int((row or {}).get("extracted_post_rows", inferred_extracted_count) or 0),
                    "event_cluster_rows": int((row or {}).get("event_cluster_rows", inferred_event_cluster_rows) or 0),
                    "topic_cluster_rows": int((row or {}).get("topic_cluster_rows", inferred_topic_cluster_rows) or 0),
                    "extracted_at": (row or {}).get("extracted_at", self._latest_extracted_at_in_window(
                        platform=normalized_platform,
                        week_start=week_start_value,
                        week_end=week_end_value,
                    )),
                    "note": (row or {}).get(
                        "note",
                        "Inferred from existing extracted posts." if inferred_extracted_count > 0 else "",
                    ),
                }
            )

        return {
            "platform": normalized_platform,
            "window_mode": normalized_window_mode,
            "items": items,
            "weeks": safe_weeks,
        }

    def submit_cluster_feedback(
        self,
        *,
        platform: str,
        board_type: str,
        action: str,
        source_cluster_key: str,
        target_cluster_key: str = "",
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
        quarter_key: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        self.ensure_schema()
        normalized_platform = self._normalize_platform_filter(platform)
        if not normalized_platform:
            raise ValueError("platform is required.")
        normalized_board_type = str(board_type or "event").strip().lower()
        if normalized_board_type not in {"event", "topic"}:
            raise ValueError(f"Unsupported board_type: {board_type}")
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in {"noise", "merge"}:
            raise ValueError(f"Unsupported feedback action: {action}")
        normalized_source = str(source_cluster_key or "").strip()
        normalized_target = str(target_cluster_key or "").strip()
        if not normalized_source:
            raise ValueError("source_cluster_key is required.")
        if normalized_action == "merge":
            if not normalized_target:
                raise ValueError("target_cluster_key is required for merge.")
            if normalized_target == normalized_source:
                raise ValueError("target_cluster_key must differ from source_cluster_key.")
        scope_type = "global"
        if quarter_key:
            scope_type = "quarterly"
        elif month_key:
            scope_type = "monthly"
        elif week_start or week_end:
            self._validate_week_window(week_start=week_start, week_end=week_end)
            scope_type = "weekly"
        created_at = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cluster_feedback (
                    platform, board_type, scope_type, week_start, week_end, month_key, quarter_key,
                    source_cluster_key, action, target_cluster_key, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_platform,
                    normalized_board_type,
                    scope_type,
                    week_start,
                    week_end,
                    month_key,
                    quarter_key,
                    normalized_source,
                    normalized_action,
                    normalized_target,
                    str(note or "").strip(),
                    created_at,
                ),
            )
        return {
            "platform": normalized_platform,
            "board_type": normalized_board_type,
            "scope_type": scope_type,
            "source_cluster_key": normalized_source,
            "action": normalized_action,
            "target_cluster_key": normalized_target,
            "created_at": created_at,
        }

    def list_event_clusters(
        self,
        platform: Optional[str] = "wb",
        q: str = "",
        dashboard_category: str = "",
        limit: int = 30,
        offset: int = 0,
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
    ) -> dict[str, Any]:
        self.ensure_schema()
        safe_limit = max(1, min(limit, 100))
        safe_offset = max(offset, 0)
        weekly_mode = bool(week_start or week_end)
        monthly_mode = bool(month_key)
        if weekly_mode and monthly_mode:
            raise ValueError("Use either a weekly window or month_key, not both.")
        if weekly_mode:
            self._validate_week_window(week_start=week_start, week_end=week_end)
        if monthly_mode:
            month_key = self._validate_month_key(month_key)

        clauses = []
        params: list[Any] = []
        normalized_platform = self._normalize_platform_filter(platform)
        if weekly_mode and normalized_platform:
            self._ensure_weekly_snapshot_materialized(
                platform=normalized_platform,
                week_start=week_start,
                week_end=week_end,
            )
        if monthly_mode and normalized_platform:
            self._ensure_monthly_snapshot_materialized(
                platform=normalized_platform,
                month_key=month_key,
            )
        if normalized_platform:
            clauses.append("platform = ?")
            params.append(normalized_platform)
        normalized_dashboard_category = self._normalize_dashboard_category_filter(dashboard_category)
        if normalized_dashboard_category:
            clauses.append("dashboard_category = ?")
            params.append(normalized_dashboard_category)
        if q:
            clauses.append("(cluster_key LIKE ? OR keywords LIKE ? OR organizer_name LIKE ?)")
            keyword = f"%{q}%"
            params.extend([keyword, keyword, keyword])
        if weekly_mode:
            clauses.append("week_start = ?")
            clauses.append("week_end = ?")
            params.extend([week_start, week_end])
        if monthly_mode:
            clauses.append("month_key = ?")
            params.append(month_key)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        table_name = "event_clusters"
        if weekly_mode:
            table_name = "weekly_event_clusters"
        elif monthly_mode:
            table_name = "monthly_event_clusters"

        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table_name} {where_clause}",
                params,
            ).fetchone()["count"]
            rows = conn.execute(
                f"""
                SELECT platform, cluster_key, cluster_type, post_count, unique_authors, post_type_breakdown,
                       total_like_count, total_comment_count, total_share_count, total_engagement, discussion_total,
                       keywords, top_posts, top_comments, organizer_key, organizer_name, organizer_type,
                       organizer_breakdown, organizer_evidence, dashboard_category, dashboard_category_score,
                       engagement_component, discussion_component,
                       diversity_component, velocity_component, heat_score, extracted_at
                FROM {table_name}
                {where_clause}
                ORDER BY heat_score DESC, cluster_key ASC
                LIMIT ? OFFSET ?
                """,
                [*params, safe_limit, safe_offset],
            ).fetchall()

        items = []
        for row in rows:
            row_dict = dict(row)
            for key in ("keywords", "top_posts", "top_comments", "organizer_evidence"):
                row_dict[key] = self._json_to_list_of_any(row_dict.get(key))
            for key in ("post_type_breakdown", "organizer_breakdown"):
                row_dict[key] = self._json_to_dict(row_dict.get(key))
            row_dict["platform_label"] = PLATFORM_LABELS.get(row_dict["platform"], row_dict["platform"])
            items.append(row_dict)

        items = self._apply_cluster_feedback(
            items,
            platform=normalized_platform or "",
            board_type="event",
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
        )

        return {
            "platform": normalized_platform,
            "dashboard_category": normalized_dashboard_category,
            "week_start": week_start,
            "week_end": week_end,
            "month_key": month_key,
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "items": items,
        }

    def list_topic_clusters(
        self,
        platform: Optional[str] = "wb",
        q: str = "",
        dashboard_category: str = "",
        limit: int = 30,
        offset: int = 0,
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
    ) -> dict[str, Any]:
        self.ensure_schema()
        safe_limit = max(1, min(limit, 100))
        safe_offset = max(offset, 0)
        weekly_mode = bool(week_start or week_end)
        monthly_mode = bool(month_key)
        if weekly_mode and monthly_mode:
            raise ValueError("Use either a weekly window or month_key, not both.")
        if weekly_mode:
            self._validate_week_window(week_start=week_start, week_end=week_end)
        if monthly_mode:
            month_key = self._validate_month_key(month_key)

        clauses = []
        params: list[Any] = []
        normalized_platform = self._normalize_platform_filter(platform)
        if weekly_mode and normalized_platform:
            self._ensure_weekly_snapshot_materialized(
                platform=normalized_platform,
                week_start=week_start,
                week_end=week_end,
            )
        if monthly_mode and normalized_platform:
            self._ensure_monthly_snapshot_materialized(
                platform=normalized_platform,
                month_key=month_key,
            )
        if normalized_platform:
            clauses.append("platform = ?")
            params.append(normalized_platform)
        normalized_dashboard_category = self._normalize_dashboard_category_filter(dashboard_category)
        if normalized_dashboard_category:
            clauses.append("dashboard_category = ?")
            params.append(normalized_dashboard_category)
        if q:
            clauses.append("(cluster_key LIKE ? OR keywords LIKE ? OR organizer_name LIKE ?)")
            keyword = f"%{q}%"
            params.extend([keyword, keyword, keyword])
        if weekly_mode:
            clauses.append("week_start = ?")
            clauses.append("week_end = ?")
            params.extend([week_start, week_end])
        if monthly_mode:
            clauses.append("month_key = ?")
            params.append(month_key)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        table_name = "topic_clusters"
        if weekly_mode:
            table_name = "weekly_topic_clusters"
        elif monthly_mode:
            table_name = "monthly_topic_clusters"

        with self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table_name} {where_clause}",
                params,
            ).fetchone()["count"]
            rows = conn.execute(
                f"""
                SELECT platform, cluster_key, cluster_type, post_count, unique_authors, post_type_breakdown,
                       total_like_count, total_comment_count, total_share_count, total_engagement, discussion_total,
                       keywords, top_posts, top_comments, organizer_key, organizer_name, organizer_type,
                       organizer_breakdown, organizer_evidence, dashboard_category, dashboard_category_score,
                       engagement_component, discussion_component,
                       diversity_component, velocity_component, heat_score, extracted_at
                FROM {table_name}
                {where_clause}
                ORDER BY heat_score DESC, cluster_key ASC
                LIMIT ? OFFSET ?
                """,
                [*params, safe_limit, safe_offset],
            ).fetchall()

        items = []
        for row in rows:
            row_dict = dict(row)
            for key in ("keywords", "top_posts", "top_comments", "organizer_evidence"):
                row_dict[key] = self._json_to_list_of_any(row_dict.get(key))
            for key in ("post_type_breakdown", "organizer_breakdown"):
                row_dict[key] = self._json_to_dict(row_dict.get(key))
            row_dict["platform_label"] = PLATFORM_LABELS.get(row_dict["platform"], row_dict["platform"])
            items.append(row_dict)

        items = self._apply_cluster_feedback(
            items,
            platform=normalized_platform or "",
            board_type="topic",
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
        )

        return {
            "platform": normalized_platform,
            "dashboard_category": normalized_dashboard_category,
            "week_start": week_start,
            "week_end": week_end,
            "month_key": month_key,
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "items": items,
        }

    def get_event_discussion_trend(
        self,
        platform: Optional[str] = "wb",
        event_family_key: str = "",
        days: int = 7,
        start_date: str = "",
        end_date: str = "",
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
    ) -> dict[str, Any]:
        self.ensure_schema()
        normalized_platform = self._normalize_platform_filter(platform) or "wb"
        normalized_event_family_key = str(event_family_key or "").strip()
        if not normalized_event_family_key:
            raise ValueError("event_family_key is required.")
        weekly_mode = bool(week_start or week_end)
        monthly_mode = bool(month_key)
        if weekly_mode and monthly_mode:
            raise ValueError("Use either a weekly window or month_key, not both.")
        if weekly_mode:
            self._validate_week_window(week_start=week_start, week_end=week_end)
            start_date = week_start
            end_date = week_end
            self._ensure_weekly_snapshot_materialized(
                platform=normalized_platform,
                week_start=week_start,
                week_end=week_end,
            )
        if monthly_mode:
            month_key = self._validate_month_key(month_key)
            month_start, month_end = self._resolve_month_window(month_key)
            start_date = month_start
            end_date = month_end
            self._ensure_monthly_snapshot_materialized(
                platform=normalized_platform,
                month_key=month_key,
            )
        canonical_cluster_key, aliases = self._resolve_feedback_cluster_selection(
            platform=normalized_platform,
            board_type="event",
            selected_cluster_key=normalized_event_family_key,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
        )

        local_tz = datetime.now().astimezone().tzinfo or timezone.utc
        parsed_start_day = datetime.fromisoformat(start_date).date() if start_date else None
        parsed_end_day = datetime.fromisoformat(end_date).date() if end_date else None
        if parsed_start_day and parsed_end_day and parsed_start_day > parsed_end_day:
            raise ValueError("start_date must be earlier than or equal to end_date.")

        if parsed_start_day or parsed_end_day:
            if parsed_start_day is None and parsed_end_day is not None:
                safe_days = max(1, min(days, 90))
                parsed_start_day = parsed_end_day - timedelta(days=safe_days - 1)
            elif parsed_end_day is None and parsed_start_day is not None:
                safe_days = max(1, min(days, 90))
                parsed_end_day = parsed_start_day + timedelta(days=safe_days - 1)
            start_day = parsed_start_day or parsed_end_day or datetime.now(local_tz).date()
            end_day = parsed_end_day or start_day
            safe_days = max(1, min((end_day - start_day).days + 1, 90))
            end_day = start_day + timedelta(days=safe_days - 1)
        else:
            safe_days = max(3, min(days, 30))
            end_day = datetime.now(local_tz).date()
            start_day = end_day - timedelta(days=safe_days - 1)
        day_buckets = {
            start_day + timedelta(days=index): {
                "date": (start_day + timedelta(days=index)).isoformat(),
                "post_count": 0,
                "discussion_total": 0,
                "engagement_total": 0,
                "unique_authors": 0,
                "velocity": 0,
            }
            for index in range(safe_days)
        }
        author_buckets: dict[datetime.date, set[str]] = {
            start_day + timedelta(days=index): set()
            for index in range(safe_days)
        }
        latest_monitored_day: datetime.date | None = None

        with self._connect() as conn:
            summary_row = self._aggregate_cluster_summary_rows([], canonical_cluster_key)
            rows: list[sqlite3.Row] = []
            if aliases:
                alias_placeholders = ", ".join(["?"] * len(aliases))
                cluster_filters = []
                cluster_params: list[Any] = [normalized_platform, *aliases]
                if weekly_mode:
                    cluster_filters.extend(["week_start = ?", "week_end = ?"])
                    cluster_params.extend([week_start, week_end])
                elif monthly_mode:
                    cluster_filters.append("month_key = ?")
                    cluster_params.append(month_key)
                cluster_where = " AND " + " AND ".join(cluster_filters) if cluster_filters else ""
                summary_rows = conn.execute(
                    f"""
                    SELECT cluster_key, dashboard_category, heat_score, total_engagement, discussion_total, post_count, unique_authors
                    FROM {"weekly_event_clusters" if weekly_mode else "monthly_event_clusters" if monthly_mode else "event_clusters"}
                    WHERE platform = ? AND cluster_key IN ({alias_placeholders}){cluster_where}
                    """,
                    cluster_params,
                ).fetchall()
                summary_row = self._aggregate_cluster_summary_rows(summary_rows, canonical_cluster_key)

                extracted_filters = []
                extracted_params: list[Any] = [normalized_platform, *aliases, *aliases]
                if weekly_mode:
                    extracted_filters.extend(["week_start = ?", "week_end = ?"])
                    extracted_params.extend([week_start, week_end])
                elif monthly_mode:
                    extracted_filters.append("month_key = ?")
                    extracted_params.append(month_key)
                extracted_where = " AND " + " AND ".join(extracted_filters) if extracted_filters else ""
                rows = conn.execute(
                    f"""
                    SELECT published_ts, discussion_total, engagement_total, author_name
                    FROM {"weekly_event_extracted_posts" if weekly_mode else "monthly_event_extracted_posts" if monthly_mode else "event_extracted_posts"}
                    WHERE platform = ?
                      AND event_promoted = 1
                      AND (
                        event_family_key IN ({alias_placeholders})
                        OR (event_family_key = '' AND event_key IN ({alias_placeholders}))
                      ){extracted_where}
                      AND published_ts > 0
                    ORDER BY published_ts ASC
                    """,
                    extracted_params,
                ).fetchall()
            if monthly_mode:
                platform_row = conn.execute(
                    """
                    SELECT MAX(published_ts) AS latest_published_ts
                    FROM social_posts
                    WHERE platform = ?
                      AND published_ts > 0
                      AND published_at >= ?
                      AND published_at < ?
                    """,
                    (
                        normalized_platform,
                        start_day.isoformat(),
                        (end_day + timedelta(days=1)).isoformat(),
                    ),
                ).fetchone()
                latest_platform_ts = int(platform_row["latest_published_ts"] or 0) if platform_row else 0
                if latest_platform_ts:
                    if latest_platform_ts > 10_000_000_000:
                        latest_platform_ts //= 1000
                    latest_monitored_day = datetime.fromtimestamp(latest_platform_ts, tz=local_tz).date()

        for row in rows:
            published_ts = int(row["published_ts"] or 0)
            if not published_ts:
                continue
            if published_ts > 10_000_000_000:
                published_ts //= 1000

            bucket_day = datetime.fromtimestamp(published_ts, tz=local_tz).date()
            if bucket_day < start_day or bucket_day > end_day:
                continue

            bucket = day_buckets[bucket_day]
            bucket["post_count"] += 1
            bucket["discussion_total"] += int(row["discussion_total"] or 0)
            bucket["engagement_total"] += int(row["engagement_total"] or 0)
            bucket["velocity"] += 1
            author_name = str(row["author_name"] or "").strip()
            if author_name:
                author_buckets[bucket_day].add(author_name)

        for bucket_day, authors in author_buckets.items():
            day_buckets[bucket_day]["unique_authors"] = len(authors)

        current_local_day = datetime.now(local_tz).date()
        is_open_month_window = monthly_mode and end_day >= current_local_day

        if is_open_month_window and latest_monitored_day is not None and latest_monitored_day < end_day:
            for bucket_day, bucket in day_buckets.items():
                if bucket_day > latest_monitored_day:
                    bucket["post_count"] = None
                    bucket["discussion_total"] = None
                    bucket["engagement_total"] = None
                    bucket["unique_authors"] = None
                    bucket["velocity"] = None

        series = [day_buckets[day] for day in sorted(day_buckets)]
        metrics = {
            "discussion_total": [
                {"date": item["date"], "value": item["discussion_total"]}
                for item in series
            ],
            "engagement_total": [
                {"date": item["date"], "value": item["engagement_total"]}
                for item in series
            ],
            "unique_authors": [
                {"date": item["date"], "value": item["unique_authors"]}
                for item in series
            ],
            "velocity": [
                {"date": item["date"], "value": item["velocity"]}
                for item in series
            ],
        }
        return {
            "platform": normalized_platform,
            "event_family_key": canonical_cluster_key,
            "days": safe_days,
            "start_date": start_day.isoformat(),
            "end_date": end_day.isoformat(),
            "month_key": month_key,
            "series": series,
            "metrics": metrics,
            "summary": {
                "cluster_key": summary_row["cluster_key"] if summary_row else canonical_cluster_key,
                "dashboard_category": summary_row["dashboard_category"] if summary_row else "",
                "heat_score": float(summary_row["heat_score"] or 0.0) if summary_row else 0.0,
                "total_engagement": int(summary_row["total_engagement"] or 0) if summary_row else 0,
                "discussion_total": int(summary_row["discussion_total"] or 0) if summary_row else 0,
                "post_count": int(summary_row["post_count"] or 0) if summary_row else 0,
                "unique_authors": int(summary_row["unique_authors"] or 0) if summary_row else 0,
                "start_date": start_day.isoformat(),
                "end_date": end_day.isoformat(),
            },
        }

    def _build_weekly_event_post_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        week_start: str,
        week_end: str,
    ) -> list[dict[str, Any]]:
        return [
            row | {
                "week_start": week_start,
                "week_end": week_end,
            }
            for row in rows
        ]

    def _build_weekly_cluster_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        week_start: str,
        week_end: str,
    ) -> list[dict[str, Any]]:
        return [
            row | {
                "week_start": week_start,
                "week_end": week_end,
            }
            for row in rows
        ]

    def _build_monthly_event_post_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        month_key: str,
        month_start: str,
        month_end: str,
    ) -> list[dict[str, Any]]:
        return [
            row | {
                "month_key": month_key,
                "month_start": month_start,
                "month_end": month_end,
            }
            for row in rows
        ]

    def _build_monthly_cluster_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        month_key: str,
        month_start: str,
        month_end: str,
    ) -> list[dict[str, Any]]:
        return [
            row | {
                "month_key": month_key,
                "month_start": month_start,
                "month_end": month_end,
            }
            for row in rows
        ]

    def _count_posts_in_window(self, *, platform: str, week_start: str, week_end: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM social_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 10) >= ?
                  AND substr(published_at, 1, 10) <= ?
                """,
                (platform, week_start, week_end),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_posts_in_month(self, *, platform: str, month_key: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM social_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 7) = ?
                """,
                (platform, month_key),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_extracted_posts_in_window(self, *, platform: str, week_start: str, week_end: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM event_extracted_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 10) >= ?
                  AND substr(published_at, 1, 10) <= ?
                """,
                (platform, week_start, week_end),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_extracted_posts_in_month(self, *, platform: str, month_key: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM event_extracted_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 7) = ?
                """,
                (platform, month_key),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_ready_posts_in_window(self, *, platform: str, week_start: str, week_end: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM event_ready_posts
                WHERE platform = ?
                  AND status = 'ready'
                  AND substr(published_at, 1, 10) >= ?
                  AND substr(published_at, 1, 10) <= ?
                """,
                (platform, week_start, week_end),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_ready_posts_in_month(self, *, platform: str, month_key: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM event_ready_posts
                WHERE platform = ?
                  AND status = 'ready'
                  AND substr(published_at, 1, 7) = ?
                """,
                (platform, month_key),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _has_weekly_snapshot(self, *, platform: str, week_start: str, week_end: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM weekly_event_extracted_posts WHERE platform = ? AND week_start = ? AND week_end = ?) AS post_count,
                    (SELECT COUNT(*) FROM weekly_event_clusters WHERE platform = ? AND week_start = ? AND week_end = ?) AS event_count,
                    (SELECT COUNT(*) FROM weekly_topic_clusters WHERE platform = ? AND week_start = ? AND week_end = ?) AS topic_count
                """,
                (
                    platform,
                    week_start,
                    week_end,
                    platform,
                    week_start,
                    week_end,
                    platform,
                    week_start,
                    week_end,
                ),
            ).fetchone()
        return bool(row and ((row["post_count"] or 0) > 0 or (row["event_count"] or 0) > 0 or (row["topic_count"] or 0) > 0))

    def _has_monthly_snapshot(self, *, platform: str, month_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM monthly_event_extracted_posts WHERE platform = ? AND month_key = ?) AS post_count,
                    (SELECT COUNT(*) FROM monthly_event_clusters WHERE platform = ? AND month_key = ?) AS event_count,
                    (SELECT COUNT(*) FROM monthly_topic_clusters WHERE platform = ? AND month_key = ?) AS topic_count
                """,
                (
                    platform,
                    month_key,
                    platform,
                    month_key,
                    platform,
                    month_key,
                ),
            ).fetchone()
        return bool(row and ((row["post_count"] or 0) > 0 or (row["event_count"] or 0) > 0 or (row["topic_count"] or 0) > 0))

    def _ensure_weekly_snapshot_materialized(self, *, platform: str, week_start: str, week_end: str) -> None:
        if self._has_weekly_snapshot(platform=platform, week_start=week_start, week_end=week_end):
            return
        if self._count_ready_posts_in_window(platform=platform, week_start=week_start, week_end=week_end) <= 0:
            return
        self.extract_events_weekly(
            platform=platform,
            week_start=week_start,
            week_end=week_end,
            status="ready",
            replace=True,
        )

    def _ensure_monthly_snapshot_materialized(self, *, platform: str, month_key: str) -> None:
        if self._has_monthly_snapshot(platform=platform, month_key=month_key):
            return
        if self._count_ready_posts_in_month(platform=platform, month_key=month_key) <= 0:
            return
        self.extract_events_monthly(
            platform=platform,
            month_key=month_key,
            status="ready",
            replace=True,
        )

    def _count_event_clusters_in_window(self, *, platform: str, week_start: str, week_end: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT CASE
                    WHEN event_family_key != '' THEN event_family_key
                    ELSE event_key
                END) AS count
                FROM event_extracted_posts
                WHERE platform = ?
                  AND event_promoted = 1
                  AND substr(published_at, 1, 10) >= ?
                  AND substr(published_at, 1, 10) <= ?
                  AND (event_family_key != '' OR event_key != '')
                """,
                (platform, week_start, week_end),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_event_clusters_in_month(self, *, platform: str, month_key: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT CASE
                    WHEN event_family_key != '' THEN event_family_key
                    ELSE event_key
                END) AS count
                FROM event_extracted_posts
                WHERE platform = ?
                  AND event_promoted = 1
                  AND substr(published_at, 1, 7) = ?
                  AND (event_family_key != '' OR event_key != '')
                """,
                (platform, month_key),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_topic_clusters_in_window(self, *, platform: str, week_start: str, week_end: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT primary_topic) AS count
                FROM event_extracted_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 10) >= ?
                  AND substr(published_at, 1, 10) <= ?
                  AND primary_topic != ''
                """,
                (platform, week_start, week_end),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _count_topic_clusters_in_month(self, *, platform: str, month_key: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT primary_topic) AS count
                FROM event_extracted_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 7) = ?
                  AND primary_topic != ''
                """,
                (platform, month_key),
            ).fetchone()
        return int(row["count"] or 0) if row else 0

    def _latest_extracted_at_in_window(self, *, platform: str, week_start: str, week_end: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(extracted_at) AS extracted_at
                FROM event_extracted_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 10) >= ?
                  AND substr(published_at, 1, 10) <= ?
                """,
                (platform, week_start, week_end),
            ).fetchone()
        return str(row["extracted_at"] or "") if row else ""

    def _latest_extracted_at_in_month(self, *, platform: str, month_key: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(extracted_at) AS extracted_at
                FROM event_extracted_posts
                WHERE platform = ?
                  AND substr(published_at, 1, 7) = ?
                """,
                (platform, month_key),
            ).fetchone()
        return str(row["extracted_at"] or "") if row else ""

    def _validate_week_window(self, *, week_start: str, week_end: str) -> None:
        if not week_start or not week_end:
            raise ValueError("week_start and week_end are required.")
        start_day = datetime.fromisoformat(week_start).date()
        end_day = datetime.fromisoformat(week_end).date()
        if start_day.weekday() != 6:
            raise ValueError("week_start must be a Sunday.")
        if end_day.weekday() != 5:
            raise ValueError("week_end must be a Saturday.")
        if end_day - start_day != timedelta(days=6):
            raise ValueError("Week windows must span Sunday through Saturday.")

    def _validate_month_key(self, month_key: str) -> str:
        normalized = str(month_key or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}", normalized):
            raise ValueError("month_key must use YYYY-MM format.")
        datetime.strptime(f"{normalized}-01", "%Y-%m-%d")
        return normalized

    def _resolve_month_window(self, month_key: str) -> tuple[str, str]:
        normalized = self._validate_month_key(month_key)
        month_start = datetime.strptime(f"{normalized}-01", "%Y-%m-%d").date()
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1, day=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1, day=1)
        month_end = next_month - timedelta(days=1)
        return month_start.isoformat(), month_end.isoformat()

    def _shift_month(self, month_start: datetime.date, offset: int) -> datetime.date:
        year = month_start.year
        month = month_start.month + offset
        while month <= 0:
            month += 12
            year -= 1
        while month > 12:
            month -= 12
            year += 1
        return datetime(year, month, 1).date()

    def _list_analysis_months(self, *, platform: str, months: int) -> dict[str, Any]:
        safe_months = max(1, min(int(months), 24))
        with self._connect() as conn:
            max_month_row = conn.execute(
                """
                SELECT MAX(substr(published_at, 1, 7)) AS latest_month
                FROM social_posts
                WHERE platform = ?
                """,
                (platform,),
            ).fetchone()
            min_month_row = conn.execute(
                """
                SELECT MIN(substr(published_at, 1, 7)) AS earliest_month
                FROM social_posts
                WHERE platform = ?
                """,
                (platform,),
            ).fetchone()
            recorded_rows = conn.execute(
                """
                SELECT platform, month_key, month_start, month_end, status, source_ready_posts,
                       extracted_post_rows, event_cluster_rows, topic_cluster_rows, extracted_at, note
                FROM analysis_months
                WHERE platform = ?
                ORDER BY month_key DESC
                """,
                (platform,),
            ).fetchall()

        latest_month_key = max_month_row["latest_month"] if max_month_row else None
        earliest_month_key = min_month_row["earliest_month"] if min_month_row else None
        if not latest_month_key:
            return {"platform": platform, "window_mode": "monthly", "items": [], "months": safe_months}

        latest_month_start = datetime.strptime(f"{latest_month_key}-01", "%Y-%m-%d").date()
        earliest_month_start = datetime.strptime(f"{earliest_month_key}-01", "%Y-%m-%d").date()
        if earliest_month_start < FULL_WEB_ANALYSIS_FLOOR_DATE:
            earliest_month_start = FULL_WEB_ANALYSIS_FLOOR_DATE.replace(day=1)
        recorded = {row["month_key"]: dict(row) for row in recorded_rows}

        items: list[dict[str, Any]] = []
        for offset in range(safe_months):
            month_start_date = self._shift_month(latest_month_start, -offset)
            if month_start_date < earliest_month_start:
                break
            month_key = month_start_date.strftime("%Y-%m")
            month_start, month_end = self._resolve_month_window(month_key)
            row = recorded.get(month_key)
            post_count = self._count_posts_in_month(platform=platform, month_key=month_key)
            inferred_extracted_count = self._count_extracted_posts_in_month(platform=platform, month_key=month_key)
            inferred_event_cluster_rows = self._count_event_clusters_in_month(platform=platform, month_key=month_key)
            inferred_topic_cluster_rows = self._count_topic_clusters_in_month(platform=platform, month_key=month_key)
            has_completed_snapshot = bool(row) or inferred_extracted_count > 0
            status_value = "completed" if has_completed_snapshot else ("to_be_analyzed" if post_count > 0 else "to_be_updated")
            items.append(
                {
                    "platform": platform,
                    "month_key": month_key,
                    "month_start": month_start,
                    "month_end": month_end,
                    "status": status_value,
                    "post_count": post_count,
                    "source_ready_posts": int((row or {}).get("source_ready_posts", 0) or 0),
                    "extracted_post_rows": int((row or {}).get("extracted_post_rows", inferred_extracted_count) or 0),
                    "event_cluster_rows": int((row or {}).get("event_cluster_rows", inferred_event_cluster_rows) or 0),
                    "topic_cluster_rows": int((row or {}).get("topic_cluster_rows", inferred_topic_cluster_rows) or 0),
                    "extracted_at": (row or {}).get(
                        "extracted_at",
                        self._latest_extracted_at_in_month(platform=platform, month_key=month_key),
                    ),
                    "note": (row or {}).get(
                        "note",
                        "Inferred from existing extracted posts." if inferred_extracted_count > 0 else "",
                    ),
                }
            )

        return {"platform": platform, "window_mode": "monthly", "items": items, "months": safe_months}

    def _list_analysis_quarters(self, *, platform: str, quarters: int) -> dict[str, Any]:
        safe_quarters = max(1, min(int(quarters), 8))
        with self._connect() as conn:
            latest_month_row = conn.execute(
                """
                SELECT MAX(substr(published_at, 1, 7)) AS latest_month
                FROM social_posts
                WHERE platform = ?
                """,
                (platform,),
            ).fetchone()

        latest_month_key = latest_month_row["latest_month"] if latest_month_row else None
        if not latest_month_key:
            return {
                "platform": platform,
                "window_mode": "quarterly",
                "items": [],
                "quarters": safe_quarters,
                "message": "The Full-Web database is still being prepared. Quarterly reporting will appear once a complete quarter is available.",
            }

        latest_month_start = datetime.strptime(f"{latest_month_key}-01", "%Y-%m-%d").date()
        current_quarter = ((latest_month_start.month - 1) // 3) + 1
        quarter_key = f"{latest_month_start.year}-Q{current_quarter}"
        message = (
            f"Full-Web data collection began on {FULL_WEB_ANALYSIS_FLOOR_DATE.isoformat()}, so quarterly reporting is not available yet. "
            "The first complete quarterly report will be the Q2 2026 window, available after June 2026."
        )
        items = [
            {
                "platform": platform,
                "quarter_key": quarter_key,
                "quarter_start": f"{latest_month_start.year}-04-01",
                "quarter_end": f"{latest_month_start.year}-06-30",
                "status": "future",
                "post_count": 0,
                "source_ready_posts": 0,
                "extracted_post_rows": 0,
                "event_cluster_rows": 0,
                "topic_cluster_rows": 0,
                "extracted_at": "",
                "note": message,
            }
        ]
        return {
            "platform": platform,
            "window_mode": "quarterly",
            "items": items,
            "quarters": safe_quarters,
            "message": message,
        }

    def _load_cluster_feedback_actions(
        self,
        *,
        platform: str,
        board_type: str,
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
        quarter_key: str = "",
    ) -> list[dict[str, Any]]:
        scope_type = "global"
        if quarter_key:
            scope_type = "quarterly"
        elif month_key:
            scope_type = "monthly"
        elif week_start or week_end:
            scope_type = "weekly"

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT platform, board_type, scope_type, week_start, week_end, month_key, quarter_key,
                       source_cluster_key, action, target_cluster_key, note, created_at
                FROM cluster_feedback
                WHERE platform = ?
                  AND board_type = ?
                  AND scope_type = ?
                  AND COALESCE(week_start, '') = ?
                  AND COALESCE(week_end, '') = ?
                  AND COALESCE(month_key, '') = ?
                  AND COALESCE(quarter_key, '') = ?
                ORDER BY created_at ASC, id ASC
                """,
                (
                    platform,
                    board_type,
                    scope_type,
                    week_start,
                    week_end,
                    month_key,
                    quarter_key,
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def _aggregate_cluster_group(self, rows: list[dict[str, Any]], cluster_key: str) -> dict[str, Any]:
        if not rows:
            return {}
        total_posts = max(1, sum(int(row.get("post_count") or 0) for row in rows))
        combined = dict(rows[0])
        combined["cluster_key"] = cluster_key
        combined["post_count"] = sum(int(row.get("post_count") or 0) for row in rows)
        combined["unique_authors"] = sum(int(row.get("unique_authors") or 0) for row in rows)
        combined["total_like_count"] = sum(int(row.get("total_like_count") or 0) for row in rows)
        combined["total_comment_count"] = sum(int(row.get("total_comment_count") or 0) for row in rows)
        combined["total_share_count"] = sum(int(row.get("total_share_count") or 0) for row in rows)
        combined["total_engagement"] = sum(int(row.get("total_engagement") or 0) for row in rows)
        combined["discussion_total"] = sum(int(row.get("discussion_total") or 0) for row in rows)

        combined["engagement_component"] = sum(float(row.get("engagement_component") or 0.0) * int(row.get("post_count") or 0) for row in rows) / total_posts
        combined["discussion_component"] = sum(float(row.get("discussion_component") or 0.0) * int(row.get("post_count") or 0) for row in rows) / total_posts
        combined["diversity_component"] = sum(float(row.get("diversity_component") or 0.0) * int(row.get("post_count") or 0) for row in rows) / total_posts
        combined["velocity_component"] = sum(float(row.get("velocity_component") or 0.0) * int(row.get("post_count") or 0) for row in rows) / total_posts
        combined["heat_score"] = (
            float(combined["engagement_component"])
            + float(combined["discussion_component"])
            + float(combined["diversity_component"])
            + float(combined["velocity_component"])
        ) / 4.0

        combined["keywords"] = list(dict.fromkeys(value for row in rows for value in (row.get("keywords") or [])))[:20]
        combined["top_posts"] = (rows[0].get("top_posts") or [])[:10]
        combined["top_comments"] = (rows[0].get("top_comments") or [])[:10]
        combined["organizer_evidence"] = list(dict.fromkeys(value for row in rows for value in (row.get("organizer_evidence") or [])))[:10]
        combined["platform_label"] = PLATFORM_LABELS.get(combined.get("platform", ""), combined.get("platform", ""))
        return combined

    def _apply_cluster_feedback(
        self,
        rows: list[dict[str, Any]],
        *,
        platform: str,
        board_type: str,
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
        quarter_key: str = "",
    ) -> list[dict[str, Any]]:
        if not rows:
            return rows
        actions = self._load_cluster_feedback_actions(
            platform=platform,
            board_type=board_type,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
            quarter_key=quarter_key,
        )
        if not actions:
            return rows

        suppressed: set[str] = set()
        merge_map: dict[str, str] = {}
        for action in actions:
            source_key = str(action.get("source_cluster_key") or "").strip()
            target_key = str(action.get("target_cluster_key") or "").strip()
            if action.get("action") == "noise":
                suppressed.add(source_key)
            elif action.get("action") == "merge" and source_key and target_key:
                merge_map[source_key] = target_key

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            source_key = str(row.get("cluster_key") or "").strip()
            if source_key in suppressed:
                continue
            canonical_key = merge_map.get(source_key, source_key)
            grouped.setdefault(canonical_key, []).append(row)

        merged_rows = [self._aggregate_cluster_group(group_rows, cluster_key) for cluster_key, group_rows in grouped.items()]
        merged_rows.sort(key=lambda item: (-float(item.get("heat_score") or 0.0), str(item.get("cluster_key") or "")))
        return merged_rows

    def _resolve_feedback_cluster_selection(
        self,
        *,
        platform: str,
        board_type: str,
        selected_cluster_key: str,
        week_start: str = "",
        week_end: str = "",
        month_key: str = "",
        quarter_key: str = "",
    ) -> tuple[str, list[str]]:
        actions = self._load_cluster_feedback_actions(
            platform=platform,
            board_type=board_type,
            week_start=week_start,
            week_end=week_end,
            month_key=month_key,
            quarter_key=quarter_key,
        )
        normalized_selected = str(selected_cluster_key or "").strip()
        suppressed = {
            str(action.get("source_cluster_key") or "").strip()
            for action in actions
            if action.get("action") == "noise"
        }
        merge_map = {
            str(action.get("source_cluster_key") or "").strip(): str(action.get("target_cluster_key") or "").strip()
            for action in actions
            if action.get("action") == "merge"
        }
        if normalized_selected in suppressed:
            return normalized_selected, []
        canonical_key = merge_map.get(normalized_selected, normalized_selected)
        aliases = [canonical_key]
        for action in actions:
            source_key = str(action.get("source_cluster_key") or "").strip()
            target_key = str(action.get("target_cluster_key") or "").strip()
            if action.get("action") == "merge" and target_key == canonical_key and source_key not in suppressed:
                aliases.append(source_key)
        return canonical_key, sorted(set(alias for alias in aliases if alias))

    def _aggregate_cluster_summary_rows(self, rows: list[sqlite3.Row], cluster_key: str) -> dict[str, Any]:
        if not rows:
            return {
                "cluster_key": cluster_key,
                "dashboard_category": "",
                "heat_score": 0.0,
                "total_engagement": 0,
                "discussion_total": 0,
                "post_count": 0,
                "unique_authors": 0,
            }
        normalized_rows = [dict(row) for row in rows]
        total_posts = max(1, sum(int(row.get("post_count") or 0) for row in normalized_rows))
        return {
            "cluster_key": cluster_key,
            "dashboard_category": "",
            "heat_score": (
                sum(float(row.get("heat_score") or 0.0) * int(row.get("post_count") or 0) for row in normalized_rows) / total_posts
            ),
            "total_engagement": sum(int(row.get("total_engagement") or 0) for row in normalized_rows),
            "discussion_total": sum(int(row.get("discussion_total") or 0) for row in normalized_rows),
            "post_count": sum(int(row.get("post_count") or 0) for row in normalized_rows),
            "unique_authors": sum(int(row.get("unique_authors") or 0) for row in normalized_rows),
        }

    def _load_event_ready_rows(
        self,
        conn: sqlite3.Connection,
        *,
        platform: str,
        status: str,
        week_start: str = "",
        week_end: str = "",
    ) -> list[dict[str, Any]]:
        clauses = ["er.platform = ?"]
        params: list[Any] = [platform]
        if status:
            clauses.append("er.status = ?")
            params.append(status)
        if week_start:
            clauses.append("substr(er.published_at, 1, 10) >= ?")
            params.append(week_start)
        if week_end:
            clauses.append("substr(er.published_at, 1, 10) <= ?")
            params.append(week_end)

        rows = conn.execute(
            f"""
            SELECT er.platform, er.source_post_id, er.author_name, er.published_at, er.published_ts, er.note_url,
                   er.clean_content, er.analysis_content, er.hashtags, er.mentions, er.source_keywords,
                   er.topic_seed_terms, er.relevance_flags, er.status, er.source_file, er.raw_json,
                   COALESCE(sp.author_id, '') AS author_id
            FROM event_ready_posts er
            LEFT JOIN social_posts sp
              ON sp.platform = er.platform
             AND sp.source_post_id = er.source_post_id
            WHERE {' AND '.join(clauses)}
            ORDER BY er.published_ts DESC, er.id DESC
            """,
            params,
        ).fetchall()
        return [self._deserialize_event_ready_row(dict(row)) for row in rows]

    def _deserialize_event_ready_row(self, row: dict[str, Any]) -> dict[str, Any]:
        raw_payload = self._json_to_dict(row.get("raw_json"))
        published_ts = int(row.get("published_ts") or 0)
        published_ts_seconds = published_ts // 1000 if published_ts > 10_000_000_000 else published_ts
        like_count = self._parse_metric(raw_payload.get("like_count") or raw_payload.get("liked_count"))
        comment_count = self._parse_metric(raw_payload.get("comments_count") or raw_payload.get("comment_count"))
        share_count = self._parse_metric(
            raw_payload.get("shared_count") or raw_payload.get("share_count") or raw_payload.get("reposts_count")
        )
        return {
            "platform": row.get("platform", "wb"),
            "note_id": row.get("source_post_id", ""),
            "source_post_id": row.get("source_post_id", ""),
            "user_id": str(
                row.get("author_id")
                or raw_payload.get("user_id")
                or raw_payload.get("user", {}).get("id")
                or ""
            ).strip(),
            "nickname": self._clean_text(row.get("author_name") or raw_payload.get("nickname") or raw_payload.get("author_name")),
            "author_name": self._clean_text(row.get("author_name") or raw_payload.get("nickname") or raw_payload.get("author_name")),
            "create_time": published_ts_seconds,
            "published_ts": published_ts,
            "published_at": row.get("published_at", ""),
            "create_date_time": row.get("published_at", ""),
            "note_url": row.get("note_url", ""),
            "clean_content": row.get("clean_content", ""),
            "analysis_content": row.get("analysis_content", ""),
            "hashtags": self._json_to_list(row.get("hashtags")),
            "mentions": self._json_to_list(row.get("mentions")),
            "source_keywords": self._json_to_list(row.get("source_keywords")),
            "topic_seed_terms": self._json_to_list(row.get("topic_seed_terms")),
            "flags": self._json_to_dict(row.get("relevance_flags")),
            "relevance_flags": self._json_to_dict(row.get("relevance_flags")),
            "status": row.get("status", "ready"),
            "source_file": row.get("source_file", ""),
            "raw_json": row.get("raw_json", "{}"),
            "like_count": like_count,
            "comment_count": comment_count,
            "share_count": share_count,
            "engagement_total": like_count + comment_count + share_count,
        }

    def _build_event_extraction_outputs(self, ready_rows: list[dict[str, Any]]) -> dict[str, Any]:
        alias_to_canonical, canonical_event_set = load_event_alias_registry(DEFAULT_EVENT_ALIAS_REGISTRY_PATH)
        child_to_parent = load_event_parent_registry(DEFAULT_EVENT_PARENT_REGISTRY_PATH)
        organizer_registry = load_organizer_registry(DEFAULT_EVENT_ORGANIZER_REGISTRY_PATH)
        return build_heat_outputs(
            posts=ready_rows,
            comment_map={},
            alias_to_canonical=alias_to_canonical,
            canonical_event_set=canonical_event_set,
            child_to_parent=child_to_parent,
            organizer_registry=organizer_registry,
        )

    def _build_event_post_rows(
        self,
        posts: list[dict[str, Any]],
        *,
        extracted_at: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for post in posts:
            rows.append(
                {
                    "platform": post.get("platform", "wb"),
                    "source_post_id": str(post.get("source_post_id") or post.get("note_id") or "").strip(),
                    "author_name": self._clean_text(post.get("author_name") or post.get("nickname") or ""),
                    "published_at": str(post.get("published_at") or post.get("create_date_time") or "").strip(),
                    "published_ts": int(post.get("published_ts") or 0),
                    "note_url": str(post.get("note_url") or "").strip(),
                    "clean_content": str(post.get("clean_content") or ""),
                    "analysis_content": str(post.get("analysis_content") or ""),
                    "hashtags": json.dumps(post.get("hashtags") or [], ensure_ascii=False),
                    "mentions": json.dumps(post.get("mentions") or [], ensure_ascii=False),
                    "source_keywords": json.dumps(post.get("source_keywords") or [], ensure_ascii=False),
                    "topic_seed_terms": json.dumps(post.get("topic_seed_terms") or [], ensure_ascii=False),
                    "relevance_flags": json.dumps(post.get("relevance_flags") or post.get("flags") or {}, ensure_ascii=False),
                    "source_file": str(post.get("source_file") or ""),
                    "raw_json": str(post.get("raw_json") or "{}"),
                    "post_type": str(post.get("post_type") or ""),
                    "raw_event_candidate": str(post.get("raw_event_candidate") or ""),
                    "canonical_event_name": str(post.get("canonical_event_name") or ""),
                    "event_eligible": 1 if post.get("event_eligible") else 0,
                    "event_promoted": 1 if post.get("event_promoted") else 0,
                    "event_confidence": float(post.get("event_confidence") or 0.0),
                    "event_geo_score": float(post.get("event_geo_score") or 0.0),
                    "event_leaf_name": str(post.get("event_leaf_name") or ""),
                    "event_parent_name": str(post.get("event_parent_name") or ""),
                    "event_key": str(post.get("event_key") or ""),
                    "event_family_key": str(post.get("event_family_key") or ""),
                    "organizer_key": str(post.get("organizer_key") or ""),
                    "organizer_name": str(post.get("organizer_name") or ""),
                    "organizer_type": str(post.get("organizer_type") or ""),
                    "organizer_confidence": float(post.get("organizer_confidence") or 0.0),
                    "organizer_evidence": json.dumps(post.get("organizer_evidence") or [], ensure_ascii=False),
                    "primary_topic": str(post.get("primary_topic") or ""),
                    "dashboard_category": str(post.get("dashboard_category") or ""),
                    "quality_weight": float(post.get("quality_weight") or 0.0),
                    "engagement_total": int(post.get("engagement_total") or 0),
                    "discussion_total": int(post.get("discussion_total") or 0),
                    "comment_fetch_count": int(post.get("comment_fetch_count") or 0),
                    "comment_fetch_like_sum": int(post.get("comment_fetch_like_sum") or 0),
                    "comment_fetch_sub_comment_sum": int(post.get("comment_fetch_sub_comment_sum") or 0),
                    "comment_unique_authors": int(post.get("comment_unique_authors") or 0),
                    "top_comments": json.dumps(post.get("top_comments") or [], ensure_ascii=False),
                    "post_heat": float(post.get("post_heat") or 0.0),
                    "base_engagement": float(post.get("base_engagement") or 0.0),
                    "discussion_strength": float(post.get("discussion_strength") or 0.0),
                    "comment_value": float(post.get("comment_value") or 0.0),
                    "recency_factor": float(post.get("recency_factor") or 0.0),
                    "raw_score": float(post.get("raw_score") or 0.0),
                    "extracted_at": extracted_at,
                }
            )
        return rows

    def _build_event_cluster_rows(
        self,
        *,
        platform: str,
        clusters: list[dict[str, Any]],
        extracted_at: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for cluster in clusters:
            rows.append(
                {
                    "platform": platform,
                    "cluster_key": str(cluster.get("cluster_key") or ""),
                    "cluster_type": str(cluster.get("cluster_type") or "event_key"),
                    "post_count": int(cluster.get("post_count") or 0),
                    "unique_authors": int(cluster.get("unique_authors") or 0),
                    "post_type_breakdown": json.dumps(cluster.get("post_type_breakdown") or {}, ensure_ascii=False),
                    "total_like_count": int(cluster.get("total_like_count") or 0),
                    "total_comment_count": int(cluster.get("total_comment_count") or 0),
                    "total_share_count": int(cluster.get("total_share_count") or 0),
                    "total_engagement": int(cluster.get("total_engagement") or 0),
                    "discussion_total": int(cluster.get("discussion_total") or 0),
                    "keywords": json.dumps(cluster.get("keywords") or [], ensure_ascii=False),
                    "top_posts": json.dumps(cluster.get("top_posts") or [], ensure_ascii=False),
                    "top_comments": json.dumps(cluster.get("top_comments") or [], ensure_ascii=False),
                    "organizer_key": str(cluster.get("organizer_key") or ""),
                    "organizer_name": str(cluster.get("organizer_name") or ""),
                    "organizer_type": str(cluster.get("organizer_type") or ""),
                    "organizer_breakdown": json.dumps(cluster.get("organizer_breakdown") or {}, ensure_ascii=False),
                    "organizer_evidence": json.dumps(cluster.get("organizer_evidence") or [], ensure_ascii=False),
                    "dashboard_category": str(cluster.get("dashboard_category") or ""),
                    "dashboard_category_score": float(cluster.get("dashboard_category_score") or 0.0),
                    "engagement_component": float(cluster.get("engagement_component") or 0.0),
                    "discussion_component": float(cluster.get("discussion_component") or 0.0),
                    "diversity_component": float(cluster.get("diversity_component") or 0.0),
                    "velocity_component": float(cluster.get("velocity_component") or 0.0),
                    "heat_score": float(cluster.get("heat_score") or 0.0),
                    "extracted_at": extracted_at,
                }
            )
        return rows

    def _build_topic_cluster_rows(
        self,
        *,
        platform: str,
        clusters: list[dict[str, Any]],
        extracted_at: str,
    ) -> list[dict[str, Any]]:
        rows = self._build_event_cluster_rows(
            platform=platform,
            clusters=clusters,
            extracted_at=extracted_at,
        )
        for row in rows:
            if not row.get("cluster_type"):
                row["cluster_type"] = "primary_topic"
        return rows

    def _sync_single_file(self, conn: sqlite3.Connection, source_file: Path) -> dict[str, int]:
        platform = self._infer_platform(source_file)
        source_type = self._infer_source_type(source_file)

        posts_by_id: dict[tuple[str, str], dict[str, Any]] = {}
        source_rows: list[dict[str, Any]] = []

        for record in self._load_records(source_file):
            normalized = self._normalize_record(
                platform=platform,
                source_type=source_type,
                source_file=source_file,
                record=record,
            )
            if not normalized:
                continue

            key = (normalized["platform"], normalized["source_post_id"])
            existing = posts_by_id.get(key)
            posts_by_id[key] = self._merge_posts(existing, normalized) if existing else normalized
            source_rows.extend(self._build_source_rows(normalized))

        if not posts_by_id:
            return {"posts_imported": 0, "sources_imported": 0, "event_posts_imported": 0}

        post_rows = list(posts_by_id.values())
        conn.executemany(
            """
            INSERT INTO social_posts (
                platform,
                source_type,
                source_post_id,
                author_id,
                author_name,
                title,
                content,
                clean_content,
                analysis_content,
                note_url,
                published_at,
                published_ts,
                like_count,
                comment_count,
                share_count,
                collect_count,
                media_urls,
                tags,
                source_keyword,
                source_keywords,
                source_file,
                raw_json,
                imported_at
            ) VALUES (
                :platform,
                :source_type,
                :source_post_id,
                :author_id,
                :author_name,
                :title,
                :content,
                :clean_content,
                :analysis_content,
                :note_url,
                :published_at,
                :published_ts,
                :like_count,
                :comment_count,
                :share_count,
                :collect_count,
                :media_urls,
                :tags,
                :source_keyword,
                :source_keywords,
                :source_file,
                :raw_json,
                :imported_at
            )
            ON CONFLICT(platform, source_post_id) DO UPDATE SET
                source_type = excluded.source_type,
                author_id = excluded.author_id,
                author_name = excluded.author_name,
                title = excluded.title,
                content = excluded.content,
                clean_content = excluded.clean_content,
                analysis_content = excluded.analysis_content,
                note_url = excluded.note_url,
                published_at = excluded.published_at,
                published_ts = excluded.published_ts,
                like_count = excluded.like_count,
                comment_count = excluded.comment_count,
                share_count = excluded.share_count,
                collect_count = excluded.collect_count,
                media_urls = excluded.media_urls,
                tags = excluded.tags,
                source_keyword = excluded.source_keyword,
                source_keywords = excluded.source_keywords,
                source_file = excluded.source_file,
                raw_json = excluded.raw_json,
                imported_at = excluded.imported_at
            """,
            post_rows,
        )

        deduped_source_rows = self._dedupe_source_rows(source_rows)
        if deduped_source_rows:
            conn.executemany(
                """
                INSERT INTO social_post_sources (
                    platform,
                    source_type,
                    source_post_id,
                    source_keyword,
                    source_file,
                    imported_at,
                    raw_json
                ) VALUES (
                    :platform,
                    :source_type,
                    :source_post_id,
                    :source_keyword,
                    :source_file,
                    :imported_at,
                    :raw_json
                )
                ON CONFLICT(platform, source_post_id, source_file, source_keyword) DO UPDATE SET
                    source_type = excluded.source_type,
                    imported_at = excluded.imported_at,
                    raw_json = excluded.raw_json
                """,
                deduped_source_rows,
            )

        event_rows = [row for row in (self._build_event_ready_row(post_row) for post_row in post_rows) if row]
        if event_rows:
            conn.executemany(
                """
                INSERT INTO event_ready_posts (
                    platform,
                    source_post_id,
                    author_name,
                    published_at,
                    published_ts,
                    note_url,
                    clean_content,
                    analysis_content,
                    hashtags,
                    mentions,
                    source_keywords,
                    topic_seed_terms,
                    relevance_flags,
                    status,
                    source_file,
                    raw_json,
                    updated_at
                ) VALUES (
                    :platform,
                    :source_post_id,
                    :author_name,
                    :published_at,
                    :published_ts,
                    :note_url,
                    :clean_content,
                    :analysis_content,
                    :hashtags,
                    :mentions,
                    :source_keywords,
                    :topic_seed_terms,
                    :relevance_flags,
                    :status,
                    :source_file,
                    :raw_json,
                    :updated_at
                )
                ON CONFLICT(platform, source_post_id) DO UPDATE SET
                    author_name = excluded.author_name,
                    published_at = excluded.published_at,
                    published_ts = excluded.published_ts,
                    note_url = excluded.note_url,
                    clean_content = excluded.clean_content,
                    analysis_content = excluded.analysis_content,
                    hashtags = excluded.hashtags,
                    mentions = excluded.mentions,
                    source_keywords = excluded.source_keywords,
                    topic_seed_terms = excluded.topic_seed_terms,
                    relevance_flags = excluded.relevance_flags,
                    status = excluded.status,
                    source_file = excluded.source_file,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at
                """,
                event_rows,
            )

        return {
            "posts_imported": len(post_rows),
            "sources_imported": len(deduped_source_rows),
            "event_posts_imported": len(event_rows),
        }

    def _upsert_sync_run(
        self,
        conn: sqlite3.Connection,
        source_file: Path,
        sync_result: dict[str, int],
        status: str,
        error_message: str,
    ) -> None:
        stat = source_file.stat()
        conn.execute(
            """
            INSERT INTO sync_runs (
                source_file,
                platform,
                source_type,
                file_size,
                file_mtime,
                imported_rows,
                source_rows,
                event_rows,
                status,
                error_message,
                synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_file) DO UPDATE SET
                platform = excluded.platform,
                source_type = excluded.source_type,
                file_size = excluded.file_size,
                file_mtime = excluded.file_mtime,
                imported_rows = excluded.imported_rows,
                source_rows = excluded.source_rows,
                event_rows = excluded.event_rows,
                status = excluded.status,
                error_message = excluded.error_message,
                synced_at = excluded.synced_at
            """,
            (
                str(source_file),
                self._infer_platform(source_file),
                self._infer_source_type(source_file),
                stat.st_size,
                stat.st_mtime,
                sync_result["posts_imported"],
                sync_result["sources_imported"],
                sync_result["event_posts_imported"],
                status,
                error_message,
                self._now_iso(),
            ),
        )

    def _load_records(self, source_file: Path) -> Iterable[dict[str, Any]]:
        if source_file.suffix.lower() == ".jsonl":
            with source_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    if isinstance(payload, dict):
                        yield payload
            return

        with source_file.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
        elif isinstance(payload, dict):
            yield payload

    def _normalize_record(
        self,
        platform: str,
        source_type: str,
        source_file: Path,
        record: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        if platform == "wb":
            return self._normalize_weibo_record(source_type, source_file, record)
        if platform == "fb":
            return self._normalize_generic_social_record(platform, source_type, source_file, record)
        return None

    def _normalize_weibo_record(
        self,
        source_type: str,
        source_file: Path,
        record: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        source_post_id = str(record.get("note_id") or record.get("id") or "").strip()
        content = self._clean_text(record.get("content") or record.get("desc") or "")
        if not source_post_id or not content:
            return None

        published_at, published_ts = self._normalize_datetime(
            record.get("created_date_time") or record.get("create_date_time") or record.get("created_at")
        )
        author_name = self._clean_text(record.get("author_name") or record.get("nickname") or "")
        source_keywords = self._normalize_source_keywords(record)
        clean_content = build_clean_content(content)
        analysis_content = build_analysis_content(content)

        return {
            "platform": "wb",
            "source_type": source_type,
            "source_post_id": source_post_id,
            "author_id": str(record.get("user_id") or "").strip(),
            "author_name": author_name,
            "title": self._build_title(content),
            "content": content,
            "clean_content": clean_content,
            "analysis_content": analysis_content,
            "note_url": str(record.get("note_url") or record.get("url") or "").strip(),
            "published_at": published_at,
            "published_ts": published_ts,
            "like_count": self._parse_metric(record.get("like_count") or record.get("liked_count")),
            "comment_count": self._parse_metric(record.get("comments_count") or record.get("comment_count")),
            "share_count": self._parse_metric(
                record.get("shared_count") or record.get("share_count") or record.get("reposts_count")
            ),
            "collect_count": self._parse_metric(record.get("collected_count")),
            "media_urls": json.dumps(self._normalize_media_urls(record), ensure_ascii=False),
            "tags": json.dumps(self._normalize_tags(record, content), ensure_ascii=False),
            "source_keyword": source_keywords[0] if source_keywords else self._clean_text(record.get("source_keyword") or ""),
            "source_keywords": json.dumps(source_keywords, ensure_ascii=False),
            "source_file": str(source_file),
            "raw_json": json.dumps(record, ensure_ascii=False),
            "imported_at": self._now_iso(),
        }

    def _normalize_generic_social_record(
        self,
        platform: str,
        source_type: str,
        source_file: Path,
        record: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        source_post_id = str(
            record.get("source_post_id")
            or record.get("post_id")
            or record.get("note_id")
            or record.get("id")
            or record.get("postId")
            or record.get("shortCode")
            or record.get("code")
            or record.get("url")
            or ""
        ).strip()
        title = self._clean_text(record.get("title") or "")
        content = self._clean_text(
            record.get("content")
            or record.get("caption")
            or record.get("text")
            or record.get("description")
            or record.get("desc")
            or title
        )
        if not source_post_id or not content:
            return None

        published_at, published_ts = self._normalize_datetime(
            record.get("published_at")
            or record.get("createTime")
            or record.get("create_time")
            or record.get("created_date_time")
            or record.get("timestamp")
            or record.get("takenAt")
            or record.get("takenAtTimestamp")
            or record.get("createdAt")
            or record.get("time")
        )
        author_name = self._clean_text(
            record.get("author_name")
            or record.get("author_username")
            or record.get("authorUsername")
            or record.get("nickname")
            or record.get("ownerUsername")
            or record.get("username")
            or record.get("ownerFullName")
            or record.get("pageName")
            or record.get("profileName")
            or ""
        )
        source_keywords = self._normalize_source_keywords(record)

        return {
            "platform": platform,
            "source_type": source_type,
            "source_post_id": source_post_id,
            "author_id": str(
                record.get("author_id")
                or record.get("author_user_id")
                or record.get("authorUserId")
                or record.get("user_id")
                or record.get("ownerId")
                or record.get("userId")
                or record.get("pageId")
                or ""
            ).strip(),
            "author_name": author_name,
            "title": title or self._build_title(content),
            "content": content,
            "clean_content": self._clean_text(content),
            "analysis_content": self._clean_text(content),
            "note_url": str(
                record.get("note_url") or record.get("post_url") or record.get("url") or record.get("postUrl") or ""
            ).strip(),
            "published_at": published_at,
            "published_ts": published_ts,
            "like_count": self._parse_metric(
                record.get("like_count")
                or record.get("liked_count")
                or record.get("likeCount")
                or record.get("likesCount")
                or record.get("likes")
            ),
            "comment_count": self._parse_metric(
                record.get("comment_count")
                or record.get("comments_count")
                or record.get("commentCount")
                or record.get("commentsCount")
                or record.get("comments")
            ),
            "share_count": self._parse_metric(
                record.get("share_count")
                or record.get("shared_count")
                or record.get("shareCount")
                or record.get("sharesCount")
                or record.get("shares")
            ),
            "collect_count": self._parse_metric(
                record.get("collect_count") or record.get("collected_count") or record.get("savesCount") or record.get("saves")
            ),
            "media_urls": json.dumps(self._normalize_media_urls(record), ensure_ascii=False),
            "tags": json.dumps(self._normalize_tags(record, content), ensure_ascii=False),
            "source_keyword": source_keywords[0] if source_keywords else self._clean_text(record.get("source_keyword") or ""),
            "source_keywords": json.dumps(source_keywords, ensure_ascii=False),
            "source_file": str(source_file),
            "raw_json": json.dumps(record, ensure_ascii=False),
            "imported_at": self._now_iso(),
        }

    def _merge_posts(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        preferred = incoming if incoming.get("published_ts", 0) >= existing.get("published_ts", 0) else existing
        fallback = existing if preferred is incoming else incoming
        merged = dict(preferred)

        merged["content"] = self._prefer_richer_text(existing.get("content", ""), incoming.get("content", ""))
        merged["clean_content"] = self._prefer_richer_text(
            existing.get("clean_content", ""),
            incoming.get("clean_content", ""),
        )
        merged["analysis_content"] = self._prefer_richer_text(
            existing.get("analysis_content", ""),
            incoming.get("analysis_content", ""),
        )
        merged["title"] = preferred.get("title") or fallback.get("title") or self._build_title(merged["content"])
        merged["author_id"] = preferred.get("author_id") or fallback.get("author_id") or ""
        merged["author_name"] = preferred.get("author_name") or fallback.get("author_name") or ""
        merged["note_url"] = preferred.get("note_url") or fallback.get("note_url") or ""
        merged["like_count"] = max(int(existing.get("like_count", 0) or 0), int(incoming.get("like_count", 0) or 0))
        merged["comment_count"] = max(
            int(existing.get("comment_count", 0) or 0),
            int(incoming.get("comment_count", 0) or 0),
        )
        merged["share_count"] = max(int(existing.get("share_count", 0) or 0), int(incoming.get("share_count", 0) or 0))
        merged["collect_count"] = max(
            int(existing.get("collect_count", 0) or 0),
            int(incoming.get("collect_count", 0) or 0),
        )

        merged_media_urls = sorted(
            set(self._json_to_list(existing.get("media_urls"))) | set(self._json_to_list(incoming.get("media_urls")))
        )
        merged_tags = sorted(set(self._json_to_list(existing.get("tags"))) | set(self._json_to_list(incoming.get("tags"))))
        merged_keywords = sorted(
            set(self._json_to_list(existing.get("source_keywords"))) | set(self._json_to_list(incoming.get("source_keywords")))
        )

        merged["media_urls"] = json.dumps(merged_media_urls, ensure_ascii=False)
        merged["tags"] = json.dumps(merged_tags, ensure_ascii=False)
        merged["source_keywords"] = json.dumps(merged_keywords, ensure_ascii=False)
        merged["source_keyword"] = preferred.get("source_keyword") or (merged_keywords[0] if merged_keywords else "")
        merged["imported_at"] = self._now_iso()
        return merged

    def _build_source_rows(self, normalized: dict[str, Any]) -> list[dict[str, Any]]:
        keywords = self._json_to_list(normalized.get("source_keywords"))
        if not keywords:
            keywords = [self._clean_text(normalized.get("source_keyword") or "")]

        rows = []
        for keyword in keywords:
            rows.append(
                {
                    "platform": normalized["platform"],
                    "source_type": normalized["source_type"],
                    "source_post_id": normalized["source_post_id"],
                    "source_keyword": self._clean_text(keyword),
                    "source_file": normalized["source_file"],
                    "imported_at": normalized["imported_at"],
                    "raw_json": normalized["raw_json"],
                }
            )
        return rows

    def _dedupe_source_rows(self, source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for row in source_rows:
            key = (
                row["platform"],
                row["source_post_id"],
                row["source_file"],
                row["source_keyword"],
            )
            deduped[key] = row
        return list(deduped.values())

    def _build_event_ready_row(self, post_row: dict[str, Any]) -> Optional[dict[str, Any]]:
        platform = str(post_row.get("platform") or "").strip().lower()
        if platform not in {"wb", "fb"}:
            return None
        if platform == "fb" and not self._is_facebook_heat_relevant(post_row):
            return None

        content = str(post_row.get("content") or "")
        clean_content = str(post_row.get("clean_content") or build_clean_content(content))
        analysis_content = str(post_row.get("analysis_content") or build_analysis_content(content))
        source_keywords = self._json_to_list(post_row.get("source_keywords"))
        hashtags = extract_hashtags(content)
        mentions = extract_mentions(content)
        topic_seed_terms = make_topic_seed(hashtags, source_keywords, analysis_content)
        relevance_flags = build_flags(clean_content, analysis_content, hashtags)

        return {
            "platform": platform,
            "source_post_id": post_row["source_post_id"],
            "author_name": post_row.get("author_name", ""),
            "published_at": post_row.get("published_at", ""),
            "published_ts": post_row.get("published_ts", 0),
            "note_url": post_row.get("note_url", ""),
            "clean_content": clean_content,
            "analysis_content": analysis_content,
            "hashtags": json.dumps(hashtags, ensure_ascii=False),
            "mentions": json.dumps(mentions, ensure_ascii=False),
            "source_keywords": json.dumps(source_keywords, ensure_ascii=False),
            "topic_seed_terms": json.dumps(topic_seed_terms, ensure_ascii=False),
            "relevance_flags": json.dumps(relevance_flags, ensure_ascii=False),
            "status": "ready",
            "source_file": post_row.get("source_file", ""),
            "raw_json": post_row.get("raw_json", "{}"),
            "updated_at": self._now_iso(),
        }

    def _is_facebook_heat_relevant(self, post_row: dict[str, Any]) -> bool:
        content = self._clean_text(post_row.get("content") or "")
        title = self._clean_text(post_row.get("title") or "")
        author_name = self._clean_text(post_row.get("author_name") or "")
        note_url = self._clean_text(post_row.get("note_url") or "")
        tags = " ".join(self._json_to_list(post_row.get("tags")))
        source_keywords = " ".join(self._json_to_list(post_row.get("source_keywords")))

        context_text = " ".join(part for part in (title, content, author_name, note_url, tags) if part).lower()
        keyword_text = source_keywords.lower()
        combined_text = " ".join(part for part in (context_text, keyword_text) if part)

        has_macau_location = any(cue in context_text for cue in FB_MACAU_LOCATION_CUES)
        has_local_entity = any(cue in context_text for cue in FB_LOCAL_ENTITY_CUES)

        if has_macau_location or has_local_entity:
            return True

        if any(cue in combined_text for cue in FB_SJM_BOOKTOK_NOISE_CUES):
            return False

        ambiguous_hit = any(re.search(rf"\b{re.escape(cue)}\b", combined_text) for cue in FB_AMBIGUOUS_SHORT_CUES)
        if ambiguous_hit:
            return False

        return False

    def _build_post_where_clause(
        self,
        platform: Optional[str] = None,
        q: str = "",
        source_type: Optional[str] = None,
        author_name: str = "",
    ) -> tuple[str, list[Any]]:
        clauses = []
        params: list[Any] = []

        normalized_platform = self._normalize_platform_filter(platform)
        if normalized_platform:
            clauses.append("platform = ?")
            params.append(normalized_platform)
        if source_type:
            clauses.append("source_type = ?")
            params.append(source_type)
        if author_name:
            clauses.append("author_name LIKE ?")
            params.append(f"%{author_name}%")
        if q:
            clauses.append(
                "(title LIKE ? OR content LIKE ? OR clean_content LIKE ? OR analysis_content LIKE ? OR author_name LIKE ?)"
            )
            keyword = f"%{q}%"
            params.extend([keyword, keyword, keyword, keyword, keyword])

        return (f"WHERE {' AND '.join(clauses)}" if clauses else "", params)

    def _normalize_media_urls(self, record: dict[str, Any]) -> list[str]:
        media_urls: list[str] = []

        for key in ("image_list", "images", "image_urls", "pictures", "video_list"):
            value = record.get(key)
            if isinstance(value, str):
                media_urls.extend([item.strip() for item in value.split(",") if item.strip()])
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.strip():
                        media_urls.append(item.strip())
                    elif isinstance(item, dict):
                        for nested_key in ("url", "image_url", "large", "origin"):
                            nested_value = item.get(nested_key)
                            if isinstance(nested_value, str) and nested_value.strip():
                                media_urls.append(nested_value.strip())
                                break

        pics = record.get("pics")
        if isinstance(pics, list):
            for item in pics:
                if isinstance(item, str) and item.strip():
                    media_urls.append(item.strip())
                elif isinstance(item, dict):
                    for nested_key in ("large", "url", "videoSrc", "pid"):
                        nested_value = item.get(nested_key)
                        if isinstance(nested_value, str) and nested_value.strip():
                            media_urls.append(nested_value.strip())
                            break

        video_url = record.get("video_url") or record.get("video_download_url")
        if isinstance(video_url, str) and video_url.strip():
            media_urls.append(video_url.strip())

        return sorted(set(media_urls))

    def _normalize_tags(self, record: dict[str, Any], content: str) -> list[str]:
        tag_value = record.get("tag_list") or record.get("tags") or []
        if isinstance(tag_value, list):
            tags = [self._clean_text(item) for item in tag_value if self._clean_text(item)]
        elif isinstance(tag_value, str):
            tags = [self._clean_text(item) for item in re.split(r"[,，#\s]+", tag_value) if self._clean_text(item)]
        else:
            tags = []

        hashtags = extract_hashtags(content)
        return sorted(set(tags) | set(hashtags))

    def _normalize_source_keywords(self, record: dict[str, Any]) -> list[str]:
        source_keywords: list[str] = []

        raw_keywords = record.get("source_keywords")
        if isinstance(raw_keywords, list):
            source_keywords.extend(self._clean_text(item) for item in raw_keywords if self._clean_text(item))
        elif isinstance(raw_keywords, str):
            parsed = self._json_to_list(raw_keywords)
            if parsed:
                source_keywords.extend(self._clean_text(item) for item in parsed if self._clean_text(item))
            else:
                source_keywords.extend(
                    self._clean_text(item) for item in re.split(r"[,，]+", raw_keywords) if self._clean_text(item)
                )

        source_keyword = self._clean_text(record.get("source_keyword") or "")
        if source_keyword:
            source_keywords.append(source_keyword)

        deduped: list[str] = []
        seen = set()
        for item in source_keywords:
            if item in seen:
                continue
            deduped.append(item)
            seen.add(item)
        return deduped

    def _normalize_datetime(self, value: Any) -> tuple[str, int]:
        if value is None or value == "":
            return "", 0

        if isinstance(value, (int, float)):
            timestamp = int(value)
            if timestamp > 10_000_000_000:
                timestamp_ms = timestamp
            else:
                timestamp_ms = timestamp * 1000
            dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            return dt.astimezone().isoformat(), timestamp_ms

        raw = str(value).strip()
        if raw.isdigit():
            return self._normalize_datetime(int(raw))

        cleaned = raw.replace("Z", "+00:00")
        for parser in (
            lambda text: datetime.fromisoformat(text),
            lambda text: datetime.strptime(text, "%Y-%m-%d %H:%M:%S"),
            lambda text: datetime.strptime(text, "%Y-%m-%d %H:%M"),
            lambda text: datetime.strptime(text, "%Y-%m-%d"),
        ):
            try:
                dt = parser(cleaned)
                break
            except ValueError:
                dt = None
        if dt is not None:
            timestamp_ms = int(dt.timestamp() * 1000)
            return (dt.isoformat() if dt.tzinfo else raw), timestamp_ms

        weibo_ts = parse_weibo_created_at_to_timestamp(raw)
        if weibo_ts:
            dt = datetime.fromtimestamp(weibo_ts, tz=timezone.utc).astimezone()
            return dt.isoformat(), weibo_ts * 1000

        return raw, 0

    def _serialize_post_row(self, row: dict[str, Any]) -> dict[str, Any]:
        for key in ("media_urls", "tags", "source_keywords"):
            row[key] = self._json_to_list(row.get(key))
        row["platform_label"] = PLATFORM_LABELS.get(row.get("platform", ""), row.get("platform", ""))
        row["source_file"] = self._relative_source_path(row.get("source_file", ""))
        return row

    def _relative_source_path(self, value: str) -> str:
        if not value:
            return ""
        path = Path(value)
        try:
            return str(path.relative_to(PROJECT_ROOT))
        except ValueError:
            return str(path)

    def _parse_metric(self, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)

        text = str(value).strip().replace(",", "")
        if not text or text.lower() in {"none", "null", "nan"}:
            return 0

        match = re.search(r"(\d+(?:\.\d+)?)\s*([万亿kKmM]?)", text)
        if not match:
            return 0

        number = float(match.group(1))
        unit = match.group(2)
        multiplier = {
            "": 1,
            "k": 1_000,
            "K": 1_000,
            "m": 1_000_000,
            "M": 1_000_000,
            "万": 10_000,
            "亿": 100_000_000,
        }[unit]
        return int(number * multiplier)

    def _build_title(self, content: str, max_length: int = 42) -> str:
        compact = self._clean_text(content)
        if len(compact) <= max_length:
            return compact
        return f"{compact[: max_length - 1]}…"

    def _clean_text(self, value: Any) -> str:
        text = str(value or "")
        return re.sub(r"\s+", " ", text).strip()

    def _prefer_richer_text(self, left: str, right: str) -> str:
        left_text = self._clean_text(left)
        right_text = self._clean_text(right)
        return right_text if len(right_text) >= len(left_text) else left_text

    def _json_to_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [self._clean_text(item) for item in value if self._clean_text(item)]
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return []
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, list):
                return [self._clean_text(item) for item in payload if self._clean_text(item)]
            return [self._clean_text(item) for item in re.split(r"[,，]+", value) if self._clean_text(item)]
        return []

    def _json_to_list_of_any(self, value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str) and value.strip():
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                return self._json_to_list(value)
            if isinstance(payload, list):
                return payload
        return []

    def _json_to_dict(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                payload = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(payload, dict):
                return payload
        return {}

    def _is_content_file(self, path: Path) -> bool:
        name = path.name.lower()
        if "comment" in name or "creator_creators" in name or "heat_" in name:
            return False
        if "ready" in name or "dedup" in name:
            return False
        return "contents" in name

    def _infer_platform(self, path: Path) -> str:
        parts = {part.lower() for part in path.parts}
        if "weibo" in parts:
            return "wb"
        if "facebook" in parts or "fb" in parts:
            return "fb"
        raise ValueError(f"Unsupported platform path: {path}")

    def _infer_source_type(self, path: Path) -> str:
        name = path.name.lower()
        if "creator_" in name:
            return "creator"
        if "search_" in name:
            return "search"
        return "dataset"

    def _normalize_platform_filter(self, platform: Optional[str]) -> Optional[str]:
        if not platform:
            return None
        normalized = platform.strip().lower()
        aliases = {
            "weibo": "wb",
            "wb": "wb",
            "facebook": "fb",
            "fb": "fb",
        }
        if normalized not in aliases:
            raise ValueError(f"Unsupported platform: {platform}")
        return aliases[normalized]

    def _normalize_window_mode(self, window_mode: Optional[str]) -> str:
        normalized = str(window_mode or "weekly").strip().lower()
        if normalized not in {"weekly", "monthly", "quarterly"}:
            raise ValueError(f"Unsupported window_mode: {window_mode}")
        return normalized

    def _normalize_dashboard_category_filter(self, dashboard_category: Optional[str]) -> str:
        normalized = str(dashboard_category or "").strip().lower()
        allowed = {
            "",
            "accommodation",
            "experience",
            "food",
            "exhibition",
            "shopping",
            "entertainment",
        }
        if normalized not in allowed:
            raise ValueError(f"Unsupported dashboard_category: {dashboard_category}")
        return normalized

    def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _now_iso(self) -> str:
        return datetime.now().astimezone().isoformat()


full_web_analytics_service = ProjectAnalyticsService()
