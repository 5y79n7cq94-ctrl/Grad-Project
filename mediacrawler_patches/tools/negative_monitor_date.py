from __future__ import annotations

import datetime
import os
import re
from typing import List, Optional, Tuple

from tools import utils


def _parse_yyyy_mm_dd(s: str) -> Optional[datetime.date]:
    s = (s or "").strip()[:10]
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def negative_monitor_crawl_range_from_env() -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
    """子進程由 task_manager 設置 NEGATIVE_MONITOR_CRAWL_FROM_DATE / TO_DATE（YYYY-MM-DD）。"""
    fd = _parse_yyyy_mm_dd(os.environ.get("NEGATIVE_MONITOR_CRAWL_FROM_DATE", ""))
    td = _parse_yyyy_mm_dd(os.environ.get("NEGATIVE_MONITOR_CRAWL_TO_DATE", ""))
    return fd, td


def negative_monitor_note_date_in_range(
    d: Optional[datetime.date],
    from_d: Optional[datetime.date],
    to_d: Optional[datetime.date],
) -> bool:
    if not from_d and not to_d:
        return True
    if d is None:
        return True
    if from_d and d < from_d:
        return False
    if to_d and d > to_d:
        return False
    return True


def xhs_note_detail_publish_date(note: dict) -> Optional[datetime.date]:
    """小紅書 note 詳情里的 time 一般為毫秒時間戳。"""
    t = note.get("time")
    if not isinstance(t, (int, float)) or t <= 0:
        return None
    try:
        if t > 1e12:
            sec = t / 1000.0
        elif t > 1e9:
            sec = float(t)
        else:
            return None
        dt = datetime.datetime.utcfromtimestamp(sec)
        return dt.date()
    except Exception:
        return None


def weibo_mblog_candidate_dates(mblog: dict) -> List[datetime.date]:
    """微博 mblog：發布與可選編輯時間。"""
    out: List[datetime.date] = []
    seen = set()
    for key in ("created_at", "edit_at"):
        val = (mblog or {}).get(key)
        if not val:
            continue
        try:
            d = utils.rfc2822_to_china_datetime(str(val)).date()
            if d not in seen:
                seen.add(d)
                out.append(d)
        except Exception:
            continue
    return out


def weibo_mblog_any_candidate_in_range(
    mblog: dict,
    from_d: Optional[datetime.date],
    to_d: Optional[datetime.date],
) -> bool:
    if not from_d and not to_d:
        return True
    for d in weibo_mblog_candidate_dates(mblog):
        if negative_monitor_note_date_in_range(d, from_d, to_d):
            return True
    return False


def weibo_api_comment_tree_any_in_range(
    comments: Optional[list],
    from_d: Optional[datetime.date],
    to_d: Optional[datetime.date],
) -> bool:
    """微博的contents和comments任一created_at在區間內即 True。"""
    if not from_d and not to_d:
        return True
    if not comments:
        return False
    stack: list = list(comments)
    while stack:
        c = stack.pop()
        if not isinstance(c, dict):
            continue
        ca = c.get("created_at")
        if ca:
            try:
                d = utils.rfc2822_to_china_datetime(str(ca)).date()
                if negative_monitor_note_date_in_range(d, from_d, to_d):
                    return True
            except Exception:
                pass
        subs = c.get("comments")
        if isinstance(subs, list):
            stack.extend(subs)
    return False
