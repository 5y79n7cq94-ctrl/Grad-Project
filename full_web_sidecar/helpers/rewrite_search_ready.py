# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/tools/rewrite_search_ready.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#
# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

from __future__ import annotations

import argparse
import html
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


HASHTAG_PATTERN = re.compile(r"#([^#\n]+)#")
MENTION_PATTERN = re.compile(r"@([\w\-\u4e00-\u9fff]+)")
URL_PATTERN = re.compile(r"https?://\S+")
MULTISPACE_PATTERN = re.compile(r"\s+")
EMOJI_ONLY_PATTERN = re.compile(r"[\W_]+", re.UNICODE)

REPOST_NOISE_PATTERNS = [
    re.compile(r"(?:网页链接\s*){1,}"),
    re.compile(r"的微博视频"),
    re.compile(r"微博视频"),
    re.compile(r"复制一下这行字.*?看笔记。?"),
    re.compile(r"\bcr[.:：]?\s*[A-Za-z0-9_\-./]+", re.IGNORECASE),
    re.compile(r"[©️]+\s*[A-Za-z0-9_\-./]+"),
]

EVENT_HINT_PATTERN = re.compile(
    r"演唱会|音乐节|官宣|开票|售票|巡演|见面会|fancon|fm|day1|day2|比赛|联赛|嘉年华|展览|快闪",
    re.IGNORECASE,
)
TRAVEL_HINT_PATTERN = re.compile(
    r"攻略|打卡|酒店|住宿|口岸|过关|景点|缆车|发财车|蛋挞|美食|拍照|路线|一日游|citywalk",
    re.IGNORECASE,
)
QUESTION_HINT_PATTERN = re.compile(r"有没有|求推荐|蹲个|怎么|吗[？?]?|可不可以|能不能")
FANDOM_HINT_PATTERN = re.compile(
    r"一巡|二巡|day1|day2|直拍|补邮|成长册|招新|pb|ccl|for u|饭拍|签售|物料|图包|舞台",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="重写微博搜索 ready 数据，生成更适合 event/topic/heat analysis 的中间层 JSONL。",
    )
    parser.add_argument(
        "--dedup",
        default="data/weibo/jsonl/search_contents_dedup.jsonl",
        help="去重后的基础输入文件。",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/weibo/jsonl",
        help="原始搜索内容文件所在目录，会自动聚合 search_contents_YYYY-MM-DD.jsonl 中的补充元数据。",
    )
    parser.add_argument(
        "--output",
        default="data/weibo/jsonl/search_contents_ready.jsonl",
        help="输出 ready 文件路径。",
    )
    parser.add_argument(
        "--backup",
        default="data/weibo/jsonl/search_contents_ready.legacy.jsonl",
        help="若原 ready 已存在，则先备份到该路径。",
    )
    parser.add_argument(
        "--overwrite-backup",
        action="store_true",
        help="允许覆盖已存在的 backup 文件。",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


def normalize_whitespace(text: str) -> str:
    return MULTISPACE_PATTERN.sub(" ", text).strip()


def normalize_text(text: str) -> str:
    return normalize_whitespace(html.unescape(text or ""))


def extract_hashtags(text: str) -> list[str]:
    seen: set[str] = set()
    hashtags: list[str] = []
    for match in HASHTAG_PATTERN.findall(text):
        value = normalize_whitespace(match)
        if value and value not in seen:
            seen.add(value)
            hashtags.append(value)
    return hashtags


def extract_mentions(text: str) -> list[str]:
    seen: set[str] = set()
    mentions: list[str] = []
    for match in MENTION_PATTERN.findall(text):
        value = normalize_whitespace(match)
        if value and value not in seen:
            seen.add(value)
            mentions.append(value)
    return mentions


def build_clean_content(raw_content: str) -> str:
    text = normalize_text(raw_content)
    text = URL_PATTERN.sub(" ", text)
    text = HASHTAG_PATTERN.sub(r" \1 ", text)
    text = MENTION_PATTERN.sub(r" \1 ", text)
    return normalize_whitespace(text)


def build_analysis_content(raw_content: str) -> str:
    text = normalize_text(raw_content)
    text = URL_PATTERN.sub(" ", text)
    text = HASHTAG_PATTERN.sub(" ", text)
    text = MENTION_PATTERN.sub(" ", text)

    for pattern in REPOST_NOISE_PATTERNS:
        text = pattern.sub(" ", text)

    text = re.sub(r"[|｜]+", " ", text)
    text = re.sub(r"[•·]+", " ", text)
    text = re.sub(r"\s*[🛒📱📖🎨🚡🌸💜✨🍉📷📹]+", " ", text)
    text = normalize_whitespace(text)
    return text


def to_int(value: Any) -> int:
    if value is None:
        return 0
    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def build_raw_index(raw_dir: Path) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    source_keywords_map: dict[str, set[str]] = defaultdict(set)

    for path in sorted(raw_dir.glob("search_contents_20*.jsonl")):
        rows = load_jsonl(path)
        for row in rows:
            note_id = str(row.get("note_id") or "").strip()
            if not note_id:
                continue

            if note_id not in indexed:
                indexed[note_id] = row
            else:
                # Prefer the row with richer metadata / latest modification.
                current = indexed[note_id]
                if to_int(row.get("last_modify_ts")) >= to_int(current.get("last_modify_ts")):
                    indexed[note_id] = row

            source_keyword = normalize_whitespace(str(row.get("source_keyword") or ""))
            if source_keyword:
                source_keywords_map[note_id].add(source_keyword)

    for note_id, row in indexed.items():
        row["source_keywords"] = sorted(source_keywords_map.get(note_id, set()))

    return indexed


def make_topic_seed(hashtags: list[str], source_keywords: list[str], analysis_content: str) -> list[str]:
    seeds: list[str] = []
    seen: set[str] = set()

    for item in hashtags + source_keywords:
        value = normalize_whitespace(item)
        if value and value not in seen:
            seen.add(value)
            seeds.append(value)

    compact_patterns = [
        r"[\w\u4e00-\u9fff]{2,40}演唱会",
        r"[\w\u4e00-\u9fff]{2,40}音乐节",
        r"[\w\u4e00-\u9fff]{2,40}联赛",
        r"[\w\u4e00-\u9fff]{2,40}展",
    ]
    for pattern in compact_patterns:
        for match in re.findall(pattern, analysis_content, re.IGNORECASE):
            value = normalize_whitespace(match)
            if value and value not in seen:
                seen.add(value)
                seeds.append(value)

    return seeds[:8]


def build_flags(clean_content: str, analysis_content: str, hashtags: list[str]) -> dict[str, bool]:
    clean_len = len(clean_content)
    analysis_len = len(analysis_content)
    return {
        "low_info": analysis_len < 12,
        "emoji_or_symbol_only": bool(clean_content) and not EMOJI_ONLY_PATTERN.sub("", clean_content),
        "link_heavy": clean_content.count("网页链接") >= 2 or clean_content.count("http") >= 2,
        "likely_repost": "网页链接" in clean_content or "微博视频" in clean_content,
        "likely_event": bool(EVENT_HINT_PATTERN.search(analysis_content)),
        "likely_travel": bool(TRAVEL_HINT_PATTERN.search(analysis_content)),
        "likely_question": bool(QUESTION_HINT_PATTERN.search(analysis_content)),
        "likely_fandom": bool(FANDOM_HINT_PATTERN.search(clean_content)) or any(len(tag) >= 8 for tag in hashtags),
        "has_hashtags": bool(hashtags),
        "short_text": clean_len <= 20,
    }


def build_analysis_text(
    analysis_content: str,
    hashtags: list[str],
    mentions: list[str],
    source_keywords: list[str],
    create_date_time: str,
    nickname: str,
) -> str:
    lines = [
        f"正文: {analysis_content or '(空)'}",
        f"话题: {hashtags}",
        f"提及: {mentions}",
        f"来源关键词: {source_keywords}",
        f"作者: {nickname or '未知'}",
        f"发布时间: {create_date_time or '未知'}",
    ]
    return "\n".join(lines)


def rewrite_ready(dedup_rows: list[dict[str, Any]], raw_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ready_rows: list[dict[str, Any]] = []

    for row in dedup_rows:
        note_id = str(row.get("note_id") or "")
        raw_row = raw_index.get(note_id, {})
        raw_content = normalize_text(str(raw_row.get("content") or row.get("content") or ""))

        hashtags = extract_hashtags(raw_content)
        mentions = extract_mentions(raw_content)
        clean_content = build_clean_content(raw_content)
        analysis_content = build_analysis_content(raw_content)
        source_keywords = raw_row.get("source_keywords") or []

        like_count = to_int(raw_row.get("liked_count", row.get("like_count")))
        comment_count = to_int(raw_row.get("comments_count", row.get("comment_count")))
        share_count = to_int(raw_row.get("shared_count", row.get("share_count")))
        engagement_total = like_count + comment_count + share_count
        engagement_weighted = like_count + comment_count * 3 + share_count * 2

        flags = build_flags(clean_content, analysis_content, hashtags)
        topic_seed_terms = make_topic_seed(hashtags, source_keywords, analysis_content)

        ready_rows.append(
            {
                "note_id": note_id,
                "note_url": raw_row.get("note_url") or row.get("note_url"),
                "create_time": raw_row.get("create_time"),
                "create_date_time": raw_row.get("create_date_time") or row.get("create_date_time"),
                "last_modify_ts": raw_row.get("last_modify_ts"),
                "user_id": raw_row.get("user_id"),
                "nickname": raw_row.get("nickname"),
                "gender": raw_row.get("gender"),
                "ip_location": raw_row.get("ip_location"),
                "profile_url": raw_row.get("profile_url"),
                "avatar": raw_row.get("avatar"),
                "source_keywords": source_keywords,
                "content": raw_content,
                "hashtags": hashtags,
                "mentions": mentions,
                "clean_content": clean_content,
                "analysis_content": analysis_content,
                "analysis_text": build_analysis_text(
                    analysis_content=analysis_content,
                    hashtags=hashtags,
                    mentions=mentions,
                    source_keywords=source_keywords,
                    create_date_time=str(raw_row.get("create_date_time") or row.get("create_date_time") or ""),
                    nickname=str(raw_row.get("nickname") or ""),
                ),
                "topic_seed_terms": topic_seed_terms,
                "text_len": len(clean_content),
                "analysis_len": len(analysis_content),
                "like_count": like_count,
                "comment_count": comment_count,
                "share_count": share_count,
                "engagement_total": engagement_total,
                "engagement_weighted": engagement_weighted,
                "flags": flags,
            }
        )

    return ready_rows


def main() -> int:
    args = parse_args()
    dedup_path = Path(args.dedup)
    raw_dir = Path(args.raw_dir)
    output_path = Path(args.output)
    backup_path = Path(args.backup)

    dedup_rows = load_jsonl(dedup_path)
    raw_index = build_raw_index(raw_dir)

    if output_path.exists():
        if backup_path.exists() and not args.overwrite_backup:
            raise FileExistsError(f"backup 文件已存在，请先处理或使用 --overwrite-backup: {backup_path}")
        dump_jsonl(backup_path, load_jsonl(output_path))

    ready_rows = rewrite_ready(dedup_rows, raw_index)
    dump_jsonl(output_path, ready_rows)

    summary = {
        "input_rows": len(dedup_rows),
        "output_rows": len(ready_rows),
        "output": str(output_path),
        "backup": str(backup_path) if output_path.exists() else None,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
