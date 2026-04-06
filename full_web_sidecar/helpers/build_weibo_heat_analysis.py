# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/tools/build_weibo_heat_analysis.py
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
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import jieba.analyse


EVENT_SUFFIX_PATTERN = re.compile(
    r"("
    r"演唱会|音乐节|美食节|美食節|联赛|比赛|嘉年华|巡遊|巡游|展览|快闪|巡演|见面会|发布会|论坛|論壇|博览会|博覽會|"
    r"东超联赛|世界杯|锦标赛|公开赛|冠军赛|大师赛|总决赛|巡回赛|系列赛|挑战赛|资格赛|"
    r"concert|festival|carnival|parade|forum|expo|world\s*cups?|world\s*cup|championships?|grand\s*prix|match\s*race|regatta"
    r")",
    re.IGNORECASE,
)
DATE_NOISE_PATTERN = re.compile(
    r"\b(?:20\d{2}[./-]?\d{2}[./-]?\d{2}|20\d{2}|26\d{4}|[0-1]?\d\.[0-3]?\d(?:-\d{1,2})?)\b|(?:[0-1]?\d月[0-3]?\d(?:-\d{1,2})?)"
)
DAY_NOISE_PATTERN = re.compile(r"\b(?:day\s*[12]|d[12]|day1|day2|澳门场|澳门站|澳门d[12])\b", re.IGNORECASE)
SPACE_PATTERN = re.compile(r"\s+")
ENGLISH_EVENT_SUFFIX_PATTERN = re.compile(
    r"(concert|festival|carnival|parade|forum|expo|world\s*cups?|world\s*cup|championships?|grand\s*prix|match\s*race|regatta)$",
    re.IGNORECASE,
)

TOPIC_RULES: list[tuple[str, list[str]]] = [
    ("澳门抢票与票务规则", ["抢票", "开票", "售票", "票务", "出票", "购票券", "大麦", "实名", "高会", "门票", "票价"]),
    ("澳门住宿与拼房", ["酒店", "住宿", "拼房", "住在珠海", "平价一点", "订了酒店", "入住", "房型", "房价", "酒店服务"]),
    ("澳门通关与交通", ["口岸", "过关", "发财车", "高铁", "珠海", "打车", "巴士", "交通", "出关", "接驳"]),
    ("澳门景点打卡与旅行攻略", ["攻略", "打卡", "缆车", "大炮台", "威尼斯人", "巴黎人", "伦敦人", "官也街", "一日游", "路线", "citywalk", "景点", "玩法"]),
    ("澳门美食体验", ["蛋挞", "美食", "餐厅", "下午茶", "甜品", "餐酒", "主厨", "探店", "咖啡", "自助餐", "餐单"]),
    ("澳门展览与艺术展陈", ["展览", "艺术展", "展陈", "展会", "博览会", "艺文", "画展", "快闪展", "装置展"]),
    ("澳门购物与商场消费", ["购物", "逛街", "商场", "免税", "折扣", "买手信", "手信", "专柜", "四季名店", "新八佰伴", "金沙广场", "品牌店"]),
    ("澳门体育赛事与观赛", ["联赛", "比赛", "篮球", "足球", "东超", "马拉松", "观赛", "世界杯", "锦标赛", "公开赛", "冠军赛", "大师赛", "总决赛", "wtt", "ufc"]),
    ("澳门博彩与赌场话题", ["赌场", "博彩", "赌王", "借款", "欠款", "永利", "发财树"]),
    ("赴澳门看演出的行程讨论", ["演唱会", "音乐节", "一巡", "二巡", "场馆", "拼盘音乐节", "见面会", "巡演", "嘉年华", "专场"]),
]

TOPIC_TO_DASHBOARD_CATEGORY = {
    "澳门住宿与拼房": "accommodation",
    "澳门景点打卡与旅行攻略": "experience",
    "澳门通关与交通": "experience",
    "澳门美食体验": "food",
    "澳门展览与艺术展陈": "exhibition",
    "澳门购物与商场消费": "shopping",
    "澳门体育赛事与观赛": "entertainment",
    "赴澳门看演出的行程讨论": "entertainment",
    "澳门抢票与票务规则": "entertainment",
    "澳门博彩与赌场话题": "entertainment",
    "澳门娱乐活动讨论": "entertainment",
}

NON_DASHBOARD_TOPIC_KEYS = {
    "泛澳门讨论",
    "低信息量泛澳门表达",
    "粉圈物料搬运与返图",
}

ENGLISH_EVENT_GENERIC_PREFIX_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "every",
    "flower",
    "for",
    "hours",
    "in",
    "just",
    "like",
    "macau",
    "macao",
    "more",
    "the",
    "this",
    "with",
}

DASHBOARD_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "accommodation": ["酒店", "住宿", "入住", "房型", "房价", "度假村", "客房", "拼房"],
    "experience": ["攻略", "打卡", "景点", "路线", "玩法", "citywalk", "口岸", "过关", "交通", "发财车", "设施", "乐园"],
    "food": ["美食", "餐厅", "下午茶", "甜品", "咖啡", "蛋挞", "自助餐", "探店", "餐酒", "主厨"],
    "exhibition": ["展览", "艺术展", "展会", "博览会", "画展", "展陈", "快闪展", "装置展"],
    "shopping": ["购物", "商场", "免税", "折扣", "手信", "专柜", "品牌店", "新八佰伴", "四季名店", "金沙广场"],
    "entertainment": ["演唱会", "音乐节", "见面会", "巡演", "嘉年华", "联赛", "比赛", "世界杯", "锦标赛", "公开赛", "冠军赛", "大师赛", "总决赛", "观赛", "夜场", "博彩", "赌场", "wtt", "ufc"],
}

EVENT_FIRST_CATEGORY_CHECK_ORDER = (
    "entertainment",
    "exhibition",
    "shopping",
    "food",
    "accommodation",
    "experience",
)

STRUCTURED_EVENT_TERMS = [
    "世界杯",
    "锦标赛",
    "公开赛",
    "冠军赛",
    "大师赛",
    "总决赛",
    "巡回赛",
    "系列赛",
    "挑战赛",
    "资格赛",
    "揭幕战",
    "观赛",
    "澳门站",
    "澳门赛",
    "巡遊",
    "巡游",
    "論壇",
    "论坛",
    "博覽會",
    "博览会",
    "parade",
    "forum",
    "expo",
    "festival",
    "concert",
    "world cup",
    "world cups",
    "championship",
    "grand prix",
    "match race",
    "regatta",
]

SPORT_OR_EVENT_ACRONYM_PATTERN = re.compile(r"\b(?:UFC|WTT|ATP|WTA|F1|FE|NBA|CBA|ITTF)\b", re.IGNORECASE)
EVENT_FAMILY_SUFFIX_REGEX = (
    r"(?:演唱会|音乐节|美食节|美食節|联赛|比赛|嘉年华|巡遊|巡游|展览|快闪|巡演|见面会|发布会|论坛|論壇|博览会|博覽會|"
    r"世界杯|锦标赛|公开赛|冠军赛|大师赛|总决赛|巡回赛|系列赛|挑战赛|资格赛|"
    r"concert|festival|carnival|parade|forum|expo|world\s*cups?|world\s*cup|championships?|grand\s*prix|match\s*race|regatta)"
)
MACAU_LEADING_EVENT_PATTERN = re.compile(
    rf"(澳门[A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{{0,24}}{EVENT_FAMILY_SUFFIX_REGEX})",
    re.IGNORECASE,
)
MACAU_TRAILING_EVENT_PATTERN = re.compile(
    rf"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-]{{1,18}}\s*澳门{EVENT_FAMILY_SUFFIX_REGEX})",
    re.IGNORECASE,
)
EVENT_FAMILY_CONTEXT_MARKERS = [
    "出发",
    "抵达",
    "亮相",
    "回顾",
    "直播",
    "倒计时",
    "冲击",
    "赞助商",
    "阵容揭晓",
    "将不参加",
    "已抵达",
    "最新名单",
    "名单",
    "希望",
    "成员",
    "最终名额",
    "起落平安",
    "官摄",
    "官宣",
]
TABLE_TENNIS_CUES = [
    "乒乓",
    "乒乓球",
    "国乒",
    "国际乒联",
    "ittf",
    "wtt",
    "单打",
    "单打世界杯",
    "抽签仪式",
    "场地适应训练",
    "孙颖莎",
    "王楚钦",
    "王曼昱",
    "王艺迪",
    "蒯曼",
    "陈幸同",
    "覃予萱",
    "梁靖崑",
    "周启豪",
    "温瑞博",
    "陈垣宇",
    "樊振东",
    "林诗栋",
    "李春丽",
    "银河综艺馆",
    "澳门银河综艺馆",
]

NON_TABLE_TENNIS_WORLD_CUP_CUES = [
    "足球",
    "篮球",
    "排球",
    "电竞",
    "fifa",
    "世界杯预选赛",
]

GENERIC_EVENT_PREFIXES = [
    "澳门演唱会",
    "澳门音乐节",
    "澳门东超联赛",
    "澳门嘉年华",
]

EVENT_BRAND_MARKERS = [
    "世界演唱会",
    "巡回演唱会",
    "粉丝见面会",
    "见面会",
    "荣耀之战",
    "专场",
]

SOFT_EVENT_HASHTAG_MARKERS = [
    "计划",
    "荣耀之战",
    "浪漫主义",
    "春风来信",
    "大无畏",
    "巡回",
    "粉丝",
]

MACAU_LOCATION_CUES = [
    "澳门",
    "澳門",
    "macao",
    "macau",
    "氹仔",
    "凼仔",
    "路氹",
    "新濠影汇",
    "银河综艺馆",
    "金光综艺馆",
    "澳门威尼斯人",
    "威尼斯人",
    "澳门巴黎人",
    "巴黎人",
    "澳门伦敦人",
    "伦敦人",
    "澳门户外表演区",
    "户外表演区",
    "澳门新濠影汇",
    "澳门银河",
]

MACAU_EVENT_BINDING_CUES = [
    "澳门站",
    "澳門站",
    "澳门场",
    "澳門場",
    "澳门专场",
    "澳門專場",
    "澳门户外表演区",
    "澳門戶外表演區",
    "澳门新濠影汇",
    "澳門新濠影匯",
    "澳门银河",
    "澳門銀河",
    "澳门威尼斯人",
    "澳門威尼斯人",
    "澳门巴黎人",
    "澳門巴黎人",
    "澳门伦敦人",
    "澳門倫敦人",
    "澳门演唱会",
    "澳門演唱會",
    "澳门音乐节",
    "澳門音樂節",
    "澳门见面会",
    "澳門見面會",
    "macau",
    "macao",
    "新濠影汇",
    "银河综艺馆",
    "金光综艺馆",
    "户外表演区",
]

NON_MACAU_LOCATION_CUES = [
    "深圳",
    "广州",
    "香港",
    "北京",
    "上海",
    "杭州",
    "海口",
    "南宁",
    "长沙",
    "武汉",
    "成都",
    "重庆",
    "天津",
    "南京",
    "西安",
    "厦门",
    "苏州",
    "青岛",
    "福州",
    "郑州",
    "沈阳",
    "长春",
    "哈尔滨",
    "台北",
    "高雄",
    "首尔",
    "东京",
    "新加坡",
    "曼谷",
]

GENERIC_EVENT_NAMES = {
    "演唱会",
    "澳门演唱会",
    "这次演唱会",
    "第一次去看演唱会",
    "第一看演唱会",
    "如果有演唱会",
    "还有演唱会",
    "反正是都在看演唱会",
    "去之前他还说哎呀演唱会",
}

BAD_EVENT_SUBSTRINGS = [
    "抢票",
    "购票",
    "票权",
    "门槛",
    "群",
    "对比",
    "状态",
    "重要的关口",
    "出收楼",
    "愿意出",
    "痛包",
    "中转站",
    "变卦",
    "自己一个人",
    "不想去",
    "有没有宝宝",
    "所有tzn",
    "流量扶持",
    "微博旅行家",
    "带着微博去旅行",
    "生活手记",
    "日常分享",
    "一分钟视频创作季",
    "金牌剪刀手",
    "守护星愿",
    "陪伴信",
]

BAD_EVENT_NOISE_PATTERNS = [
    re.compile(r"^(?:庆祝.{0,12})?给大家抽(?:一个|一波|点)", re.IGNORECASE),
    re.compile(r"^(?:一起来|一起去|一起看|快来).{0,8}(?:看)?(?:演唱会|音乐节|见面会|嘉年华)", re.IGNORECASE),
    re.compile(r"^偶遇几位.{0,16}(?:看|去).{0,6}(?:演唱会|音乐节|见面会)", re.IGNORECASE),
    re.compile(r"^现在去刷.{0,24}(?:演唱会|音乐节|见面会|嘉年华)", re.IGNORECASE),
    re.compile(r"^不想去(?:演唱会|音乐节|见面会|嘉年华)的", re.IGNORECASE),
    re.compile(r"^马上.{0,16}(?:演唱会|音乐节|专场|见面会)", re.IGNORECASE),
]

EVENT_CANDIDATE_NOISE_PATTERNS = [
    re.compile(r"^(?:想知道|想问问?|问问有没有|帮抢|代抢|一个人去|是怎么|谁要去|心心念念|记性可能|最终名额|如果|回顾一下|距离去年|看pad|中午回看|宝宝起落平安|优酷的|已(?:经)?正式锁定|就连我都抢不到|人人都骂你贵|吗的)", re.IGNORECASE),
]

EVENT_ACTION_PREFIX_PATTERNS = [
    re.compile(r"^(?:去|去完|看|追|想去|想看|想和|一起来看|一起去看|来|开|和|考完试|第一次经历)", re.IGNORECASE),
    re.compile(r"^(?:我|你|他|她|我们|大家|周末|今天|现在|突然|刚开始|当天)", re.IGNORECASE),
]

EVENT_BAD_PREFIX_MARKERS = [
    "来不了",
    "来中国开",
    "想起来",
    "才知道",
    "还记得",
    "限定记忆",
    "这两个团",
    "又没那么多",
    "第一次去看",
    "看不上",
    "预感这次",
    "不会又",
    "评论",
    "打卡",
    "扩散",
]

EVENT_SENTENCE_NOISE_MARKERS = [
    "第一次去看",
    "第一次看",
    "看不上",
    "预感这次",
    "不会又",
    "有没有宝能",
    "我有点好奇",
    "并转发扩散",
    "转发扩散",
]

FUTURE_EVENT_CUES = [
    "将于",
    "将在",
    "即将",
    "开售",
    "开票",
    "预售",
    "官宣",
    "举行",
    "举办",
    "定档",
    "抢票",
]


@dataclass
class CommentStats:
    fetched_count: int
    fetched_like_sum: int
    fetched_sub_comment_sum: int
    unique_comment_authors: int
    top_comments: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="构建微博搜索结果的 event/topic heat analysis 产物。")
    parser.add_argument(
        "--ready",
        default="data/weibo/jsonl/search_contents_ready.jsonl",
        help="重写后的 ready 文件。",
    )
    parser.add_argument(
        "--comments",
        nargs="*",
        default=[
            "data/weibo/jsonl/search_comments_2026-03-22.jsonl",
            "data/weibo/jsonl/search_comments_2026-03-23.jsonl",
        ],
        help="评论 JSONL 文件列表。",
    )
    parser.add_argument(
        "--output-dir",
        default="data/weibo/heat",
        help="输出目录。",
    )
    parser.add_argument(
        "--alias-registry",
        default="config/weibo_event_aliases.json",
        help="事件别名映射文件。",
    )
    parser.add_argument(
        "--parent-registry",
        default="config/weibo_event_parent_groups.json",
        help="事件父级归并映射文件。",
    )
    parser.add_argument(
        "--organizer-registry",
        default="config/weibo_organizer_registry.json",
        help="主办方/场馆归属映射文件。",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="cluster 榜单截取条数。",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def dump_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def normalize_text(value: Any) -> str:
    return SPACE_PATTERN.sub(" ", str(value or "")).strip()


def text_contains_any(text: str, keywords: list[str]) -> bool:
    normalized = normalize_text(text).lower()
    return any(normalize_text(keyword).lower() in normalized for keyword in keywords)


def has_structured_event_signal(text: str) -> bool:
    normalized = normalize_text(text)
    lowered = normalized.lower()
    if any(term in normalized for term in STRUCTURED_EVENT_TERMS):
        return True
    if SPORT_OR_EVENT_ACRONYM_PATTERN.search(normalized):
        return True
    if ("macau" in lowered or "macao" in lowered or "澳门" in normalized) and (
        "比赛" in normalized or "联赛" in normalized or "演唱会" in normalized or "展览" in normalized
    ):
        return True
    return False


def normalize_alias_key(value: str) -> str:
    text = normalize_text(value).lower()
    text = re.sub(r"[\"'“”‘’《》【】#@]+", " ", text)
    text = DATE_NOISE_PATTERN.sub(" ", text)
    text = re.sub(r"(?<!\d)20\d{2}(?!\d)", " ", text)
    text = DAY_NOISE_PATTERN.sub(" ", text)
    text = SPACE_PATTERN.sub(" ", text).strip()
    return text


def load_event_alias_registry(path: Path) -> tuple[dict[str, str], set[str]]:
    if not path.exists():
        return {}, set()

    payload = json.loads(path.read_text(encoding="utf-8"))
    alias_to_canonical: dict[str, str] = {}
    canonical_set: set[str] = set()
    for canonical, aliases in payload.items():
        canonical_text = normalize_text(canonical)
        canonical_set.add(canonical_text)
        alias_to_canonical[normalize_alias_key(canonical_text)] = canonical_text
        for alias in aliases:
            alias_to_canonical[normalize_alias_key(alias)] = canonical_text
    return alias_to_canonical, canonical_set


def load_event_parent_registry(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    child_to_parent: dict[str, str] = {}
    for parent, children in payload.items():
        parent_text = normalize_text(parent)
        for child in children:
            child_to_parent[normalize_alias_key(child)] = parent_text
            child_to_parent[normalize_text(child)] = parent_text
    return child_to_parent


def load_organizer_registry(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    registry: dict[str, dict[str, Any]] = {}
    for key, profile in payload.items():
        registry[key] = {
            "key": key,
            "name": normalize_text(profile.get("name") or key),
            "type": normalize_text(profile.get("type") or ""),
            "host_terms": [normalize_alias_key(item) for item in profile.get("host_terms") or []],
            "venue_terms": [normalize_alias_key(item) for item in profile.get("venue_terms") or []],
            "brand_terms": [normalize_alias_key(item) for item in profile.get("brand_terms") or []],
            "event_terms": [normalize_alias_key(item) for item in profile.get("event_terms") or []],
        }
    return registry


def lookup_alias(alias_to_canonical: dict[str, str] | None, value: str) -> str | None:
    if not alias_to_canonical:
        return None
    return alias_to_canonical.get(value) or alias_to_canonical.get(normalize_alias_key(value))


def lookup_parent_event(child_to_parent: dict[str, str] | None, value: str | None) -> str | None:
    if not child_to_parent or not value:
        return None
    normalized = normalize_text(value)
    return child_to_parent.get(normalized) or child_to_parent.get(normalize_alias_key(normalized))


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


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


def normalize_comment_text(text: str) -> str:
    text = normalize_text(text)
    text = re.sub(r"@[\w\-\u4e00-\u9fff]+", " ", text)
    return SPACE_PATTERN.sub(" ", text).strip()


def build_comment_map(paths: list[Path]) -> dict[str, CommentStats]:
    indexed: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    for path in paths:
        if not path.exists():
            continue
        for row in load_jsonl(path):
            comment_id = str(row.get("comment_id") or "")
            note_id = str(row.get("note_id") or "")
            if not comment_id or not note_id:
                continue
            indexed[note_id][comment_id] = row

    stats_map: dict[str, CommentStats] = {}
    for note_id, comment_dict in indexed.items():
        comments = list(comment_dict.values())
        comments.sort(
            key=lambda item: (
                to_int(item.get("comment_like_count")),
                to_int(item.get("sub_comment_count")),
                len(normalize_comment_text(item.get("content") or "")),
            ),
            reverse=True,
        )

        top_comments: list[dict[str, Any]] = []
        for item in comments:
            content = normalize_comment_text(item.get("content") or "")
            if not content:
                continue
            top_comments.append(
                {
                    "comment_id": item.get("comment_id"),
                    "content": content[:160],
                    "like_count": to_int(item.get("comment_like_count")),
                    "sub_comment_count": to_int(item.get("sub_comment_count")),
                    "nickname": item.get("nickname"),
                }
            )
            if len(top_comments) >= 5:
                break

        stats_map[note_id] = CommentStats(
            fetched_count=len(comments),
            fetched_like_sum=sum(to_int(item.get("comment_like_count")) for item in comments),
            fetched_sub_comment_sum=sum(to_int(item.get("sub_comment_count")) for item in comments),
            unique_comment_authors=len({str(item.get("user_id") or "") for item in comments if item.get("user_id")}),
            top_comments=top_comments,
        )

    return stats_map


def canonicalize_event_name(name: str, alias_to_canonical: dict[str, str] | None = None) -> str:
    text = normalize_text(name)
    if alias_to_canonical:
        mapped = lookup_alias(alias_to_canonical, text)
        if mapped:
            return mapped
    text = DATE_NOISE_PATTERN.sub(" ", text)
    text = re.sub(r"(?<!\d)20\d{2}(?!\d)", " ", text)
    text = DAY_NOISE_PATTERN.sub(" ", text)
    text = re.sub(r"[|｜•·【】#@]+", " ", text)
    text = re.sub(r"(?:官宣了?|相关|图片|视频|返图|返场|更新|合集|直拍)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"澳门的(?=(?:演唱会|音乐节|见面会|嘉年华))", "澳门", text)
    text = re.sub(r"^\s*年(?=[A-Za-z0-9\u4e00-\u9fff])", " ", text)
    text = re.sub(r"^\d+(?:\s*-\s*\d+)?", " ", text)
    text = re.sub(r"^[\"'“”‘’《》\[\]\(\)\-:：,，.;；!?！？\s]+", "", text)
    text = re.sub(r"[\"'“”‘’《》\[\]\(\)\-:：,，.;；!?！？\s]+$", "", text)
    text = SPACE_PATTERN.sub(" ", text).strip()

    if re.search(r"TF家族荣耀之战\s*演唱会", text):
        return "TF家族荣耀之战演唱会"
    if re.search(r"荣耀之战\s*演唱会", text):
        return "荣耀之战演唱会"
    if re.search(r"浪漫主义\s*演唱会", text):
        return "浪漫主义演唱会"
    if re.search(r"大无畏\s*世界演唱会", text):
        return "大无畏世界演唱会"
    if re.search(r"澳门\s*东超联赛", text):
        return "澳门东超联赛"
    if "澳门演唱会" in text:
        prefix = text.split("澳门演唱会")[0].strip()
        if prefix:
            latin_match = re.search(r"([A-Za-z]{2,12})$", prefix)
            chinese_match = re.search(r"([\u4e00-\u9fff]{2,6})$", prefix)
            extracted = ""
            if latin_match:
                extracted = latin_match.group(1)
            elif chinese_match:
                extracted = chinese_match.group(1)
            if extracted and not any(marker in extracted for marker in BAD_EVENT_SUBSTRINGS):
                return f"{extracted} 澳门演唱会"
        return "澳门演唱会"
    if re.search(r"\b(?:ittf\s*)?world\s*cups?\s*(?:macao|macau)\b", text, re.IGNORECASE):
        return "澳门乒乓球世界杯"
    if re.search(r"\bmacau\s+international\s+parade\b", text, re.IGNORECASE):
        return "澳门国际幻彩大巡游"
    if re.search(r"\bmacao\s+international\s+parade\b", text, re.IGNORECASE):
        return "澳门国际幻彩大巡游"
    if alias_to_canonical:
        mapped = lookup_alias(alias_to_canonical, text)
        if mapped:
            return mapped
    return text


def compact_text(value: Any) -> str:
    return normalize_text(value).replace(" ", "").lower()


def looks_like_loose_english_event_name(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False

    suffix_match = ENGLISH_EVENT_SUFFIX_PATTERN.search(normalized)
    if not suffix_match:
        return False

    prefix = normalize_text(normalized[: suffix_match.start()])
    prefix = re.sub(r"\b(?:macau|macao)\b", " ", prefix, flags=re.IGNORECASE)
    prefix = re.sub(r"[澳门澳門]", " ", prefix)
    prefix = SPACE_PATTERN.sub(" ", prefix).strip()
    if not prefix:
        return True

    if re.search(r"[，。！？,!?]", prefix):
        return True

    tokens = re.findall(r"[A-Za-z0-9]+", prefix)
    if not tokens:
        return True
    if len(tokens) > 4:
        return True

    lowercase_tokens = [token.lower() for token in tokens]
    if all(token in ENGLISH_EVENT_GENERIC_PREFIX_STOPWORDS for token in lowercase_tokens):
        return True

    has_uppercase_signal = any(any(char.isupper() for char in token) for token in tokens)
    has_acronym_signal = any(token.isupper() and len(token) >= 2 for token in tokens)
    has_digit_signal = any(any(char.isdigit() for char in token) for token in tokens)
    if not (has_uppercase_signal or has_acronym_signal or has_digit_signal):
        return True

    return False


def split_text_segments(text: str) -> list[str]:
    return [segment.strip() for segment in re.split(r"[。！？!?；;\n]+", text) if segment.strip()]


def looks_like_event_noise_phrase(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    compact = normalized.replace(" ", "")
    return any(pattern.search(compact) for pattern in BAD_EVENT_NOISE_PATTERNS)


def has_sentence_like_event_noise(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    compact = normalized.replace(" ", "")
    return any(marker in compact for marker in EVENT_SENTENCE_NOISE_MARKERS)


def has_candidate_noise_prefix(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    compact = normalized.replace(" ", "")
    return any(pattern.search(compact) for pattern in EVENT_CANDIDATE_NOISE_PATTERNS)


def looks_like_contextual_event_family(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    compact = normalized.replace(" ", "")
    if has_candidate_noise_prefix(normalized):
        return True
    return any(marker in compact for marker in EVENT_FAMILY_CONTEXT_MARKERS)


def is_historical_event_reference(post: dict[str, Any]) -> bool:
    text = normalize_text(post.get("analysis_content") or "")
    if not text:
        return False

    years = [int(year) for year in re.findall(r"(?<!\d)(20\d{2})(?!\d)", text)]
    if not years:
        return False

    create_time = to_int(post.get("create_time"))
    if not create_time:
        return False

    post_year = datetime.fromtimestamp(create_time).year
    if max(years) >= post_year:
        return False
    if any(cue in text for cue in FUTURE_EVENT_CUES):
        return False
    return True


def is_specific_event_name(text: str, alias_to_canonical: dict[str, str] | None = None) -> bool:
    normalized = canonicalize_event_name(text, alias_to_canonical)
    if not normalized:
        return False
    if looks_like_event_noise_phrase(normalized):
        return False
    if has_sentence_like_event_noise(normalized):
        return False
    if has_candidate_noise_prefix(normalized):
        return False
    if looks_like_loose_english_event_name(normalized):
        return False
    if is_generic_event_name(normalized, alias_to_canonical):
        return False
    if any(marker in normalized for marker in ['《', '》', '“', '”', '"', "'"]):
        return True
    if any(marker in normalized for marker in EVENT_BRAND_MARKERS):
        prefix = normalize_text(EVENT_SUFFIX_PATTERN.split(normalized, maxsplit=1)[0])
        if prefix and not any(pattern.search(prefix) for pattern in EVENT_ACTION_PREFIX_PATTERNS):
            return True

    match = EVENT_SUFFIX_PATTERN.search(normalized)
    if not match:
        return False

    prefix = normalize_text(normalized[: match.start()])
    compact_prefix = prefix.replace(" ", "")
    if len(compact_prefix) < 2 or len(compact_prefix) > 20:
        return False
    if re.search(r"[，。！？,!?]", prefix):
        return False
    if prefix.endswith(("的", "了", "吗", "吧", "呢")):
        return False
    if any(pattern.search(prefix) for pattern in EVENT_ACTION_PREFIX_PATTERNS):
        return False
    if any(marker in prefix for marker in EVENT_BAD_PREFIX_MARKERS):
        return False
    return True


def find_alias_mentions(
    post: dict[str, Any],
    alias_to_canonical: dict[str, str] | None = None,
) -> list[str]:
    if not alias_to_canonical:
        return []

    haystacks = [
        normalize_alias_key(post.get("analysis_content") or ""),
        *[normalize_alias_key(item) for item in post.get("hashtags") or []],
        *[normalize_alias_key(item) for item in post.get("topic_seed_terms") or []],
    ]
    haystacks = [item for item in haystacks if item]

    seen: set[str] = set()
    matches: list[str] = []
    normalized_alias_items = [
        (normalize_alias_key(alias_key), canonical)
        for alias_key, canonical in alias_to_canonical.items()
    ]
    for alias_key, canonical in sorted(normalized_alias_items, key=lambda item: len(item[0]), reverse=True):
        if not alias_key or len(alias_key.replace(" ", "")) < 3:
            continue
        if any(alias_key in haystack for haystack in haystacks):
            if canonical not in seen:
                seen.add(canonical)
                matches.append(canonical)
    return matches


def build_event_mention_terms(
    post: dict[str, Any],
    raw_event_candidate: str | None,
    canonical_event_name: str | None,
    alias_to_canonical: dict[str, str] | None = None,
) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for item in [raw_event_candidate, canonical_event_name, *(post.get("hashtags") or []), *(post.get("topic_seed_terms") or [])]:
        value = normalize_text(item)
        if not value:
            continue
        if canonical_event_name and canonicalize_event_name(value, alias_to_canonical) != canonical_event_name:
            continue
        compact = compact_text(value)
        if compact and compact not in seen:
            seen.add(compact)
            terms.append(value)
    return terms


def find_event_context_windows(text: str, mention_terms: list[str], radius: int = 18) -> list[str]:
    compact = compact_text(text)
    windows: list[str] = []
    seen: set[tuple[int, int]] = set()
    for term in mention_terms:
        compact_term = compact_text(term)
        if len(compact_term) < 2:
            continue
        start = 0
        while True:
            idx = compact.find(compact_term, start)
            if idx < 0:
                break
            left = max(0, idx - radius)
            right = min(len(compact), idx + len(compact_term) + radius)
            key = (left, right)
            if key not in seen:
                seen.add(key)
                windows.append(compact[left:right])
            start = idx + len(compact_term)
    return windows


def compute_event_geo_score(
    post: dict[str, Any],
    raw_event_candidate: str | None,
    canonical_event_name: str | None,
    alias_to_canonical: dict[str, str] | None = None,
) -> float:
    if not canonical_event_name:
        return 0.0

    text = normalize_text(post.get("analysis_content") or "")
    compact_full_text = compact_text(text)
    compact_event_name = compact_text(canonical_event_name)
    mention_terms = build_event_mention_terms(post, raw_event_candidate, canonical_event_name, alias_to_canonical)
    windows = find_event_context_windows(text, mention_terms)

    macau_hits = 0
    macau_binding_hits = 0
    non_macau_hits = 0
    for window in windows:
        if any(cue in window for cue in MACAU_LOCATION_CUES):
            macau_hits += 1
        if any(cue in window for cue in MACAU_EVENT_BINDING_CUES):
            macau_binding_hits += 1
        if any(cue in window for cue in NON_MACAU_LOCATION_CUES):
            non_macau_hits += 1

    score = 0.0
    if any(cue in compact_event_name for cue in ("澳门", "澳門", "macao", "macau")):
        score += 3.0
    if macau_binding_hits:
        score += 2.5
    elif macau_hits:
        score += 0.5
    if non_macau_hits and not macau_binding_hits:
        score -= 3.0

    if not windows:
        if any(canonicalize_event_name(tag, alias_to_canonical) == canonical_event_name for tag in post.get("hashtags") or []):
            seed_terms = [compact_text(item) for item in post.get("topic_seed_terms") or []]
            if any(cue in compact_full_text for cue in MACAU_LOCATION_CUES) or "澳门" in seed_terms or "澳門" in seed_terms:
                score += 1.5
        elif any(cue in compact_full_text for cue in MACAU_LOCATION_CUES):
            score += 0.5

    segments = split_text_segments(text)
    if segments:
        event_segments = [
            segment for segment in segments if any(compact_text(term) in compact_text(segment) for term in mention_terms if compact_text(term))
        ]
        if event_segments:
            if any(any(cue in compact_text(segment) for cue in MACAU_EVENT_BINDING_CUES) for segment in event_segments):
                score += 1.0
            if any(any(cue in compact_text(segment) for cue in NON_MACAU_LOCATION_CUES) for segment in event_segments):
                score -= 1.0

    return round(score, 3)


def resolve_event_group_key(
    canonical_event_name: str | None,
    child_to_parent: dict[str, str] | None = None,
) -> str | None:
    if not canonical_event_name:
        return None
    return lookup_parent_event(child_to_parent, canonical_event_name) or canonical_event_name


def build_macau_event_variant(canonical_event_name: str | None) -> str | None:
    if not canonical_event_name:
        return None
    normalized = normalize_text(canonical_event_name)
    if "澳门" in normalized or any(marker in normalized for marker in EVENT_BRAND_MARKERS):
        return normalized

    suffix_match = EVENT_SUFFIX_PATTERN.search(normalized)
    if not suffix_match:
        return normalized

    suffix = suffix_match.group(1)
    prefix = normalize_text(normalized[: suffix_match.start()])
    if not prefix:
        return normalized
    if any(marker in normalized for marker in ['《', '》', '“', '”', '"', "'"]):
        return normalized
    if len(prefix.split()) > 2:
        return normalized
    if re.search(r"[，。！？,!?]", prefix):
        return normalized
    if any(pattern.search(prefix) for pattern in EVENT_ACTION_PREFIX_PATTERNS):
        return normalized
    if any(marker in prefix for marker in EVENT_BAD_PREFIX_MARKERS):
        return normalized
    if len(prefix.replace(" ", "")) > 20:
        return normalized
    return f"{prefix} 澳门{suffix}"


def infer_organizer(
    post: dict[str, Any],
    canonical_event_name: str | None,
    organizer_registry: dict[str, dict[str, Any]] | None = None,
) -> tuple[str | None, str | None, str | None, float, list[str]]:
    if not canonical_event_name or not organizer_registry:
        return "other", "Other / 其他", "other", 0.0, []

    text = normalize_alias_key(post.get("analysis_content") or "")
    event_name = normalize_alias_key(canonical_event_name)
    hashtags = [normalize_alias_key(item) for item in post.get("hashtags") or []]
    seeds = [normalize_alias_key(item) for item in post.get("topic_seed_terms") or []]
    haystacks = [item for item in [text, event_name, *hashtags, *seeds] if item]

    best_profile: dict[str, Any] | None = None
    best_score = 0.0
    best_evidence: list[str] = []

    for profile in organizer_registry.values():
        score = 0.0
        evidence: list[str] = []

        for term in profile.get("host_terms") or []:
            if term and any(term in haystack for haystack in haystacks):
                score += 4.0
                evidence.append(term)
        for term in profile.get("venue_terms") or []:
            if term and any(term in haystack for haystack in haystacks):
                score += 3.0
                evidence.append(term)
        for term in profile.get("event_terms") or []:
            if term and term in event_name:
                score += 2.5
                evidence.append(term)
        for term in profile.get("brand_terms") or []:
            if term and any(term in haystack for haystack in haystacks):
                score += 1.0
                evidence.append(term)

        if score > best_score:
            best_score = score
            best_profile = profile
            best_evidence = evidence

    if not best_profile or best_score < 2.0:
        fallback = organizer_registry.get("other") or {
            "key": "other",
            "name": "Other / 其他",
            "type": "other",
        }
        return fallback["key"], fallback["name"], fallback["type"], 0.0, []

    confidence = min(1.0, round(best_score / 6.0, 3))
    deduped_evidence: list[str] = []
    seen: set[str] = set()
    for item in best_evidence:
        if item and item not in seen:
            seen.add(item)
            deduped_evidence.append(item)

    return (
        best_profile["key"],
        best_profile["name"],
        best_profile["type"],
        confidence,
        deduped_evidence[:4],
    )


def summarize_organizer(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores: dict[str, float] = defaultdict(float)
    names: dict[str, str] = {}
    types: dict[str, str] = {}
    evidence_map: dict[str, list[str]] = defaultdict(list)

    for row in rows:
        key = normalize_text(row.get("organizer_key") or "")
        if not key:
            continue
        confidence = float(row.get("organizer_confidence") or 0.0)
        scores[key] += max(confidence, 0.1) + float(row.get("post_heat") or 0.0) * 0.05
        names[key] = normalize_text(row.get("organizer_name") or key)
        types[key] = normalize_text(row.get("organizer_type") or "")
        for item in row.get("organizer_evidence") or []:
            value = normalize_text(item)
            if value and value not in evidence_map[key]:
                evidence_map[key].append(value)

    if not scores:
        return {
            "organizer_key": "other",
            "organizer_name": "Other / 其他",
            "organizer_type": "other",
            "organizer_breakdown": {"other": 0.0},
            "organizer_evidence": [],
        }

    organizer_breakdown = {
        key: round(score, 3)
        for key, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)
    }
    top_key = next(iter(organizer_breakdown))
    return {
        "organizer_key": top_key,
        "organizer_name": names.get(top_key),
        "organizer_type": types.get(top_key),
        "organizer_breakdown": organizer_breakdown,
        "organizer_evidence": evidence_map.get(top_key, [])[:4],
    }


def has_event_marker(text: str) -> bool:
    return bool(EVENT_SUFFIX_PATTERN.search(text) or any(marker in text for marker in EVENT_BRAND_MARKERS))


def is_generic_event_name(text: str, alias_to_canonical: dict[str, str] | None = None) -> bool:
    normalized = canonicalize_event_name(text, alias_to_canonical)
    return normalized in GENERIC_EVENT_NAMES or normalized.startswith("澳门演唱会")


def is_bad_event_candidate(text: str, alias_to_canonical: dict[str, str] | None = None) -> bool:
    normalized = canonicalize_event_name(text, alias_to_canonical)
    if not normalized:
        return True
    if looks_like_event_noise_phrase(normalized):
        return True
    if has_sentence_like_event_noise(normalized):
        return True
    if has_candidate_noise_prefix(normalized):
        return True
    if looks_like_loose_english_event_name(normalized):
        return True
    if len(normalized) > 24 and not any(marker in normalized for marker in EVENT_BRAND_MARKERS):
        return True
    if any(marker in normalized for marker in BAD_EVENT_SUBSTRINGS):
        return True
    if normalized.count(" ") >= 4 and not any(marker in normalized for marker in EVENT_BRAND_MARKERS):
        return True
    return False


def candidate_score(source: str, text: str, alias_to_canonical: dict[str, str] | None = None) -> int:
    score = 0
    if source == "alias":
        score += 10
    elif source == "hashtag":
        score += 4
    elif source == "explicit":
        score += 3
    elif source == "special":
        score += 3
    elif source == "seed":
        score += 2

    normalized = canonicalize_event_name(text, alias_to_canonical)
    if any(marker in normalized for marker in EVENT_BRAND_MARKERS):
        score += 3
    if EVENT_SUFFIX_PATTERN.search(normalized):
        score += 2
    if is_generic_event_name(normalized, alias_to_canonical):
        score -= 2
    if is_bad_event_candidate(normalized, alias_to_canonical):
        score -= 5
    return score


def build_hashtag_event_candidates(post: dict[str, Any], alias_to_canonical: dict[str, str] | None = None) -> list[str]:
    hashtags = [canonicalize_event_name(item, alias_to_canonical) for item in post.get("hashtags") or []]
    analysis_content = normalize_text(post.get("analysis_content") or "")
    candidates: list[str] = []
    for hashtag in hashtags:
        if not hashtag:
            continue
        if looks_like_event_noise_phrase(hashtag):
            continue
        if is_bad_event_candidate(hashtag, alias_to_canonical):
            continue
        if has_event_marker(hashtag):
            candidates.append(hashtag)
            continue
        if (
            EVENT_SUFFIX_PATTERN.search(analysis_content)
            and 4 <= len(hashtag) <= 18
            and ("澳门" in hashtag or any(marker in hashtag for marker in SOFT_EVENT_HASHTAG_MARKERS))
        ):
            candidates.append(hashtag)
    return candidates


def extract_raw_event_candidate(
    post: dict[str, Any],
    post_type: str,
    alias_to_canonical: dict[str, str] | None = None,
) -> str | None:
    text = normalize_text(post.get("analysis_content") or "")
    hashtags = [normalize_text(item) for item in post.get("hashtags") or []]
    seeds = [normalize_text(item) for item in post.get("topic_seed_terms") or []]

    candidates: list[tuple[int, str]] = []

    for alias_hit in find_alias_mentions(post, alias_to_canonical):
        candidates.append((candidate_score("alias", alias_hit, alias_to_canonical), alias_hit))

    special_match = re.search(r"([A-Za-z0-9\u4e00-\u9fff]{2,20}).{0,10}(澳门演唱会)", text)
    if special_match:
        special = f"{special_match.group(1)} {special_match.group(2)}"
        candidates.append((candidate_score("special", special, alias_to_canonical), special))

    hashtag_candidates = build_hashtag_event_candidates(post, alias_to_canonical)
    for item in hashtag_candidates:
        candidates.append((candidate_score("hashtag", item, alias_to_canonical), item))

    for item in hashtags + seeds:
        if EVENT_SUFFIX_PATTERN.search(item):
            source = "hashtag" if item in hashtags else "seed"
            candidates.append((candidate_score(source, item, alias_to_canonical), item))

    explicit_patterns = [
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}演唱会)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}音乐节)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}东超联赛)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}联赛)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}(?:世界杯|锦标赛|公开赛|冠军赛|大师赛|总决赛|巡回赛|系列赛|挑战赛|资格赛))", re.IGNORECASE),
        re.compile(r"((?:\b(?:UFC|WTT|ATP|WTA|F1|FE|NBA|CBA|ITTF)\b)[A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{0,32}(?:澳门|Macau|Macao|澳门站|澳门赛)?)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}嘉年华)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}展览)", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,40}(?:巡遊|巡游|論壇|论坛|博覽會|博览会))", re.IGNORECASE),
        re.compile(r"([A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{1,60}(?:concert|festival|carnival|parade|forum|expo|world\s*cups?|world\s*cup|championships?|grand\s*prix|match\s*race|regatta)(?:[A-Za-z0-9\u4e00-\u9fff《》“”'·\-\s]{0,24})?(?:Macau|Macao)?)", re.IGNORECASE),
    ]

    for pattern in explicit_patterns:
        for match in pattern.findall(text):
            candidates.append((candidate_score("explicit", match, alias_to_canonical), match))

    lowered = text.lower()
    if "东超联赛" in text and "澳门" in text:
        candidates.append((candidate_score("explicit", "澳门东超联赛", alias_to_canonical), "澳门东超联赛"))
    if "concert" in lowered and "macau" in lowered:
        snippet = re.search(r"([A-Za-z0-9\s:'\-]{2,60}concert[^\n]{0,30}macau)", text, re.IGNORECASE)
        if snippet:
            candidates.append((candidate_score("explicit", snippet.group(1), alias_to_canonical), snippet.group(1)))
    for pattern in (
        re.compile(r"([A-Za-z0-9\s:'“”\"'\-]{2,80}(?:festival|carnival|parade|forum|expo|world\s*cups?|world\s*cup|championships?|grand\s*prix|match\s*race|regatta)[^\n]{0,24}(?:macau|macao))", re.IGNORECASE),
        re.compile(r"((?:macau|macao)[A-Za-z0-9\s:'“”\"'\-]{0,36}(?:festival|carnival|parade|forum|expo|world\s*cups?|world\s*cup|championships?|grand\s*prix|match\s*race|regatta))", re.IGNORECASE),
    ):
        for match in pattern.findall(text):
            candidates.append((candidate_score("explicit", match, alias_to_canonical), match))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0], reverse=True)
    best_candidate = None
    for score, candidate in candidates:
        normalized = canonicalize_event_name(candidate, alias_to_canonical)
        if score < 1:
            continue
        if looks_like_event_noise_phrase(normalized):
            continue
        if is_bad_event_candidate(normalized, alias_to_canonical):
            continue
        if post_type == "event_related_logistics" and is_generic_event_name(normalized, alias_to_canonical) and any(marker in text for marker in BAD_EVENT_SUBSTRINGS):
            continue
        best_candidate = normalized
        break

    if best_candidate:
        if is_generic_event_name(best_candidate, alias_to_canonical):
            for hashtag_candidate in hashtag_candidates:
                normalized_hashtag = canonicalize_event_name(hashtag_candidate, alias_to_canonical)
                if normalized_hashtag and not is_generic_event_name(normalized_hashtag, alias_to_canonical) and not is_bad_event_candidate(normalized_hashtag, alias_to_canonical):
                    return normalized_hashtag
        return best_candidate

    for generic in GENERIC_EVENT_PREFIXES:
        if generic in text and post_type in {"event_explicit", "generic_discussion", "news_discussion"}:
            return generic

    return None


def resolve_canonical_event_name(
    raw_event_candidate: str | None,
    alias_to_canonical: dict[str, str] | None = None,
) -> str | None:
    if not raw_event_candidate:
        return None
    normalized = canonicalize_event_name(raw_event_candidate, alias_to_canonical)
    if not normalized:
        return None
    if alias_to_canonical:
        mapped = lookup_alias(alias_to_canonical, normalized)
        if mapped:
            return mapped
    return normalized


def is_event_eligible(
    post: dict[str, Any],
    post_type: str,
    raw_event_candidate: str | None,
    canonical_event_name: str | None,
    canonical_event_set: set[str],
    alias_to_canonical: dict[str, str],
) -> tuple[bool, float]:
    text = normalize_text(post.get("analysis_content") or "")
    hashtags = [normalize_text(item) for item in post.get("hashtags") or []]
    if not canonical_event_name:
        return False, 0.0
    if post_type in {"noise_low_info", "travel_local_topic"}:
        return False, 0.0
    if looks_like_event_noise_phrase(canonical_event_name):
        return False, 0.0
    if has_sentence_like_event_noise(canonical_event_name):
        return False, 0.0
    if is_historical_event_reference(post):
        return False, 0.0
    if canonical_event_name not in canonical_event_set and not is_specific_event_name(canonical_event_name, alias_to_canonical):
        return False, 0.0

    geo_score = compute_event_geo_score(post, raw_event_candidate, canonical_event_name, alias_to_canonical)
    if geo_score <= 0:
        return False, 0.0

    score = 0.0
    if post_type == "event_explicit":
        score += 2.5
    elif post_type == "event_related_logistics":
        score += 1.5
    elif post_type == "fan_repost":
        score += 1.25
    elif post_type == "news_discussion":
        score += 1.5

    if canonical_event_name in canonical_event_set:
        score += 2.0
    if raw_event_candidate and normalize_alias_key(raw_event_candidate) in alias_to_canonical:
        score += 1.5
    if any(canonicalize_event_name(tag, alias_to_canonical) == canonical_event_name for tag in hashtags):
        score += 1.5
    if any(marker in canonical_event_name for marker in EVENT_BRAND_MARKERS):
        score += 1.0
    if not is_generic_event_name(canonical_event_name, alias_to_canonical):
        score += 1.0
    score += min(2.0, geo_score)

    noise_hits = sum(1 for marker in BAD_EVENT_SUBSTRINGS if marker in text)
    if noise_hits:
        score -= min(2.5, noise_hits * 0.5)
    if post_type == "event_related_logistics" and is_generic_event_name(canonical_event_name, alias_to_canonical):
        score -= 1.0

    eligible = score >= 3.0
    return eligible, round(score, 3)


def extract_event_name(
    post: dict[str, Any],
    post_type: str,
    alias_to_canonical: dict[str, str] | None = None,
    canonical_event_set: set[str] | None = None,
) -> str | None:
    alias_to_canonical = alias_to_canonical or {}
    canonical_event_set = canonical_event_set or set()
    raw_candidate = extract_raw_event_candidate(post, post_type, alias_to_canonical)
    canonical_name = resolve_canonical_event_name(raw_candidate, alias_to_canonical)
    eligible, _ = is_event_eligible(
        post=post,
        post_type=post_type,
        raw_event_candidate=raw_candidate,
        canonical_event_name=canonical_name,
        canonical_event_set=canonical_event_set,
        alias_to_canonical=alias_to_canonical,
    )
    return canonical_name if eligible else None


def classify_post_type(post: dict[str, Any]) -> str:
    flags = post.get("flags") or {}
    text = normalize_text(post.get("analysis_content") or "")

    if flags.get("low_info") or flags.get("emoji_or_symbol_only"):
        return "noise_low_info"
    if flags.get("likely_question") and ("酒店" in text or "拼房" in text or "口岸" in text or "珠海" in text):
        return "event_related_logistics"
    if any(keyword in text for keyword in ["抢票", "实名", "大麦", "出票", "购票券"]):
        return "event_related_logistics"
    if any(keyword in text for keyword in ["酒店", "住宿", "拼房", "发财车", "口岸", "珠海"]):
        return "event_related_logistics"
    if any(keyword in text for keyword in ["攻略", "打卡", "缆车", "景点", "蛋挞", "大炮台", "官也街", "威尼斯人", "巴黎人", "伦敦人", "美食"]):
        return "travel_local_topic"
    if has_structured_event_signal(text):
        return "event_explicit"
    if any(keyword in text for keyword in ["演唱会", "音乐节", "巡演", "见面会", "快闪", "嘉年华", "展览"]):
        return "event_explicit"
    if any(keyword in text for keyword in ["赌场", "博彩", "借款", "欠款", "赌王"]):
        return "news_discussion"
    if flags.get("likely_repost"):
        return "fan_repost"
    return "generic_discussion"


def infer_primary_topic(post: dict[str, Any], post_type: str) -> str:
    text = normalize_text(post.get("analysis_content") or "")

    for topic, keywords in TOPIC_RULES:
        if text_contains_any(text, keywords):
            return topic

    if post_type == "event_explicit":
        return "澳门娱乐活动讨论"
    if post_type == "event_related_logistics":
        return "澳门娱乐活动讨论"
    if post_type == "travel_local_topic":
        return "澳门景点打卡与旅行攻略"
    if post_type == "noise_low_info":
        return "低信息量泛澳门表达"
    return "泛澳门讨论"


def infer_dashboard_category(post: dict[str, Any], post_type: str) -> str:
    event_context = normalize_text(
        " ".join(
            [
                normalize_text(post.get("event_family_key") or ""),
                normalize_text(post.get("event_key") or ""),
                normalize_text(post.get("canonical_event_name") or ""),
                normalize_text(post.get("raw_event_candidate") or ""),
            ]
        )
    )
    if post_type in {"event_explicit", "event_related_logistics", "fan_repost", "news_discussion"} and event_context:
        for category in EVENT_FIRST_CATEGORY_CHECK_ORDER:
            if text_contains_any(event_context, DASHBOARD_CATEGORY_KEYWORDS[category]):
                return category

    primary_topic = normalize_text(post.get("primary_topic") or "")
    if primary_topic in TOPIC_TO_DASHBOARD_CATEGORY:
        return TOPIC_TO_DASHBOARD_CATEGORY[primary_topic]

    text = normalize_text(post.get("analysis_content") or "")
    for category in ("accommodation", "food", "exhibition", "shopping", "experience", "entertainment"):
        if text_contains_any(text, DASHBOARD_CATEGORY_KEYWORDS[category]):
            return category

    if post_type in {"event_explicit", "event_related_logistics"}:
        return "entertainment"
    if post_type == "travel_local_topic":
        return "experience"
    return ""


def has_high_precision_event_name(
    event_name: str | None,
    alias_to_canonical: dict[str, str] | None = None,
    canonical_event_set: set[str] | None = None,
) -> bool:
    normalized = canonicalize_event_name(event_name, alias_to_canonical)
    if not normalized:
        return False
    if is_bad_event_candidate(normalized, alias_to_canonical):
        return False
    if has_candidate_noise_prefix(normalized):
        return False
    if canonical_event_set and normalized in canonical_event_set:
        return True
    if any(marker in normalized for marker in ['《', '》', '“', '”', '"', "'"]):
        return True
    if any(marker in normalized for marker in EVENT_BRAND_MARKERS):
        return True
    lowered = normalized.lower()
    if SPORT_OR_EVENT_ACRONYM_PATTERN.search(normalized) and any(cue in lowered for cue in ["macau", "macao", "澳门"]):
        return True
    if any(term in normalized for term in STRUCTURED_EVENT_TERMS) and any(cue in lowered for cue in ["macau", "macao", "澳门"]):
        return True
    return is_specific_event_name(normalized, alias_to_canonical)


def resolve_event_family_key(
    event_key: str | None,
    *,
    analysis_text: str = "",
    alias_to_canonical: dict[str, str] | None = None,
    child_to_parent: dict[str, str] | None = None,
) -> str | None:
    normalized = canonicalize_event_name(event_key, alias_to_canonical)
    if not normalized:
        return None

    manual_parent = lookup_parent_event(child_to_parent, normalized)
    if manual_parent:
        return manual_parent

    combined_text = normalize_text(f"{normalized} {analysis_text}")
    combined_lower = combined_text.lower()
    normalized_lower = normalized.lower()

    if "世界杯" in normalized and "澳门" in combined_text:
        if any(cue in combined_lower for cue in NON_TABLE_TENNIS_WORLD_CUP_CUES):
            return "澳门世界杯"
        if normalized == "澳门世界杯" or normalized.endswith("澳门世界杯"):
            return "澳门乒乓球世界杯"
        if any(cue in combined_lower for cue in TABLE_TENNIS_CUES):
            return "澳门乒乓球世界杯"
        return "澳门世界杯"

    if any(marker in normalized_lower for marker in ["ittf world cup", "ittf world cups", "world cup macao", "world cups macao", "world cup macau", "world cups macau"]):
        return "澳门乒乓球世界杯"

    if "东超" in combined_text and "澳门" in combined_text:
        return "澳门东超联赛"

    suffix_match = EVENT_SUFFIX_PATTERN.search(normalized)
    if suffix_match and "澳门" in normalized:
        macau_index = normalized.find("澳门")
        prefix = normalize_text(normalized[:macau_index]) if macau_index > 0 else ""
        compact_prefix = prefix.replace(" ", "")
        if (
            compact_prefix
            and len(compact_prefix) <= 12
            and not any(pattern.search(prefix) for pattern in EVENT_ACTION_PREFIX_PATTERNS)
            and not any(marker in prefix for marker in EVENT_BAD_PREFIX_MARKERS)
            and not looks_like_contextual_event_family(normalized)
        ):
            return normalized

    for pattern in (MACAU_LEADING_EVENT_PATTERN, MACAU_TRAILING_EVENT_PATTERN):
        for match in pattern.findall(normalized):
            candidate = canonicalize_event_name(match, alias_to_canonical)
            if not candidate:
                continue
            if looks_like_contextual_event_family(candidate):
                continue
            if is_bad_event_candidate(candidate, alias_to_canonical):
                continue
            return candidate

    if "澳门" in normalized and EVENT_SUFFIX_PATTERN.search(normalized) and not looks_like_contextual_event_family(normalized):
        return normalized

    if "macau" in normalized_lower or "macao" in normalized_lower:
        return normalized
    return normalized


def compute_quality_weight(post: dict[str, Any], post_type: str) -> float:
    flags = post.get("flags") or {}
    weight = 1.0
    if flags.get("low_info"):
        weight *= 0.2
    if flags.get("emoji_or_symbol_only"):
        weight *= 0.2
    if flags.get("link_heavy"):
        weight *= 0.65
    if flags.get("likely_repost"):
        weight *= 0.8
    if flags.get("likely_question"):
        weight *= 1.05
    if post_type == "event_explicit":
        weight *= 1.1
    if post_type == "event_related_logistics":
        weight *= 1.05
    if post_type == "fan_repost":
        weight *= 0.9
    return round(weight, 4)


def compute_post_scores(post: dict[str, Any], comment_stats: CommentStats | None, latest_ts: int) -> dict[str, float]:
    like_count = to_int(post.get("like_count"))
    comment_count = to_int(post.get("comment_count"))
    share_count = to_int(post.get("share_count"))

    fetched_comment_count = comment_stats.fetched_count if comment_stats else 0
    fetched_comment_like_sum = comment_stats.fetched_like_sum if comment_stats else 0
    fetched_sub_comment_sum = comment_stats.fetched_sub_comment_sum if comment_stats else 0

    base_engagement = like_count + comment_count * 3 + share_count * 2
    discussion_strength = comment_count + fetched_comment_count + fetched_sub_comment_sum
    comment_value = fetched_comment_like_sum * 0.5

    create_time = to_int(post.get("create_time"))
    age_hours = max(0.0, (latest_ts - create_time) / 3600) if latest_ts and create_time else 0.0
    recency_factor = math.exp(-age_hours / 72.0)

    return {
        "base_engagement": base_engagement,
        "discussion_strength": discussion_strength,
        "comment_value": comment_value,
        "recency_factor": recency_factor,
        "raw_score": math.log1p(base_engagement + discussion_strength + comment_value) * recency_factor,
    }


def build_cluster_keywords(texts: list[str], hashtags: list[str]) -> list[str]:
    joined = "\n".join([text for text in texts if text]).strip()
    if not joined:
        return hashtags[:6]

    keywords = [word for word, _ in jieba.analyse.extract_tags(joined, topK=12, withWeight=True)]
    result: list[str] = []
    seen: set[str] = set()
    for item in hashtags + keywords:
        value = normalize_text(item)
        if len(value) < 2:
            continue
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result[:8]


def select_top_posts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=lambda row: row["post_heat"], reverse=True)
    result: list[dict[str, Any]] = []
    for row in sorted_rows[:5]:
        result.append(
            {
                "note_id": row["note_id"],
                "nickname": row.get("nickname"),
                "snippet": normalize_text(row.get("analysis_content") or "")[:160],
                "post_heat": round(row["post_heat"], 4),
                "engagement_total": row.get("engagement_total"),
                "event_key": row.get("event_key"),
                "primary_topic": row.get("primary_topic"),
                "dashboard_category": row.get("dashboard_category"),
            }
        )
    return result


def select_top_comments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        for comment in row.get("top_comments") or []:
            candidates.append(
                {
                    **comment,
                    "note_id": row["note_id"],
                    "post_heat": row["post_heat"],
                }
            )

    candidates.sort(
        key=lambda item: (
            to_int(item.get("like_count")),
            to_int(item.get("sub_comment_count")),
            item.get("post_heat", 0.0),
        ),
        reverse=True,
    )
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in candidates:
        text = normalize_text(item.get("content"))
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(
            {
                "note_id": item.get("note_id"),
                "content": text[:160],
                "like_count": to_int(item.get("like_count")),
                "sub_comment_count": to_int(item.get("sub_comment_count")),
                "nickname": item.get("nickname"),
            }
        )
        if len(deduped) >= 5:
            break
    return deduped


def resolve_author_identity(post: dict[str, Any]) -> str:
    for field in ("user_id", "author_name", "nickname"):
        value = normalize_text(post.get(field) or "")
        if value:
            return value
    return ""


def compute_cluster_heat(rows: list[dict[str, Any]], latest_ts: int) -> dict[str, float]:
    total_engagement = sum(to_int(row.get("engagement_total")) for row in rows)
    total_discussion = sum(row.get("discussion_total", 0) for row in rows)
    unique_authors = len({identity for identity in (resolve_author_identity(row) for row in rows) if identity})
    recent_post_count = sum(1 for row in rows if latest_ts - to_int(row.get("create_time")) <= 24 * 3600)

    engagement_component = math.log1p(total_engagement)
    discussion_component = math.log1p(total_discussion)
    diversity_component = math.log1p(unique_authors)
    velocity_component = math.log1p(recent_post_count)

    heat = (
        engagement_component * 0.45
        + discussion_component * 0.25
        + diversity_component * 0.15
        + velocity_component * 0.15
    )
    return {
        "engagement_component": engagement_component,
        "discussion_component": discussion_component,
        "diversity_component": diversity_component,
        "velocity_component": velocity_component,
        "heat_score": round(heat, 4),
    }


def summarize_dashboard_category(
    rows: list[dict[str, Any]],
    *,
    cluster_key: str,
    cluster_field: str,
) -> tuple[str, float]:
    if cluster_key in NON_DASHBOARD_TOPIC_KEYS:
        return "", 0.0
    if cluster_key in TOPIC_TO_DASHBOARD_CATEGORY:
        return TOPIC_TO_DASHBOARD_CATEGORY[cluster_key], 1.0

    weighted_votes: Counter[str] = Counter()
    total_weight = 0.0
    for row in rows:
        category = normalize_text(row.get("dashboard_category") or "")
        if not category:
            continue
        weight = max(0.1, float(row.get("post_heat") or 0.0))
        weighted_votes[category] += weight
        total_weight += weight

    if not weighted_votes and cluster_field == "event_key":
        probe_text = cluster_key
        for category in ("entertainment", "exhibition", "shopping", "food", "accommodation", "experience"):
            if text_contains_any(probe_text, DASHBOARD_CATEGORY_KEYWORDS[category]):
                return category, 0.75

    if not weighted_votes or total_weight <= 0:
        return "", 0.0

    winner, winner_score = weighted_votes.most_common(1)[0]
    return winner, round(winner_score / total_weight, 4)


def build_promoted_event_posts(
    posts: list[dict[str, Any]],
    *,
    alias_to_canonical: dict[str, str] | None = None,
    canonical_event_set: set[str] | None = None,
) -> list[dict[str, Any]]:
    alias_to_canonical = alias_to_canonical or {}
    canonical_event_set = canonical_event_set or set()

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in posts:
        family_key = normalize_text(row.get("event_family_key") or "")
        if family_key:
            grouped[family_key].append(row)

    promoted: list[dict[str, Any]] = []
    for family_key, rows in grouped.items():
        strong_name = has_high_precision_event_name(
            family_key,
            alias_to_canonical=alias_to_canonical,
            canonical_event_set=canonical_event_set,
        )
        if not strong_name:
            continue

        post_count = len(rows)
        unique_authors = len({identity for identity in (resolve_author_identity(row) for row in rows) if identity})
        max_confidence = max(float(row.get("event_confidence") or 0.0) for row in rows)

        cluster_is_promoted = False
        if post_count >= 3 and unique_authors >= 2 and max_confidence >= 4.0:
            cluster_is_promoted = True
        elif post_count == 2 and max_confidence >= 4.5:
            cluster_is_promoted = True
        elif post_count == 1:
            row = rows[0]
            text = normalize_text(row.get("analysis_content") or "")
            has_alias_support = (
                normalize_alias_key(str(row.get("raw_event_candidate") or "")) in alias_to_canonical
                if row.get("raw_event_candidate")
                else False
            )
            has_hashtag_support = any(
                canonicalize_event_name(tag, alias_to_canonical) == row.get("canonical_event_name")
                for tag in (row.get("hashtags") or [])
            )
            has_context_support = has_alias_support or has_hashtag_support or has_structured_event_signal(text)
            if (
                row.get("post_type") == "event_explicit"
                and float(row.get("event_confidence") or 0.0) >= 5.0
                and has_context_support
            ):
                cluster_is_promoted = True

        if not cluster_is_promoted:
            continue

        for row in rows:
            confidence = float(row.get("event_confidence") or 0.0)
            if row.get("post_type") == "event_related_logistics" and confidence < 4.5:
                continue
            if row.get("post_type") == "fan_repost" and confidence < 4.5:
                continue
            promoted.append(row)

    return promoted


def build_cluster_rows(
    posts: list[dict[str, Any]],
    cluster_field: str,
    latest_ts: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in posts:
        key = normalize_text(row.get(cluster_field) or "")
        if key:
            grouped[key].append(row)

    cluster_rows: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        heat_info = compute_cluster_heat(rows, latest_ts)
        organizer_info = summarize_organizer(rows)
        dashboard_category, dashboard_category_score = summarize_dashboard_category(
            rows,
            cluster_key=key,
            cluster_field=cluster_field,
        )
        hashtags = [tag for row in rows for tag in (row.get("hashtags") or [])]
        keywords = build_cluster_keywords(
            texts=[normalize_text(row.get("analysis_content") or "") for row in rows]
            + [normalize_text(comment.get("content")) for row in rows for comment in (row.get("top_comments") or [])],
            hashtags=hashtags,
        )

        cluster_rows.append(
            {
                "cluster_key": key,
                "cluster_type": cluster_field,
                "post_count": len(rows),
                "unique_authors": len({identity for identity in (resolve_author_identity(row) for row in rows) if identity}),
                "post_type_breakdown": dict(Counter(row.get("post_type") for row in rows)),
                "total_like_count": sum(to_int(row.get("like_count")) for row in rows),
                "total_comment_count": sum(to_int(row.get("comment_count")) for row in rows),
                "total_share_count": sum(to_int(row.get("share_count")) for row in rows),
                "total_engagement": sum(to_int(row.get("engagement_total")) for row in rows),
                "discussion_total": sum(row.get("discussion_total", 0) for row in rows),
                "keywords": keywords,
                "top_posts": select_top_posts(rows),
                "top_comments": select_top_comments(rows),
                "dashboard_category": dashboard_category,
                "dashboard_category_score": dashboard_category_score,
                **organizer_info,
                **heat_info,
            }
        )

    cluster_rows.sort(key=lambda row: row["heat_score"], reverse=True)
    return cluster_rows


def resolve_post_timestamp_seconds(post: dict[str, Any]) -> int:
    for field in ("create_time", "published_ts", "created_ts"):
        timestamp = to_int(post.get(field))
        if not timestamp:
            continue
        if timestamp > 10_000_000_000:
            timestamp //= 1000
        if timestamp > 0:
            return timestamp

    for field in ("create_date_time", "published_at"):
        dt = parse_dt(str(post.get(field) or ""))
        if dt is not None:
            return int(dt.timestamp())

    return 0


def build_heat_outputs(
    posts: list[dict[str, Any]],
    *,
    comment_map: dict[str, CommentStats] | None = None,
    alias_to_canonical: dict[str, str] | None = None,
    canonical_event_set: set[str] | None = None,
    child_to_parent: dict[str, str] | None = None,
    organizer_registry: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    comment_map = comment_map or {}
    alias_to_canonical = alias_to_canonical or {}
    canonical_event_set = canonical_event_set or set()
    child_to_parent = child_to_parent or {}
    organizer_registry = organizer_registry or {}

    latest_ts = max((resolve_post_timestamp_seconds(row) for row in posts), default=0)
    platforms = {
        normalize_text(row.get("platform") or "").lower()
        for row in posts
        if normalize_text(row.get("platform") or "")
    }
    is_facebook_run = platforms == {"fb"}

    enriched_posts: list[dict[str, Any]] = []
    for post in posts:
        normalized_post = dict(post)
        create_time = resolve_post_timestamp_seconds(normalized_post)
        if create_time:
            normalized_post["create_time"] = create_time

        note_id = str(normalized_post.get("note_id") or "")
        comment_stats = comment_map.get(note_id)
        post_type = classify_post_type(normalized_post)
        raw_event_candidate = extract_raw_event_candidate(normalized_post, post_type, alias_to_canonical)
        canonical_event_name = resolve_canonical_event_name(raw_event_candidate, alias_to_canonical)
        event_eligible, event_confidence = is_event_eligible(
            post=normalized_post,
            post_type=post_type,
            raw_event_candidate=raw_event_candidate,
            canonical_event_name=canonical_event_name,
            canonical_event_set=canonical_event_set,
            alias_to_canonical=alias_to_canonical,
        )
        event_leaf_name = canonical_event_name if event_eligible else None
        geo_score = (
            compute_event_geo_score(normalized_post, raw_event_candidate, canonical_event_name, alias_to_canonical)
            if event_leaf_name
            else 0.0
        )
        if event_leaf_name and geo_score > 0 and event_leaf_name not in canonical_event_set:
            event_leaf_name = build_macau_event_variant(event_leaf_name) or event_leaf_name
        event_parent_name = lookup_parent_event(child_to_parent, event_leaf_name) if event_leaf_name else None
        event_key = resolve_event_group_key(event_leaf_name, child_to_parent) if event_leaf_name else None
        event_family_key = resolve_event_family_key(
            event_key,
            analysis_text=normalize_text(normalized_post.get("analysis_content") or ""),
            alias_to_canonical=alias_to_canonical,
            child_to_parent=child_to_parent,
        ) if event_key else None
        organizer_key, organizer_name, organizer_type, organizer_confidence, organizer_evidence = infer_organizer(
            post=normalized_post,
            canonical_event_name=event_family_key or event_key,
            organizer_registry=organizer_registry,
        )
        primary_topic = infer_primary_topic(normalized_post, post_type)
        dashboard_category = infer_dashboard_category(
            {
                **normalized_post,
                "raw_event_candidate": raw_event_candidate,
                "canonical_event_name": canonical_event_name,
                "event_key": event_key,
                "event_family_key": event_family_key,
                "primary_topic": primary_topic,
            },
            post_type,
        )
        quality_weight = compute_quality_weight(normalized_post, post_type)
        score_info = compute_post_scores(normalized_post, comment_stats, latest_ts)
        like_count = to_int(normalized_post.get("like_count"))
        comment_count = to_int(normalized_post.get("comment_count"))
        share_count = to_int(normalized_post.get("share_count"))

        discussion_total = (
            comment_count
            + (comment_stats.fetched_count if comment_stats else 0)
            + (comment_stats.fetched_sub_comment_sum if comment_stats else 0)
        )
        engagement_total = like_count + comment_count + share_count
        post_heat = round(score_info["raw_score"] * quality_weight, 4)

        enriched_posts.append(
            {
                **normalized_post,
                "post_type": post_type,
                "raw_event_candidate": raw_event_candidate,
                "canonical_event_name": canonical_event_name,
                "event_eligible": event_eligible,
                "event_confidence": event_confidence,
                "event_geo_score": geo_score,
                "event_leaf_name": event_leaf_name,
                "event_parent_name": event_parent_name,
                "event_key": event_key,
                "event_family_key": event_family_key,
                "organizer_key": organizer_key,
                "organizer_name": organizer_name,
                "organizer_type": organizer_type,
                "organizer_confidence": organizer_confidence,
                "organizer_evidence": organizer_evidence,
                "primary_topic": primary_topic,
                "dashboard_category": dashboard_category,
                "event_promoted": False,
                "quality_weight": quality_weight,
                "engagement_total": engagement_total,
                "discussion_total": discussion_total,
                "comment_fetch_count": comment_stats.fetched_count if comment_stats else 0,
                "comment_fetch_like_sum": comment_stats.fetched_like_sum if comment_stats else 0,
                "comment_fetch_sub_comment_sum": comment_stats.fetched_sub_comment_sum if comment_stats else 0,
                "comment_unique_authors": comment_stats.unique_comment_authors if comment_stats else 0,
                "top_comments": comment_stats.top_comments if comment_stats else [],
                "post_heat": post_heat,
                **score_info,
            }
        )

    if is_facebook_run:
        promoted_event_posts = [
            row
            for row in enriched_posts
            if row.get("event_family_key")
            and bool(row.get("event_eligible"))
            and row.get("post_type") != "noise_low_info"
        ]
    else:
        promoted_event_posts = build_promoted_event_posts(
            enriched_posts,
            alias_to_canonical=alias_to_canonical,
            canonical_event_set=canonical_event_set,
        )
    promoted_event_ids = {
        str(row.get("source_post_id") or row.get("note_id") or "")
        for row in promoted_event_posts
        if str(row.get("source_post_id") or row.get("note_id") or "")
    }
    for row in enriched_posts:
        row_id = str(row.get("source_post_id") or row.get("note_id") or "")
        if is_facebook_run:
            row["event_promoted"] = bool(row.get("event_family_key")) and bool(row.get("event_eligible")) and row_id in promoted_event_ids
        else:
            row["event_promoted"] = bool(row.get("event_key")) and row_id in promoted_event_ids

    event_clusters = build_cluster_rows(
        posts=promoted_event_posts,
        cluster_field="event_family_key",
        latest_ts=latest_ts,
    )
    topic_clusters = build_cluster_rows(
        posts=[row for row in enriched_posts if row.get("primary_topic") and row.get("post_type") != "noise_low_info"],
        cluster_field="primary_topic",
        latest_ts=latest_ts,
    )

    return {
        "posts": enriched_posts,
        "event_clusters": event_clusters,
        "topic_clusters": topic_clusters,
        "latest_ts": latest_ts,
    }


def main() -> int:
    args = parse_args()
    ready_path = Path(args.ready)
    output_dir = Path(args.output_dir)
    comment_paths = [Path(path) for path in args.comments]
    alias_registry_path = Path(args.alias_registry)
    parent_registry_path = Path(args.parent_registry)
    organizer_registry_path = Path(args.organizer_registry)

    posts = load_jsonl(ready_path)
    comment_map = build_comment_map(comment_paths)
    alias_to_canonical, canonical_event_set = load_event_alias_registry(alias_registry_path)
    child_to_parent = load_event_parent_registry(parent_registry_path)
    organizer_registry = load_organizer_registry(organizer_registry_path)
    outputs = build_heat_outputs(
        posts=posts,
        comment_map=comment_map,
        alias_to_canonical=alias_to_canonical,
        canonical_event_set=canonical_event_set,
        child_to_parent=child_to_parent,
        organizer_registry=organizer_registry,
    )
    enriched_posts = outputs["posts"]
    event_clusters = outputs["event_clusters"]
    topic_clusters = outputs["topic_clusters"]

    dump_jsonl(output_dir / "heat_posts_enriched.jsonl", enriched_posts)
    dump_jsonl(output_dir / "heat_event_clusters.jsonl", event_clusters)
    dump_jsonl(output_dir / "heat_topic_clusters.jsonl", topic_clusters)

    summary = {
        "ready_input": str(ready_path),
        "post_count": len(enriched_posts),
        "event_cluster_count": len(event_clusters),
        "topic_cluster_count": len(topic_clusters),
        "top_event_clusters": event_clusters[: args.top_n],
        "top_topic_clusters": topic_clusters[: args.top_n],
    }
    dump_json(output_dir / "heat_summary.json", summary)
    print(json.dumps(
        {
            "output_dir": str(output_dir),
            "post_count": len(enriched_posts),
            "event_cluster_count": len(event_clusters),
            "topic_cluster_count": len(topic_clusters),
            "top_event_cluster": event_clusters[0]["cluster_key"] if event_clusters else None,
            "top_topic_cluster": topic_clusters[0]["cluster_key"] if topic_clusters else None,
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
