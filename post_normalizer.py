"""
post_normalizer.py — 將各平台 raw post 標準化入庫

四個平台各有獨立 table：
  posts_xhs / posts_weibo / posts_ig / posts_fb

整合點：
  - ingest_crawler_data() 入庫 macau_events 後自動呼叫 auto_normalize_new_post()
  - 支援 XHS / Weibo（MediaCrawler）及 IG / FB（Apify）四個平台
  - content 純文字化：去 emoji、去特殊符號、去 hashtag、去多餘空白
  - 微博 / IG / FB 唔設 title 欄位（只有 XHS 有 title）
  - IG share count 目前 Apify scraper 唔提供，shares 欄位留 None
"""

import sqlite3
import json
import re
import datetime
import os
import unicodedata

DB_PATH = os.environ.get("DB_PATH", "macau_analytics.db")

# ── opencc 繁簡轉換 ───────────────────────────────────────
try:
    import opencc as _opencc
    _s2t = _opencc.OpenCC('s2t')
    def to_trad(text: str) -> str:
        return _s2t.convert(text) if text else text
except ImportError:
    def to_trad(text: str) -> str:
        return text


# ══════════════════════════════════════════════════════════
# 1. 文字清洗工具
# ══════════════════════════════════════════════════════════

_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"   # emoticons
    "\U0001F300-\U0001F5FF"   # symbols & pictographs
    "\U0001F680-\U0001F9FF"   # transport & misc symbols
    "\U0001FA00-\U0001FA6F"   # chess symbols etc
    "\U0001FA70-\U0001FAFF"   # food & drink etc
    "\U00002702-\U000027B0"   # dingbats
    "\U0000200D"               # zero width joiner
    "\U0000FE0F"               # variation selector-16
    "\U00002194-\U00002199"   # arrows
    "\U00002934-\U00002935"   # curved arrows
    "\U000025AA-\U000025FE"   # geometric shapes (small)
    "\U00002614-\U00002615"   # umbrella/coffee
    "\U00002648-\U00002653"   # zodiac signs
    "\U0000231A-\U0000231B"   # watch/hourglass
    "\U000023E9-\U000023F3"   # fast-forward etc
    "\U000023F8-\U000023FA"   # pause/stop
    "\U00002600-\U000026FF"   # misc symbols (☀⚡☎ etc)
    "]+",
    flags=re.UNICODE
)

_DECORATION_PATTERN = re.compile(
    r'[★☆♦♥♠♣◆◇○●□■△▲▽▼→←↑↓↗↙✓✔✗✘©®™°•·]'
    r'|[-─═━]{3,}'
)

_HASHTAG_PATTERN    = re.compile(r'#[^\s#\[，。！？\n]+(?:\[话题\])?#?')
_URL_PATTERN        = re.compile(r'https?://\S+|www\.\S+')
_MENTION_PATTERN    = re.compile(r'@\S+')
_WHITESPACE_PATTERN = re.compile(r'[\s\u3000\xa0\u200b]+')


def clean_text(text: str) -> str:
    """
    清洗帖文文字 → 純文字，減少 AI token：
    去 emoji、去裝飾符號、去 hashtag、去 URL、去 @mention、壓縮空白、繁體化
    """
    if not text:
        return ""
    t = str(text)
    t = _EMOJI_PATTERN.sub(' ', t)
    t = _DECORATION_PATTERN.sub(' ', t)
    t = _HASHTAG_PATTERN.sub(' ', t)
    t = _URL_PATTERN.sub(' ', t)
    t = _MENTION_PATTERN.sub(' ', t)
    t = ''.join(c for c in t if unicodedata.category(c)[0] != 'C' or c in '\n\t')
    t = _WHITESPACE_PATTERN.sub(' ', t).strip()
    t = to_trad(t)
    return t


def extract_hashtags(text: str, tag_list_str: str = None) -> str:
    """返回 JSON array string of hashtags"""
    if tag_list_str:
        tags = [t.strip() for t in str(tag_list_str).split(',') if t.strip()]
        if tags:
            return json.dumps(tags, ensure_ascii=False)
    tags = _HASHTAG_PATTERN.findall(text or '')
    tags = [re.sub(r'^#|\[话题\]#?$|#$', '', t).strip() for t in tags]
    return json.dumps(list(dict.fromkeys(t for t in tags if t)), ensure_ascii=False)


# ══════════════════════════════════════════════════════════
# 2. 建表 DDL
# ══════════════════════════════════════════════════════════

# XHS 有 title（帖文有獨立標題），其他平台唔設 title
_XHS_EXTRA_COLS = """
    ,title          TEXT
    ,note_type      TEXT
    ,ip_location    TEXT
"""
_WEIBO_EXTRA_COLS = """
    ,ip_location    TEXT
    ,gender         TEXT
    ,weibo_profile  TEXT
"""
_IG_EXTRA_COLS = """
    ,shortcode          TEXT
    ,product_type       TEXT
    ,tagged_users       TEXT
    ,video_duration     REAL
    ,is_pinned          INTEGER
"""
_FB_EXTRA_COLS = """
    ,page_name      TEXT
    ,page_url       TEXT
    ,top_reactions  INTEGER
"""

_COMMON_DDL = """
    post_id            TEXT PRIMARY KEY,
    platform           TEXT NOT NULL,
    operator           TEXT,
    author_id          TEXT,
    author_name        TEXT,
    content            TEXT,
    published_at       TEXT,
    event_date         TEXT,
    likes              INTEGER,
    comments           INTEGER,
    shares             INTEGER,
    collects           INTEGER,
    views              INTEGER,
    media_type         TEXT,
    media_urls         TEXT,
    hashtags           TEXT,
    post_url           TEXT,
    category           TEXT,
    sub_type           TEXT,
    ingested_at        TEXT NOT NULL DEFAULT (datetime('now')),
    raw_json           TEXT
"""

_EXTRA_COLS = {
    "xhs":   _XHS_EXTRA_COLS,
    "weibo": _WEIBO_EXTRA_COLS,
    "ig":    _IG_EXTRA_COLS,
    "fb":    _FB_EXTRA_COLS,
}


def init_post_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    for plat in ("xhs", "weibo", "ig", "fb"):
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS posts_{plat} (
                {_COMMON_DDL}
                {_EXTRA_COLS.get(plat, '')}
            )
        """)
    conn.commit()
    print("✅ posts_xhs / posts_weibo / posts_ig / posts_fb 表已就緒")


# ══════════════════════════════════════════════════════════
# 4. 平台專屬 normalizer
# ══════════════════════════════════════════════════════════

def _ms_to_iso(ts) -> str:
    try:
        return datetime.datetime.fromtimestamp(
            int(ts) / 1000, tz=datetime.timezone.utc).isoformat()
    except Exception:
        return ""

def _s_to_iso(ts) -> str:
    try:
        return datetime.datetime.fromtimestamp(
            int(ts), tz=datetime.timezone.utc).isoformat()
    except Exception:
        return ""

def _safe_int(v):
    try:
        return int(v) if v not in (None, "", "None") else None
    except Exception:
        return None


def normalize_xhs(raw: dict, operator=None, event_date=None,
                  category=None, sub_type=None) -> dict:
    raw_text = raw.get('desc') or raw.get('title') or ""
    # XHS title 保留（但也清洗）
    title_raw = raw.get('title') or ""
    images_raw = raw.get('image_list') or ""
    L, C, S, K = (_safe_int(raw.get('liked_count')), _safe_int(raw.get('comment_count')),
                  _safe_int(raw.get('share_count')), _safe_int(raw.get('collected_count')))
    return {
        "post_id":          f"xhs_{raw.get('note_id', '')}",
        "platform":         "xhs",
        "operator":         operator,
        "author_id":        raw.get('user_id'),
        "author_name":      to_trad(raw.get('nickname') or ""),
        "content":          clean_text(raw_text),
        "published_at":     _ms_to_iso(raw.get('time')) if raw.get('time') else "",
        "event_date":       event_date or "",
        "likes":            L, "comments": C, "shares": S, "collects": K, "views": None,
        "media_type":       "video" if raw.get('video_url') else ("image" if images_raw else "text"),
        "media_urls":       json.dumps([u.strip() for u in images_raw.split(',') if u.strip()], ensure_ascii=False),
        "hashtags":         extract_hashtags(raw_text, tag_list_str=raw.get('tag_list')),
        "post_url":         raw.get('note_url') or "",
        "category":         category, "sub_type": sub_type,
        "raw_json":         json.dumps(raw, ensure_ascii=False),
        # XHS 專屬
        "title":            clean_text(title_raw),
        "note_type":        raw.get('type'),
        "ip_location":      raw.get('ip_location'),
    }


def normalize_weibo(raw: dict, operator=None, event_date=None,
                    category=None, sub_type=None) -> dict:
    raw_text = raw.get('content') or ""
    L, C, S  = (_safe_int(raw.get('liked_count')), _safe_int(raw.get('comments_count')),
                _safe_int(raw.get('shared_count')))
    return {
        "post_id":          f"weibo_{raw.get('note_id', '')}",
        "platform":         "weibo",
        "operator":         operator,
        "author_id":        raw.get('user_id'),
        "author_name":      to_trad(raw.get('nickname') or ""),
        "content":          clean_text(raw_text),
        "published_at":     raw.get('create_date_time') or _s_to_iso(raw.get('create_time')) or "",
        "event_date":       event_date or "",
        "likes":            L, "comments": C, "shares": S,
        "collects":         None, "views": None,
        "media_type":       "text", "media_urls": "[]",
        "hashtags":         extract_hashtags(raw_text),
        "post_url":         raw.get('note_url') or "",
        "category":         category, "sub_type": sub_type,
        "raw_json":         json.dumps(raw, ensure_ascii=False),
        # Weibo 專屬
        "ip_location":      raw.get('ip_location'),
        "gender":           raw.get('gender'),
        "weibo_profile":    raw.get('profile_url'),
    }


def _extract_chinese_only(text: str) -> str:
    """
    IG 帖文通常係中英雙語，英文係中文翻譯。
    提取中文段落，去掉英文部分，減少去重時嘅語言干擾。
    策略：按段落分割，只保留含有中文字符嘅段落。
    如果完全冇中文（純英文帖），返回原文。
    """
    if not text:
        return text
    paragraphs = re.split(r'\n{1,}', text.strip())
    chinese_paras = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if re.search(r'[\u4e00-\u9fff\u3400-\u4dbf]', para):
            chinese_paras.append(para)
    return '\n'.join(chinese_paras) if chinese_paras else text


def normalize_ig(raw: dict, operator=None, event_date=None,
                 category=None, sub_type=None) -> dict:
    raw_text    = raw.get('desc') or raw.get('title') or ""
    hashtags_r  = raw.get('hashtags') or []
    if isinstance(hashtags_r, str):
        try: hashtags_r = json.loads(hashtags_r)
        except: hashtags_r = []
    if not hashtags_r:
        hashtags_r = re.findall(r'#(\w+)', raw_text)
    images      = raw.get('images') or []
    if isinstance(images, str):
        try: images = json.loads(images)
        except: images = []
    video_url   = raw.get('videoUrl') or ""
    tagged      = raw.get('taggedUsers') or []
    if isinstance(tagged, str):
        try: tagged = json.loads(tagged)
        except: tagged = []
    L = _safe_int(raw.get('likesCount'))
    C = _safe_int(raw.get('commentsCount'))
    V = _safe_int(raw.get('videoViewCount') or raw.get('videoPlayCount'))
    return {
        "post_id":          f"ig_{raw.get('note_id', '')}",
        "platform":         "ig",
        "operator":         operator,
        "author_id":        raw.get('ownerId'),
        "author_name":      to_trad(raw.get('ownerFullName') or raw.get('ownerUsername') or ""),
        "content":          clean_text(_extract_chinese_only(raw_text)),
        "published_at":     raw.get('create_date_time') or "",
        "event_date":       event_date or "",
        "likes":            L, "comments": C,
        "shares":           None,   # Apify IG scraper 不提供 share count
        "collects":         None, "views": V,
        "media_type":       "video" if video_url else ("image" if images else "text"),
        "media_urls":       json.dumps([video_url] if video_url else images, ensure_ascii=False),
        "hashtags":         json.dumps(list(dict.fromkeys(hashtags_r)), ensure_ascii=False),
        "post_url":         raw.get('url') or "",
        "category":         category, "sub_type": sub_type,
        "raw_json":         json.dumps(raw, ensure_ascii=False),
        # IG 專屬
        "shortcode":        raw.get('shortCode'),
        "product_type":     raw.get('productType'),
        "tagged_users":     json.dumps([u.get('username') for u in tagged
                                        if isinstance(u, dict) and u.get('username')], ensure_ascii=False),
        "video_duration":   raw.get('videoDuration'),
        "is_pinned":        1 if raw.get('isPinned') else 0,
    }


def normalize_fb(raw: dict, operator=None, event_date=None,
                 category=None, sub_type=None) -> dict:
    raw_text    = raw.get('content') or raw.get('title') or ""
    media_items = raw.get('media') or []
    if isinstance(media_items, str):
        try: media_items = json.loads(media_items)
        except: media_items = []
    media_urls  = [m.get('thumbnail') or m.get('url') or m.get('fullUrl') or ""
                   for m in media_items if isinstance(m, dict)]
    media_urls  = [u for u in media_urls if u]
    media_type  = "video" if any(m.get('type','').lower()=='video'
                                  for m in media_items if isinstance(m,dict)) \
                  else ("image" if media_urls else "text")
    user = raw.get('user') or {}
    L, C, S = (_safe_int(raw.get('likes')), _safe_int(raw.get('comments')),
               _safe_int(raw.get('shares')))
    return {
        "post_id":          f"fb_{raw.get('id') or raw.get('postId', '')}",
        "platform":         "fb",
        "operator":         operator,
        "author_id":        user.get('id') if isinstance(user, dict) else None,
        "author_name":      to_trad((user.get('name') if isinstance(user,dict) else None) or raw.get('pageName') or ""),
        "content":          clean_text(raw_text),
        "published_at":     raw.get('time') or _s_to_iso(raw.get('timestamp')) or "",
        "event_date":       event_date or "",
        "likes":            L, "comments": C, "shares": S,
        "collects":         None, "views": None,
        "media_type":       media_type,
        "media_urls":       json.dumps(media_urls, ensure_ascii=False),
        "hashtags":         extract_hashtags(raw_text),
        "post_url":         raw.get('url') or raw.get('facebookUrl') or "",
        "category":         category, "sub_type": sub_type,
        "raw_json":         json.dumps(raw, ensure_ascii=False),
        # FB 專屬
        "page_name":        raw.get('pageName'),
        "page_url":         raw.get('facebookUrl'),
        "top_reactions":    _safe_int(raw.get('topReactionsCount')),
    }


# ── dispatcher ────────────────────────────────────────────
_NORMALIZERS = {
    "xhs":   normalize_xhs,
    "weibo": normalize_weibo,
    "ig":    normalize_ig,
    "fb":    normalize_fb,
}

_COMMON_KEYS = [
    "post_id", "platform", "operator", "author_id", "author_name",
    "content", "published_at", "event_date",
    "likes", "comments", "shares", "collects", "views",
    "media_type", "media_urls", "hashtags", "post_url",
    "category", "sub_type", "raw_json",
]

_PLATFORM_EXTRA_KEYS = {
    "xhs":   ["title", "note_type", "ip_location"],
    "weibo": ["ip_location", "gender", "weibo_profile"],
    "ig":    ["shortcode", "product_type", "tagged_users", "video_duration", "is_pinned"],
    "fb":    ["page_name", "page_url", "top_reactions"],
}


# ══════════════════════════════════════════════════════════
# 5. 入庫函數
# ══════════════════════════════════════════════════════════

def normalize_and_insert(conn: sqlite3.Connection, platform: str,
                         raw: dict, operator=None, event_date=None,
                         category=None, sub_type=None) -> bool:
    normalizer = _NORMALIZERS.get(platform)
    if not normalizer:
        return False
    data         = normalizer(raw, operator=operator, event_date=event_date,
                              category=category, sub_type=sub_type)
    all_keys     = _COMMON_KEYS + _PLATFORM_EXTRA_KEYS.get(platform, [])
    cols         = ", ".join(all_keys)
    placeholders = ", ".join("?" * len(all_keys))
    values       = [data.get(k) for k in all_keys]
    cur = conn.cursor()
    cur.execute(
        f"INSERT OR IGNORE INTO posts_{platform} ({cols}) VALUES ({placeholders})",
        values
    )
    return cur.rowcount > 0


# ══════════════════════════════════════════════════════════
# 6. 爬蟲 hook
#
# 在 db_manager.py 的 ingest_crawler_data() 裡，
# INSERT OR IGNORE 成功後（cursor.rowcount > 0）加：
#
#     from post_normalizer import auto_normalize_new_post
#     if cursor.rowcount > 0:
#         count += 1
#         auto_normalize_new_post(conn, platform, post,
#             operator=op, event_date=event_date, category=category_str)
#
# 呢樣做法：
#   - XHS 爬完就立即標準化 XHS 帖
#   - Weibo 爬完就立即標準化 Weibo 帖
#   - IG / FB（Apify）爬完 _ingest_apify_posts() 調用 ingest_crawler_data()
#     同樣自動觸發，唔需要平台特判
# ══════════════════════════════════════════════════════════

def auto_normalize_new_post(conn: sqlite3.Connection, platform: str,
                            raw: dict, operator=None, event_date=None,
                            category=None, sub_type=None):
    """在 ingest_crawler_data 成功 INSERT 一條新帖後立即呼叫。"""
    plat = "weibo" if platform == "wb" else platform
    if plat not in _NORMALIZERS:
        return
    normalize_and_insert(conn, plat, raw,
                         operator=operator, event_date=event_date,
                         category=category, sub_type=sub_type)


# ══════════════════════════════════════════════════════════
# 7. Backfill
# ══════════════════════════════════════════════════════════

def backfill_all_platforms(db_path: str = None):
    path = db_path or DB_PATH
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    init_post_tables(conn)

    cur = conn.cursor()
    cur.execute("""
        SELECT id, platform, operator, raw_json, event_date, category, sub_type
        FROM macau_events
        WHERE platform IN ('xhs','weibo','ig','fb') AND raw_json IS NOT NULL
    """)
    rows = cur.fetchall()
    print(f"📦 macau_events 共 {len(rows)} 條社媒帖文待標準化")

    counts = {"xhs": 0, "weibo": 0, "ig": 0, "fb": 0, "skipped": 0}
    for row in rows:
        plat = row['platform']
        if plat not in _NORMALIZERS:
            counts["skipped"] += 1
            continue
        try:
            raw = json.loads(row['raw_json'])
        except Exception:
            counts["skipped"] += 1
            continue
        if normalize_and_insert(conn, plat, raw,
                                operator=row['operator'],
                                event_date=row['event_date'],
                                category=row['category'],
                                sub_type=row['sub_type']):
            counts[plat] += 1

    conn.commit()
    conn.close()
    print("✅ Backfill 完成：")
    for k, v in counts.items():
        print(f"   {k}: {v}")


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    backfill_all_platforms(db)