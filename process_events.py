"""
process_events.py — 基於 AI 活動實體抽取去重，建立 events_deduped table

核心架構（新）：
  Step 1: 每條 post → AI 抽取佢提及嘅所有活動名（extracted_events JSON array）
          roundup post 可以抽出多個活動，普通 post 通常得一個
  Step 2: 以「活動名」做 grouping key（唔再係 content 相似度）
          同 operator 內：活動名完全相符 → 同一 group
          跨 operator：活動名 embedding 相似度 >= ACTIVITY_SIM_CROSS_OP → 同一 group
  Step 3: 每個 post 按佢講嘅活動，分別貢獻去對應 group
          source_post_ids 只包含「呢條 post 係講呢個活動」嘅 posts
          roundup post 自動拆開，貢獻去多個 group，但唔污染各自嘅 source_posts

用法:
  python process_events.py --db macau_analytics.db
  python process_events.py --db macau_analytics.db --skip-extract   # 跳過 AI 抽取（用 DB cached）
  python process_events.py --db macau_analytics.db --operator Wynn
"""

import sqlite3
import json
import re
import math
import unicodedata
import argparse
from datetime import datetime, timedelta, date
from difflib import SequenceMatcher
from collections import defaultdict

DATE_WINDOW_DAYS        = 90
LOOKBACK_DAYS           = 30
ACTIVITY_SIM_CROSS_OP   = 0.92   # 跨 operator 活動名 embedding 相似度閾值
MAX_CONTENT_LEN         = 300

import os as _os
from dotenv import load_dotenv as _load_dotenv
_load_dotenv()
_DASHSCOPE_API_KEY = _os.getenv("DASHSCOPE_API_KEY", "")
_OPENAI_API_KEY    = _os.getenv("OPENAI_API_KEY", "")
_DEEPSEEK_API_KEY  = _os.getenv("DEEPSEEK_API_KEY", "")

# ── Embedding cache ───────────────────────────────────────
_emb_cache: dict = {}

def _get_emb_db_conn():
    import sqlite3 as _sq
    db_path = _os.getenv("DB_PATH", "macau_analytics.db")
    return _sq.connect(db_path)

def _get_embedding(text: str, post_id: str = None):
    text = text.strip()[:500]
    if not text:
        return None
    if text in _emb_cache:
        return _emb_cache[text]

    if post_id:
        try:
            conn = _get_emb_db_conn()
            plat  = post_id.split("_")[0]
            table = f"posts_{plat}"
            row = conn.execute(
                f"SELECT embedding FROM {table} WHERE post_id=? AND embedding IS NOT NULL",
                (post_id,)
            ).fetchone()
            conn.close()
            if row:
                vec = json.loads(row[0])
                _emb_cache[text] = vec
                return vec
        except Exception:
            pass

    if not _DASHSCOPE_API_KEY:
        return None
    try:
        import httpx
        resp = httpx.post(
            "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings",
            headers={
                "Authorization": f"Bearer {_DASHSCOPE_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"model": "text-embedding-v3", "input": text},
            timeout=20,
        )
        vec = resp.json()["data"][0]["embedding"]
        _emb_cache[text] = vec

        if post_id:
            try:
                conn = _get_emb_db_conn()
                plat  = post_id.split("_")[0]
                table = f"posts_{plat}"
                conn.execute(
                    f"UPDATE {table} SET embedding=? WHERE post_id=?",
                    (json.dumps(vec), post_id)
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
        return vec
    except Exception as e:
        print(f"  ⚠️ embedding 失敗: {e}")
        return None


def _cosine(a, b) -> float:
    dot  = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x*x for x in a)) * math.sqrt(sum(x*x for x in b))
    return dot / norm if norm else 0.0


# ── 文字正規化 ────────────────────────────────────────────
def normalize_content(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r'[^\w\s\u4e00-\u9fff]', '', str(text), flags=re.UNICODE)
    t = unicodedata.normalize('NFKC', t)
    t = re.sub(r'\s+', '', t).lower()
    return t[:MAX_CONTENT_LEN]


# ── 日期處理 ──────────────────────────────────────────────
def parse_date_range(date_str: str):
    if not date_str:
        return None, None
    first = date_str.split(',')[0].strip()
    if '~' in first:
        parts = first.split('~')
        try:
            return (datetime.strptime(parts[0].strip(), '%Y-%m-%d').date(),
                    datetime.strptime(parts[1].strip(), '%Y-%m-%d').date())
        except ValueError:
            return None, None
    try:
        d = datetime.strptime(first.strip(), '%Y-%m-%d').date()
        return d, d
    except ValueError:
        return None, None


def dates_overlap(start1, end1, start2, end2) -> bool:
    if None in (start1, end1, start2, end2):
        return True
    return start1 <= end2 and start2 <= end1


# ══════════════════════════════════════════════════════════
# Step 1: AI 活動實體抽取
# ══════════════════════════════════════════════════════════

_EXTRACT_SYSTEM = """你係一個活動資訊抽取助手。
用戶會給你一條社交媒體帖文，你需要抽取帖文中提及的所有具體活動或推廣項目。

規則：
- 每個活動/推廣/展覽/演出/禮遇係獨立一個 item
- 只抽具體活動，唔抽 operator 名稱、場地名稱、或者泛泛描述（例如「度假體驗」、「精彩盛事」唔算）
- 活動名用帖文原文（中文優先），唔好自己創作
- 如果帖文只係講一個活動，返回一個 item
- 如果係 roundup（一條帖文介紹多個活動），返回多個 item

必須只返回 JSON array，例如：
["媽咪雞蛋仔登場", "張天賦永利音樂會", "Wing Lei Bar & Friends"]

或者只有一個：
["春旅賺放住宿禮遇"]

唔好返回任何 JSON 以外嘅文字。"""


def extract_events_from_post(content: str, post_id: str, conn: sqlite3.Connection) -> list[str]:
    """
    用 AI 從一條 post 抽取所有活動名。
    優先從 DB cache 讀（extracted_events 欄）。
    """
    if not content or not content.strip():
        return []

    # 1. 嘗試讀 DB cache
    try:
        plat  = post_id.split("_")[0]
        table = f"posts_{plat}"
        row   = conn.execute(
            f"SELECT extracted_events FROM {table} WHERE post_id=?", (post_id,)
        ).fetchone()
        if row and row[0]:
            cached = json.loads(row[0])
            if isinstance(cached, list) and cached:
                return cached
    except Exception:
        pass

    # 2. Call AI
    events = _call_ai_extract(content)

    # 3. 存返落 DB
    if events:
        try:
            plat  = post_id.split("_")[0]
            table = f"posts_{plat}"
            _ensure_extracted_events_col(conn, table)
            conn.execute(
                f"UPDATE {table} SET extracted_events=? WHERE post_id=?",
                (json.dumps(events, ensure_ascii=False), post_id)
            )
            conn.commit()
        except Exception as e:
            print(f"  ⚠️ 存 extracted_events 失敗 ({post_id}): {e}")

    return events


def _ensure_extracted_events_col(conn: sqlite3.Connection, table: str):
    """確保 posts_* 表有 extracted_events 欄（TEXT）。"""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN extracted_events TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # 欄位已存在


def _call_ai_extract(content: str) -> list[str]:
    """Call DashScope / DeepSeek / OpenAI 抽取活動名，返回 list[str]。"""
    # 優先 DashScope（qwen-plus，中文理解強）
    if _DASHSCOPE_API_KEY:
        try:
            import httpx
            resp = httpx.post(
                "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {_DASHSCOPE_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "qwen-turbo",
                    "messages": [
                        {"role": "system", "content": _EXTRACT_SYSTEM},
                        {"role": "user",   "content": content[:1500]},
                    ],
                    "temperature": 0,
                },
                timeout=20,
            )
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            return _parse_json_list(raw)
        except Exception as e:
            print(f"  ⚠️ DashScope 抽取失敗: {e}")

    # Fallback 1: DeepSeek（OpenAI-compatible）
    if _DEEPSEEK_API_KEY:
        try:
            import httpx
            resp = httpx.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {_DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": _EXTRACT_SYSTEM},
                        {"role": "user",   "content": content[:1500]},
                    ],
                    "temperature": 0,
                },
                timeout=20,
            )
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            return _parse_json_list(raw)
        except Exception as e:
            print(f"  ⚠️ DeepSeek 抽取失敗: {e}")

    # Fallback 2: OpenAI
    if _OPENAI_API_KEY:
        try:
            import httpx
            resp = httpx.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {_OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": _EXTRACT_SYSTEM},
                        {"role": "user",   "content": content[:1500]},
                    ],
                    "temperature": 0,
                },
                timeout=20,
            )
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            return _parse_json_list(raw)
        except Exception as e:
            print(f"  ⚠️ OpenAI 抽取失敗: {e}")

    return []


def _parse_json_list(raw: str) -> list[str]:
    """安全 parse AI 返回嘅 JSON array。"""
    try:
        cleaned = re.sub(r'^```[a-z]*\n?|```$', '', raw.strip())
        result  = json.loads(cleaned)
        if isinstance(result, list):
            return [str(x).strip() for x in result if str(x).strip()]
    except Exception:
        pass
    m = re.search(r'\[.*?\]', raw, re.DOTALL)
    if m:
        try:
            result = json.loads(m.group())
            if isinstance(result, list):
                return [str(x).strip() for x in result if str(x).strip()]
        except Exception:
            pass
    return []


# ══════════════════════════════════════════════════════════
# Step 2: 以活動名做 grouping
# ══════════════════════════════════════════════════════════

def activity_name_similarity(a: str, b: str) -> float:
    """計算兩個活動名嘅相似度。優先 embedding，fallback SequenceMatcher。"""
    if not a or not b:
        return 0.0
    if normalize_content(a) == normalize_content(b):
        return 1.0
    va = _get_embedding(a)
    vb = _get_embedding(b)
    if va and vb:
        return _cosine(va, vb)
    na, nb = normalize_content(a), normalize_content(b)
    return SequenceMatcher(None, na, nb).ratio()


def group_by_activity(
    posts: list[dict],
    skip_extract: bool = False,
    conn: sqlite3.Connection = None,
) -> list[dict]:
    """
    主去重邏輯：
    1. 每條 post 抽取 extracted_events（活動名 list）
    2. 以活動名做 key 建立 activity_groups
       - 同 operator：normalize 後完全相符 → 同一 group
       - 跨 operator：embedding 相似度 >= ACTIVITY_SIM_CROSS_OP → 合併
    3. 每個 activity group 整理成 event record
       - source_post_ids 只包含真正講呢個活動嘅 posts
       - representative post 優先揀非 roundup post

    返回 list[dict]，每個 dict 可直接傳入 write_deduped_events。
    """
    print(f"\n🤖 Step 1: AI 活動實體抽取（{len(posts)} 條帖文）...")

    # 預載 embedding cache
    for p in posts:
        emb_raw = p.get('embedding')
        if emb_raw and p.get('content'):
            try:
                _emb_cache[p['content'].strip()[:500]] = json.loads(emb_raw)
            except Exception:
                pass

    # 每條 post 抽取活動名
    for i, p in enumerate(posts):
        if skip_extract:
            cached = []
            if conn:
                try:
                    plat  = p['post_id'].split("_")[0]
                    table = f"posts_{plat}"
                    row   = conn.execute(
                        f"SELECT extracted_events FROM {table} WHERE post_id=?",
                        (p['post_id'],)
                    ).fetchone()
                    if row and row[0]:
                        cached = json.loads(row[0])
                except Exception:
                    pass
            p['_extracted_events'] = cached if isinstance(cached, list) else []
        else:
            p['_extracted_events'] = extract_events_from_post(
                p.get('content') or '', p['post_id'], conn
            )
        p['_is_roundup'] = len(p['_extracted_events']) > 1
        if (i + 1) % 50 == 0:
            print(f"  ... {i+1}/{len(posts)} 完成")

    roundup_count = sum(1 for p in posts if p['_is_roundup'])
    print(f"✅ 抽取完成：{roundup_count} 條 roundup posts（提及多個活動）")

    # Pass 1: 同 operator，活動名完全相符 → 同一 group
    # key: "{operator}||{normalized_activity_name}"
    activity_groups: dict[str, dict] = {}

    def _get_or_create_group(operator: str, activity_name: str) -> str:
        str_key = f"{operator}||{normalize_content(activity_name)}"
        if str_key not in activity_groups:
            activity_groups[str_key] = {
                "event_name":       activity_name,
                "operator":         operator,
                "post_event_pairs": [],
            }
        return str_key

    for p in posts:
        op = p.get('operator') or ''
        for act_name in p['_extracted_events']:
            key = _get_or_create_group(op, act_name)
            activity_groups[key]["post_event_pairs"].append((p, act_name))

    # Fallback：AI 抽取失敗嘅 posts，唔想佢哋消失
    for p in posts:
        if not p['_extracted_events']:
            fallback_key = f"__fallback_{p['post_id']}"
            activity_groups[fallback_key] = {
                "event_name":       (p.get('content') or '')[:40],
                "operator":         p.get('operator') or '',
                "post_event_pairs": [(p, '')],
            }

    print(f"🔄 Pass 1 (同 operator 活動名聚類): {len(posts)} posts → {len(activity_groups)} activity groups")

    # Pass 2: 跨 operator，活動名 embedding 相似度 >= 閾值 → 合併（union-find）
    keys   = list(activity_groups.keys())
    parent = {k: k for k in keys}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for i, k1 in enumerate(keys):
        g1 = activity_groups[k1]
        if k1.startswith("__fallback"):
            continue
        for k2 in keys[i+1:]:
            g2 = activity_groups[k2]
            if k2.startswith("__fallback"):
                continue
            if g1["operator"] == g2["operator"]:
                continue  # 同 operator 已 Pass 1 處理
            sim = activity_name_similarity(g1["event_name"], g2["event_name"])
            if sim >= ACTIVITY_SIM_CROSS_OP:
                union(k1, k2)

    # 按 union-find 合併
    merged: dict[str, dict] = {}
    for k in keys:
        root = find(k)
        if root not in merged:
            merged[root] = {
                "event_name":       activity_groups[root]["event_name"],
                "operator":         activity_groups[root]["operator"],
                "post_event_pairs": [],
            }
        merged[root]["post_event_pairs"].extend(activity_groups[k]["post_event_pairs"])

    print(f"🔄 Pass 2 (跨 operator 合併): {len(activity_groups)} → {len(merged)} activity groups")

    # 整理成 event records
    results        = []
    used_event_ids: dict[str, int] = {}  # 追蹤已用過嘅 event_id，避免 UNIQUE 衝突

    for root_key, g in merged.items():
        pairs = g["post_event_pairs"]
        if not pairs:
            continue

        # source_post_ids：dedup by post_id
        seen_pids    = set()
        source_posts = []
        for (p, _act) in pairs:
            if p['post_id'] not in seen_pids:
                seen_pids.add(p['post_id'])
                source_posts.append(p)

        # representative：優先非 roundup，再揀最長 content
        non_roundup = [p for p in source_posts if not p.get('_is_roundup')]
        pool        = non_roundup if non_roundup else source_posts
        rep         = max(pool, key=lambda p: len(p.get('content') or ''))

        # 生成唯一 event_id：同一個 post_id 被多個 activity group 用到時加 suffix
        base_id = rep['post_id']
        if base_id not in used_event_ids:
            used_event_ids[base_id] = 0
            event_id = base_id
        else:
            used_event_ids[base_id] += 1
            event_id = f"{base_id}__act{used_event_ids[base_id]}"

        # merge category
        cats = set()
        for p in source_posts:
            for c in (p.get('_merged_category') or p.get('category') or '').split('|'):
                c = c.strip()
                if c:
                    cats.add(c)

        results.append({
            "post_id":            event_id,
            "event_name":         g["event_name"],
            "platform":           rep.get('platform') or '',
            "operator":           rep.get('operator') or g["operator"],
            "content":            rep.get('content') or '',
            "event_date":         rep.get('event_date') or '',
            "category":           rep.get('category') or '',
            "sub_type":           rep.get('sub_type') or '',
            "published_at":       rep.get('published_at') or '',
            "_source_post_ids":   [p['post_id'] for p in source_posts if not p.get('_is_roundup')] or [p['post_id'] for p in source_posts],
            "_source_count":      len([p for p in source_posts if not p.get('_is_roundup')]) or len(source_posts),                       
            "_merged_category":   '|'.join(sorted(cats)) if cats else '',
            "_is_roundup_group":  all(p.get('_is_roundup') for p in source_posts),
        })

    print(f"✅ 最終 event groups：{len(results)}")
    return results


# ── 從四張 posts_* 表載入帖文 ────────────────────────────
def load_posts(conn: sqlite3.Connection,
               window_days: int = DATE_WINDOW_DAYS,
               operator: str = None) -> list[dict]:
    today       = date.today()
    cutoff_past = (today - timedelta(days=window_days + LOOKBACK_DAYS)).isoformat()
    cur         = conn.cursor()
    posts       = []

    for table in ['posts_xhs', 'posts_ig', 'posts_fb', 'posts_weibo']:
        try:
            op_clause = "AND operator = ?" if operator else ""
            op_params = [operator] if operator else []

            cur.execute(f"PRAGMA table_info({table})")
            col_names = {r[1] for r in cur.fetchall()}
            ext_col   = ", extracted_events" if "extracted_events" in col_names else ", NULL as extracted_events"

            cur.execute(f"""
                SELECT post_id, platform, operator,
                       CASE WHEN media_text IS NOT NULL AND media_text != '' AND media_text NOT IN ('（無可分析內容）','（圖片無文字）')
                            THEN content || ' ' || media_text
                            ELSE content
                       END AS content,
                       event_date, category, sub_type, published_at, raw_json,
                       media_text, post_url, embedding
                       {ext_col}
                FROM {table}
                WHERE (published_at >= ? OR published_at IS NULL)
                  AND content IS NOT NULL AND content != ''
                  {op_clause}
                ORDER BY published_at DESC
            """, [cutoff_past] + op_params)
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                r = dict(zip(cols, row))
                r['_table'] = table
                posts.append(r)
        except sqlite3.OperationalError as e:
            print(f"  ⚠️ {table} 讀取失敗: {e}")

    print(f"✅ 載入 {len(posts)} 條帖文（window={window_days}d）")
    return posts


# ── 寫入 events_deduped ──────────────────────────────────
def ensure_deduped_table(conn: sqlite3.Connection):
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(events_deduped)")
        cols = {r[1] for r in cur.fetchall()}
        if cols and 'content' not in cols:
            conn.execute("DROP TABLE events_deduped")
            conn.commit()
            print("🔄 舊 events_deduped schema 已清除，重建中...")
    except Exception:
        pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_deduped (
            event_id         TEXT PRIMARY KEY,
            platform         TEXT,
            operator         TEXT,
            content          TEXT,
            event_date       TEXT,
            category         TEXT,
            sub_type         TEXT,
            published_at     TEXT,
            source_post_ids  TEXT,
            source_count     INTEGER DEFAULT 1,
            ai_name          TEXT,
            ai_description   TEXT,
            ai_category      TEXT,
            ai_location      TEXT,
            ai_processed     INTEGER DEFAULT 0,
            updated_at       TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.commit()


def write_deduped_events(conn: sqlite3.Connection, groups: list[dict]):
    ensure_deduped_table(conn)
    conn.execute("DELETE FROM events_deduped")
    cur   = conn.cursor()
    count = 0
    for g in groups:
        cur.execute("""
            INSERT INTO events_deduped
            (event_id, platform, operator, content, event_date,
             category, sub_type, published_at,
             source_post_ids, source_count,
             ai_name, ai_description, ai_category, ai_location, ai_processed,
             updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    NULL, NULL, NULL, NULL, 0,
                    datetime('now','localtime'))
        """, (
            g['post_id'],
            g.get('platform') or '',
            g.get('operator') or '',
            g.get('content') or '',
            g.get('event_date') or '',
            g.get('_merged_category') or g.get('category') or '',
            g.get('sub_type') or '',
            g.get('published_at') or '',
            json.dumps(g.get('_source_post_ids', [g['post_id']]), ensure_ascii=False),
            g.get('_source_count', 1),
        ))
        count += 1
    conn.commit()
    print(f"✅ events_deduped 寫入完成：{count} 個 event groups")


def fill_missing_extractions(db_path: str, window_days: int = DATE_WINDOW_DAYS, operator: str = None):
    """
    只補跑 extracted_events IS NULL 嘅 posts。
    已 cache 嘅唔動。補完後再做完整 grouping 同寫入。
    """
    conn  = sqlite3.connect(db_path)
    posts = load_posts(conn, window_days=window_days, operator=operator)

    # 篩出未 cache 嘅 posts
    missing = []
    for p in posts:
        raw = p.get('extracted_events')
        if not raw:
            missing.append(p)
        else:
            # 預載 cache
            try:
                cached = json.loads(raw)
                if isinstance(cached, list):
                    p['_extracted_events'] = cached
                    p['_is_roundup'] = len(cached) > 1
            except Exception:
                missing.append(p)

    print(f"📋 需要補跑：{len(missing)} 條（已 cache：{len(posts)-len(missing)} 條）")

    for i, p in enumerate(missing):
        events = extract_events_from_post(p.get('content') or '', p['post_id'], conn)
        p['_extracted_events'] = events
        p['_is_roundup']       = len(events) > 1
        if (i + 1) % 50 == 0:
            print(f"  ... 補跑 {i+1}/{len(missing)} 完成")

    print(f"✅ 補跑完成，開始重新 grouping...")
    groups = group_by_activity(posts, skip_extract=True, conn=conn)
    write_deduped_events(conn, groups)
    conn.close()
    return groups


# ── Pipeline 入口 ─────────────────────────────────────────
def run_dedup_pipeline(db_path: str = 'macau_analytics.db',
                       window_days: int = DATE_WINDOW_DAYS,
                       operator: str = None,
                       skip_extract: bool = False):
    """入庫後自動觸發，供 db_manager 呼叫。"""
    conn = sqlite3.connect(db_path)
    try:
        posts  = load_posts(conn, window_days=window_days, operator=operator)
        groups = group_by_activity(posts, skip_extract=skip_extract, conn=conn)
        write_deduped_events(conn, groups)
        return groups
    finally:
        conn.close()


# ── CLI ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',           default='macau_analytics.db')
    parser.add_argument('--window',       type=int, default=DATE_WINDOW_DAYS)
    parser.add_argument('--operator',     default=None)
    parser.add_argument('--skip-extract', action='store_true',
                        help='跳過 AI 抽取，只用 DB 已有嘅 extracted_events cache')
    parser.add_argument('--fill-missing', action='store_true',
                        help='只補跑 extracted_events IS NULL 嘅 posts，已 cache 唔動')
    args = parser.parse_args()

    if args.fill_missing:
        fill_missing_extractions(args.db, args.window, args.operator)
        return

    conn   = sqlite3.connect(args.db)
    posts  = load_posts(conn, window_days=args.window, operator=args.operator)
    groups = group_by_activity(posts, skip_extract=args.skip_extract, conn=conn)

    src_counts = [g['_source_count'] for g in groups]
    multi      = sum(1 for c in src_counts if c > 1)
    roundup_g  = sum(1 for g in groups if g.get('_is_roundup_group'))
    print(f"\n📊 統計：")
    print(f"   總帖文：{len(posts)}")
    print(f"   Event groups：{len(groups)}")
    print(f"   多來源 groups：{multi} ({multi/max(len(groups),1)*100:.1f}%)")
    print(f"   純 roundup groups（冇專屬 posts）：{roundup_g}")
    print(f"   平均每組來源：{sum(src_counts)/max(len(src_counts),1):.2f}")
    cat_counts = defaultdict(int)
    for g in groups:
        for c in (g.get('_merged_category') or '').split('|'):
            c = c.strip()
            if c:
                cat_counts[c] += 1
    print(f"   By category: {dict(sorted(cat_counts.items(), key=lambda x: -x[1]))}")

    write_deduped_events(conn, groups)
    conn.close()


if __name__ == '__main__':
    main()