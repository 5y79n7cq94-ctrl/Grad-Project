"""
process_events.py — 基於帖文內容相似度去重，建立 events_deduped table

核心改動：
  - 直接從 posts_xhs/ig/fb/weibo 四表抽數據（唔係 macau_events）
  - 用帖文 content 相似度去重（唔係 title）
  - 每個 group 記錄所有 source_post_ids（真實帖文 ID）
  - events_deduped 每條 = 一個真實活動 group，等待查詢時喂 AI 總結

用法:
  python process_events.py --db macau_analytics.db
"""

import sqlite3
import json
import re
import unicodedata
import argparse
from datetime import datetime, timedelta, date
from difflib import SequenceMatcher
from collections import defaultdict

DATE_WINDOW_DAYS       = 90
LOOKBACK_DAYS          = 30
CONTENT_SIM_SAME_OP    = 0.55
CONTENT_SIM_CROSS_OP   = 0.72
MAX_CONTENT_LEN        = 300


# ── 文字正規化 ────────────────────────────────────────────
def normalize_content(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r'[^\w\s\u4e00-\u9fff]', '', str(text), flags=re.UNICODE)
    t = unicodedata.normalize('NFKC', t)
    t = re.sub(r'\s+', '', t).lower()
    return t[:MAX_CONTENT_LEN]


def content_similarity(a: str, b: str) -> float:
    na, nb = normalize_content(a), normalize_content(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


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
            cur.execute(f"""
                SELECT post_id, platform, operator,
                       CASE WHEN media_text IS NOT NULL AND media_text != '' AND media_text NOT IN ('（無可分析內容）','（圖片無文字）')
                            THEN content || ' ' || media_text
                            ELSE content
                       END AS content,
                       event_date, category, sub_type, published_at, raw_json,
                       media_text, post_url
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


# ── 去重：content 相似度聚類 ─────────────────────────────
def dedup_by_content(posts: list[dict]) -> list[dict]:
    """
    兩階段去重：
    Pass 1: 同 operator 內，content 相似度 >= CONTENT_SIM_SAME_OP
    Pass 2: 跨 operator，content 相似度 >= CONTENT_SIM_CROSS_OP
    """

    def _group(candidates, threshold, cross_op):
        assigned = set()
        groups   = []
        for p in candidates:
            if p['post_id'] in assigned:
                continue
            group = [p]
            assigned.add(p['post_id'])
            for q in candidates:
                if q['post_id'] in assigned:
                    continue
                if not cross_op and p['operator'] != q['operator']:
                    continue
                if cross_op and p['operator'] == q['operator']:
                    continue
                sim = content_similarity(p['content'] or '', q['content'] or '')
                if sim < threshold:
                    continue
                ps, pe = parse_date_range(p.get('event_date') or '')
                qs, qe = parse_date_range(q.get('event_date') or '')
                if not dates_overlap(ps, pe, qs, qe):
                    continue
                group.append(q)
                assigned.add(q['post_id'])
            groups.append(group)
        return groups

    def _make_rep(group):
        best = max(group, key=lambda p: len(p.get('content') or ''))
        best = best.copy()
        # 收集所有 source_post_ids
        all_ids = []
        for p in group:
            all_ids += p.get('_source_post_ids', [p['post_id']])
        best['_source_post_ids'] = list(dict.fromkeys(all_ids))
        best['_source_count']    = len(best['_source_post_ids'])
        # merge category
        cats = set()
        for p in group:
            for c in (p.get('_merged_category') or p.get('category') or '').split('|'):
                c = c.strip()
                if c: cats.add(c)
        best['_merged_category'] = '|'.join(sorted(cats)) if cats else ''
        return best

    # Pass 1: 同 operator
    groups_1 = [_make_rep(g) for g in _group(posts, CONTENT_SIM_SAME_OP, cross_op=False)]
    print(f"🔄 Pass 1 (同 operator, sim≥{CONTENT_SIM_SAME_OP}): {len(posts)} → {len(groups_1)}")

    # Pass 2: 跨 operator
    groups_2 = [_make_rep(g) for g in _group(groups_1, CONTENT_SIM_CROSS_OP, cross_op=True)]
    print(f"🔄 Pass 2 (跨 operator, sim≥{CONTENT_SIM_CROSS_OP}): {len(groups_1)} → {len(groups_2)}")

    return groups_2


# ── 寫入 events_deduped ──────────────────────────────────
def ensure_deduped_table(conn: sqlite3.Connection):
    # 自動處理舊 schema（冇 content 欄位就 drop 重建）
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
    # 清空重寫（全量去重）
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


def run_dedup_pipeline(db_path: str = 'macau_analytics.db',
                       window_days: int = DATE_WINDOW_DAYS,
                       operator: str = None):
    """入庫後自動觸發，供 db_manager 呼叫。"""
    conn = sqlite3.connect(db_path)
    try:
        posts  = load_posts(conn, window_days=window_days, operator=operator)
        groups = dedup_by_content(posts)
        write_deduped_events(conn, groups)
        return groups
    finally:
        conn.close()


# ── CLI ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',       default='macau_analytics.db')
    parser.add_argument('--window',   type=int, default=DATE_WINDOW_DAYS)
    parser.add_argument('--operator', default=None)
    args = parser.parse_args()

    conn   = sqlite3.connect(args.db)
    posts  = load_posts(conn, window_days=args.window, operator=args.operator)
    groups = dedup_by_content(posts)

    src_counts = [g['_source_count'] for g in groups]
    multi      = sum(1 for c in src_counts if c > 1)
    print(f"\n📊 統計：")
    print(f"   總帖文：{len(posts)}")
    print(f"   Event groups：{len(groups)}")
    print(f"   多來源 groups：{multi} ({multi/max(len(groups),1)*100:.1f}%)")
    print(f"   平均每組來源：{sum(src_counts)/max(len(src_counts),1):.2f}")
    cat_counts = defaultdict(int)
    for g in groups:
        for c in (g.get('_merged_category') or '').split('|'):
            c = c.strip()
            if c: cat_counts[c] += 1
    print(f"   By category: {dict(sorted(cat_counts.items(), key=lambda x: -x[1]))}")

    write_deduped_events(conn, groups)
    conn.close()


if __name__ == '__main__':
    main()