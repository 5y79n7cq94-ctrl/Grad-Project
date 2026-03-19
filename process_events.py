"""
Macau Events Pipeline — 去重、日期補充、Category過濾
用法: python process_events.py [--db PATH] [--categories sport,crossover,accommodation] [--window 90]
"""

import sqlite3
import json
import re
import unicodedata
from datetime import datetime, timedelta, date
from difflib import SequenceMatcher
from collections import defaultdict
import argparse

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
TARGET_CATEGORIES = {"sport", "crossover", "accommodation"}
DATE_WINDOW_DAYS = 90          # 90日視窗
LOOKBACK_DAYS = 30             # 保留已開始但未結束的活動往前30日
DEDUP_TITLE_THRESHOLD = 0.85   # title相似度閾值
CROSS_OPERATOR_THRESHOLD = 0.90  # 跨operator合併閾值（更嚴）


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def normalize_title(text: str) -> str:
    """移除emoji、標點、空白，轉小寫，用於相似度比較"""
    if not text:
        return ""
    # Remove emoji
    text = re.sub(r'[^\w\s\u4e00-\u9fff]', '', text, flags=re.UNICODE)
    # Normalize unicode
    text = unicodedata.normalize('NFKC', text)
    # Collapse whitespace, lowercase
    return re.sub(r'\s+', '', text).lower()[:80]


def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize_title(a), normalize_title(b)).ratio()


def parse_date_range(date_str: str) -> tuple[date | None, date | None]:
    """Parse '2026-03-15~2026-03-28' or '2026-03-15' → (start, end)"""
    if not date_str:
        return None, None
    # Take first range if multiple comma-separated
    first = date_str.split(',')[0].strip()
    if '~' in first:
        parts = first.split('~')
        try:
            start = datetime.strptime(parts[0].strip(), '%Y-%m-%d').date()
            end = datetime.strptime(parts[1].strip(), '%Y-%m-%d').date()
            return start, end
        except ValueError:
            return None, None
    else:
        try:
            d = datetime.strptime(first.strip(), '%Y-%m-%d').date()
            return d, d
        except ValueError:
            return None, None


def parse_published_at(ts: str) -> date | None:
    """Parse ISO timestamp or unix ms timestamp"""
    if not ts:
        return None
    # Try ISO format
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d'):
        try:
            return datetime.strptime(ts[:26], fmt[:len(ts)]).date()
        except (ValueError, TypeError):
            continue
    # Try unix milliseconds
    try:
        return datetime.fromtimestamp(int(ts) / 1000).date()
    except (ValueError, TypeError):
        return None


def has_category(cat_str: str | None, targets: set) -> bool:
    if not cat_str:
        return False
    cats = set(c.strip() for c in cat_str.split('|'))
    return bool(cats & targets)


def get_category_tags(cat_str: str | None) -> list[str]:
    if not cat_str:
        return []
    return [c.strip() for c in cat_str.split('|') if c.strip()]


def get_primary_category(cat_str: str | None) -> str | None:
    tags = get_category_tags(cat_str)
    # Priority order
    for preferred in ['sport', 'crossover', 'accommodation']:
        if preferred in tags:
            return preferred
    return tags[0] if tags else None


def dates_overlap(start1, end1, start2, end2) -> bool:
    """Check if two date ranges overlap"""
    if None in (start1, end1, start2, end2):
        return True  # unknown dates → assume may overlap
    return start1 <= end2 and start2 <= end1


# ──────────────────────────────────────────────
# Step 1-3: Load + Filter + Date Enrichment
# ──────────────────────────────────────────────
def load_and_enrich(conn: sqlite3.Connection, window_days: int = DATE_WINDOW_DAYS) -> list[dict]:
    today = date.today()
    cutoff_future = today + timedelta(days=window_days)
    cutoff_past = today - timedelta(days=LOOKBACK_DAYS)
    published_cutoff = today - timedelta(days=window_days)

    cur = conn.cursor()

    # Load all events — category filter applied after date enrichment
    # (NULL category events need date enrichment first, then AI re-classify)
    cur.execute("""
        SELECT id, platform, operator, title, description, event_date,
               category, sub_type, raw_json, created_at
        FROM macau_events
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    # Build published_at lookup from all post tables
    pub_lookup: dict[str, date | None] = {}
    for table in ('posts_fb', 'posts_ig', 'posts_xhs', 'posts_weibo'):
        try:
            cur.execute(f"SELECT post_id, published_at FROM {table}")
            for pid, pub in cur.fetchall():
                pub_lookup[pid] = parse_published_at(pub)
        except sqlite3.OperationalError:
            pass

    enriched = []
    for row in rows:
        ev = dict(zip(cols, row))
        eid = ev['id']

        # ── Date enrichment ──
        event_date_str = ev.get('event_date') or ''
        date_start, date_end = parse_date_range(event_date_str)
        date_source = 'event_date' if date_start else None

        published_date = pub_lookup.get(eid)
        if not date_source and published_date:
            date_source = 'published_at'

        is_permanent = (not date_start and not published_date)

        # ── Date window filter ──
        in_window = False
        if is_permanent:
            in_window = True
        elif date_start:
            effective_end = date_end or date_start
            in_window = (date_start <= cutoff_future and effective_end >= cutoff_past)
        elif published_date:
            in_window = (published_date >= published_cutoff)

        if not in_window:
            continue

        # ── Category filter ──
        cat = ev.get('category')
        cat_match = has_category(cat, TARGET_CATEGORIES)
        cat_is_null = cat is None or cat == ''

        # Drop if categorized but not in target
        if not cat_match and not cat_is_null:
            continue

        # ── Extract venue from raw_json ──
        venue = None
        raw = ev.get('raw_json')
        if raw:
            try:
                rj = json.loads(raw)
                loc = rj.get('location')
                if loc and isinstance(loc, list) and loc:
                    venue = loc[0].get('name')
                elif isinstance(loc, str):
                    venue = loc
                if not venue:
                    venue = rj.get('venue') or rj.get('place')
            except (json.JSONDecodeError, TypeError):
                pass

        # ── Extract title / description ──
        title = ev.get('title') or ''
        desc = ev.get('description') or ''

        # For social posts, title may be in raw_json
        if not title and raw:
            try:
                rj = json.loads(raw)
                title = rj.get('title') or rj.get('name') or ''
                if not desc:
                    desc = rj.get('desc') or rj.get('content') or ''
            except (json.JSONDecodeError, TypeError):
                pass

        enriched.append({
            'id': eid,
            'platform': ev.get('platform'),
            'operator': ev.get('operator'),
            'title': title,
            'description': desc,
            'event_date': event_date_str,
            'date_start': date_start,
            'date_end': date_end,
            'date_source': date_source,
            'published_date': published_date,
            'effective_date': (date_start or published_date),
            'is_permanent': is_permanent,
            'category': cat,
            'category_tags': get_category_tags(cat),
            'category_primary': get_primary_category(cat),
            'needs_ai_classify': cat_is_null,
            'venue': venue,
            'created_at': ev.get('created_at'),
            'source': 'gov' if eid.startswith('gov_') else 'social',
        })

    print(f"✅ After date filter: {len(enriched)} events "
          f"(from {len(rows)} total, window={window_days}d)")
    needs_classify = sum(1 for e in enriched if e['needs_ai_classify'])
    print(f"   └─ {needs_classify} events with NULL category → need AI re-classify")
    return enriched


# ──────────────────────────────────────────────
# Step 5: Deduplication
# ──────────────────────────────────────────────
def dedup_score(ev: dict) -> tuple:
    """Higher = keep this one. Returns tuple for sorting (higher = better)."""
    return (
        1 if ev['source'] == 'gov' else 0,         # gov > social
        1 if ev['date_source'] == 'event_date' else 0,  # has event_date
        1 if ev.get('venue') else 0,
        len(ev.get('description') or ''),           # longer description
    )


def deduplicate(events: list[dict]) -> list[dict]:
    """
    Two-pass dedup:
    Pass 1: Same operator + similar title + same date → merge
    Pass 2: Cross-operator + very similar title + overlapping date → merge
    """

    def group_and_merge(candidates: list[dict], threshold: float, cross_op: bool) -> list[dict]:
        merged_ids = set()
        groups: list[list[dict]] = []

        for i, ev in enumerate(candidates):
            if ev['id'] in merged_ids:
                continue
            group = [ev]
            for j, other in enumerate(candidates):
                if i == j or other['id'] in merged_ids:
                    continue
                if cross_op and ev['operator'] != other['operator']:
                    # Cross-operator: require venue match too
                    if ev.get('venue') and other.get('venue') and ev['venue'] != other['venue']:
                        continue
                elif not cross_op and ev['operator'] != other['operator']:
                    continue

                sim = title_similarity(ev['title'], other['title'])
                if sim < threshold:
                    continue

                date_ok = dates_overlap(
                    ev['date_start'], ev['date_end'],
                    other['date_start'], other['date_end']
                )
                if not date_ok:
                    continue

                group.append(other)
                merged_ids.add(other['id'])

            merged_ids.add(ev['id'])
            groups.append(group)

        result = []
        for group in groups:
            # Pick best representative
            best = max(group, key=dedup_score)
            # Merge metadata
            all_ops = list({g['operator'] for g in group if g.get('operator')})
            all_sources = [g['id'] for g in group]
            best = best.copy()
            best['source_operators'] = all_ops
            best['source_posts'] = all_sources
            best['source_count'] = len(all_sources)
            # Merge descriptions (pick longest)
            best['description'] = max(
                (g.get('description') or '' for g in group), key=len
            )
            result.append(best)

        return result

    print(f"🔄 Deduplication: {len(events)} events →")

    # Pass 1: Same operator
    same_op = dedup_score  # reuse
    step1 = group_and_merge(events, DEDUP_TITLE_THRESHOLD, cross_op=False)
    print(f"   Pass 1 (same operator, sim≥{DEDUP_TITLE_THRESHOLD}): {len(step1)}")

    # Pass 2: Cross operator
    step2 = group_and_merge(step1, CROSS_OPERATOR_THRESHOLD, cross_op=True)
    print(f"   Pass 2 (cross operator, sim≥{CROSS_OPERATOR_THRESHOLD}): {len(step2)}")

    return step2


# ──────────────────────────────────────────────
# Step 6: Format for AI
# ──────────────────────────────────────────────
def format_for_ai(events: list[dict], categories: list[str] = None) -> list[dict]:
    """Output structured records ready to feed AI for bilingual content generation."""
    out = []
    for ev in events:
        # Filter by category (exclude NULL-category ones unless re-classified externally)
        if not has_category(ev.get('category'), TARGET_CATEGORIES) and not ev.get('needs_ai_classify'):
            continue

        date_display = ev.get('event_date') or ''
        if not date_display and ev.get('published_date'):
            date_display = str(ev['published_date'])

        out.append({
            'event_id': ev['id'],
            'title_zh': ev.get('title') or '',
            'title_en': '',  # AI to fill if empty
            'operator': ev.get('operator'),
            'source_operators': ev.get('source_operators', [ev.get('operator')]),
            'category_primary': ev.get('category_primary'),
            'category_tags': ev.get('category_tags', []),
            'needs_ai_classify': ev.get('needs_ai_classify', False),
            'venue': ev.get('venue') or '',
            'event_date': date_display,
            'date_type': (
                'permanent' if ev.get('is_permanent') else
                'range' if ev.get('date_start') != ev.get('date_end') else
                'fixed'
            ),
            'is_permanent': ev.get('is_permanent', False),
            'date_source': ev.get('date_source'),
            'description_zh': ev.get('description') or '',
            'description_en': '',  # AI to fill if empty
            'source_posts': ev.get('source_posts', [ev['id']]),
            'source_count': ev.get('source_count', 1),
        })

    return out


# ──────────────────────────────────────────────
# AI Prompt Template
# ──────────────────────────────────────────────
AI_SYSTEM_PROMPT = """你係一個澳門旅遊活動資訊助理。你會收到一批活動記錄，每條記錄係JSON格式。

你嘅任務：
1. 如果 needs_ai_classify=true，先判斷呢個活動係咪屬於 sport / crossover / accommodation 其中之一。唔係就回傳 "drop": true。
2. 填寫 title_en（英文標題，自然流暢，唔係機械翻譯）
3. 填寫 description_en（英文描述，150字以內，適合FB/IG帖文，tone輕鬆吸引）
4. 如果 description_zh 太長，亦幫忙精簡 description_zh 至150字以內

回傳格式：JSON array，每條記錄保留原有欄位，加上/更新以下欄位：
- title_en
- description_en  
- description_zh（精簡版）
- category_primary（如needs_ai_classify=true，填寫判斷結果）
- drop（boolean，唔符合category先填true）

只回傳JSON，唔好有任何其他文字。"""


def build_ai_batch_prompt(events: list[dict], batch_size: int = 10) -> list[str]:
    """Split events into batches and generate prompts."""
    prompts = []
    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        prompt = f"請處理以下{len(batch)}個活動記錄：\n\n{json.dumps(batch, ensure_ascii=False, indent=2)}"
        prompts.append(prompt)
    return prompts


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='/home/claude/macau_analytics.db')
    parser.add_argument('--window', type=int, default=DATE_WINDOW_DAYS)
    parser.add_argument('--output', default='/home/claude/events_processed.json')
    parser.add_argument('--prompts', default='/home/claude/ai_prompts.json')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)

    # Step 1-3: Load, enrich, filter
    events = load_and_enrich(conn, window_days=args.window)

    # Step 5: Dedup
    deduped = deduplicate(events)

    # Step 6: Format
    formatted = format_for_ai(deduped)

    # Stats
    print(f"\n📊 Final output: {len(formatted)} unique events")
    print(f"   Permanent: {sum(1 for e in formatted if e['is_permanent'])}")
    print(f"   Need AI classify: {sum(1 for e in formatted if e['needs_ai_classify'])}")
    cat_counts = defaultdict(int)
    for e in formatted:
        cat_counts[e['category_primary'] or 'NULL'] += 1
    print(f"   By category: {dict(cat_counts)}")

    # Save processed events
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(formatted, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n✅ Saved to {args.output}")

    # Generate AI prompts
    prompts = build_ai_batch_prompt(formatted)
    prompt_data = {
        'system': AI_SYSTEM_PROMPT,
        'batches': prompts,
        'total_events': len(formatted),
        'batch_count': len(prompts),
    }
    with open(args.prompts, 'w', encoding='utf-8') as f:
        json.dump(prompt_data, f, ensure_ascii=False, indent=2)
    print(f"✅ AI prompts saved to {args.prompts} ({len(prompts)} batches)")

    conn.close()
    return formatted


if __name__ == '__main__':
    main()
