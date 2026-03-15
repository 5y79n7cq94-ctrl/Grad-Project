import sqlite3
import json
import pandas as pd
import os
import datetime

DB_PATH = "C:/Users/user/MediaCrawler/macau_analytics.db"

CRAWL_EXPIRY_DAYS = 7  # operator+category 超過幾日先重新爬

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)  # 等最多30秒
    conn.execute("PRAGMA journal_mode=WAL")       # 允許讀寫並行
    conn.execute("PRAGMA busy_timeout=30000")     # 底層再等30秒
    return conn


# ── Operator 識別 ──────────────────────────────────────────
def get_operator_from_text(text):
    if not text:
        return "others"
    text = str(text).upper()
    categories = {
        "wynn":       ["永利", "WYNN", "永利皇宮", "永利澳門", "WYNN PALACE",
                       "永利宫", "永利澳门"],
        "sands":      ["金沙", "SANDS", "威尼斯人", "倫敦人", "巴黎人", "VENETIAN", "LONDONER", "PARISIAN", "百利宮", "四季", "FOUR SEASONS",
                       "伦敦人", "澳门金沙", "澳门威尼斯人", "澳门巴黎人"],
        "galaxy":     ["銀河", "GALAXY", "JW", "RITZ", "麗思卡爾頓", "安達仕", "百老匯", "BROADWAY",
                       "银河", "丽思卡尔顿", "安达仕", "百老汇", "澳门银河"],
        "mgm":        ["美高梅", "MGM", "天幕"],
        "melco":      ["新濠", "MELCO", "摩珀斯", "MORPHEUS", "影匯", "STUDIO CITY",
                       "新濠影汇", "新濠天地"],
        "sjm":        ["葡京", "SJM", "上葡京", "GRAND LISBOA", "葡京人", "澳娛綜合", "澳博",
                       "澳娱综合"],
        "government": ["澳門政府", "旅遊局", "文化局", "體育局", "市政署", "gov",
                       "澳门政府", "旅游局", "澳门旅游局"],
    }
    for operator in ["wynn", "sands", "galaxy", "mgm", "melco", "sjm", "government"]:
        for word in categories[operator]:
            if word in text:
                return operator
    return "others"


# ── Crawl Log ──────────────────────────────────────────────
def needs_crawl(operator, expiry_days=CRAWL_EXPIRY_DAYS):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT crawled_at FROM crawl_log WHERE operator=? ORDER BY crawled_at DESC LIMIT 1",
            (operator,)
        )
        row = cursor.fetchone()
    except Exception:
        row = None
    conn.close()
    if row is None:
        return True
    crawled_at = datetime.datetime.fromisoformat(row[0])
    age = (datetime.datetime.now() - crawled_at).days
    return age >= expiry_days

def mark_as_crawled(operator, category=""):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO crawl_log (operator, category, crawled_at)
           VALUES (?, '', CURRENT_TIMESTAMP)
           ON CONFLICT(operator, category)
           DO UPDATE SET crawled_at = CURRENT_TIMESTAMP""",
        (operator,)
    )
    conn.commit()
    conn.close()
    print(f"✅ crawl_log 已記錄: {operator}")

def get_ops_needing_crawl(target_ops, category="", expiry_days=CRAWL_EXPIRY_DAYS):
    return [op for op in target_ops if needs_crawl(op, expiry_days)]


# ── 入庫：政府 Excel ───────────────────────────────────────
def _is_gov_noise(name, short_desc, types_str, event_date, location_str):
    """
    判斷一條政府數據係咪純資訊噪音，唔應該入庫。
    返回 True = 噪音（跳過），False = 正常入庫。
    """
    import re as _re
    has_type_other = '其他' in str(types_str)
    has_date = bool(event_date and str(event_date) not in ('nan', 'None', '', '[]', 'NaN'))
    if not has_type_other or has_date:
        return False  # 有日期或非「其他」類 → 保留

    text = f"{name} {short_desc}"

    # 明確資訊類關鍵詞 → 一定剷走
    DEFINITE_INFO = [
        '規例', '熱線', '認可計劃', '認證計劃', '自助通關', '隨車通關',
        '聯繫方式', '無人機放飛', '推動.*綠色', '誠信店', '輕軌路線',
        '公共廁所', '實時資訊平台', '免費wi', '智慧客流',
        '入住合法旅館', '控酒', '煙草', '清真認證', '環保酒店獎',
        'M嘜.*認證', '外籍人士.*通關',
    ]
    for kw in DEFINITE_INFO:
        if _re.search(kw, text, _re.IGNORECASE):
            return True

    # 有票價或開放時間 → 係設施/場地，保留
    if '票價' in text or _re.search(r'上午\d|下午\d|晚上\d|中午\d', text):
        return False

    # 有真實地點座標 → 係場地，保留
    has_location = str(location_str) not in ('[]', '', 'nan', 'None', 'NaN')
    if has_location:
        return False

    # 冇票價/時間/地點 + 有網址/電話 → 純資訊頁，剷走
    if _re.search(r'www\.|\.gov\.mo|\(853\)', text.lower()):
        return True

    return False


def ingest_government_data(file_path):
    if not os.path.exists(file_path):
        print(f"❌ 搵唔到檔案: {file_path}")
        return
    conn = get_connection()
    df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
    cursor = conn.cursor()
    count = 0
    skipped_noise = 0
    for index, row in df.iterrows():
        name       = str(row.get('name', ''))
        location   = str(row.get('location', ''))
        short_desc = str(row.get('shortDesc', ''))
        types_str  = str(row.get('types', ''))
        event_date = row.get('eventDate', '')

        # ✏️ 過濾純資訊噪音，唔入庫
        if _is_gov_noise(name, short_desc, types_str, event_date, location):
            skipped_noise += 1
            continue

        op    = get_operator_from_text(f"{name} {location} {short_desc}")
        raw_id = row.get('id')
        db_id  = f"gov_{raw_id}" if raw_id else f"gov_idx_{index}"
        cursor.execute('''
            INSERT OR IGNORE INTO macau_events
            (id, platform, operator, title, description, event_date, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (db_id, 'government', op, name, short_desc,
              str(event_date),
              json.dumps(row.to_dict(), ensure_ascii=False)))
        if cursor.rowcount > 0:
            count += 1
    conn.commit()
    conn.close()
    print(f"🚀 政府數據入庫成功：共 {count} 條｜跳過噪音: {skipped_noise} 條")

import re
import calendar

# ── 日期抽取輔助函數 ────────────────────────────────────────

def _parse_pub_dt(post_publish_dt, default_year=2026):
    if post_publish_dt:
        try:
            pub = datetime.datetime.fromisoformat(
                str(post_publish_dt).replace('+08:00','').replace('+0800','').strip()
            )
            return pub.year, pub.month, pub.date()
        except Exception:
            pass
    return default_year, 1, None


def _infer_year(ev_month_str, pub_year, pub_month):
    ev_month = int(ev_month_str)
    if ev_month >= pub_month - 1:
        return pub_year
    return pub_year + 1


def _normalise(text):
    text = str(text).replace('\n',' ').replace('\r',' ').replace('\xa0',' ')
    text = re.sub(r'(\d)\s+([月日年])', r'\1\2', text)
    text = re.sub(r'([月])\s+(\d)',     r'\1\2', text)
    return text


def _fmt(y, m, d):
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


def _shield_ticket_dates(text):
    """屏蔽開票/售票日期段，避免誤當演出日期（支援已 normalise 嘅單行文字）"""
    # 匹配「開票/售票...」到下一個演出/地點關鍵字或句末
    return re.sub(
        r'(?:开票|售票|发售|预售|公开发售|优先发售|会员预售|开售|票务开售)'
        r'[时時]?[間间]?[：:日期]{0,4}.{0,80}?(?=演出|📍|⏰|📅|$)',
        lambda m: ' ' * len(m.group()),
        text
    )


def _scan_all_dates(text, pub_year, pub_month):
    """
    掃描 text，回傳 list of (date_str, is_jiri) tuples，按出現順序排列。
    is_jiri=True 代表「即日起至」segment（start=today），
    is_jiri=False 代表真正的活動日期。
    """
    results   = []   # (pos, date_str, is_jiri)
    used_spans= []

    def overlaps(span):
        for s, e in used_spans:
            if span[0] < e and span[1] > s:
                return True
        return False

    def add(date_str, span, is_jiri=False):
        if not overlaps(span):
            results.append((span[0], date_str, is_jiri))
            used_spans.append(span)

    # ── 即日起/至 ── 最優先，標記 is_jiri=True ──────────────────
    for m in re.finditer(r'即日(?:起至|至|起)\s*(\d{4})年(\d{1,2})月(\d{1,2})日', text):
        end = _fmt(m.group(1), m.group(2), m.group(3))
        # 儲存 end~end，is_jiri=True 標記係「即日起至」結束日
        add(f"{end}~{end}", m.span(), is_jiri=True)

    # ✏️ NEW: 即日起至 YYYY年M月（冇日）→ 自動補該月最後一日
    for m in re.finditer(r'即日(?:起至|至|起)\s*(\d{4})年(\d{1,2})月(?!\d*日)', text):
        if overlaps(m.span()): continue
        y, mo = int(m.group(1)), int(m.group(2))
        last_day = calendar.monthrange(y, mo)[1]
        end = _fmt(y, mo, last_day)
        add(f"{end}~{end}", m.span(), is_jiri=True)

    for m in re.finditer(r'即日(?:起至|至|起)\s*(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        mo, d = m.group(1), m.group(2)
        y     = _infer_year(mo, pub_year, pub_month)
        end   = _fmt(y, mo, d)
        add(f"{end}~{end}", m.span(), is_jiri=True)

    # ── M月D日起（無「至」）→ 單日開始 ─────────────────────────
    for m in re.finditer(r'(?<!\d)(\d{1,2})月(\d{1,2})日起(?!至)', text):
        if overlaps(m.span()): continue
        mo, d = m.group(1), m.group(2)
        if not (1 <= int(mo) <= 12 and 1 <= int(d) <= 31): continue
        y  = _infer_year(mo, pub_year, pub_month)
        dt = _fmt(y, mo, d)
        add(f"{dt}~{dt}", m.span())

    # ── 有年份多日 ────────────────────────────────────────────
    for m in re.finditer(
        r'(\d{4})年(\d{1,2})月(\d{1,2})日\s*[至–\-~到]\s*(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        y,m1,d1,m2,d2 = m.groups()
        add(f"{_fmt(y,m1,d1)}~{_fmt(y,m2,d2)}", m.span())

    for m in re.finditer(
        r'(\d{4})年(\d{1,2})月(\d{1,2})日[^\n]{0,20}?[及,、]\s*(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        y,m1,d1,m2,d2 = m.groups()
        add(f"{_fmt(y,m1,d1)}~{_fmt(y,m2,d2)}", m.span())

    for m in re.finditer(
        r'(\d{4})年(\d{1,2})月(\d{1,2})\s*[及,、]\s*(\d{1,2})日(?!\d)', text):
        if overlaps(m.span()): continue
        y,mo,d1,d2 = m.groups()
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    for m in re.finditer(
        r'(\d{4})年(\d{1,2})月(\d{1,2})日[^\n]{0,15}?[至及\-–~到]\s*(\d{1,2})日(?!\d)', text):
        if overlaps(m.span()): continue
        y,mo,d1,d2 = m.groups()
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    for m in re.finditer(
        r'(\d{4})年(\d{1,2})月(\d{1,2})\s*[-–]\s*(\d{1,2})日(?!\d)', text):
        if overlaps(m.span()): continue
        y,mo,d1,d2 = m.groups()
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    # 有年份單日
    for m in re.finditer(r'(\d{4})年(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        y,mo,d = m.groups()
        if not (1 <= int(mo) <= 12 and 1 <= int(d) <= 31): continue
        add(f"{_fmt(y,mo,d)}~{_fmt(y,mo,d)}", m.span())

    # ── 無年份多日 ────────────────────────────────────────────
    for m in re.finditer(
        r'(?<!\d)(\d{1,2})月(\d{1,2})日\s*[至–\-~到]\s*(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        m1,d1,m2,d2 = m.groups()
        y = _infer_year(m1, pub_year, pub_month)
        add(f"{_fmt(y,m1,d1)}~{_fmt(y,m2,d2)}", m.span())

    for m in re.finditer(
        r'(?<!\d)(\d{1,2})月(\d{1,2})日[^\n]{0,15}?[及至,]\s*\1月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        mo,d1,d2 = m.group(1),m.group(2),m.group(3)
        y = _infer_year(mo, pub_year, pub_month)
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    for m in re.finditer(
        r'(?<!\d)(\d{1,2})月(\d{1,2})(?:日)?\s*[至–\-~到及]\s*(\d{1,2})日(?!\d)', text):
        if overlaps(m.span()): continue
        mo,d1,d2 = m.groups()
        if not (1<=int(mo)<=12 and 1<=int(d1)<=31 and 1<=int(d2)<=31): continue
        y = _infer_year(mo, pub_year, pub_month)
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    for m in re.finditer(
        r'(?<!\d)(\d{1,2})月(\d{1,2})\s*[及、]\s*(\d{1,2})日(?!\d)', text):
        if overlaps(m.span()): continue
        mo,d1,d2 = m.groups()
        if not (1<=int(mo)<=12): continue
        y = _infer_year(mo, pub_year, pub_month)
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    # 無年份單日
    for m in re.finditer(r'(?<!\d)(\d{1,2})月(\d{1,2})日(?!\d)', text):
        if overlaps(m.span()): continue
        mo,d = m.groups()
        if not (1<=int(mo)<=12 and 1<=int(d)<=31): continue
        y = _infer_year(mo, pub_year, pub_month)
        add(f"{_fmt(y,mo,d)}~{_fmt(y,mo,d)}", m.span())

    # ── M月每個周末 ──────────────────────────────────────────────
    for m in re.finditer(r'(?<!\d)(\d{1,2})月(?:每[個个]?)?周[末末]', text):
        if overlaps(m.span()): continue
        mo = m.group(1)
        y  = _infer_year(mo, pub_year, pub_month)
        last = calendar.monthrange(y, int(mo))[1]
        add(f"{_fmt(y,mo,1)}~{_fmt(y,mo,last)}", m.span())

    # ── YYYY.MM.DD-DD 範圍 (e.g. 2025.12.24-25) ─────────────────
    for m in re.finditer(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})\s*[-–]\s*(\d{1,2})(?!\d)', text):
        if overlaps(m.span()): continue
        y,mo,d1,d2 = m.groups()
        if not (1<=int(mo)<=12 and 1<=int(d1)<=31 and 1<=int(d2)<=31): continue
        add(f"{_fmt(y,mo,d1)}~{_fmt(y,mo,d2)}", m.span())

    # ── YYYY.MM.DD / YYYY-MM-DD 單日 ─────────────────────────────
    for m in re.finditer(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})(?!\d)', text):
        if overlaps(m.span()): continue
        y,mo,d = m.groups()
        if not (1<=int(mo)<=12 and 1<=int(d)<=31): continue
        add(f"{_fmt(y,mo,d)}~{_fmt(y,mo,d)}", m.span())

    # ── YYMMDD 短格式 ─────────────────────────────────────────
    for m in re.finditer(r'(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)', text):
        if overlaps(m.span()): continue
        yy,mm,dd = m.groups()
        year,month,day = 2000+int(yy), int(mm), int(dd)
        if 1<=month<=12 and 1<=day<=31:
            add(f"{_fmt(year,month,day)}~{_fmt(year,month,day)}", m.span())

    results.sort(key=lambda x: x[0])
    return [(r, j) for _, r, j in results]


# ── 主函數 ────────────────────────────────────────────────
def extract_event_date(text, default_year=2026, post_publish_dt=None):
    """
    從 title/description 抽取演出日期，回傳 'YYYY-MM-DD~YYYY-MM-DD'。
    多場演出取 first_event~last_event。
    即日起至: 若唔係唯一結果，以結束日同其他活動日合並計算範圍。
    """
    if not text:
        return None
    text      = _normalise(text)
    pub_year, pub_month, _ = _parse_pub_dt(post_publish_dt, default_year)
    # 特殊：最先檢查「同月兩段非連續日期」（e.g. 11月14-16及21-23日）
    # _scan_all_dates 識別唔到呢個 pattern，必須喺 early return 之前做
    if re.search(
        r'(?<![0-9])([0-9]{1,2})月([0-9]{1,2})\s*[-–]\s*([0-9]{1,2})\s*[及、]\s*([0-9]{1,2})\s*[-–]\s*([0-9]{1,2})日',
        text
    ):
        segs = extract_multi_event_dates(text, default_year, post_publish_dt)
        if segs:
            _s2, _e2 = [], []
            for seg in segs:
                parts = seg.split('~')
                try:
                    _s2.append(datetime.date.fromisoformat(parts[0].strip()))
                    _e2.append(datetime.date.fromisoformat(parts[-1].strip()))
                except Exception:
                    pass
            if _s2:
                return f"{min(_s2).isoformat()}~{max(_e2).isoformat()}"

    clean     = _shield_ticket_dates(text)
    tagged    = _scan_all_dates(clean, pub_year, pub_month)
    if not tagged:
        return None

    # 分開「即日起」同「真實活動日期」
    jiri_dates  = [r for r, j in tagged if j]
    event_dates = [r for r, j in tagged if not j]


    # 策略：
    # 1. 如果有真實活動日期 → 用活動日期計 first~last，再將 jiri 嘅結束日納入 ends
    # 2. 如果只有 jiri → 直接回傳 jiri 結束日（即日起至X → X~X）
    all_working = event_dates if event_dates else jiri_dates
    if not all_working:
        return None

    starts, ends = [], []
    for seg in all_working:
        parts = seg.split('~')
        try:
            starts.append(datetime.date.fromisoformat(parts[0].strip()))
            ends.append(datetime.date.fromisoformat(parts[-1].strip()))
        except Exception:
            pass

    if not starts:
        return all_working[0]

    # 納入 jiri 結束日（如果有真實活動日期）
    if event_dates and jiri_dates:
        for seg in jiri_dates:
            parts = seg.split('~')
            try:
                ends.append(datetime.date.fromisoformat(parts[-1].strip()))
            except Exception:
                pass

    first = min(starts).isoformat()
    last  = max(ends).isoformat()
    return f"{first}~{last}"


def extract_multi_event_dates(text, default_year=2026, post_publish_dt=None):
    """
    回傳所有獨立日期段落 list（用於一帖多活動、非連續日期）。
    e.g. "11月14–16及21–23日" → ["2025-11-14~2025-11-16","2025-11-21~2025-11-23"]
    """
    if not text:
        return []
    text      = _normalise(text)
    pub_year, pub_month, _ = _parse_pub_dt(post_publish_dt, default_year)

    # 特殊：同月兩段「M月D-D及D-D日」
    seg_results = []
    for m in re.finditer(
        r'(?<!\d)(\d{1,2})月(\d{1,2})\s*[-–]\s*(\d{1,2})\s*[及、]\s*(\d{1,2})\s*[-–]\s*(\d{1,2})日',
        text
    ):
        mo,d1s,d1e,d2s,d2e = m.groups()
        y = _infer_year(mo, pub_year, pub_month)
        seg_results.append(f"{_fmt(y,mo,d1s)}~{_fmt(y,mo,d1e)}")
        seg_results.append(f"{_fmt(y,mo,d2s)}~{_fmt(y,mo,d2e)}")
    if seg_results:
        return seg_results

    clean  = _shield_ticket_dates(text)
    tagged = _scan_all_dates(clean, pub_year, pub_month)
    return [r for r, _ in tagged]


def extract_all_event_dates(text, default_year=2026, post_publish_dt=None):
    """向下兼容，現用 extract_multi_event_dates 實現"""
    return extract_multi_event_dates(text, default_year, post_publish_dt)


def ingest_crawler_data(json_file, platform, keyword, operator=None,
                        skip_ids=None, max_age_days=90):
    if not os.path.exists(json_file):
        print(f"❌ 搵唔到 JSON: {json_file}")
        return
    conn = get_connection()
    cursor = conn.cursor()
    with open(json_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)

    # ✏️ CHANGED: 計算截止日期
    cutoff_dt = datetime.datetime.now() - datetime.timedelta(days=max_age_days)
    skip_ids  = skip_ids or set()

    count = 0
    skipped_old = 0
    skipped_dup = 0

    for idx, post in enumerate(posts):
        raw_id  = post.get('note_id') or post.get('id') or post.get('mid') or str(idx)
        post_id = f"{platform}_{raw_id}"

        # ✏️ CHANGED: 跳過「爬之前已存在 JSON 裡嘅帖」——呢啲係之前已入庫或不需重複處理的
        if str(raw_id) in skip_ids:
            skipped_dup += 1
            continue

        # ✏️ CHANGED: 跳過超過 max_age_days 的舊帖
        pub_dt_str = post.get('create_date_time') or post.get('time') or post.get('create_time')
        if pub_dt_str:
            try:
                pub_dt = datetime.datetime.fromisoformat(
                    str(pub_dt_str).replace('+08:00', '').replace('+0800', '').strip()
                )
                if pub_dt < cutoff_dt:
                    skipped_old += 1
                    continue
            except Exception:
                pass  # parse 唔到就照入，唔強制過濾

        if platform == "weibo":
            title = ''  # 微博冇title字段
            desc  = post.get('content', '') or ''
        else:
            title = post.get('title', '') or post.get('name', '') or ''
            desc  = post.get('desc',  '') or post.get('content', '') or ''

        op  = operator or get_operator_from_text(f"{title} {desc}")

        # 抽取帖文發佈時間，用於推算無年份日期的正確年份
        pub_dt = post.get('create_date_time') or post.get('time') or post.get('create_time') or None

        # ✏️ CHANGED: 用 extract_multi_event_dates 支援多段日期
        # 多段日期（非連續活動）以逗號分隔儲存，bridge.py 的 dates_overlap() 已支援
        combined = f"{title} {desc}".strip()
        multi = (
            extract_multi_event_dates(combined, post_publish_dt=pub_dt) or
            extract_multi_event_dates(title,    post_publish_dt=pub_dt) or
            extract_multi_event_dates(desc,     post_publish_dt=pub_dt)
        )
        if multi and len(multi) > 1:
            event_date = ','.join(multi)
        elif multi:
            event_date = multi[0]
        else:
            event_date = None

        cursor.execute('''
            INSERT OR IGNORE INTO macau_events
            (id, platform, operator, keyword, title, description, event_date, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (post_id, platform, op, keyword, title, desc, event_date,
              json.dumps(post, ensure_ascii=False)))
        if cursor.rowcount > 0:
            count += 1

    conn.commit()
    conn.close()
    # ✏️ CHANGED: 更詳細的入庫報告
    print(f"📦 {platform.upper()} 入庫成功：{count} 條新帖 | "
          f"跳過舊帖(>{max_age_days}日): {skipped_old} | "
          f"跳過已有: {skipped_dup} | 總帖數: {len(posts)} (Keyword: {keyword})")


# ── 查詢 ──────────────────────────────────────────────────
def query_db_by_keyword(keyword):
    conn = get_connection()
    search_term = f'%{keyword}%'
    df = pd.read_sql_query(
        "SELECT * FROM macau_events WHERE title LIKE ? OR description LIKE ? OR keyword LIKE ?",
        conn, params=(search_term, search_term, search_term)
    )
    conn.close()
    return df

def query_db_by_filters(keyword, operators, category, max_pub_age_days=180):
    """
    精確查詢：按 keyword + operator + category 過濾
    用 title/description 關鍵字做 category 匹配，唔再豁免任何 platform
    category 必須係單一字串（由 bridge.py 負責逐個傳入）

    # ✏️ CHANGED: max_pub_age_days — 只查最近 N 日發佈嘅社媒帖文（預設180日）
    # 政府數據唔受此限制
    """
    conn = get_connection()
    ops_placeholder = ','.join(['?' for _ in operators])
    # ✏️ CHANGED: 計算社媒帖文發佈截止日期
    pub_cutoff = (datetime.datetime.now() - datetime.timedelta(days=max_pub_age_days)).strftime('%Y-%m-%d')

    # Category → 必須包含的關鍵字（title OR description）
    CATEGORY_KEYWORDS = {
        "food":          ["美食", "餐廳", "餐飲", "自助餐", "下午茶", "食評", "restaurant", "buffet", "dining", "茶餐廳", "扒房", "點心",
                          "餐厅", "餐饮", "茶餐厅", "美食地图", "自助餐", "下午茶", "晚宴", "宴席", "春茗",
                          # ✏️ 酒吧/調酒
                          "酒吧", "調酒", "雞尾酒", "特調", "微醺", "bar", "cocktail", "调酒", "鸡尾酒", "特调"],
        # ✏️ 移除「售票」「澳門站」「入場須知」(太廣)
        "concert":       ["演唱會", "音樂會", "fanmeeting", "見面會", "concert", "fancon", "演出", "音樂",
                          "演唱会", "音乐会", "见面会", "巡演", "世巡", "开唱", "抢票", "開始售票", "即將開售",
                          "live tour", "live in", "showcase",
                          "演出季", "音樂劇", "歌劇", "話劇", "舞劇", "京劇", "粵劇",
                          "音乐剧", "歌剧", "话剧", "舞剧", "京剧", "粤剧"],
        # ✏️ 移除「游泳」「GT」「賽車」「赛车」「sport」「race」「运动」「體育」(太廣或誤觸)
        # ✏️ 移除「GOLF」「高爾夫」— J.LINDEBERG時裝品牌描述有「高爾夫」主線誤觸
        "sport":         ["長跑", "馬拉松", "運動比賽", "marathon", "十公里", "10公里", "10k", "長跑賽", "乒乓球",
                          "马拉松", "长跑", "ufc", "格斗", "格蘭披治大賽", "格兰披治大赛", "fise",
                          "高爾夫球賽", "高爾夫賽事", "高尔夫球赛", "高尔夫赛事"],
        # ✏️ 移除「主題展」(動物標本館有「主題展室」)
        "crossover":     ["聯名", "快閃", "pop-up", "popup", "限定", "crossover", "泡泡瑪特", "popmart", "主題展覽",
                          "联名", "快闪", "泡泡玛特"],
        "entertainment": ["演唱會", "音樂會", "fanmeeting", "見面會", "concert", "長跑", "馬拉松", "marathon", "十公里",
                          "演唱会", "音乐会", "见面会", "马拉松", "巡演", "世巡"],
        # ✏️ 新增「博物館」「博物馆」
        "exhibition":    ["展覽", "展出", "藝術展", "art exhibition", "expo", "teamlab", "博物館", "展示館", "紀念館",
                          "展览", "艺术展", "艺术", "博物馆", "展示馆", "纪念馆"],
        "experience":    ["常駐", "vr", "sandbox", "沉浸式", "體驗館", "水舞間", "主題樂園",
                          "沉浸式", "体验", "主题乐园", "天浪淘园", "星动银河"],
        "accommodation": ["酒店優惠", "住宿", "套票", "hotel package",
                          "酒店", "住宿", "套餐", "度假"],
        # ✏️ 移除「SALE」「discount」(酒精免責聲明誤觸)、「商場」「优惠」(太廣)
        "shopping":      ["購物", "折扣", "優惠券",
                          "购物", "优惠券", "时尚汇", "旗舰店", "精品店"],
        "gaming":        ["博彩", "賭場", "casino", "積分兌換", "貴賓",
                          "博彩", "赌场", "积分", "贵宾"],
    }
    # 排除詞：title 含呢啲詞就排除
    CATEGORY_EXCLUDES = {
        "food": ["演唱會", "concert", "馬拉松", "長跑", "marathon", "10公里", "10k", "跑賽", "賽事攻略",
                 "演唱会", "马拉松", "长跑", "世界杯", "格斗"],
    }

    effective_cat = category.strip().lower() if category else ""
    cat_kws = CATEGORY_KEYWORDS.get(effective_cat, [])
    cat_excl = CATEGORY_EXCLUDES.get(effective_cat, [])

    if effective_cat and cat_kws:
        inc_clauses = " OR ".join(
            [f"title LIKE ?" for _ in cat_kws] +
            [f"description LIKE ?" for _ in cat_kws]
        )
        inc_params = [f"%{k}%" for k in cat_kws] * 2

        if cat_excl:
            excl_sql = "AND (" + " AND ".join([f"title NOT LIKE ?" for _ in cat_excl]) + ")"
            excl_params = [f"%{k}%" for k in cat_excl]
        else:
            excl_sql = ""
            excl_params = []

        category_filter = f"""
            AND (
                category = ?
                OR ((category IS NULL OR category = '') AND ({inc_clauses}) {excl_sql})
            )
        """
        extra_params = [effective_cat] + inc_params + excl_params
    else:
        category_filter = ""
        extra_params = []

    if keyword and keyword.strip():
        kw = f'%{keyword.strip()}%'
        keyword_clause = "(title LIKE ? OR description LIKE ? OR keyword LIKE ?)"
        kw_params = [kw, kw, kw]
    else:
        keyword_clause = "1=1"
        kw_params = []

    # ✏️ CHANGED: 社媒帖文只查近期發佈嘅（政府數據唔限）
    pub_date_filter = """
        AND (
            platform = 'government'
            OR json_extract(raw_json, '$.create_date_time') >= ?
            OR json_extract(raw_json, '$.time') >= ?
        )
    """

    query = f"""
        SELECT * FROM macau_events
        WHERE {keyword_clause}
        AND operator IN ({ops_placeholder})
        {category_filter}
        {pub_date_filter}
        ORDER BY CASE WHEN platform = 'government' THEN 0 ELSE 1 END,
                 COALESCE(json_extract(raw_json, '$.create_date_time'),
                          json_extract(raw_json, '$.time'), created_at) DESC
    """

    params = kw_params + list(operators) + extra_params + [pub_cutoff, pub_cutoff]

    try:
        df = pd.read_sql(query, conn, params=params)
        print(f"✅ DB 查詢成功，搵到 {len(df)} 條結果 (cat={effective_cat}, kw={keyword})")
    except Exception as e:
        print(f"❌ DB query 出錯: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    return df


# ── 寫回 AI 結果 ───────────────────────────────────────────
def update_ai_category(title, category, sub_type):
    """AI 分析完後將 category/sub_type 寫回 DB（只更新仍為空的記錄）"""
    if not title or not category:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE macau_events SET category=?, sub_type=?
           WHERE title LIKE ? AND (category IS NULL OR category='')""",
        (category, sub_type, f'%{title[:30]}%')
    )
    conn.commit()
    conn.close()

def update_ai_analysis(post_id, transcript, summary):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE macau_events SET transcript=?, ai_summary=? WHERE id=?",
        (transcript, summary, post_id)
    )
    conn.commit()
    conn.close()


# ── Backfill：補全/修正舊記錄的 event_date ────────────────
def backfill_event_dates():
    """
    每次 bridge.py 啟動時執行，修復 DB 裡嘅日期問題：
    1) 冇 event_date 嘅記錄 → 重新 parse
    2) 含「即日起至」被舊版錯誤 parse 嘅記錄 → 修正
    3) 2025 年帖文但 event_date 被錯設成 2026 → 修正
    4) ✏️ NEW: 舊版只取第一個日期嘅多場演出記錄（EXO/RIIZE/SuperJunior 等）→ 修正
    """
    conn = get_connection()
    cursor = conn.cursor()

    def get_pub_dt(raw_json_str):
        if not raw_json_str:
            return None
        try:
            rj = json.loads(raw_json_str)
            return rj.get('create_date_time') or rj.get('time') or rj.get('create_time') or None
        except Exception:
            return None

    def compute_date(title, desc, pub_dt):
        """用新版 extract_multi_event_dates 計算正確 event_date 字串"""
        combined = f"{title or ''} {desc or ''}".strip()
        multi = (
            extract_multi_event_dates(combined, post_publish_dt=pub_dt) or
            extract_multi_event_dates(title or '', post_publish_dt=pub_dt) or
            extract_multi_event_dates(desc  or '', post_publish_dt=pub_dt)
        )
        if not multi:
            return None
        if len(multi) > 1:
            return ','.join(multi)
        return multi[0]

    # 1) 冇 event_date 的記錄
    cursor.execute(
        "SELECT id, title, description, raw_json FROM macau_events "
        "WHERE (event_date IS NULL OR event_date='') AND platform != 'government'"
    )
    updated = 0
    for row_id, title, desc, raw_json_str in cursor.fetchall():
        pub_dt = get_pub_dt(raw_json_str)
        date = compute_date(title, desc, pub_dt)
        if date:
            cursor.execute("UPDATE macau_events SET event_date=? WHERE id=?", (date, row_id))
            updated += 1

    # 2) 含「即日起至」被舊版錯誤 parse（舊版會把 today 混入 start）
    cursor.execute(
        "SELECT id, title, description, raw_json FROM macau_events "
        "WHERE platform != 'government' AND event_date IS NOT NULL"
    )
    fixed_jiri = 0
    for row_id, title, desc, raw_json_str in cursor.fetchall():
        combined = f"{title or ''} {desc or ''}"
        if '即日起至' in combined or '即日至' in combined:
            pub_dt  = get_pub_dt(raw_json_str)
            correct = compute_date(title, desc, pub_dt)
            if correct:
                cursor.execute("UPDATE macau_events SET event_date=? WHERE id=?", (correct, row_id))
                fixed_jiri += 1

    # 3) 2025 年發佈但 event_date 被錯設成 2026
    cursor.execute(
        "SELECT id, title, description, raw_json FROM macau_events "
        "WHERE platform != 'government' AND event_date LIKE '2026%'"
    )
    fixed_year = 0
    for row_id, title, desc, raw_json_str in cursor.fetchall():
        pub_dt = get_pub_dt(raw_json_str)
        if not pub_dt:
            continue
        try:
            pub_year = int(str(pub_dt)[:4])
        except Exception:
            continue
        if pub_year != 2025:
            continue
        correct = compute_date(title, desc, pub_dt)
        if correct and correct[:4] == '2025':
            cursor.execute("UPDATE macau_events SET event_date=? WHERE id=?", (correct, row_id))
            fixed_year += 1

    # 4) ✏️ NEW: 舊版只取第一個日期（多場演出 start==end）→ 重新 parse 所有非 government 記錄
    #    判斷：event_date 格式係 YYYY-MM-DD~YYYY-MM-DD（start==end），
    #    但 title/desc 包含多個獨立日期 → 應為多場
    cursor.execute(
        "SELECT id, title, description, raw_json FROM macau_events "
        "WHERE platform != 'government' AND event_date IS NOT NULL "
        "AND event_date NOT LIKE '%,%'"   # 唔係已經多段格式
    )
    fixed_multi = 0
    for row_id, title, desc, raw_json_str in cursor.fetchall():
        pub_dt  = get_pub_dt(raw_json_str)
        correct = compute_date(title, desc, pub_dt)
        if correct and correct != cursor.execute(
            "SELECT event_date FROM macau_events WHERE id=?", (row_id,)
        ).fetchone()[0]:
            cursor.execute("UPDATE macau_events SET event_date=? WHERE id=?", (correct, row_id))
            fixed_multi += 1

    conn.commit()
    conn.close()
    print(f"🔧 backfill 完成：新增 {updated} 條 | 修正即日起至 {fixed_jiri} 條 | "
          f"修正年份 {fixed_year} 條 | 修正多日期 {fixed_multi} 條")