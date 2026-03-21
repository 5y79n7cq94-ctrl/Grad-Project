import sqlite3
import json
import pandas as pd
import os
import datetime
import threading as _threading
from post_normalizer import auto_normalize_new_post, init_post_tables
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
    init_post_tables(conn) 
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
            auto_normalize_new_post(
                conn, platform, post,
                operator=op,
                event_date=event_date,
                category=category_str,
            )

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


_CN_NUM = {'〇':'0','一':'1','二':'2','三':'3','四':'4','五':'5',
           '六':'6','七':'7','八':'8','九':'9','十':'10',
           '十一':'11','十二':'12'}

def _convert_cn_month(text):
    """將中文月份（三月、十一月等）轉為阿拉伯數字月份"""
    # 先處理兩位中文月（十一、十二）
    for cn, ar in [('十二', '12'), ('十一', '11'), ('十', '10')]:
        text = text.replace(f'{cn}月', f'{ar}月')
    # 再處理一位中文月（一至九）
    for cn, ar in [('一','1'),('二','2'),('三','3'),('四','4'),('五','5'),
                   ('六','6'),('七','7'),('八','8'),('九','9')]:
        text = re.sub(rf'(?<![十一二三四五六七八九]){cn}月', f'{ar}月', text)
    return text


def _normalise(text):
    text = str(text).replace('\n',' ').replace('\r',' ').replace('\xa0',' ')
    text = _convert_cn_month(text)                               # ✏️ NEW: 中文月份→阿拉伯
    text = re.sub(r'(\d)\s+([月日年])', r'\1\2', text)
    text = re.sub(r'([月年])\s+(\d)',   r'\1\2', text)           # ✏️ FIX: 年後空格 (2026年 3月→2026年3月)
    # ✏️ FIX: 去除日期後嘅星期括號 (3月9日（星期一） → 3月9日)，避免干擾跨月range regex
    text = re.sub(r'(\d{1,2}月\d{1,2}日)[（(]星期[一二三四五六日天][）)]', r'\1', text)
    # ✏️ NEW: 統一時間分隔符，去除日期後面的時間部分干擾 (3月3日12:00 → 3月3日)
    text = re.sub(r'(\d{1,2}月\d{1,2}日)\s*\d{1,2}[時时:點点]\d{0,2}', r'\1', text)
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


def _scan_all_dates(text, pub_year, pub_month, pub_day=None):
    """
    掃描 text，回傳 list of (date_str, is_jiri) tuples，按出現順序排列。
    is_jiri=True 代表「即日起至」segment（start=帖文發佈日），
    is_jiri=False 代表真正的活動日期。
    pub_day: 帖文發佈日（整數），用於「即日起」嘅精確 start date。
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
    def _pub_start():
        d = pub_day if pub_day and 1 <= pub_day <= 31 else 1
        return _fmt(pub_year, pub_month, d)

    for m in re.finditer(r'即日(?:起至|至|起)\s*(\d{4})年(\d{1,2})月(\d{1,2})日', text):
        end = _fmt(m.group(1), m.group(2), m.group(3))
        add(f"{_pub_start()}~{end}", m.span(), is_jiri=True)

    for m in re.finditer(r'即日(?:起至|至|起)\s*(\d{4})年(\d{1,2})月(?!\d*日)', text):
        if overlaps(m.span()): continue
        y, mo = int(m.group(1)), int(m.group(2))
        last_day = calendar.monthrange(y, mo)[1]
        end = _fmt(y, mo, last_day)
        add(f"{_pub_start()}~{end}", m.span(), is_jiri=True)

    for m in re.finditer(r'即日(?:起至|至|起)\s*(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        mo, d = m.group(1), m.group(2)
        y     = _infer_year(mo, pub_year, pub_month)
        end   = _fmt(y, mo, d)
        add(f"{_pub_start()}~{end}", m.span(), is_jiri=True)

    # ── ✏️ FIX: YYYY年M月D日起至M月D日 (e.g. 2026年3月6日起至4月5日) ─────────────
    for m in re.finditer(r'(\d{4})年(\d{1,2})月(\d{1,2})日起至(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        y,m1,d1,m2,d2 = m.groups()
        add(f"{_fmt(y,m1,d1)}~{_fmt(y,m2,d2)}", m.span())

    # ── ✏️ FIX: M月D日起至M月D日 無年份 (e.g. 3月6日起至4月5日) ─────────────────
    for m in re.finditer(r'(?<!\d)(\d{1,2})月(\d{1,2})日起至(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        m1,d1,m2,d2 = m.groups()
        y = _infer_year(m1, pub_year, pub_month)
        add(f"{_fmt(y,m1,d1)}~{_fmt(y,m2,d2)}", m.span())

    # ── M月D日起（無「至」）→ 單日開始 ─────────────────────────
    for m in re.finditer(r'(?<!\d)(\d{1,2})月(\d{1,2})日起(?!至)', text):
        if overlaps(m.span()): continue
        mo, d = m.group(1), m.group(2)
        if not (1 <= int(mo) <= 12 and 1 <= int(d) <= 31): continue
        y  = _infer_year(mo, pub_year, pub_month)
        dt = _fmt(y, mo, d)
        add(f"{dt}~{dt}", m.span())

    # ── 有年份多日 ────────────────────────────────────────────
    # ✏️ NEW: YYYY年M月D日 至 YYYY年M月D日（兩邊都有年份，跨月或跨年範圍）
    for m in re.finditer(
        r'(\d{4})年(\d{1,2})月(\d{1,2})日\s*[至–\-~到]\s*(\d{4})年(\d{1,2})月(\d{1,2})日', text):
        if overlaps(m.span()): continue
        y1,m1,d1,y2,m2,d2 = m.groups()
        add(f"{_fmt(y1,m1,d1)}~{_fmt(y2,m2,d2)}", m.span())

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

    # ── ✏️ NEW: M.DD-M.DD 無年份點分範圍 (e.g. 3.26-4.19, 2.13-2.23) ────────
    for m in re.finditer(
        r'(?<!\d)(\d{1,2})[./](\d{1,2})\s*[-–]\s*(\d{1,2})[./](\d{1,2})(?!\d)', text):
        if overlaps(m.span()): continue
        m1,d1,m2,d2 = m.groups()
        if not (1<=int(m1)<=12 and 1<=int(d1)<=31 and 1<=int(m2)<=12 and 1<=int(d2)<=31): continue
        y = _infer_year(m1, pub_year, pub_month)
        add(f"{_fmt(y,m1,d1)}~{_fmt(y,m2,d2)}", m.span())

    # ── ✏️ NEW: M.DD 無年份單日 (e.g. 3.26) ─────────────────────────────────
    for m in re.finditer(r'(?<!\d)(\d{1,2})[./](\d{2})(?!\d)', text):
        if overlaps(m.span()): continue
        mo,d = m.groups()
        if not (1<=int(mo)<=12 and 1<=int(d)<=31): continue
        y = _infer_year(mo, pub_year, pub_month)
        add(f"{_fmt(y,mo,d)}~{_fmt(y,mo,d)}", m.span())

    # ── ✏️ NEW: 單獨 M月（冇日）→ 補全為該月第1日至最後一日 ─────────────────
    # ✏️ FIX: 排除「月」後緊跟中文字嘅描述性用法（如「5月的澳門」「5月份」「5月初」）
    # 只保留「月」後接數字、標點、空白、或句末嘅真正日期用法（如「5月」「5月演出」）
    _LONE_MONTH_NOISE_RE = re.compile(
        r'[\u4e00-\u9fff]'  # 中文字
    )
    for m in re.finditer(r'(?<!\d)(\d{1,2})月(?!\d)', text):
        if overlaps(m.span()): continue
        mo = m.group(1)
        if not (1<=int(mo)<=12): continue
        # 檢查「月」後一個字符係咪中文——係就係描述性詞語，跳過
        after = text[m.end():m.end()+1]
        if after and _LONE_MONTH_NOISE_RE.match(after):
            continue
        y    = _infer_year(mo, pub_year, pub_month)
        last = calendar.monthrange(y, int(mo))[1]
        add(f"{_fmt(y,mo,1)}~{_fmt(y,mo,last)}", m.span())

    results.sort(key=lambda x: x[0])

    # ✏️ FIX: 若同月已有更精確嘅日期，移除整月展開（避免「5月」被展開成 5/1~5/31 蓋過 5/1~5/3）
    import datetime as _dt2
    def _is_whole_month(seg):
        parts = seg.split('~')
        if len(parts) != 2: return None
        try:
            s = _dt2.date.fromisoformat(parts[0].strip())
            e = _dt2.date.fromisoformat(parts[1].strip())
            if (s.day == 1 and e.day == calendar.monthrange(s.year, s.month)[1]
                    and s.month == e.month and s.year == e.year):
                return (s.year, s.month)
        except Exception:
            pass
        return None

    precise_months = set()
    for _, seg, _ in results:
        ym = _is_whole_month(seg)
        if ym is None:  # 非整月 → 記錄為「有精確日期」
            parts = seg.split('~')
            try:
                s = _dt2.date.fromisoformat(parts[0].strip())
                precise_months.add((s.year, s.month))
            except Exception:
                pass

    filtered = [(pos, seg, jiri) for pos, seg, jiri in results
                if _is_whole_month(seg) is None or _is_whole_month(seg) not in precise_months]
    results = filtered

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
    pub_year, pub_month, pub_date_obj = _parse_pub_dt(post_publish_dt, default_year)
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
    pub_day   = pub_date_obj.day if pub_date_obj else None
    tagged    = _scan_all_dates(clean, pub_year, pub_month, pub_day)
    if not tagged:
        return None

    jiri_dates  = [r for r, j in tagged if j]
    event_dates = [r for r, j in tagged if not j]

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

    if event_dates and jiri_dates:
        for seg in jiri_dates:
            parts = seg.split('~')
            try:
                starts.append(datetime.date.fromisoformat(parts[0].strip()))
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
    pub_year, pub_month, pub_date_obj = _parse_pub_dt(post_publish_dt, default_year)
    pub_day   = pub_date_obj.day if pub_date_obj else None

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
    tagged = _scan_all_dates(clean, pub_year, pub_month, pub_day)
    # ✏️ FIX: dedup完全相同嘅日期段（同一帖文多句提及同一日期會重複）
    seen = set()
    deduped = []
    for r, _ in tagged:
        if r not in seen:
            seen.add(r)
            deduped.append(r)
    return deduped


def extract_all_event_dates(text, default_year=2026, post_publish_dt=None):
    """向下兼容，現用 extract_multi_event_dates 實現"""
    return extract_multi_event_dates(text, default_year, post_publish_dt)


def _detect_all_categories(text):
    """
    ✏️ NEW: 偵測一段文字包含嘅所有 category，回傳 list。
    解決一帖多活動只被歸入第一個 category 的問題。
    """
    if not text:
        return []
    text_upper = str(text).upper()

    CATEGORY_KEYWORDS = {
        "food":          ["美食","餐廳","餐飲","自助餐","下午茶","食評","RESTAURANT","BUFFET",
                          "DINING","茶餐廳","扒房","點心","餐厅","餐饮","茶餐厅","晚宴","宴席",
                          "春茗","酒吧","調酒","雞尾酒","特調","微醺","BAR","COCKTAIL","调酒",
                          "吃什么","咖啡","食物","喝茶","佳肴","美味","口感","料理","风味","口味",
                          "一口","年糕","甜度","餐桌","汤","饮品","米其林","地道小食","茶楼","雪糕","酒"],
        "concert":       ["演唱會","音樂會","FANMEETING","見面會","CONCERT","FANCON","演出","音樂",
                          "演唱会","音乐会","见面会","巡演","世巡","开唱","抢票","LIVE TOUR","SHOWCASE",
                          "演出季","音樂劇","歌劇","話劇","舞劇","京劇","粵劇",
                          "音乐剧","歌剧","话剧","舞剧","京剧","粤剧",
                          "银河综艺馆","百老汇舞台","歌手","银河票务","门票","伦敦人综艺馆"],
        "sport":         ["長跑","馬拉松","MARATHON","十公里","10公里","10K","長跑賽","乒乓球",
                          "马拉松","长跑","UFC","格斗","格蘭披治大賽","FISE",
                          "高爾夫球賽","高爾夫賽事","高尔夫球赛","高尔夫赛事","乒兵球","选手"],
        "crossover":     ["聯名","快閃","POP-UP","POPUP","限定","CROSSOVER","泡泡瑪特","POPMART",
                          "主題展覽","联名","快闪","泡泡玛特","主題快閃","主題餐飲","贝克汉姆"],
        "entertainment": ["演唱會","音樂會","FANMEETING","見面會","CONCERT","長跑","馬拉松",
                          "演唱会","音乐会","见面会","马拉松","巡演","世巡"],
        "exhibition":    ["展覽","展出","藝術展","ART EXHIBITION","EXPO","TEAMLAB","博物館",
                          "展示館","紀念館","展览","艺术展","艺术","博物馆","展示馆","纪念馆",
                          "博览","特展","作品展","展品","展区","畫展","藝術","花展"],
        "experience":    ["常駐","VR","SANDBOX","沉浸式","體驗館","水舞間","主題樂園",
                          "体验","主题乐园","天浪淘园","星动银河",
                          "喜剧节","新春市集","贺岁","游戏","spa","健身","水疗","跑步机",
                          "魔法","魔术","打卡","游乐","乐园","主題音樂匯演"],
        "accommodation": ["酒店優惠","住宿","套票","HOTEL PACKAGE","酒店","套餐","度假",
                          "嘉佩乐","套房","客房","早餐","福布斯","瑞吉酒店",
                          "伦敦人御园","伦敦人酒店","豪华房"],
        "shopping":      ["購物","折扣","優惠券","购物","优惠券","时尚汇","旗舰店","精品店",
                          "百货","好物","產品","紀念品","手信","购物中心","消费"],
        "gaming":        ["博彩","賭場","CASINO","積分兌換","貴賓","博彩","赌场","积分","贵宾"],
    }
    CATEGORY_EXCLUDES = {
        "food": ["演唱會","CONCERT","馬拉松","長跑","MARATHON","10公里","10K","跑賽","賽事攻略",
                 "演唱会","马拉松","长跑","世界杯","格斗"],
    }

    found = []
    for cat, kws in CATEGORY_KEYWORDS.items():
        excl = CATEGORY_EXCLUDES.get(cat, [])
        if any(kw.upper() in text_upper for kw in kws):
            if not any(ex.upper() in text_upper for ex in excl):
                if cat not in found:
                    found.append(cat)
    return found


def ingest_crawler_data(json_file, platform, keyword, operator=None,
                        skip_ids=None, max_age_days=90):
    if not os.path.exists(json_file):
        print(f"❌ 搵唔到 JSON: {json_file}")
        return
    conn = get_connection()
    init_post_tables(conn)
    cursor = conn.cursor()
    with open(json_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)

    # ✏️ CHANGED: 計算截止日期
    cutoff_dt = datetime.datetime.now() - datetime.timedelta(days=max_age_days)
    skip_ids  = skip_ids or set()

    count = 0
    skipped_old = 0
    skipped_dup = 0
    new_post_ids = []  # 今次新入庫嘅 post_id，用於觸發後處理

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

        # ✏️ NEW: 偵測帖文包含嘅所有 category，以 | 分隔儲存
        # 避免一帖多活動只被歸入第一個 category 而漏掉其他
        detected_categories = _detect_all_categories(combined)
        category_str = '|'.join(detected_categories) if detected_categories else None

        cursor.execute('''
            INSERT OR IGNORE INTO macau_events
            (id, platform, operator, keyword, title, description, event_date, category, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (post_id, platform, op, keyword, title, desc, event_date,
              category_str,
              json.dumps(post, ensure_ascii=False)))
        if cursor.rowcount > 0:
            count += 1
            new_post_ids.append(post_id)
            auto_normalize_new_post(
                conn, platform, post,
                operator=op,
                event_date=event_date,
                category=category_str,
            )

    conn.commit()
    conn.close()
    print(f"📦 {platform.upper()} 入庫成功：{count} 條新帖 | "
          f"跳過舊帖(>{max_age_days}日): {skipped_old} | "
          f"跳過已有: {skipped_dup} | 總帖數: {len(posts)} (Keyword: {keyword})")

    # ── 自動觸發後處理（OCR + 去重），唔阻塞主流程 ──
    if new_post_ids:
        _trigger_post_ingest_pipeline(new_post_ids)


# ── 入庫後自動後處理 ───────────────────────────────────────

def _post_ingest_pipeline(new_post_ids: list):
    """
    入庫後依序執行：
      1. 圖片 OCR（只處理今次新入嘅帖文）
      2. 全量去重 → 更新 events_deduped table
    全程喺 background thread，唔阻塞主流程。
    """
    db_path = os.getenv("DB_PATH", "macau_analytics.db")
    print(f"⚙️  [後處理] 開始處理 {len(new_post_ids)} 條新帖...")

    # Step 1: 圖片 OCR
    try:
        from media_analyzer import run as _run_ocr
        _run_ocr(db_path=db_path, post_ids=new_post_ids)
    except Exception as e:
        print(f"⚠️  [後處理] OCR 出錯（唔影響主流程）: {e}")

    # Step 2: 去重 + 更新 events_deduped
    try:
        from process_events import run_dedup_pipeline
        run_dedup_pipeline(db_path=db_path)
    except Exception as e:
        print(f"⚠️  [後處理] 去重出錯（唔影響主流程）: {e}")

    print(f"✅ [後處理] 完成")


def _trigger_post_ingest_pipeline(new_post_ids: list):
    """另開 background thread 觸發後處理，唔阻塞入庫主流程。"""
    if not new_post_ids:
        return
    t = _threading.Thread(
        target=_post_ingest_pipeline,
        args=(new_post_ids,),
        daemon=True,
        name="post-ingest-pipeline",
    )
    t.start()
    print(f"🚀 [後處理] Background thread 已啟動（{len(new_post_ids)} 條新帖）")


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

def _expand_trad_simp(keyword):
    """繁簡互轉，返回所有變體。依賴 trad_simp.py 模組。"""
    try:
        from trad_simp import expand_variants
        return expand_variants(keyword)
    except ImportError:
        # Fallback：萬一模組唔存在，直接返回原字
        return [keyword]


def query_db_by_filters(keyword, operators, category, max_pub_age_days=180, from_date=None):
    """
    精確查詢：按 keyword + operator + category 過濾
    用 title/description 關鍵字做 category 匹配，唔再豁免任何 platform
    category 必須係單一字串（由 bridge.py 負責逐個傳入）

    # ✏️ CHANGED: pub_cutoff 改為跟 from_date - 90 日，令查詢範圍更準確
    # 若冇 from_date，fallback 用 now - max_pub_age_days
    # 政府數據唔受此限制
    """
    conn = get_connection()
    ops_placeholder = ','.join(['?' for _ in operators])

    # ✏️ CHANGED: pub_cutoff 跟 from_date - 90 日，唔再死板係 now - 180 日
    if from_date:
        try:
            base = datetime.datetime.strptime(str(from_date)[:10], '%Y-%m-%d')
            pub_cutoff = (base - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        except Exception:
            pub_cutoff = (datetime.datetime.now() - datetime.timedelta(days=max_pub_age_days)).strftime('%Y-%m-%d')
    else:
        pub_cutoff = (datetime.datetime.now() - datetime.timedelta(days=max_pub_age_days)).strftime('%Y-%m-%d')
    # 換算成毫秒 timestamp 供 XHS 比較
    pub_cutoff_ms = int(datetime.datetime.strptime(pub_cutoff, '%Y-%m-%d').timestamp() * 1000)

    # Category → 必須包含的關鍵字（title OR description）
    CATEGORY_KEYWORDS = {
        "food":          ["美食", "餐廳", "餐飲", "自助餐", "下午茶", "食評", "restaurant", "buffet", "dining", "茶餐廳", "扒房", "點心",
                          "餐厅", "餐饮", "茶餐厅", "美食地图", "自助餐", "下午茶", "晚宴", "宴席", "春茗",
                          # ✏️ 酒吧/調酒
                          "酒吧", "調酒", "雞尾酒", "特調", "微醺", "bar", "cocktail", "调酒", "鸡尾酒", "特调",
                          # ✏️ 新增關鍵字
                          "吃什么", "咖啡", "食物", "喝茶", "佳肴", "美味", "口感", "料理", "风味", "口味",
                          "一口", "年糕", "甜度", "餐桌", "汤", "饮品", "米其林", "地道小食", "茶楼", "雪糕", "酒"],
        # ✏️ 移除「售票」「澳門站」「入場須知」(太廣)
        "concert":       ["演唱會", "音樂會", "fanmeeting", "見面會", "concert", "fancon", "演出", "音樂",
                          "演唱会", "音乐会", "见面会", "巡演", "世巡", "开唱", "抢票", "開始售票", "即將開售",
                          "live tour", "live in", "showcase",
                          "演出季", "音樂劇", "歌劇", "話劇", "舞劇", "京劇", "粵劇",
                          "音乐剧", "歌剧", "话剧", "舞剧", "京剧", "粤剧",
                          # ✏️ 新增關鍵字
                          "银河综艺馆", "百老汇舞台", "歌手", "银河票务", "门票", "伦敦人综艺馆"],
        # ✏️ 移除「游泳」「GT」「賽車」「赛车」「sport」「race」「运动」「體育」(太廣或誤觸)
        # ✏️ 移除「GOLF」「高爾夫」— J.LINDEBERG時裝品牌描述有「高爾夫」主線誤觸
        "sport":         ["長跑", "馬拉松", "運動比賽", "marathon", "十公里", "10公里", "10k", "長跑賽", "乒乓球",
                          "马拉松", "长跑", "ufc", "格斗", "格蘭披治大賽", "格兰披治大赛", "fise",
                          "高爾夫球賽", "高爾夫賽事", "高尔夫球赛", "高尔夫赛事",
                          # ✏️ 新增關鍵字
                          "乒兵球", "选手"],
        # ✏️ 移除「主題展」(動物標本館有「主題展室」)
        "crossover":     ["聯名", "快閃", "pop-up", "popup", "限定", "crossover", "泡泡瑪特", "popmart", "主題展覽",
                          "联名", "快闪", "泡泡玛特",
                          # ✏️ 新增關鍵字
                          "主題快閃", "主題餐飲", "贝克汉姆"],
        "entertainment": ["演唱會", "音樂會", "fanmeeting", "見面會", "concert", "長跑", "馬拉松", "marathon", "十公里",
                          "演唱会", "音乐会", "见面会", "马拉松", "巡演", "世巡"],
        # ✏️ 新增「博物館」「博物馆」
        "exhibition":    ["展覽", "展出", "藝術展", "art exhibition", "expo", "teamlab", "博物館", "展示館", "紀念館",
                          "展览", "艺术展", "艺术", "博物馆", "展示馆", "纪念馆",
                          # ✏️ 新增關鍵字
                          "博览", "特展", "作品展", "展品", "展区", "畫展", "藝術", "花展"],
        "experience":    ["常駐", "vr", "sandbox", "沉浸式", "體驗館", "水舞間", "主題樂園",
                          "沉浸式", "体验", "主题乐园", "天浪淘园", "星动银河",
                          # ✏️ 新增關鍵字
                          "喜剧节", "新春市集", "贺岁", "游戏", "spa", "健身", "水疗", "跑步机",
                          "魔法", "魔术", "打卡", "游乐", "乐园", "主題音樂匯演"],
        "accommodation": ["酒店優惠", "住宿套票", "hotel package",
                          "酒店", "住宿", "套餐", "度假",
                          # ✏️ 新增關鍵字
                          "嘉佩乐", "套房", "客房", "早餐", "福布斯", "瑞吉酒店",
                          "伦敦人御园", "伦敦人酒店", "豪华房"],
        # ✏️ 移除「SALE」「discount」(酒精免責聲明誤觸)、「商場」「优惠」(太廣)
        "shopping":      ["購物", "折扣", "優惠券",
                          "购物", "优惠券", "时尚汇", "旗舰店", "精品店",
                          # ✏️ 新增關鍵字
                          "百货", "好物", "產品", "紀念品", "手信", "购物中心", "消费"],
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
        # Social posts_* 表：用 content 欄位
        inc_clauses_social = " OR ".join([f"content LIKE ?" for _ in cat_kws])
        inc_params_social  = [f"%{k}%" for k in cat_kws]

        # Gov macau_events 表：用 title + description 欄位
        inc_clauses_gov = " OR ".join(
            [f"title LIKE ?" for _ in cat_kws] +
            [f"description LIKE ?" for _ in cat_kws]
        )
        inc_params_gov = [f"%{k}%" for k in cat_kws] * 2

        if cat_excl:
            excl_sql_social = "AND (" + " AND ".join([f"content NOT LIKE ?"     for _ in cat_excl]) + ")"
            excl_sql_gov    = "AND (" + " AND ".join([f"title NOT LIKE ?"       for _ in cat_excl]) + ")"
            excl_params     = [f"%{k}%" for k in cat_excl]
        else:
            excl_sql_social = ""
            excl_sql_gov    = ""
            excl_params     = []

        category_filter_social = f"""
            AND (
                category LIKE ?
                OR ({inc_clauses_social}) {excl_sql_social}
            )
        """
        category_filter_gov = f"""
            AND (
                category LIKE ?
                OR ({inc_clauses_gov}) {excl_sql_gov}
            )
        """
        extra_params_social = [f"%{effective_cat}%"] + inc_params_social + excl_params
        extra_params_gov    = [f"%{effective_cat}%"] + inc_params_gov    + excl_params
    else:
        category_filter_social = ""
        category_filter_gov    = ""
        extra_params_social    = []
        extra_params_gov       = []

    if keyword and keyword.strip():
        # 多關鍵字 OR：按逗號拆分，每個 keyword 獨立搜尋 content，OR 合併
        # ✏️ 用 content 代替 title/description/keyword，兼容 posts_* 表
        # gov 資料嘅 macau_events 子查詢用同一 clause（macau_events 有 description 欄位，
        # 但 gov content 會係 description，WHERE clause 搵 content 係搵唔到）
        # → gov 子查詢單獨用 title/description/keyword
        all_clauses_social = []
        all_clauses_gov = []
        all_kw_params = []
        for single_kw in [k.strip() for k in keyword.split(',') if k.strip()]:
            variants = _expand_trad_simp(single_kw)
            per_kw_social = " OR ".join(["content LIKE ?"] * len(variants))
            per_kw_gov    = " OR ".join(
                ["title LIKE ? OR description LIKE ? OR keyword LIKE ?"] * len(variants)
            )
            all_clauses_social.append(f"({per_kw_social})")
            all_clauses_gov.append(f"({per_kw_gov})")
            all_kw_params += [f"%{v}%" for v in variants]
        keyword_clause_social = "(" + " OR ".join(all_clauses_social) + ")"
        keyword_clause_gov    = "(" + " OR ".join(all_clauses_gov) + ")"
        kw_params_social = all_kw_params
        kw_params_gov    = [f"%{v}%" for v in
                            [v for single_kw in [k.strip() for k in keyword.split(',') if k.strip()]
                             for v in _expand_trad_simp(single_kw)
                             for _ in range(3)]]
    else:
        keyword_clause_social = "1=1"
        keyword_clause_gov    = "1=1"
        kw_params_social = []
        kw_params_gov    = []

    # ✏️ CHANGED: 改從 posts_* 四個表 query（content 更整齊），
    # 政府資料繼續從 macau_events 抽（posts_* 冇 gov 資料）
    # 統一輸出欄位：id, platform, operator, title, description, event_date,
    #               category, sub_type, published_at, raw_json
    pub_date_filter_social = f"published_at >= '{pub_cutoff}'"

    query = f"""
        SELECT * FROM (
            SELECT
                post_id        AS id,
                platform,
                operator,
                substr(content, 1, 80) AS title,
                content        AS description,
                event_date,
                category,
                sub_type,
                published_at,
                raw_json,
                NULL           AS keyword,
                1              AS gov_rank,
                media_text
            FROM posts_ig
            WHERE {keyword_clause_social}
              AND operator IN ({ops_placeholder})
              {category_filter_social}
              AND {pub_date_filter_social}

            UNION ALL

            SELECT
                post_id, platform, operator,
                substr(content, 1, 80),
                content, event_date, category, sub_type,
                published_at, raw_json, NULL, 1,
                media_text
            FROM posts_fb
            WHERE {keyword_clause_social}
              AND operator IN ({ops_placeholder})
              {category_filter_social}
              AND {pub_date_filter_social}

            UNION ALL

            SELECT
                post_id, platform, operator,
                COALESCE(title, substr(content, 1, 50)),
                content, event_date, category, sub_type,
                published_at, raw_json, NULL, 1,
                media_text
            FROM posts_xhs
            WHERE {keyword_clause_social}
              AND operator IN ({ops_placeholder})
              {category_filter_social}
              AND {pub_date_filter_social}

            UNION ALL

            SELECT
                post_id, platform, operator,
                substr(content, 1, 80),
                content, event_date, category, sub_type,
                published_at, raw_json, NULL, 1,
                NULL AS media_text
            FROM posts_weibo
            WHERE {keyword_clause_social}
              AND operator IN ({ops_placeholder})
              {category_filter_social}
              AND {pub_date_filter_social}

            UNION ALL

            SELECT
                id, platform, operator,
                title, description,
                event_date, category, sub_type,
                created_at, raw_json, keyword,
                0,
                NULL AS media_text
            FROM macau_events
            WHERE platform = 'government'
              AND {keyword_clause_gov}
              AND operator IN ({ops_placeholder})
              {category_filter_gov}
        )
        ORDER BY gov_rank ASC, published_at DESC
    """

    params = (
        kw_params_social + list(operators) + extra_params_social +   # posts_ig
        kw_params_social + list(operators) + extra_params_social +   # posts_fb
        kw_params_social + list(operators) + extra_params_social +   # posts_xhs
        kw_params_social + list(operators) + extra_params_social +   # posts_weibo
        kw_params_gov    + list(operators) + extra_params_gov        # macau_events gov
    )

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
    """
    AI 分析完後將 category/sub_type 寫回 DB。
    ✏️ CHANGED: 若記錄已有 category（多 category pipe 格式），
    只在唔包含新 category 時 append，唔再覆蓋整個欄位。
    """
    if not title or not category:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, category FROM macau_events WHERE title LIKE ?",
        (f'%{title[:30]}%',)
    )
    rows = cursor.fetchall()
    for row_id, existing_cat in rows:
        if existing_cat and category in existing_cat.split('|'):
            # 已包含，只更新 sub_type
            cursor.execute(
                "UPDATE macau_events SET sub_type=? WHERE id=?",
                (sub_type, row_id)
            )
        elif existing_cat:
            # Append 新 category
            new_cat = f"{existing_cat}|{category}"
            cursor.execute(
                "UPDATE macau_events SET category=?, sub_type=? WHERE id=?",
                (new_cat, sub_type, row_id)
            )
        else:
            # 冇 category，直接寫入
            cursor.execute(
                "UPDATE macau_events SET category=?, sub_type=? WHERE id=?",
                (category, sub_type, row_id)
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

    # 5) ✏️ NEW: 含「整月範圍」嘅 event_date（跨度 ≥ 28 日）→ 重新 parse
    #    針對「5月的澳門」呢類描述性月份被舊版錯誤展開成 YYYY-MM-01~YYYY-MM-31 嘅情況
    #    新版 extract_multi_event_dates 已加 context check，重新 parse 可得正確結果
    import datetime as _dt

    def _has_wide_range(event_date_str):
        """檢查 event_date 字串是否含有跨度 ≥ 28 日嘅段落"""
        for seg in event_date_str.split(','):
            parts = seg.strip().split('~')
            if len(parts) != 2:
                continue
            try:
                s = _dt.date.fromisoformat(parts[0].strip())
                e = _dt.date.fromisoformat(parts[1].strip())
                if (e - s).days >= 28:
                    return True
            except Exception:
                pass
        return False

    cursor.execute(
        "SELECT id, title, description, raw_json, event_date FROM macau_events "
        "WHERE platform != 'government' AND event_date IS NOT NULL"
    )
    fixed_wide = 0
    for row_id, title, desc, raw_json_str, old_date in cursor.fetchall():
        if not _has_wide_range(old_date):
            continue
        pub_dt  = get_pub_dt(raw_json_str)
        correct = compute_date(title, desc, pub_dt)
        # correct 為 None 表示新版 parse 唔到任何日期，唔亂改
        if correct and correct != old_date:
            cursor.execute("UPDATE macau_events SET event_date=? WHERE id=?", (correct, row_id))
            fixed_wide += 1
            print(f"   📅 fix wide range [{row_id}]: {old_date!r} → {correct!r}")

    conn.commit()
    conn.close()
    print(f"🔧 backfill 完成：新增 {updated} 條 | 修正即日起至 {fixed_jiri} 條 | "
          f"修正年份 {fixed_year} 條 | 修正多日期 {fixed_multi} 條 | 修正整月範圍 {fixed_wide} 條")