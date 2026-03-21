import os, sys, json, uvicorn, re, hashlib, sqlite3
import pandas as pd
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from collections import defaultdict
from db_manager import query_db_by_filters, get_ops_needing_crawl, backfill_event_dates, DB_PATH
from task_manager import run_task_master
import threading
from datetime import datetime, timezone

# ✏️ CHANGED: 防止重複爬蟲 thread
# key = operator, value = True 表示而家正在爬緊
_crawling_ops: set = set()
_crawling_lock = threading.Lock()

backfill_event_dates()
app = FastAPI()

# ── Custom CORS middleware：處理本地 HTML file 嘅 null origin ──────────────
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse

class NullOriginCORSMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        origin = request.headers.get("origin", "")
        if request.method == "OPTIONS":
            response = StarletteResponse(status_code=200)
            response.headers["Access-Control-Allow-Origin"] = origin or "*"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "*"
            response.headers["Access-Control-Max-Age"] = "3600"
            return response
        response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"] = origin or "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

app.add_middleware(NullOriginCORSMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*", "null"],
    allow_origin_regex=".*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = OpenAI(api_key="sk-06452010eeff43f59e36f4d86d4d5076", base_url="https://api.deepseek.com")

# ── 繁簡轉換（共用 trad_simp 模組） ──────────────────────────────────────────
try:
    from trad_simp import expand_variants as _expand_variants
except ImportError:
    def _expand_variants(kw): return [kw]

def _kw_variants_for_filter(keyword):
    """返回 keyword 繁簡所有變體，用於 Python 後過濾相關性判斷"""
    return _expand_variants(keyword)

# ── 運營商關鍵字對照 ──────────────────────────────────────
OP_KEYWORDS = {
    "wynn":       ["永利", "WYNN", "永利皇宮", "永利澳門", "WYNN PALACE"],
    "sands":      ["金沙", "SANDS", "威尼斯人", "倫敦人", "巴黎人", "VENETIAN", "LONDONER", "PARISIAN", "百利宮", "四季", "FOUR SEASONS"],
    "galaxy":     ["銀河", "GALAXY", "JW", "RITZ", "麗思卡爾頓", "安達仕", "百老匯", "BROADWAY"],
    "mgm":        ["美高梅", "MGM", "天幕"],
    "melco":      ["新濠", "MELCO", "摩珀斯", "MORPHEUS", "影匯", "STUDIO CITY"],
    "sjm":        ["葡京", "SJM", "上葡京", "GRAND LISBOA", "澳娛綜合", "澳博"],
    # ✏️ 移除「gov」— 太短，substring match 到 .gov.mo 網址，導致其他 operator 的 gov 帖文誤入
    "government": ["澳門政府", "旅遊局", "文化局", "體育局", "市政署"],
}

# ── 分類規則（順序即優先級）────────────────────────────────
CAT_RULES = [
    # Concert
    # ✏️ 移除「售票」「澳門站」「入場須知」(太廣)
    # ✏️ 新增「演出季」「音樂劇」「歌劇」「話劇」「舞劇」— 演出類節目
    ("entertainment", "concert", [
        "演唱會", "音樂會", "FANMEETING", "見面會", "CONCERT", "FANCON", "SHOWCASE",
        "演唱会", "音乐会", "见面会", "巡演", "世巡", "开唱",
        "LIVE TOUR", "LIVE IN", "IN MACAU", "IN MACAO",
        "抢票", "開始售票", "票務開售", "即將開售", "开始售票", "即将开售",
        "演出季", "音樂劇", "歌劇", "話劇", "舞劇", "京劇", "粵劇",
        "音乐剧", "歌剧", "话剧", "舞剧", "京剧", "粤剧",
        "银河综艺馆", "百老汇舞台", "歌手", "银河票务", "门票", "伦敦人综艺馆",
    ]),

    # Sport
    # ✏️ 移除「游泳」「GT」「賽車」「格蘭披治」「極限運動」(各種誤觸)
    # ✏️ 移除「GOLF」「高爾夫」「高尔夫」— J.LINDEBERG 時裝品牌有「高爾夫」主線誤觸 sport
    #    改用更精確嘅高爾夫球賽詞
    ("entertainment", "sport", [
        "馬拉松", "MARATHON", "長跑", "长跑",
        "十公里", "10公里", "10K", "5K", "半馬", "半马",
        "乒乓球", "羽毛球", "籃球", "足球", "網球", "排球", "跑步",
        "篮球", "网球",
        "F1大獎賽", "格蘭披治大賽", "格兰披治大赛",
        "全運會", "全运会", "奧運", "奥运",
        "UFC", "格鬥賽", "格斗赛", "拳擊賽", "拳击赛",
        "高爾夫球賽", "高爾夫賽事", "高尔夫球赛", "高尔夫赛事", "GOLF TOURNAMENT", "GOLF OPEN",
        "WTT", "FISE", "乒兵球", "选手",
    ]),

    # Crossover: 聯名/快閃
    # ✏️ 移除「主題展」(動物標本館有「主題展室」)、改用「主題展覽」
    ("entertainment", "crossover", [
        "聯名", "快閃", "POP-UP", "POPUP", "泡泡瑪特", "POPMART", "主題展覽",
        "联名", "快闪", "泡泡玛特",
        "主題快閃", "主題餐飲", "贝克汉姆",
    ]),

    # Experience: 沉浸式/常駐體驗
    ("experience", None, [
        "VR", "SANDBOX", "沉浸式", "體驗館", "水舞間", "主題樂園", "常駐",
        "体验馆", "主题乐园", "天浪淘园", "星动银河", "ILLUMINARIUM", "幻影空間",
        "喜剧节", "新春市集", "贺岁", "游戏", "spa", "健身", "水疗", "跑步机",
        "魔法", "魔术", "打卡", "游乐", "乐园", "体验", "主題音樂匯演",
    ]),

    # Exhibition: 展覽
    # ✏️ 新增「博物館」作為 exhibition 觸發詞
    ("exhibition", None, [
        "展覽", "展出", "藝術展", "TEAMLAB", "EXPO", "球拍珍品", "博物館", "展示館", "紀念館",
        "展览", "艺术展", "艺荟", "博物馆", "展示馆", "纪念馆",
        "博览", "特展", "作品展", "展品", "展区", "畫展", "藝術", "花展",
    ]),

    # Food
    # ✏️ 新增酒吧/調酒詞 + 晚宴/宴 + 新關鍵字
    ("food", None, [
        "美食", "餐廳", "餐飲", "自助餐", "下午茶", "食評", "扒房", "點心", "茶餐廳",
        "火鍋", "煲仔", "葡萄酒", "品酒", "美酒", "佳釀", "評酒", "酒宴", "餐酒",
        "大師班", "品鑑", "晚宴", "宴席", "春茗",
        "BUFFET", "RESTAURANT", "DINING", "STEAKHOUSE", "WINE", "DEGUSTATION",
        "餐厅", "餐饮", "茶餐厅", "美食地图", "火锅", "品鉴",
        # 酒吧/調酒活動
        "酒吧", "調酒", "雞尾酒", "特調", "微醺", "BAR", "COCKTAIL",
        "调酒", "鸡尾酒", "特调",
        # 新增關鍵字
        "吃什么", "咖啡", "食物", "喝茶", "佳肴", "美味", "口感", "料理",
        "风味", "口味", "一口", "年糕", "甜度", "餐桌", "汤", "饮品",
        "米其林", "地道小食", "茶楼", "雪糕", "酒",
    ]),

    # Accommodation
    ("accommodation", None, [
        "酒店優惠", "住宿套票", "HOTEL PACKAGE", "住宿", "度假套", "住宿禮遇",
        "酒店住客",
        "嘉佩乐", "套房", "客房", "早餐", "福布斯", "瑞吉酒店",
        "伦敦人御园", "伦敦人酒店", "豪华房",
    ]),

    # Shopping
    # ✏️ 移除「SALE」(酒精飲品免責聲明有 "THE SALE OR SUPPLY...")
    ("shopping", None, [
        "購物", "折扣", "優惠券", "購物返現",
        "购物", "优惠券", "购物返现", "时尚汇", "旗舰店",
        "百货", "好物", "產品", "紀念品", "手信", "购物中心", "消费",
    ]),

    # Gaming
    ("gaming", None, [
        "博彩", "賭場", "CASINO", "積分兌換", "貴賓",
        "赌场", "积分", "贵宾",
    ]),
]
CAT_FOCUS = {
    "concert":       "藝人名稱、演出日期、地點、票價",
    "sport":         "賽事名稱、日期、地點、報名方式",
    "crossover":     "聯名品牌、限定商品、地點、時間",
    "experience":    "體驗名稱、特色、票價",
    "exhibition":    "展覽名稱、主題、日期、票價、地點",
    "food":          "餐廳名稱、菜式種類、限時優惠、價格",
    "accommodation": "酒店名稱、套票內容、價格",
    "shopping":      "折扣幅度、優惠期限、品牌名稱",
    "gaming":        "活動名稱、積分優惠、貴賓禮遇",
}

def classify_post(p):
    """
    根據 CAT_RULES 判斷帖文類別。
    ✏️ CHANGED: 返回所有 match 嘅 (cat, sub) 組合，唔再只返回第一個。
    呼叫方用 classify_post_all() 取 list，或 classify_post() 取 first（向下兼容）。
    """
    text = (str(p.get('title', '')) + ' ' + str(p.get('description', ''))).upper()
    for cat, sub, kws in CAT_RULES:
        if any(k.upper() in text for k in kws):
            return cat, sub
    return "experience", None


def classify_post_all(p):
    """
    ✏️ NEW: 返回帖文所有 matching (cat, sub) 組合。
    用於一帖含多種活動（如 concert + food）時唔漏掉任何 category。
    """
    text = (str(p.get('title', '')) + ' ' + str(p.get('description', ''))).upper()
    results = []
    seen = set()
    for cat, sub, kws in CAT_RULES:
        if any(k.upper() in text for k in kws):
            key = (cat, sub)
            if key not in seen:
                seen.add(key)
                results.append(key)
    return results if results else [("experience", None)]

def make_description(p):
    desc = (p.get('description', '') or '').strip()
    if (not desc or desc in ('暫無描述', 'nan', '')) and p.get('raw_json'):
        try:
            raw = json.loads(p['raw_json'])
            desc = (raw.get('shortDesc') or raw.get('description') or '').strip()
        except:
            pass
    if not desc or desc in ('(空)', 'nan', ''):
        return "暫無描述"
    if str(p.get('platform', '')) == 'government':
        parts = []
        m = re.search(r'地點[｜|]([^\s票（(]+)', desc)
        if m: parts.append(f"📍{m.group(1)}")
        m = re.search(r'票價[｜|]([^\s（(]+)', desc)
        if m: parts.append(f"票價{m.group(1)}")
        m = re.search(r'時間[｜|]([^\s地]+)', desc)
        if m: parts.append(m.group(1))
        return "　".join(parts) if parts else desc[:60].strip()
    clean = re.sub(r'#[^\s#\[]+(\[话题\])?', '', desc).strip()
    clean = re.sub(r'\s+', ' ', clean)
    return clean[:50].strip() or "暫無描述"

def _segs_have_overlap(segs: list[str]) -> bool:
    """
    檢查日期段 list 裡係咪有任何兩段互相重疊或包含。
    重疊定義：一段嘅 start <= 另一段嘅 end，且另一段嘅 start <= 此段 end。
    """
    import datetime as _dt
    parsed = []
    for seg in segs:
        parts = seg.split("~")
        try:
            s = _dt.date.fromisoformat(parts[0].strip())
            e = _dt.date.fromisoformat(parts[-1].strip())
            parsed.append((s, e))
        except Exception:
            return False  # parse 唔到就唔介入
    for i in range(len(parsed)):
        for j in range(i + 1, len(parsed)):
            s1, e1 = parsed[i]
            s2, e2 = parsed[j]
            if s1 <= e2 and s2 <= e1:  # 有重疊（包含 = 完全重疊）
                return True
    return False


def _resolve_overlapping_dates(date_str: str, post_text: str, activity_name: str) -> str:
    """
    當多段日期存在重疊/包含關係時，問 DeepSeek 判斷：
    - 係「同一活動被重複描述」→ 返回主日期段（單段）
    - 係「多場獨立場次」→ 返回原多段（全保留，前綴標記各段含義）
    - 係「不同性質日期（如預訂期+入住期）」→ 返回各段加上語意標籤

    若只有一段或冇重疊，直接返回原字串，唔問 DeepSeek。
    """
    if not date_str or date_str in ("N/A", "null", "None", ""):
        return date_str or "N/A"

    segs = [s.strip() for s in date_str.split(",") if s.strip()]
    if len(segs) <= 1:
        return date_str  # 單段，唔需要判斷

    if not _segs_have_overlap(segs):
        return date_str  # 段段不重疊，係多場活動，全保留

    # ── 有重疊：問 DeepSeek ──────────────────────────────────
    segs_display = "\n".join(f"  段{i+1}: {s}" for i, s in enumerate(segs))
    snippet = (post_text or "")[:400]
    prompt = f"""以下係從社交媒體帖文中抽取到的活動日期段落，請判斷這些日期段落的關係。

活動名稱：{activity_name}
帖文片段：
{snippet}

抽取到的日期段落：
{segs_display}

請判斷以上日期段落屬於哪種情況，並返回 JSON：

情況A：重複描述同一活動（例如帖文中同一活動日期被提及兩次，一段包含另一段）
→ 返回 {{"type": "duplicate", "primary": "主日期段（最能代表活動的那段）"}}

情況B：同一活動的多個獨立場次（例如演唱會兩場、活動每個週末舉辦）
→ 返回 {{"type": "multi_session", "segments": ["段1", "段2", ...]}}

情況C：不同性質的日期（例如預訂期與入住期、報名期與活動期）
→ 返回 {{"type": "multi_type", "segments": [{{"label": "標籤", "date": "日期段"}}, ...]}}

只返回 JSON，唔需要解釋。日期段格式保持 YYYY-MM-DD~YYYY-MM-DD。"""

    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
        result = json.loads(raw)

        dtype = result.get("type", "")

        if dtype == "duplicate":
            primary = (result.get("primary") or "").strip()
            if primary:
                print(f"📅 DeepSeek: '{activity_name}' 重複描述 → 主日期: {primary}")
                return primary

        elif dtype == "multi_session":
            resolved_segs = [s.strip() for s in (result.get("segments") or []) if s.strip()]
            if resolved_segs:
                joined = ",".join(resolved_segs)
                print(f"📅 DeepSeek: '{activity_name}' 多場次 → {joined}")
                return joined

        elif dtype == "multi_type":
            labeled = result.get("segments") or []
            if labeled:
                parts = [f"{item['label']} {item['date']}" for item in labeled
                         if item.get("label") and item.get("date")]
                if parts:
                    joined = " | ".join(parts)
                    print(f"📅 DeepSeek: '{activity_name}' 多類型日期 → {joined}")
                    return joined

    except Exception as e:
        print(f"⚠️ _resolve_overlapping_dates 出錯 ({activity_name}): {e}")

    # fallback：原樣返回
    return date_str


def dates_overlap(db_date_str, user_start, user_end):
    """檢查event_date字串係咪與查詢範圍重疊"""
    if db_date_str in ('', 'nan', 'None', 'NaN'):
        return True  # 無日期 = 常駐活動，保留
    for segment in db_date_str.split(','):
        parts = segment.strip().split('~')
        try:
            ev_start = pd.to_datetime(parts[0].strip())
            ev_end   = pd.to_datetime(parts[1].strip()) if len(parts) == 2 else ev_start
            if ev_start <= user_end and ev_end >= user_start:
                return True
        except:
            continue
    return False

def _query_events_deduped(keyword: str, operators: list, categories: list,
                           from_date: str = "", to_date: str = "") -> "pd.DataFrame":
    """
    查詢 events_deduped table（新 schema：content-based）。
    返回每個 event group 嘅代表帖文 + source_post_ids。
    """
    import sqlite3 as _sq
    try:
        from db_manager import DB_PATH as _DB_PATH
    except Exception:
        _DB_PATH = os.getenv("DB_PATH", "macau_analytics.db")
    db_path = _DB_PATH
    try:
        conn = _sq.connect(db_path)
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM events_deduped")
        if cur.fetchone()[0] == 0:
            conn.close()
            return pd.DataFrame()
        # 確認係新 schema（有 content 欄位）
        cur.execute("PRAGMA table_info(events_deduped)")
        cols = {r[1] for r in cur.fetchall()}
        if 'content' not in cols:
            print("⚠️  events_deduped 係舊 schema，請重跑 process_events.py")
            conn.close()
            return pd.DataFrame()
    except Exception:
        try: conn.close()
        except: pass
        return pd.DataFrame()

    try:
        conditions = []
        params     = []

        # ── Keyword 過濾 ──
        if keyword and keyword.strip():
            kw_clauses = []
            for single_kw in [k.strip() for k in keyword.split(',') if k.strip()]:
                try:
                    from trad_simp import expand_variants
                    variants = expand_variants(single_kw)
                except ImportError:
                    variants = [single_kw]
                per_kw = " OR ".join(["content LIKE ?"] * len(variants))
                kw_clauses.append(f"({per_kw})")
                for v in variants:
                    params += [f"%{v}%"]
            conditions.append("(" + " OR ".join(kw_clauses) + ")")

        # ── Operator 過濾 ──
        if operators:
            ops_ph = ",".join("?" * len(operators))
            conditions.append(f"operator IN ({ops_ph})")
            params += operators

        # ── 日期過濾 ──
        if from_date:
            conditions.append("(event_date >= ? OR event_date IS NULL OR event_date = '')")
            params.append(from_date[:10])
        if to_date:
            conditions.append("(event_date <= ? OR event_date IS NULL OR event_date = '')")
            params.append(to_date[:10])

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f"""
            SELECT
                event_id        AS id,
                platform,
                operator,
                content         AS description,
                event_date,
                category,
                sub_type,
                published_at,
                NULL            AS raw_json,
                NULL            AS media_text,
                source_post_ids,
                source_count,
                ai_name,
                ai_description,
                ai_category,
                ai_location,
                ai_processed
            FROM events_deduped
            {where}
            ORDER BY
                CASE WHEN event_date IS NOT NULL AND event_date != '' THEN 0 ELSE 1 END,
                event_date ASC
            LIMIT 500
        """
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        print(f"   └─ events_deduped: {len(df)} groups (kw='{keyword}', ops={operators})")
        return df
    except Exception as e:
        print(f"⚠️  events_deduped query 失敗: {e}")
        try: conn.close()
        except: pass
        return pd.DataFrame()


@app.get("/api/v2/analyze")
async def analyze(keyword: str, operators: str = "", category: str = "", from_date: str = "", to_date: str = ""):
    print(f"\n🕵️ --- 任務開始: '{keyword}' (類別: {category}) ---")

    # 1. 解析參數
    target_ops  = [op.strip().lower() for op in operators.split(",") if op.strip()] or \
                  ["sands", "galaxy", "wynn", "mgm", "melco", "sjm"]
    target_cats = [c.strip() for c in category.split(",") if c.strip()] if category else [""]

    # 2. DB 查詢：優先用 events_deduped（已去重），fallback 到原始 posts_*
    print("🔎 正在從資料庫檢索數據...")

    _deduped_df = _query_events_deduped(keyword, target_ops, target_cats, from_date, to_date)
    _use_deduped = not _deduped_df.empty
    if _use_deduped:
        print(f"✅ 用 events_deduped（{len(_deduped_df)} 條已去重 events）")
        df = _deduped_df
    else:
        print("⚠️  events_deduped 無結果，fallback 到原始 posts_*")
        # ── 唔再用 category filter 查 DB，全部帖文返回，由 AI 自己分 category ──
        df = query_db_by_filters(keyword, target_ops, "", from_date=from_date)

    # 3. 爬蟲觸發
    all_ops_to_crawl = set()
    for cat in target_cats:
        ops_for_cat = get_ops_needing_crawl(target_ops, cat)
        if ops_for_cat:
            # ✏️ CHANGED: 過濾掉已經喺爬緊嘅 operator，防止重複 launch thread
            with _crawling_lock:
                ops_to_start = [op for op in ops_for_cat if op not in _crawling_ops]
                _crawling_ops.update(ops_to_start)

            if ops_to_start:
                print(f"📢 [{cat}] 需要爬: {ops_to_start}")
                all_ops_to_crawl.update(ops_to_start)
                crawl_kw = keyword.strip() or cat

                def _crawl_and_release(kw, ops, c):
                    try:
                        run_task_master(kw, ",".join(ops), c)
                    finally:
                        # ✏️ CHANGED: 爬完（無論成功失敗）都釋放 lock
                        with _crawling_lock:
                            _crawling_ops.difference_update(ops)
                        print(f"🔓 爬蟲完成，釋放: {ops}")

                threading.Thread(target=_crawl_and_release, args=(crawl_kw, ops_to_start, cat), daemon=True).start()
            else:
                print(f"⏭️  [{cat}] {ops_for_cat} 已喺爬緊，跳過重複觸發")

    if all_ops_to_crawl and df.empty:
        return {"status": "loading", "message": "正在採集資料，請稍後重試..."}
    if all_ops_to_crawl:
        print("⚠️ 爬蟲進行中，目前用既有數據分析（結果可能不完整）")

    # 4. 日期過濾
    if from_date:
        user_start = pd.to_datetime(from_date)
        user_end   = pd.to_datetime(to_date) if to_date else user_start

        def check_date(row):
            # Gov：精確日期重疊
            if str(row.get('platform')) == 'government':
                return dates_overlap(str(row['event_date']) if row['event_date'] is not None else '', user_start, user_end)
            # ✏️ CHANGED: 社媒：用帖文發佈時間過濾，只保留近期帖（或有日期重疊嘅帖）
            # 舊邏輯係 return True（全部過），會帶入 2016-2023 年嘅舊帖
            # ✏️ FIX: 用 row['event_date'] 直接取值，避免 pandas .get() 截斷含逗號嘅多段日期
            raw_ed = row['event_date'] if 'event_date' in row.index else None
            ed = '' if raw_ed is None or (isinstance(raw_ed, float) and pd.isna(raw_ed)) else str(raw_ed).strip()
            if ed and ed not in ('nan', 'None', 'NaN', ''):
                return dates_overlap(ed, user_start, user_end)
            # 冇 event_date：睇帖文發佈時間，只接受近 30 日內發佈
            try:
                rj = json.loads(row.get('raw_json') or '{}')
                pub_str = rj.get('create_date_time') or rj.get('time') or ''
                if pub_str:
                    pub_dt = pd.to_datetime(str(pub_str)[:10])
                    cutoff = user_start - pd.Timedelta(days=30)
                    print(f"   → no event_date, pub_dt={str(pub_dt)[:10]}, cutoff={str(cutoff)[:10]}, keep={pub_dt >= cutoff}")
                    return pub_dt >= cutoff
            except:
                pass
            return True  # parse 失敗就保留

        df = df[df.apply(check_date, axis=1)]
        print(f"📅 日期過濾後剩餘 {len(df)} 條")

    unique_posts = df.to_dict(orient='records')
    print(f"📊 共 {len(unique_posts)} 條貼文")

    # 5. 逐運營商處理
    all_summaries = {}
    # ✏️ NEW: 跨 operator 已提取活動名稱 → {normalized_name: activity_dict}
    # 改用 dict 而唔係 set，令去重時可以 merge source_post_ids 而唔係直接丟棄
    # key 用 normalized name，避免全形/半形/標點差異導致同一活動比對失敗
    globally_extracted: dict = {}

    def _norm_name(s: str) -> str:
        """正規化活動名稱用於比對：去標點、全形轉半形、轉小寫"""
        import unicodedata as _ud
        s = _ud.normalize('NFKC', s or '')  # 全形→半形
        s = re.sub(r'[\s\-—–·・•]+', '', s)  # 去空格、各種破折號
        s = re.sub(r'[^\w\u4e00-\u9fff]', '', s)  # 去其他標點
        return s.lower().strip()
    for op_key in target_ops:
        if op_key not in OP_KEYWORDS:
            continue

        # 篩出屬於此運營商的帖文
        # ✏️ gov platform 帖文只靠 operator 字段匹配，唔做 keyword 匹配
        # 防止 operator='sands' 的 gov 帖文因 description 有 .gov.mo 而誤入 government op
        kws = OP_KEYWORDS[op_key]
        op_posts = [
            p for p in unique_posts
            if p.get('operator') == op_key or
            (p.get('platform') != 'government' and
             any(k.upper() in (str(p.get('title','')) + str(p.get('description',''))).upper() for k in kws))
        ]
        if not op_posts:
            all_summaries[op_key] = []
            continue

        # Gov posts：按 target_cats 過濾
        ent_subtypes = {"concert", "sport", "crossover"}
        gov_classified = []
        for p in [p for p in op_posts if p.get("platform") == "government"]:
            cat, sub = classify_post(p)
            if target_cats != [""]:
                wanted = any(
                    (tc in ent_subtypes and cat == "entertainment" and sub == tc) or
                    (tc not in ent_subtypes and cat == tc)
                    for tc in target_cats
                )
                if not wanted:
                    continue
            gov_classified.append({"post": p, "category": cat, "sub_type": sub})

        # Social posts：用 CAT_RULES 分類，再按 target_cats 過濾
        # 先對 social posts 排序：有 event_date 且在查詢範圍內的排前面；
        # event_date 明確在範圍外的排到後面（但不丟棄，因為帖文可能描述未來活動）
        raw_social = [p for p in op_posts if p.get("platform") != "government"]

        def social_sort_key(p):
            if from_date:
                ed = str(p.get("event_date") or "")
                if ed and ed not in ("nan", "None", "NaN", ""):
                    # 能解析日期就判斷是否在範圍內
                    try:
                        parts = ed.split(",")[0].strip().split("~")
                        ev_s = pd.to_datetime(parts[0].strip())
                        ev_e = pd.to_datetime(parts[-1].strip())
                        if ev_s <= user_end and ev_e >= user_start:
                            return 0   # 範圍內：最優先
                        else:
                            return 2   # 範圍外：最後
                    except:
                        pass
                return 1  # 無日期：中間
            return 1

        raw_social.sort(key=social_sort_key)

        social_classified = []
        seen_post_ids = set()
        has_keyword = bool(keyword and keyword.strip())
        for p in raw_social[:80]:
            # ✏️ CHANGED: 有 keyword 時用 multi-category（一帖可入多組讓後過濾保留相關活動）
            # 無 keyword 時用單一最佳 category，避免月度總結帖的所有活動都入錯組出現噪音
            if has_keyword:
                all_cats = classify_post_all(p)
            else:
                all_cats = [classify_post(p)]
            for cat, sub in all_cats:
                if target_cats != [""]:
                    wanted = any(
                        (tc in ent_subtypes and cat == "entertainment" and sub == tc) or
                        (tc not in ent_subtypes and cat == tc)
                        for tc in target_cats
                    )
                    if not wanted:
                        continue
                social_classified.append({"post": p, "category": cat, "sub_type": sub})

        classified = gov_classified + social_classified
        if not classified:
            all_summaries[op_key] = []
            continue

        gov_posts    = [c for c in classified if c["post"].get("platform") == "government"]
        social_posts = [c for c in classified if c["post"].get("platform") != "government"]
        activities   = []

        # Gov：每條獨立 card
        for c in gov_posts:
            p   = c["post"]
            loc = ""
            m   = re.search(r'地點[｜|]([^\s票（(]+)', p.get("description", "") or "")
            if m: loc = m.group(1).strip()
            try:
                from trad_simp import to_trad as _to_trad
                _name = _to_trad(p.get("title", "").strip())
                _desc = _to_trad(make_description(p))
                _loc  = _to_trad(loc)
            except Exception:
                _name = p.get("title", "").strip()
                _desc = make_description(p)
                _loc  = loc
            activities.append({
                "name":        _name,
                "description": _desc,
                "date":        str(p.get("event_date") or "N/A").replace("nan", "N/A"),
                "location":    _loc,
                "category":    c["category"],
                "sub_type":    c["sub_type"],
                "source":      "government",
            })

        # 社媒：聚類後每組一齊喂 DeepSeek，AI 自己去重 + 定 category
        if social_posts:
            # ── 收集所有 social post ──
            post_obj = {}
            for c in social_posts:
                pid = c["post"].get("id") or id(c["post"])
                post_obj[pid] = c["post"]

            # ── 如果係 events_deduped 來源，用 source_post_ids 重建 groups ──
            # events_deduped 每條 row 已經係一個 group，source_post_ids 記錄所有來源帖文
            if _use_deduped:
                # 每條 deduped row = 一個 group
                # source_post_ids 係 JSON array，但實際帖文 content 已經係 description 欄位
                groups = []
                for c in social_posts:
                    p = c["post"]
                    pid = p.get("id")
                    # source_post_ids 係 JSON string
                    try:
                        src_ids = json.loads(p.get("source_post_ids") or "[]")
                    except Exception:
                        src_ids = [pid] if pid else []
                    groups.append([pid] if src_ids == [] else src_ids)
                    # 確保 post_obj 有呢個 pid 嘅 content
                    post_obj[pid] = p
                print(f"   📦 {op_key}: {len(groups)} groups (from events_deduped)")
            else:
                # ── Fallback：用內容相似度聚類 ──
                from difflib import SequenceMatcher as _SM

                def _post_text(p):
                    t = (p.get("description") or p.get("title") or "")
                    return re.sub(r'\s+', '', t).lower()[:150]

                def _sim(a, b):
                    return _SM(None, _post_text(a), _post_text(b)).ratio()

                sorted_pids = sorted(post_obj.keys(), key=lambda pid: (
                    0 if str(post_obj[pid].get("event_date") or "").strip()
                    not in ("", "nan", "None", "NaN") else 1
                ))

                groups     = []
                assigned   = set()
                for pid in sorted_pids:
                    if pid in assigned:
                        continue
                    group = [pid]
                    assigned.add(pid)
                    for other_pid in sorted_pids:
                        if other_pid in assigned or len(group) >= 5:
                            continue
                        if _sim(post_obj[pid], post_obj[other_pid]) >= 0.55:
                            group.append(other_pid)
                            assigned.add(other_pid)
                    groups.append(group)
                print(f"   📦 {op_key}: {len(sorted_pids)} 條帖文 → {len(groups)} 組")

            # ── CAT_RULES keyword hint（供 AI 判斷 category 用）──
            CAT_HINTS = {
                "concert":       ["演唱會","音樂會","CONCERT","FANMEETING","見面會","巡演","SHOWCASE","音樂劇","歌劇","話劇"],
                "sport":         ["馬拉松","長跑","MARATHON","乒乓球","羽毛球","籃球","足球","網球","UFC","格鬥","球賽","賽事"],
                "crossover":     ["聯名","快閃","POP-UP","POPUP","限定","POPMART","泡泡瑪特","主題展覽"],
                "experience":    ["沉浸式","VR","體驗","主題樂園","水舞間","常駐","打卡","樂園"],
                "exhibition":    ["展覽","展出","藝術展","EXPO","博物館","特展","畫展"],
                "food":          ["美食","餐廳","晚餐","自助餐","下午茶","咖啡","火鍋","晚宴","米其林","酒吧","調酒"],
                "accommodation": ["酒店","住宿","套房","度假","客房","早餐"],
                "shopping":      ["購物","折扣","優惠券","紀念品","手信","旗艦店"],
                "gaming":        ["博彩","賭場","CASINO","積分","貴賓"],
            }
            cat_hint_str = "\n".join(
                f"- {cat}：{' / '.join(kws[:8])}"
                for cat, kws in CAT_HINTS.items()
            )

            # ── 逐組喂 DeepSeek ──
            for group_pids in groups:
                # 組合呢組嘅所有帖文內容
                snippets = []
                group_post_ids = []
                for i, pid in enumerate(group_pids):
                    p = post_obj.get(pid)
                    if p is None:
                        # source_post_ids 可能包含唔喺呢個 operator 嘅帖文，skip
                        group_post_ids.append(pid)  # 仍然記錄 ID 供 source_posts 用
                        continue
                    title = (p.get("title") or "").strip()
                    desc  = (p.get("description") or "").strip()
                    if len(desc) < 30 and p.get("raw_json"):
                        try:
                            raw  = json.loads(p["raw_json"])
                            desc = (raw.get("desc") or raw.get("content") or raw.get("shortDesc") or desc).strip()
                        except:
                            pass
                    if not title and not desc:
                        continue

                    post_date = ""
                    if p.get("raw_json"):
                        try:
                            raw = json.loads(p["raw_json"])
                            dt  = raw.get("create_date_time") or raw.get("time") or ""
                            if dt:
                                post_date = f"（發佈：{str(dt)[:10]}）"
                        except:
                            pass

                    media_text = (p.get("media_text") or "").strip()
                    media_text = "" if media_text in ("（無可分析內容）", "（圖片無文字）") else media_text

                    snippet = f"【帖文{i+1}】{post_date}標題: {title}\n內容: {desc[:600] or '(空)'}"
                    if media_text:
                        snippet += f"\n圖片OCR: {media_text[:400]}"
                    snippets.append(snippet)
                    group_post_ids.append(pid)

                if not snippets:
                    continue

                all_seen_names = set(a["name"] for a in activities) | \
                                 {v["name"] for v in globally_extracted.values() if v.get("name")}
                seen_hint = "、".join(sorted(all_seen_names)) if all_seen_names else "（無）"
                date_hint  = (
                    f"用戶查詢日期範圍：{from_date} 至 {to_date}。"
                    f"活動日期必須從帖文原文明確提取，嚴禁推算或捏造，冇明確日期填 null。"
                ) if from_date and to_date else ""

                prompt = f"""你係澳門活動資訊整合助手。以下係來自 {op_key} 嘅 {len(snippets)} 條相關帖文。{date_hint}

請識別所有獨立活動，相似或重複嘅活動只算一個。

Category 判斷規則：
{cat_hint_str}
- 唔符合以上任何一個 → category 填 "other"

任務：
1. 睇晒所有帖文，識別每一個獨立活動（唔同帖文講同一活動只算一個）
2. 以下活動已提取，**概念上相同嘅唔需要重複**（即使名稱唔同、語言唔同、描述角度唔同，只要係同一個活動就唔輸出）：
   {seen_hint}
3. 每個活動輸出一個 JSON object：
   - "name": 活動名稱（簡潔，20字以內，統一用繁體中文，英文活動名可保留英文）
   - "description": 重點描述，50-80字，繁體中文
   - "date": 帖文原文明確出現嘅日期（YYYY-MM-DD 或 YYYY-MM-DD~YYYY-MM-DD），冇就填 null
   - "location": 地點（冇就填 null）
   - "category": 根據上面規則判斷
   - "source_indices": 呢個活動主要來自第幾條帖文（array，例如 [1,2]）
4. 只返回 JSON array，唔需要任何前言
5. 如果帖文只係recap已過去嘅活動（例如「圓滿落幕」「感謝到場」），唔需要提取

帖文內容：
{"=" * 40}
{chr(10).join(snippets)}
{"=" * 40}

直接輸出 JSON array："""

                try:
                    resp     = client.chat.completions.create(
                        model="deepseek-chat",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=1200,
                    )
                    raw_resp = re.sub(r'^```[a-z]*\n?', '', (resp.choices[0].message.content or "").strip()).rstrip('`').strip()
                    extracted = json.loads(raw_resp)
                    print(f"✅ DeepSeek 識別 {len(extracted)} 個活動（{len(snippets)} 條帖文）")
                except Exception as e:
                    print(f"⚠️ DeepSeek 出錯: {e}")
                    p0 = next((post_obj.get(gp) for gp in group_pids if post_obj.get(gp)), {})
                    extracted = [{"name": (p0.get("title") or "")[:40] or f"{op_key}活動",
                                  "description": make_description(p0),
                                  "date": None, "location": None,
                                  "category": None, "source_indices": [1]}]

                for item in extracted:
                    item_name = (item.get("name") or "").strip()
                    item_desc = (item.get("description") or "").strip()
                    item_cat  = (item.get("category") or "").strip().lower()
                    item_date = (item.get("date") or "").strip()

                    # Drop "other" 或空 category
                    if item_cat in ("other", ""):
                        print(f"⏭️ category=other，drop: {item_name}")
                        continue

                    # 合法 category 清單
                    VALID_CATS = {"concert","sport","crossover","experience",
                                  "exhibition","food","accommodation","shopping","gaming"}
                    if item_cat not in VALID_CATS:
                        print(f"⏭️ 非法 category '{item_cat}'，drop: {item_name}")
                        continue

                    # 繁體化
                    try:
                        from trad_simp import to_trad as _to_trad
                        item_desc = _to_trad(item_desc)
                        item_name = _to_trad(item_name)
                    except:
                        pass

                    # keyword 相關性後過濾
                    if keyword and keyword.strip():
                        item_text = (item_name + " " + item_desc).upper()
                        matched = False
                        for single_kw in [k.strip() for k in keyword.split(',') if k.strip()]:
                            variants = _kw_variants_for_filter(single_kw)
                            if any(v.upper() in item_text for v in variants):
                                matched = True
                                break
                        if not matched:
                            print(f"⏭️ keyword 過濾：'{item_name}' 與關鍵字「{keyword}」無關，跳過")
                            continue

                    # concert/sport/crossover → entertainment sub_type
                    if item_cat in ("concert", "sport", "crossover"):
                        sub, cat_out = item_cat, "entertainment"
                    else:
                        sub, cat_out = None, item_cat

                    # 日期 hallucination 防護
                    all_snippets_text = "\n".join(snippets)
                    if item_date and item_date not in ("N/A", "null", "None", ""):
                        def _date_in_text(d, txt):
                            d = d.split("~")[0].strip()[:10]
                            if d in txt or d[:7] in txt:
                                return True
                            try:
                                import datetime as _dt
                                obj = _dt.date.fromisoformat(d)
                                m, day = obj.month, obj.day
                                if re.search(rf'{m}月{day}[日号]?', txt): return True
                                if re.search(rf'(?<!\d){m}[./]{day:02d}(?!\d)', txt): return True
                                EN_MON = ['','Jan','Feb','Mar','Apr','May','Jun',
                                          'Jul','Aug','Sep','Oct','Nov','Dec']
                                if re.search(rf'{EN_MON[m]}[\s.]*{day}', txt, re.IGNORECASE): return True
                            except:
                                pass
                            return False
                        if not _date_in_text(item_date, all_snippets_text):
                            print(f"⚠️ 日期 '{item_date}' 唔見於帖文，reset null")
                            item_date = ""

                    # fallback 用 DB event_date
                    if not item_date or item_date in ("N/A", "null", "None", ""):
                        for gp in group_pids:
                            gp_post = post_obj.get(gp)
                            if gp_post is None:
                                continue
                            db_date = str(gp_post.get("event_date") or "").strip()
                            if db_date and db_date not in ("nan", "None", "NaN", ""):
                                item_date = db_date
                                print(f"📅 '{item_name}' 用 DB event_date: {item_date}")
                                break

                    # 日期範圍過濾
                    if from_date and to_date:
                        if item_date and item_date not in ("N/A", "null", "None", ""):
                            if not dates_overlap(item_date, pd.to_datetime(from_date), pd.to_datetime(to_date)):
                                print(f"⏭️ 日期範圍外，跳過: {item_name} ({item_date})")
                                continue
                        else:
                            try:
                                _p0 = next((post_obj.get(gp) for gp in group_pids if post_obj.get(gp)), {})
                                rj = json.loads((_p0.get("raw_json") or "{}"))
                                pub_str = rj.get("create_date_time") or rj.get("time") or ""
                                if pub_str:
                                    pub_dt = pd.to_datetime(str(pub_str)[:10])
                                    window_start = pd.to_datetime(from_date) - pd.Timedelta(days=30)
                                    window_end   = pd.to_datetime(to_date)   + pd.Timedelta(days=30)
                                    if not (window_start <= pub_dt <= window_end):
                                        print(f"⏭️ '{item_name}' 帖文太舊，跳過")
                                        continue
                            except:
                                pass

                    # 跨 operator dedup：merge source_post_ids
                    _norm_key = _norm_name(item_name)
                    if item_name and _norm_key in globally_extracted:
                        existing = globally_extracted[_norm_key]
                        for gp in group_post_ids:
                            if gp not in existing.get("source_post_ids", []):
                                existing.setdefault("source_post_ids", []).append(gp)
                        print(f"🔀 跨 operator 重複，merge: {item_name}")
                        continue

                    new_act = {
                        "name":            item_name,
                        "description":     item_desc or "暫無描述",
                        "date":            _resolve_overlapping_dates(item_date, post_text=all_snippets_text, activity_name=item_name),
                        "location":        item.get("location") or "",
                        "category":        cat_out,
                        "sub_type":        sub,
                        "source_post_ids": group_post_ids,
                    }
                    activities.append(new_act)
                    if item_name:
                        globally_extracted[_norm_name(item_name)] = new_act

        all_summaries[op_key] = activities
        for act in activities:
            n = act.get("name", "").strip()
            nk = _norm_name(n)
            if n and nk not in globally_extracted:
                globally_extracted[nk] = act
        print(f"✅ {op_key}: {len(activities)} 張 card (gov={len(gov_posts)}, social={len(activities)-len(gov_posts)})")

    # ── 重組：by category，每個 activity 附上 operator 資訊 ────────
    # 出口 category filter：只保留用家要嘅 category
    ent_subtypes_out = {"concert", "sport", "crossover"}
    cat_summaries = defaultdict(list)
    _cat_seen_names: set = set()
    for op_key, activities in all_summaries.items():
        for act in activities:
            act_with_op = dict(act, operator=op_key)
            cat_key  = act.get('sub_type') or act.get('category') or 'experience'
            act_name = (act.get('name') or '').strip()

            # ── 出口 category filter ──
            if target_cats and target_cats != [""]:
                wanted = any(
                    (tc in ent_subtypes_out and cat_key == tc) or
                    (tc not in ent_subtypes_out and (cat_key == tc or act.get('category') == tc))
                    for tc in target_cats
                )
                if not wanted:
                    print(f"⏭️ 出口 filter：{act_name} (category={cat_key}) 唔係用家要嘅 {target_cats}")
                    continue

            dedup_key = f"{act_name}|{op_key}"
            if act_name and dedup_key in _cat_seen_names:
                print(f"⏭️ 跨 category 重複，跳過: {act_name} ({op_key})")
                continue
            if act_name:
                _cat_seen_names.add(dedup_key)
            cat_summaries[cat_key].append(act_with_op)

    # ── 為每張 card 補充 source_posts 詳情 ────────────────
    all_source_ids = set()
    for acts in cat_summaries.values():
        for act in acts:
            for sid in (act.get("source_post_ids") or []):
                all_source_ids.add(sid)

    post_details = {}
    if all_source_ids:
        try:
            import sqlite3 as _sq2
            from db_manager import DB_PATH as _SRC_DB_PATH
            _conn = _sq2.connect(_SRC_DB_PATH)
            _cur  = _conn.cursor()
            for table, platform_name in [
                ("posts_xhs", "xhs"), ("posts_ig", "ig"),
                ("posts_fb", "fb"),   ("posts_weibo", "weibo"),
            ]:
                try:
                    placeholders = ",".join("?" * len(all_source_ids))
                    _cur.execute(f"""
                        SELECT post_id, published_at, post_url,
                               substr(content, 1, 60) AS title_preview
                        FROM {table}
                        WHERE post_id IN ({placeholders})
                    """, list(all_source_ids))
                    for pid, pub_at, post_url, title_preview in _cur.fetchall():
                        post_details[pid] = {
                            "post_id":      pid,
                            "platform":     platform_name,
                            "url":          post_url or "",
                            "title":        title_preview or "",
                            "published_at": str(pub_at or "")[:10],
                        }
                except _sq2.OperationalError:
                    pass
            _conn.close()
        except Exception as e:
            print(f"⚠️ source_posts 補充失敗（唔影響主結果）: {e}")

    for acts in cat_summaries.values():
        for act in acts:
            ids = act.get("source_post_ids") or []
            act["source_posts"] = [post_details[i] for i in ids if i in post_details]

    return {
        "status":             "success",
        "operator_summaries": all_summaries,
        "category_summaries": dict(cat_summaries),
    }

@app.post("/api/hot-themes")
async def hot_themes(payload: dict):
    """
    接收 event names + descriptions，用 DeepSeek 返回 2-3 個 semantic hot themes。
    payload: { "events": [ {"name": "...", "description": "..."}, ... ] }
    """
    events = payload.get("events", [])
    if not events:
        return {"themes": []}

    lines = []
    for i, ev in enumerate(events[:200], 1):
        name = (ev.get("name") or "").strip()
        desc = (ev.get("description") or ev.get("desc") or "").strip()[:80]
        if name:
            lines.append(f"{i}. {name}{'：' + desc if desc else ''}")

    if not lines:
        return {"themes": []}

    event_list = "\n".join(lines)
    prompt = f"""以下係澳門各博企近期嘅活動列表：

{event_list}

請分析以上活動，識別出 2-3 個最突出嘅市場主題（hot themes）。
主題應該係具體嘅概念，例如「韓星演唱會」、「葡萄酒品鑑」、「沉浸式體驗」、「非遺文化」，而唔係籠統嘅字眼如「活動」、「體驗」、「娛樂」。
每個主題用 3-8 個字表達，繁體中文。

只返回 JSON array，例如：["韓星演唱會熱潮", "精品葡萄酒文化", "沉浸式視覺體驗"]
唔需要任何解釋，只輸出 JSON array。"""

    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
        themes = json.loads(raw)
        if not isinstance(themes, list):
            themes = []
        try:
            from trad_simp import to_trad as _to_trad
            themes = [_to_trad(t) for t in themes[:3]]
        except Exception:
            themes = themes[:3]
        print(f"🔥 Hot themes: {themes}")
        return {"themes": themes}
    except Exception as e:
        print(f"⚠️ hot_themes 出錯: {e}")
        return {"themes": []}


# ══════════════════════════════════════════════════════════════════════════════
# AUTH & USER MANAGEMENT ROUTES  (merged from server.py)
# All user data is stored in the users table inside macau_analytics.db
# ══════════════════════════════════════════════════════════════════════════════

def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def _get_auth_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def _ensure_users_table():
    conn = _get_auth_conn()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            "User ID"       INTEGER PRIMARY KEY AUTOINCREMENT,
            "First Name"    TEXT    NOT NULL,
            "Last Name"     TEXT    NOT NULL,
            "Email Address" TEXT    NOT NULL UNIQUE,
            "Password"      TEXT    NOT NULL,
            "Date Joined"   TEXT    NOT NULL,
            "Department"    TEXT    NOT NULL,
            "Position"      TEXT    NOT NULL,
            "Role"          TEXT    NOT NULL DEFAULT \'user\'
        )
    ''')
    try:
        conn.execute("ALTER TABLE users ADD COLUMN \"Role\" TEXT NOT NULL DEFAULT 'user'")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

_ensure_users_table()


def _require_admin(data: dict):
    """Return (admin_row, None) or (None, error_dict)."""
    admin_email = (data.get("admin_email") or "").strip().lower()
    admin_token = (data.get("admin_token") or "")
    if not admin_email or not admin_token:
        return None, {"success": False, "message": "Admin credentials required."}
    conn = _get_auth_conn()
    try:
        admin = conn.execute(
            'SELECT * FROM users WHERE "Email Address"=? AND "Password"=?',
            (admin_email, admin_token)
        ).fetchone()
        if not admin:
            return None, {"success": False, "message": "Invalid admin credentials."}
        if admin["Position"] != "IT Admin":
            return None, {"success": False, "message": "Access denied. IT Admin only."}
        return admin, None
    finally:
        conn.close()


@app.post("/register")
async def auth_register(request: Request):
    data     = await request.json()
    first    = (data.get("first_name") or "").strip()
    last     = (data.get("last_name")  or "").strip()
    email    = (data.get("email")      or "").strip().lower()
    dept     = (data.get("department") or "").strip()
    position = (data.get("position")   or "").strip()
    password = (data.get("password")   or "")
    role     = (data.get("role")       or "user").strip().lower()
    if role not in ("user", "admin"):
        role = "user"
    if not all([first, last, email, dept, position, password]):
        return {"success": False, "message": "All fields are required."}
    conn = _get_auth_conn()
    try:
        date_joined = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute(
            'INSERT INTO users ("First Name","Last Name","Email Address","Password","Date Joined","Department","Position","Role") VALUES (?,?,?,?,?,?,?,?)',
            (first, last, email, _hash_password(password), date_joined, dept, position, role)
        )
        conn.commit()
        uid = conn.execute('SELECT "User ID" FROM users WHERE "Email Address"=?', (email,)).fetchone()["User ID"]
        return {"success": True, "user_id": str(uid).zfill(4)}
    except sqlite3.IntegrityError:
        return {"success": False, "message": "An account with that email already exists."}
    finally:
        conn.close()


@app.post("/login")
async def auth_login(request: Request):
    data     = await request.json()
    email    = (data.get("email")    or "").strip().lower()
    password = (data.get("password") or "")
    if not email or not password:
        return {"success": False, "message": "Please enter your email and password."}
    conn = _get_auth_conn()
    try:
        user = conn.execute(
            'SELECT * FROM users WHERE "Email Address"=? AND "Password"=?',
            (email, _hash_password(password))
        ).fetchone()
        if not user:
            return {"success": False, "message": "Invalid email or password."}
        return {
            "success"   : True,
            "user_id"   : str(user["User ID"]).zfill(4),
            "first_name": user["First Name"],
            "last_name" : user["Last Name"],
            "department": user["Department"],
            "position"  : user["Position"],
            "role"      : user["Role"] if "Role" in user.keys() else "user",
        }
    finally:
        conn.close()


@app.post("/change-password")
async def auth_change_password(request: Request):
    data         = await request.json()
    email        = (data.get("email")        or "").strip().lower()
    old_password = (data.get("old_password") or "")
    new_password = (data.get("new_password") or "")
    if not all([email, old_password, new_password]):
        return {"success": False, "message": "All fields are required."}
    if len(new_password) < 8:
        return {"success": False, "message": "New password must be at least 8 characters."}
    conn = _get_auth_conn()
    try:
        user = conn.execute(
            'SELECT * FROM users WHERE "Email Address"=? AND "Password"=?',
            (email, _hash_password(old_password))
        ).fetchone()
        if not user:
            return {"success": False, "message": "Current password is incorrect."}
        conn.execute('UPDATE users SET "Password"=? WHERE "Email Address"=?',
                     (_hash_password(new_password), email))
        conn.commit()
        return {"success": True, "message": "Password updated successfully."}
    finally:
        conn.close()


@app.post("/check-email")
async def auth_check_email(request: Request):
    data  = await request.json()
    email = (data.get("email") or "").strip().lower()
    conn  = _get_auth_conn()
    try:
        user = conn.execute('SELECT "User ID" FROM users WHERE "Email Address"=?', (email,)).fetchone()
        return {"exists": user is not None}
    finally:
        conn.close()


@app.post("/reset-password")
async def auth_reset_password(request: Request):
    data         = await request.json()
    email        = (data.get("email")        or "").strip().lower()
    new_password = (data.get("new_password") or "")
    if not email or not new_password:
        return {"success": False, "message": "All fields are required."}
    if len(new_password) < 8:
        return {"success": False, "message": "Password must be at least 8 characters."}
    conn = _get_auth_conn()
    try:
        result = conn.execute('UPDATE users SET "Password"=? WHERE "Email Address"=?',
                              (_hash_password(new_password), email))
        conn.commit()
        if result.rowcount == 0:
            return {"success": False, "message": "Email not found."}
        return {"success": True}
    finally:
        conn.close()


# ── Admin endpoints ───────────────────────────────────────────────────────────

@app.get("/admin/users")
async def admin_get_users(admin_email: str = "", admin_token: str = ""):
    _, err = _require_admin({"admin_email": admin_email, "admin_token": admin_token})
    if err:
        return err
    conn = _get_auth_conn()
    try:
        rows = conn.execute(
            'SELECT "User ID","First Name","Last Name","Email Address","Date Joined","Department","Position","Role" FROM users ORDER BY "User ID"'
        ).fetchall()
        return {"success": True, "users": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.post("/admin/users")
async def admin_create_user(request: Request):
    data = await request.json()
    _, err = _require_admin(data)
    if err:
        return err
    first    = (data.get("first_name") or "").strip()
    last     = (data.get("last_name")  or "").strip()
    email    = (data.get("email")      or "").strip().lower()
    dept     = (data.get("department") or "").strip()
    position = (data.get("position")   or "").strip()
    password = (data.get("password")   or "")
    role     = (data.get("role")       or "user").strip().lower()
    if role not in ("user", "admin"):
        role = "user"
    if not all([first, last, email, dept, position, password]):
        return {"success": False, "message": "All fields are required."}
    conn = _get_auth_conn()
    try:
        date_joined = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute(
            'INSERT INTO users ("First Name","Last Name","Email Address","Password","Date Joined","Department","Position","Role") VALUES (?,?,?,?,?,?,?,?)',
            (first, last, email, _hash_password(password), date_joined, dept, position, role)
        )
        conn.commit()
        uid = conn.execute('SELECT "User ID" FROM users WHERE "Email Address"=?', (email,)).fetchone()["User ID"]
        return {"success": True, "user_id": str(uid).zfill(4)}
    except sqlite3.IntegrityError:
        return {"success": False, "message": "An account with that email already exists."}
    finally:
        conn.close()


@app.put("/admin/users/{user_id}")
async def admin_update_user(user_id: int, request: Request):
    data = await request.json()
    _, err = _require_admin(data)
    if err:
        return err
    first    = (data.get("first_name") or "").strip()
    last     = (data.get("last_name")  or "").strip()
    email    = (data.get("email")      or "").strip().lower()
    dept     = (data.get("department") or "").strip()
    position = (data.get("position")   or "").strip()
    role     = (data.get("role")       or "user").strip().lower()
    if role not in ("user", "admin"):
        role = "user"
    if not all([first, last, email, dept, position]):
        return {"success": False, "message": "All fields are required."}
    conn = _get_auth_conn()
    try:
        result = conn.execute(
            'UPDATE users SET "First Name"=?,"Last Name"=?,"Email Address"=?,"Department"=?,"Position"=?,"Role"=? WHERE "User ID"=?',
            (first, last, email, dept, position, role, user_id)
        )
        conn.commit()
        if result.rowcount == 0:
            return {"success": False, "message": "User not found."}
        return {"success": True}
    except sqlite3.IntegrityError:
        return {"success": False, "message": "That email is already in use."}
    finally:
        conn.close()


@app.put("/admin/users/{user_id}/password")
async def admin_reset_user_password(user_id: int, request: Request):
    data = await request.json()
    _, err = _require_admin(data)
    if err:
        return err
    new_password = (data.get("new_password") or "")
    if not new_password or len(new_password) < 8:
        return {"success": False, "message": "Password must be at least 8 characters."}
    conn = _get_auth_conn()
    try:
        result = conn.execute('UPDATE users SET "Password"=? WHERE "User ID"=?',
                              (_hash_password(new_password), user_id))
        conn.commit()
        if result.rowcount == 0:
            return {"success": False, "message": "User not found."}
        return {"success": True}
    finally:
        conn.close()


@app.delete("/admin/users/{user_id}")
async def admin_delete_user(user_id: int, request: Request):
    data = await request.json()
    _, err = _require_admin(data)
    if err:
        return err
    admin_email = (data.get("admin_email") or "").strip().lower()
    conn = _get_auth_conn()
    try:
        target = conn.execute('SELECT "Email Address" FROM users WHERE "User ID"=?', (user_id,)).fetchone()
        if not target:
            return {"success": False, "message": "User not found."}
        if target["Email Address"] == admin_email:
            return {"success": False, "message": "You cannot delete your own account."}
        conn.execute('DELETE FROM users WHERE "User ID"=?', (user_id,))
        conn.commit()
        return {"success": True}
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9038)