import os, sys, json, uvicorn, re
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from collections import defaultdict
from db_manager import query_db_by_filters, get_ops_needing_crawl, backfill_event_dates
from task_manager import run_task_master
import threading

# ✏️ CHANGED: 防止重複爬蟲 thread
# key = operator, value = True 表示而家正在爬緊
_crawling_ops: set = set()
_crawling_lock = threading.Lock()

backfill_event_dates()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*", "null"],
    allow_origin_regex=".*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = OpenAI(api_key="sk-06452010eeff43f59e36f4d86d4d5076", base_url="https://api.deepseek.com")

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
        "WTT", "FISE",
    ]),

    # Crossover: 聯名/快閃
    # ✏️ 移除「主題展」(動物標本館有「主題展室」)、改用「主題展覽」
    ("entertainment", "crossover", [
        "聯名", "快閃", "POP-UP", "POPUP", "泡泡瑪特", "POPMART", "主題展覽",
        "联名", "快闪", "泡泡玛特",
    ]),

    # Experience: 沉浸式/常駐體驗
    ("experience", None, [
        "VR", "SANDBOX", "沉浸式", "體驗館", "水舞間", "主題樂園", "常駐",
        "体验馆", "主题乐园", "天浪淘园", "星动银河", "ILLUMINARIUM", "幻影空間",
    ]),

    # Exhibition: 展覽
    # ✏️ 新增「博物館」作為 exhibition 觸發詞
    ("exhibition", None, [
        "展覽", "展出", "藝術展", "TEAMLAB", "EXPO", "球拍珍品", "博物館", "展示館", "紀念館",
        "展览", "艺术展", "艺荟", "博物馆", "展示馆", "纪念馆",
    ]),

    # Food
    # ✏️ 新增酒吧/調酒詞 + 晚宴/宴
    ("food", None, [
        "美食", "餐廳", "餐飲", "自助餐", "下午茶", "食評", "扒房", "點心", "茶餐廳",
        "火鍋", "煲仔", "葡萄酒", "品酒", "美酒", "佳釀", "評酒", "酒宴", "餐酒",
        "大師班", "品鑑", "晚宴", "宴席", "春茗",
        "BUFFET", "RESTAURANT", "DINING", "STEAKHOUSE", "WINE", "DEGUSTATION",
        "餐厅", "餐饮", "茶餐厅", "美食地图", "火锅", "品鉴",
        # 酒吧/調酒活動
        "酒吧", "調酒", "雞尾酒", "特調", "微醺", "BAR", "COCKTAIL",
        "调酒", "鸡尾酒", "特调",
    ]),

    # Accommodation
    ("accommodation", None, [
        "酒店優惠", "住宿套票", "HOTEL PACKAGE", "住宿", "度假套", "住宿禮遇",
        "酒店住客",
    ]),

    # Shopping
    # ✏️ 移除「SALE」(酒精飲品免責聲明有 "THE SALE OR SUPPLY...")
    ("shopping", None, [
        "購物", "折扣", "優惠券", "購物返現",
        "购物", "优惠券", "购物返现", "时尚汇", "旗舰店",
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
    規則已設計成無歧義：sport 只含真正體育詞，食品/酒類比賽由 food 規則捕獲，
    無需排除詞補救。
    """
    text = (str(p.get('title', '')) + ' ' + str(p.get('description', ''))).upper()
    for cat, sub, kws in CAT_RULES:
        if any(k.upper() in text for k in kws):
            return cat, sub
    return "experience", None

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

@app.get("/api/v2/analyze")
async def analyze(keyword: str, operators: str = "", category: str = "", from_date: str = "", to_date: str = ""):
    print(f"\n🕵️ --- 任務開始: '{keyword}' (類別: {category}) ---")

    # 1. 解析參數
    target_ops  = [op.strip().lower() for op in operators.split(",") if op.strip()] or \
                  ["sands", "galaxy", "wynn", "mgm", "melco", "sjm"]
    target_cats = [c.strip() for c in category.split(",") if c.strip()] if category else [""]

    # 2. DB 查詢（db_manager 已處理 category keyword 過濾）
    print("🔎 正在從資料庫檢索數據...")
    if target_cats != [""]:
        dfs = [query_db_by_filters(keyword, target_ops, cat, max_pub_age_days=180) for cat in target_cats]  # ✏️ CHANGED
        df  = pd.concat(dfs).drop_duplicates(subset=['id']).reset_index(drop=True) if dfs else pd.DataFrame()
    else:
        df = query_db_by_filters(keyword, target_ops, "", max_pub_age_days=180)  # ✏️ CHANGED

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
            ed = str(row.get('event_date') or '')
            if ed and ed not in ('nan', 'None', 'NaN', ''):
                # 有 event_date：做精確重疊判斷
                return dates_overlap(ed, user_start, user_end)
            # 冇 event_date：睇帖文發佈時間，只接受近 180 日內發佈
            try:
                rj = json.loads(row.get('raw_json') or '{}')
                pub_str = rj.get('create_date_time') or rj.get('time') or ''
                if pub_str:
                    pub_dt = pd.to_datetime(str(pub_str)[:10])
                    cutoff = user_start - pd.Timedelta(days=180)
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
    # ✏️ NEW: 跨 operator 已提取活動名稱集合，避免重複（如 BLACKPINK 同時出現喺 Sands + 政府）
    globally_extracted_names: set = set()
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
        for p in raw_social[:80]:
            cat, sub = classify_post(p)
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
            activities.append({
                "name":        p.get("title", "").strip(),
                "description": make_description(p),
                "date":        str(p.get("event_date") or "N/A").replace("nan", "N/A"),
                "location":    loc,
                "category":    c["category"],
                "sub_type":    c["sub_type"],
                "source":      "government",
            })

        # 社媒：分組送 DeepSeek 識別獨立活動
        if social_posts:
            social_by_cat = defaultdict(list)
            for c in social_posts:
                social_by_cat[(c["category"], c["sub_type"])].append(c["post"])

            for (cat, sub), posts_group in social_by_cat.items():
                snippets = []
                # 優先取有日期且在查詢範圍內的帖文，再補無日期的，最後才是範圍外的
                def post_priority(p):
                    ed = str(p.get("event_date") or "")
                    if ed and ed not in ("nan", "None", "NaN", ""):
                        try:
                            parts = ed.split(",")[0].strip().split("~")
                            ev_s = pd.to_datetime(parts[0].strip())
                            ev_e = pd.to_datetime(parts[-1].strip())
                            if from_date and to_date:
                                if ev_s <= pd.to_datetime(to_date) and ev_e >= pd.to_datetime(from_date):
                                    return 0
                                return 2
                        except:
                            pass
                    return 1
                sorted_group = sorted(posts_group, key=post_priority)
                for p in sorted_group[:20]:
                    title = (p.get("title") or "").strip()
                    desc  = (p.get("description") or "").strip()
                    if len(desc) < 30 and p.get("raw_json"):
                        try:
                            raw  = json.loads(p["raw_json"])
                            desc = (raw.get("desc") or raw.get("content") or raw.get("shortDesc") or desc).strip()
                        except:
                            pass
                    # 抽取帖文發佈時間
                    post_date = ""
                    if p.get("raw_json"):
                        try:
                            raw = json.loads(p["raw_json"])
                            dt = raw.get("create_date_time") or raw.get("time") or ""
                            if dt:
                                post_date = f"（帖文發佈：{str(dt)[:10]}）"
                        except:
                            pass
                    if title or desc:
                        snippets.append(f"【帖文{len(snippets)+1}】{post_date}標題: {title}\n內容: {desc[:200] or '(空)'}")

                if not snippets:
                    continue

                gov_names  = "、".join(a["name"] for a in activities if a.get("source") == "government") or "（無）"
                # ✏️ NEW: 加入跨 operator 已提取活動，避免重複（如 BLACKPINK 同時出現喺多個 operator）
                all_seen_names = set(a["name"] for a in activities if a.get("source") == "government") | globally_extracted_names
                seen_hint = "、".join(sorted(all_seen_names)) if all_seen_names else "（無）"
                date_hint  = (
                    f"參考資訊：用戶查詢日期範圍為 {from_date} 至 {to_date}。"
                    f"活動日期必須從帖文原文中明確提取，嚴禁根據查詢範圍推算、估計或捏造日期。"
                    f"若帖文冇明確提及活動具體日期，date 欄位必須填 null，唔好填查詢範圍內嘅任何日期。"
                ) if from_date and to_date else ""
                focus      = CAT_FOCUS.get(sub or cat, "活動名稱、日期、地點、票價")

                prompt = f"""你係澳門活動資訊整合助手。以下係來自社交媒體關於澳門{op_key}嘅帖文。{date_hint}

你的任務：
1. 只提取真正的【演出/活動】本身，唔要提取周邊優惠（如餐廳折扣、會員優惠等）
2. 相同活動只算一個（去重）
3. 以下活動已提取，唔需要重複（包括官方數據及其他運營商已識別活動）：{seen_hint}
4. 每個獨立活動輸出一個 JSON object：
   - "name": 活動名稱（簡潔，20字以內）
   - "description": 重點描述，聚焦「{focus}」，50-80字，繁體中文
   - "date": 活動日期，必須係帖文原文中明確出現嘅日期（格式 YYYY-MM-DD 或 YYYY-MM-DD~YYYY-MM-DD）。若帖文只提到星期幾、「每週」、開放時間、或冇任何具體日期，填 null。嚴禁根據查詢日期範圍推算或猜測日期，寧願填 null 都唔好估。
   - "location": 地點（冇就填 null）
5. 只返回 JSON array，唔需要任何前言

帖文內容：
{chr(10).join(snippets)}

直接輸出 JSON array："""

                try:
                    resp     = client.chat.completions.create(
                        model="deepseek-chat",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=800,
                    )
                    raw_resp = re.sub(r'^```[a-z]*\n?', '', (resp.choices[0].message.content or "").strip()).rstrip('`').strip()
                    extracted = json.loads(raw_resp)
                    print(f"✅ DeepSeek 識別 {len(extracted)} 個獨立活動 (cat={sub or cat})")
                except Exception as e:
                    print(f"⚠️ DeepSeek 出錯: {e}")
                    best = next((p for p in posts_group if p.get("title")), posts_group[0])
                    extracted = [{"name": (best.get("title") or "")[:40] or f"{op_key} {sub or cat}活動",
                                  "description": make_description(best), "date": None, "location": None}]

                for item in extracted:
                    item_date = (item.get("date") or "").strip()

                    # ✏️ NEW: 驗證日期係咪真係出現喺帖文原文，唔係就視為 hallucination → reset null
                    if item_date and item_date not in ("N/A", "null", "None", ""):
                        all_snippets_text = " ".join(snippets)
                        # 取日期前8字（YYYY-MM-D 或 YYYY-MM），睇吓帖文有冇提及
                        date_prefix = item_date[:7]  # e.g. "2026-02"
                        date_day    = item_date[:10] # e.g. "2026-02-14"
                        # 帖文文字要有年份同月份先算有明確日期
                        if date_day not in all_snippets_text and date_prefix not in all_snippets_text:
                            print(f"⚠️ 日期 '{item_date}' 唔見於帖文原文，reset 做 null（防止 hallucination）")
                            item_date = ""
                    if from_date and to_date:
                        if item_date and item_date not in ("N/A", "null", "None", ""):
                            # 有日期：做精確範圍過濾，範圍外就跳過
                            try:
                                parts    = item_date.split("~")
                                ev_start = pd.to_datetime(parts[0].strip())
                                ev_end   = pd.to_datetime(parts[-1].strip())
                                if not (ev_start <= pd.to_datetime(to_date) and ev_end >= pd.to_datetime(from_date)):
                                    continue
                            except:
                                pass
                        # 冇日期：保留（唔知日期唔代表唔係範圍內）
                    item_name = (item.get("name") or "").strip()
                    # ✏️ NEW: 跨 operator dedup — 已見過嘅活動名稱跳過
                    if item_name and item_name in globally_extracted_names:
                        print(f"⏭️ 跨 operator 重複，跳過: {item_name}")
                        continue
                    activities.append({
                        "name":        item_name,
                        "description": (item.get("description") or "暫無描述").strip(),
                        "date":        item_date or "N/A",
                        "location":    item.get("location") or "",
                        "category":    cat,
                        "sub_type":    sub,
                    })
                    if item_name:
                        globally_extracted_names.add(item_name)

        all_summaries[op_key] = activities
        # ✏️ NEW: 將呢個 operator 所有活動名稱（包括 gov cards）加入全局已見集合
        for act in activities:
            n = act.get("name", "").strip()
            if n:
                globally_extracted_names.add(n)
        print(f"✅ {op_key}: {len(activities)} 張 card (gov={len(gov_posts)}, social={len(activities)-len(gov_posts)})")

    # ── 重組：by category，每個 activity 附上 operator 資訊 ────────
    # 同時保留 operator_summaries 向下兼容
    cat_summaries = defaultdict(list)
    for op_key, activities in all_summaries.items():
        for act in activities:
            act_with_op = dict(act, operator=op_key)  # 每個活動加入 operator 欄位
            # 用 sub_type 優先，否則用 category
            cat_key = act.get('sub_type') or act.get('category') or 'experience'
            cat_summaries[cat_key].append(act_with_op)

    return {
        "status": "success",
        "operator_summaries": all_summaries,       # 向下兼容
        "category_summaries": dict(cat_summaries), # 新格式：by category
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9038)