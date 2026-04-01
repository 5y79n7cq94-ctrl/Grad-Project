import os, sys, json, subprocess, glob, datetime, time, random, tempfile, requests
from db_manager import ingest_crawler_data, ingest_fb_negative_monitor_json, ingest_ig_negative_monitor_json, ingest_weibo_negative_monitor_json, ingest_xhs_negative_monitor_json, mark_as_crawled
from dotenv import load_dotenv

_TASK_MANAGER_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_TASK_MANAGER_ROOT, ".env"))

# ── Apify 設定 ─────────────────────────────────────────────
APIFY_TOKEN   = api_token = os.getenv("APIFY_TOKEN")
IG_ACTOR_ID   = "apify~instagram-scraper"
FB_ACTOR_ID   = "apify~facebook-posts-scraper"
NEGATIVE_MONITOR_IG_ACTOR_ID = os.getenv("NEGATIVE_MONITOR_IG_ACTOR_ID", "apify~instagram-scraper")
NEGATIVE_MONITOR_FB_SEARCH_ACTOR_ID = os.getenv("NEGATIVE_MONITOR_FB_SEARCH_ACTOR_ID", "scraper_one~facebook-posts-search")
NEGATIVE_MONITOR_FETCH_IG_COMMENTS = os.getenv("NEGATIVE_MONITOR_FETCH_IG_COMMENTS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
NEGATIVE_MONITOR_FETCH_FB_COMMENTS = os.getenv("NEGATIVE_MONITOR_FETCH_FB_COMMENTS", "1").strip().lower() not in (
    "0", 
    "false", 
    "no",
)
NEGATIVE_MONITOR_FB_COMMENTS_ACTOR_ID = os.getenv("NEGATIVE_MONITOR_FB_COMMENTS_ACTOR_ID", "apify~facebook-comments-scraper")
NEGATIVE_MONITOR_FB_COMMENTS_RESULTS_LIMIT = max(10, min(10000, int(os.getenv("NEGATIVE_MONITOR_FB_COMMENTS_RESULTS_LIMIT", "500"))))
NEGATIVE_MONITOR_FB_COMMENTS_URL_BATCH = max(1, min(50, int(os.getenv("NEGATIVE_MONITOR_FB_COMMENTS_URL_BATCH", "10"))))
NEGATIVE_MONITOR_FB_COMMENTS_NESTED = os.getenv("NEGATIVE_MONITOR_FB_COMMENTS_NESTED", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
NEGATIVE_MONITOR_IG_COMMENTS_RESULTS_LIMIT = max(1, min(50, int(os.getenv("NEGATIVE_MONITOR_IG_COMMENTS_RESULTS_LIMIT", "50"))))
NEGATIVE_MONITOR_IG_COMMENTS_URL_BATCH = max(1, min(80, int(os.getenv("NEGATIVE_MONITOR_IG_COMMENTS_URL_BATCH", "20"))))
NEGATIVE_MONITOR_IG_HASHTAG_SEARCH_LIMIT = max(1, min(250, int(os.getenv("NEGATIVE_MONITOR_IG_HASHTAG_SEARCH_LIMIT", "1"))))
APIFY_POLL_INTERVAL = 5    # 秒
APIFY_TIMEOUT       = 300  # 最長等 5 分鐘

# ── 官方帳號 ID ────────────────────────────────────────────
OFFICIAL_ACCOUNTS = {
    # ── MediaCrawler 平台 ──────────────────────────────────
    "xhs": {
        "wynn":       ["5a9b76484eacab682fe03bf2"],
        "galaxy":     ["5db13f980000000001008b6c"],
        "sands":      ["5c19b3400000000006030b97", "5c19b4bf0000000007026deb", "5de7655c00000000010020ee"],
        "melco":      ["6475c09f00000000120354c3", "5caec4a80000000016001dd1", "6479b4a1000000001001fa97"],
        "sjm":        ["67da4027000000000e0125f2", "5c67d87a0000000010039c5b"],
        "mgm":        ["5f03246a0000000001007d85"],
        "government": ["5c4b97c9000000001201df61"],
    },
    "wb": {
        "mgm":        ["2507909137"],
        "wynn":       ["5786819413", "5893804607"],
        "galaxy":     ["1921176353", "5481188563", "2187009982"],
        "sands":      ["2824754694", "1771716780", "3167814947", "7051344767", "2477530130", "3803798970"],
        "melco":      ["2247181842", "1734547200", "5577774461", "2257442975"],
        "sjm":        ["7480247775", "7514371786"],
        "government": ["5492416329", "5529448477"],
    },
    # ── Apify 平台 ─────────────────────────────────────────
    "ig": {
        "wynn":       ["wynn.macau", "wynn.palace"],
        "sands":      ["thevenetianmacao", "the_londoner_macao", "parisian_macao"],
        "galaxy":     ["galaxymacau"],
        "melco":      ["cityofdreamsmacau", "studiocitymacau"],
        "sjm":        ["hotelisboamacau", "lisboetamacau"],
        "mgm":        ["mgm.mo"],
        "government": ["visitmacao"],
    },
    "fb": {
        "wynn":       ["https://www.facebook.com/wynnmacauresort",
                       "https://www.facebook.com/wynnpalace"],
        "sands":      ["https://www.facebook.com/VenetianMacao",
                       "https://www.facebook.com/LondonerMacao",
                       "https://www.facebook.com/TheParisianMacao"],
        "galaxy":     ["https://www.facebook.com/galaxymacau"],
        "melco":      ["https://www.facebook.com/cityofdreamsmacau",
                       "https://www.facebook.com/studiocitymacau"],
        "sjm":        ["https://www.facebook.com/grandlisboapalace",
                       "https://www.facebook.com/hotelisboamacau",
                       "https://www.facebook.com/LisboetaMacau"],
        "mgm":        ["https://www.facebook.com/MGMMACAU"],
        "government": ["https://www.facebook.com/visitmacao"],
    },
}

# 只入庫最近幾日嘅帖文
INGEST_MAX_AGE_DAYS = 90
MAX_POSTS_PER_ACCOUNT = 10  # Apify 每個帳號最多爬幾多帖
NEGATIVE_MONITOR_INGEST_MAX_AGE_DAYS = int(os.environ.get("NEGATIVE_MONITOR_INGEST_MAX_AGE_DAYS", "365"))  #有的时候微博九十天内负面就搜出来一两条，所以给他延长到一年
XHS_NEGATIVE_MONITOR_DEFAULT_KEYWORDS = ["澳门永利避雷"]
WEIBO_NEGATIVE_MONITOR_DEFAULT_KEYWORDS = ["澳门永利皇宫酒店避雷"，"澳门永利皇宫筹码问题"]
IG_NEGATIVE_MONITOR_DEFAULT_KEYWORDS = ["WynnMacau", "踩雷", "避雷", "scam", "horrible", "refund", "rude", "disappointed","差评", "坑", "退款", "态度差", "Wynn"]
FB_NEGATIVE_MONITOR_DEFAULT_KEYWORDS = ["澳门永利避雷"]
NEGATIVE_MONITOR_DEFAULT_KEYWORDS = XHS_NEGATIVE_MONITOR_DEFAULT_KEYWORDS


# ══════════════════════════════════════════════════════════
# MediaCrawler 部分（原有邏輯，完全不變）
# ══════════════════════════════════════════════════════════

def _snapshot_post_ids(json_file):
    """回傳 json_file 現有所有 post ID 嘅 set，file 唔存在就回傳空 set"""
    if not os.path.exists(json_file):
        return set()
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            posts = json.load(f)
        return set(
            str(p.get('note_id') or p.get('id') or p.get('mid', ''))
            for p in posts
        )
    except Exception:
        return set()


def _crawl_platform(platform, selected_ops, category):
    """爬取指定平台的所有運營商（XHS / 微博，原有邏輯）"""
    base_path = os.path.dirname(os.path.abspath(__file__))
    main_py   = os.path.join(base_path, "main.py")
    cli_platform = "wb" if platform == "wb" else platform
    data_dir     = "weibo" if platform == "wb" else platform
    json_dir     = os.path.join(base_path, "data", data_dir, "json")
    json_patterns = [os.path.join(json_dir, "creator_contents_*.json")]

    for op in selected_ops:
        creator_ids = OFFICIAL_ACCOUNTS[platform].get(op, [])

        if not creator_ids:
            print(f"⏭️  {op} / {platform.upper()} 冇帳號，跳過")
            mark_as_crawled(op, category)
            continue

        print(f"\n{'='*50}\n🏢 [{platform.upper()}] 開始爬取: {op.upper()} ({len(creator_ids)} 個帳號)")

        before_mtimes = {}
        before_ids_per_file = {}
        for pattern in json_patterns:
            for f in glob.glob(pattern):
                before_mtimes[f] = os.path.getmtime(f)
                before_ids_per_file[f] = _snapshot_post_ids(f)

        for i, uid in enumerate(creator_ids):
            print(f"\n🎯 [{i+1}/{len(creator_ids)}] {platform.upper()} UID: {uid}")
            try:
                subprocess.run(
                    [sys.executable, main_py,
                     "--platform", cli_platform,
                     "--type", "creator",
                     "--creator_id", uid,
                     "--headless", "0"],
                    check=False,
                    cwd=base_path
                )
            except Exception as e:
                print(f"⚠️ 出錯 ({uid}): {e}")

            if i < len(creator_ids) - 1:
                wait = random.randint(15, 25)
                print(f"⏳ 等待 {wait} 秒...")
                time.sleep(wait)

        all_jsons = []
        for pattern in json_patterns:
            all_jsons.extend(glob.glob(pattern))
        all_jsons = list(set(all_jsons))

        updated = [f for f in all_jsons if os.path.getmtime(f) > before_mtimes.get(f, 0)]
        if not updated and all_jsons:
            updated = [max(all_jsons, key=os.path.getmtime)]

        if updated:
            latest = max(updated, key=os.path.getmtime)
            existing_ids = before_ids_per_file.get(latest, set())
            print(f"\n📥 入庫: {latest}  (已有 {len(existing_ids)} 條舊帖會跳過，只入新帖)")
            ingest_crawler_data(
                latest,
                "weibo" if platform == "wb" else platform,
                "",
                operator=op if platform == "wb" else None,
                skip_ids=existing_ids,
                max_age_days=INGEST_MAX_AGE_DAYS,
            )
        else:
            print(f"⚠️ {op} 冇找到 JSON，跳過入庫")

        mark_as_crawled(op, category)

        if op != selected_ops[-1]:
            wait = random.randint(20, 35)
            print(f"\n⏳ {op} 完成，等 {wait} 秒再爬下一個...")
            time.sleep(wait)


# ══════════════════════════════════════════════════════════
# Apify 部分（新增）
# ══════════════════════════════════════════════════════════

def _apify_headers():
    tok = (os.getenv("APIFY_TOKEN") or "").strip()
    return {
        "Authorization": f"Bearer {tok}",
        "Content-Type":  "application/json",
    }


def _run_apify_actor(actor_id: str, input_data: dict) -> list:
    """啟動 Apify Actor，等佢完成，返回 dataset items"""
    if not (os.getenv("APIFY_TOKEN") or "").strip():
        raise ValueError("APIFY_TOKEN 未設置（請在專案根目錄 .env 中配置 APIFY_TOKEN=...）")
    # 1. 啟動
    resp = requests.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
        headers=_apify_headers(),
        json=input_data,
        timeout=30
    )
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    print(f"  🚀 Apify Actor 啟動，run_id={run_id}")

    # 2. 輪詢等完成
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    start_time = time.time()
    while True:
        time.sleep(APIFY_POLL_INTERVAL)
        s = requests.get(status_url, headers=_apify_headers(), timeout=15)
        s.raise_for_status()
        status = s.json()["data"]["status"]
        print(f"  ⏳ 狀態: {status}     ", end="\r")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            print()
            break
        if time.time() - start_time > APIFY_TIMEOUT:
            print(f"\n  ⚠️ 超時 {APIFY_TIMEOUT}s，放棄")
            return []

    if status != "SUCCEEDED":
        print(f"  ❌ Actor 失敗 (status={status})")
        return []

    # 3. 取數據
    ds_id = s.json()["data"]["defaultDatasetId"]
    ds_resp = requests.get(
        f"https://api.apify.com/v2/datasets/{ds_id}/items?format=json&clean=true",
        headers=_apify_headers(),
        timeout=60
    )
    ds_resp.raise_for_status()
    items = ds_resp.json()
    print(f"  ✅ 取得 {len(items)} 條")
    return items


def _normalise_ig(raw: dict) -> dict:
    """IG item → db_manager 認識嘅格式"""
    ts = raw.get("timestamp") or raw.get("takenAtTs") or ""
    if ts and str(ts).isdigit():
        ts = datetime.datetime.utcfromtimestamp(int(ts)).isoformat()
    caption = raw.get("caption") or ""
    return {
        "note_id":          raw.get("id") or raw.get("shortCode") or "",
        "title":            caption[:80],
        "desc":             caption,
        "create_date_time": ts,
        **{k: v for k, v in raw.items() if k not in ("id", "caption", "timestamp")},
    }


def _normalise_fb(raw: dict) -> dict:
    """FB item → db_manager 認識嘅格式"""
    ts = raw.get("time") or raw.get("date") or ""
    if ts and str(ts).isdigit():
        ts = datetime.datetime.utcfromtimestamp(int(ts)).isoformat()
    text = raw.get("text") or raw.get("message") or ""
    return {
        "id":      raw.get("postId") or raw.get("id") or "",
        "title":   text[:80],
        "content": text,
        "time":    ts,
        **{k: v for k, v in raw.items() if k not in ("id", "text", "time", "date")},
    }


def _ingest_apify_posts(posts: list, platform: str, operator: str):
    """寫成臨時 JSON → ingest_crawler_data 入庫"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", encoding="utf-8", delete=False
    ) as f:
        json.dump(posts, f, ensure_ascii=False)
        tmp_path = f.name
    print(f"\n  📥 入庫: {platform.upper()} / {operator} ({len(posts)} 條帖文)")
    try:
        ingest_crawler_data(
            json_file    = tmp_path,
            platform     = platform,
            keyword      = "",
            operator     = operator,
            skip_ids     = set(),   # DB 層 INSERT OR IGNORE 去重
            max_age_days = INGEST_MAX_AGE_DAYS,
        )
    finally:
        os.unlink(tmp_path)


def _crawl_apify_platform(platform: str, selected_ops: list, category: str):
    """
    用 Apify 爬取 IG 或 FB，邏輯同 _crawl_platform 一致：
    operator 逐個跑，每個帳號之間有隨機等待。
    """
    actor_id   = IG_ACTOR_ID if platform == "ig" else FB_ACTOR_ID
    is_ig      = (platform == "ig")

    for op in selected_ops:
        accounts = OFFICIAL_ACCOUNTS[platform].get(op, [])

        if not accounts:
            print(f"⏭️  {op} / {platform.upper()} 冇帳號，跳過")
            mark_as_crawled(op, category)
            continue

        print(f"\n{'='*50}\n{'📸' if is_ig else '📘'} [{platform.upper()}] 開始爬取: {op.upper()} ({len(accounts)} 個帳號)")

        all_posts = []

        for i, account in enumerate(accounts):
            print(f"\n  🎯 [{i+1}/{len(accounts)}] {account}")
            try:
                if is_ig:
                    # IG：apify~instagram-scraper 要用 directUrls，唔係 usernames
                    username = account.replace("https://www.instagram.com/", "").strip("/")
                    profile_url = f"https://www.instagram.com/{username}/"
                    actor_input = {
                        "directUrls":   [profile_url],
                        "resultsType":  "posts",
                        "resultsLimit": MAX_POSTS_PER_ACCOUNT,
                        "addParentData": False,
                    }
                    raw_items = _run_apify_actor(actor_id, actor_input)
                    all_posts.extend([_normalise_ig(r) for r in raw_items])
                else:
                    # FB：apify~facebook-posts-scraper
                    # startUrls 要係 object list，resultsLimit 控制條數
                    cutoff = (
                        datetime.datetime.now() - datetime.timedelta(days=INGEST_MAX_AGE_DAYS)
                    ).strftime("%Y-%m-%d")
                    actor_input = {
                        "startUrls":    [{"url": account}],  # 必須係 object，唔係純字串
                        "resultsLimit": MAX_POSTS_PER_ACCOUNT,
                        "minPostDate":  cutoff,
                    }
                    raw_items = _run_apify_actor(actor_id, actor_input)
                    all_posts.extend([_normalise_fb(r) for r in raw_items])

            except Exception as e:
                print(f"  ⚠️ 出錯 ({account}): {e}")

            # 帳號之間等一等
            if i < len(accounts) - 1:
                wait = random.randint(8, 15)
                print(f"  ⏳ 等待 {wait} 秒...")
                time.sleep(wait)

        # 一個 operator 全部帳號跑完 → 入庫
        if all_posts:
            _ingest_apify_posts(all_posts, platform, op)
        else:
            print(f"  ⚠️ {op} / {platform.upper()} 冇取到任何帖文，跳過入庫")

        mark_as_crawled(op, category)

        if op != selected_ops[-1]:
            wait = random.randint(20, 35)
            print(f"\n⏳ {op} 完成，等 {wait} 秒再爬下一個...")
            time.sleep(wait)


# ══════════════════════════════════════════════════════════
# 主入口（原有 run_task_master，加入 IG / FB）
# ══════════════════════════════════════════════════════════

def run_task_master(keyword, operators="", category=""):
    selected_ops = [op.strip().lower() for op in operators.split(",") if op.strip()]
    if not selected_ops:
        selected_ops = ["wynn", "sands", "galaxy", "mgm", "melco", "sjm"]

    # 1. XHS 同 微博（原有 MediaCrawler）
    for platform in ["xhs", "wb"]:
        ops_with_accounts = [op for op in selected_ops if OFFICIAL_ACCOUNTS[platform].get(op)]
        if not ops_with_accounts:
            continue
        print(f"\n🌐 開始爬取平台: {platform.upper()}")
        _crawl_platform(platform, selected_ops, category)

    # 2. Instagram（Apify）
    ig_ops = [op for op in selected_ops if OFFICIAL_ACCOUNTS["ig"].get(op)]
    if ig_ops:
        print(f"\n🌐 開始爬取平台: INSTAGRAM (Apify)")
        _crawl_apify_platform("ig", selected_ops, category)

    # 3. Facebook（Apify）
    fb_ops = [op for op in selected_ops if OFFICIAL_ACCOUNTS["fb"].get(op)]
    if fb_ops:
        print(f"\n🌐 開始爬取平台: FACEBOOK (Apify)")
        _crawl_apify_platform("fb", selected_ops, category)

    print(f"\n✅ 全部完成: {selected_ops}")

def run_xhs_negative_monitor_crawl(
    max_comments_per_note: int = 40,
    headless: str = "0",
    keywords_csv: str | None = None,
    get_comments: bool = True,
    max_notes: int | None = None,
    crawl_from_date: str | None = None,
    crawl_to_date: str | None = None,
) -> dict:
    """
    小紅書關鍵字搜索 → 入庫 xhs_negative_monitor。
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    main_py = os.path.join(base_path, "main.py")
    json_dir = os.path.join(base_path, "data", "xhs", "json")
    os.makedirs(json_dir, exist_ok=True)

    kws = (keywords_csv or "").strip() or ",".join(XHS_NEGATIVE_MONITOR_DEFAULT_KEYWORDS)

    before_mtimes = {}
    pattern = os.path.join(json_dir, "search_contents_*.json")
    for f in glob.glob(pattern):
        before_mtimes[f] = os.path.getmtime(f)

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["XHS_SORT_TYPE"] = "time_descending"
    if max_notes is not None:
        env["MEDIACRAWLER_MAX_NOTES"] = str(max(20, min(int(max_notes), 500)))
    cf = (crawl_from_date or "").strip()[:10]
    ct = (crawl_to_date or "").strip()[:10]
    if cf:
        env["NEGATIVE_MONITOR_CRAWL_FROM_DATE"] = cf
    if ct:
        env["NEGATIVE_MONITOR_CRAWL_TO_DATE"] = ct

    py_exe = sys.executable
    cmd = [
        py_exe,
        main_py,
        "--platform",
        "xhs",
        "--type",
        "search",
        "--keywords",
        kws,
        "--headless",
        headless,
    ]
    if get_comments:
        cmd += [
            "--get_comment",
            "yes",
            "--max_comments_count_singlenotes",
            str(max_comments_per_note),
        ]
    else:
        cmd += ["--get_comment", "no"]
    dr = ""
    if cf or ct:
        dr = f" | 發布日期: {cf or '…'} ~ {ct or '…'}"
    print(f"\n{'='*50}\n🔍 [XHS 負面監測] 關鍵字搜索{dr}\n  {kws[:120]}...")
    print(f"子進程 Python: {py_exe}")
    try:
        subprocess.run(cmd, check=False, cwd=base_path, env=env)
    except Exception as e:
        print(f"⚠️ 負面掃描子進程出錯: {e}")
        return {"ok": False, "error": str(e), "ingested": 0}

    all_jsons = list(set(glob.glob(pattern)))
    updated = [f for f in all_jsons if os.path.getmtime(f) > before_mtimes.get(f, 0)]
    if not updated and all_jsons:
        updated = [max(all_jsons, key=os.path.getmtime)]

    ingested = 0
    latest = None
    if updated:
        latest = max(updated, key=os.path.getmtime)
        print(f"\n📥 負面監測入庫 (xhs_negative_monitor): {latest}")
        ingested = int(
            ingest_xhs_negative_monitor_json(
                latest,
                skip_ids=set(),
                max_age_days=NEGATIVE_MONITOR_INGEST_MAX_AGE_DAYS,
                from_date=cf or None,
                to_date=ct or None,
            )
        )
    else:
        print("⚠️ 未找到 search_contents_*.json，無入庫")

    return {
        "ok": True,
        "contents_json": latest,
        "keywords_used": kws,
        "rows_written": ingested,
    }


def run_weibo_negative_monitor_crawl(
    max_comments_per_note: int = 40,
    headless: str = "0",
    keywords_csv: str | None = None,
    get_comments: bool = True,
    max_notes: int | None = None,
    crawl_from_date: str | None = None,
    crawl_to_date: str | None = None,
) -> dict:
    """
    微博關鍵字搜索 → 入庫 weibo_negative_monitor（不寫 posts_weibo）。
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    main_py = os.path.join(base_path, "main.py")
    json_dir = os.path.join(base_path, "data", "weibo", "json")
    os.makedirs(json_dir, exist_ok=True)

    kws = (keywords_csv or "").strip() or ",".join(WEIBO_NEGATIVE_MONITOR_DEFAULT_KEYWORDS)

    before_mtimes = {}
    pattern = os.path.join(json_dir, "search_contents_*.json")
    for f in glob.glob(pattern):
        before_mtimes[f] = os.path.getmtime(f)

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["WEIBO_SEARCH_TYPE"] = "real_time"
    if max_notes is not None:
        env["MEDIACRAWLER_MAX_NOTES"] = str(max(20, min(int(max_notes), 500)))
    cf = (crawl_from_date or "").strip()[:10]
    ct = (crawl_to_date or "").strip()[:10]
    if cf:
        env["NEGATIVE_MONITOR_CRAWL_FROM_DATE"] = cf
    if ct:
        env["NEGATIVE_MONITOR_CRAWL_TO_DATE"] = ct

    py_exe = sys.executable
    cmd = [
        py_exe,
        main_py,
        "--platform",
        "wb",
        "--type",
        "search",
        "--keywords",
        kws,
        "--headless",
        headless,
    ]
    if get_comments:
        cmd += [
            "--get_comment",
            "yes",
            "--max_comments_count_singlenotes",
            str(max_comments_per_note),
        ]
    else:
        cmd += ["--get_comment", "no"]
    dr = ""
    if cf or ct:
        dr = f" | 發布日期: {cf or '…'} ~ {ct or '…'}"
    print(f"\n{'='*50}\n🔍 [微博 負面監測] 關鍵字搜索{dr}\n  {kws[:120]}...")
    print(f"🐍 子進程 Python: {py_exe}")
    try:
        subprocess.run(cmd, check=False, cwd=base_path, env=env)
    except Exception as e:
        print(f"⚠️ 微博負面掃描子進程出錯: {e}")
        return {"ok": False, "error": str(e), "ingested": 0}

    all_jsons = list(set(glob.glob(pattern)))
    updated = [f for f in all_jsons if os.path.getmtime(f) > before_mtimes.get(f, 0)]
    if not updated and all_jsons:
        updated = [max(all_jsons, key=os.path.getmtime)]

    ingested = 0
    latest = None
    if updated:
        latest = max(updated, key=os.path.getmtime)
        print(f"\n📥 負面監測入庫 (weibo_negative_monitor): {latest}")
        ingested = int(
            ingest_weibo_negative_monitor_json(
                latest,
                skip_ids=set(),
                max_age_days=NEGATIVE_MONITOR_INGEST_MAX_AGE_DAYS,
                from_date=cf or None,
                to_date=ct or None,
            )
        )
    else:
        print("⚠️ 未找到 weibo search_contents_*.json，無入庫")

    return {
        "ok": True,
        "contents_json": latest,
        "keywords_used": kws,
        "rows_written": ingested,
        "source": "weibo",
    }


def _ig_chunked(seq: list, n: int) -> list:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def _ig_apify_row_is_error(r: dict) -> bool:
    """僅在確有錯誤訊息時視為錯誤列（避免 requestErrorMessages: [] 等誤判）。"""
    err = r.get("error")
    if err is True or (isinstance(err, str) and err.strip()):
        return True
    ed = r.get("errorDescription")
    if isinstance(ed, str) and ed.strip():
        return True
    if ed not in (None, False, "") and not isinstance(ed, str):
        return True
    rem = r.get("requestErrorMessages")
    if isinstance(rem, list):
        return any(str(x).strip() for x in rem)
    if rem:
        return True
    return False


def _ig_apify_item_looks_like_post(r: dict) -> bool:
    """Apify hashtag 模式理論上應回傳帖文；若只有 hashtag 元數據或阻擋頁會缺 shortCode / 貼文網址。"""
    if not isinstance(r, dict):
        return False
    if _ig_apify_row_is_error(r):
        return False
    if str(r.get("shortCode") or r.get("code") or r.get("shortcode") or "").strip():
        return True
    u = (r.get("url") or "").lower()
    if "instagram.com/p/" in u or "instagram.com/reel/" in u:
        return True
    # hashtag 元數據里「posts」陣列常為精簡物件：僅 id/pk + 圖片或 caption
    pid = r.get("pk") or r.get("media_id") or r.get("id")
    if pid is not None and str(pid).strip():
        if (
            (r.get("caption") or r.get("text") or r.get("accessibility_caption") or "")
            .strip()
        ):
            return True
        if (
            r.get("displayUrl")
            or r.get("display_url")
            or r.get("thumbnailSrc")
            or r.get("thumbnail_src")
            or r.get("image")
            or r.get("imageUrl")
            or r.get("image_url")
        ):
            return True
    return False


def _ig_unwrap_post_dict(d: dict) -> dict | None:
    """Apify / IG 有時包一層 node、media，或欄位在子 dict。"""
    if _ig_apify_item_looks_like_post(d):
        return d
    node = d.get("node")
    if isinstance(node, dict) and _ig_apify_item_looks_like_post(node):
        return node
    media = d.get("media")
    if isinstance(media, dict) and _ig_apify_item_looks_like_post(media):
        return media
    return None


def _ig_collect_post_rows_from_raw(raw_items: list) -> list:
    """hashtag / 標籤頁元數據裡的帖：topPosts、latestPosts、posts、edges.node 等展開為貼文列。"""
    out: list = []
    nest_keys = ("topPosts", "latestPosts", "top_posts", "latest_posts", "posts")
    for r in raw_items:
        if not isinstance(r, dict) or _ig_apify_row_is_error(r):
            continue
        if _ig_apify_item_looks_like_post(r):
            out.append(r)
            continue
        for key in nest_keys:
            nest = r.get(key)
            if not isinstance(nest, list):
                continue
            for child in nest:
                if not isinstance(child, dict):
                    continue
                got = _ig_unwrap_post_dict(child)
                if got is not None:
                    out.append(got)
                    continue
                out.extend(_ig_collect_post_rows_from_raw([child]))
        edges = r.get("edges")
        if isinstance(edges, list):
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                got = _ig_unwrap_post_dict(edge)
                if got is not None:
                    out.append(got)
                    continue
                node = edge.get("node")
                if isinstance(node, dict):
                    got2 = _ig_unwrap_post_dict(node)
                    if got2 is not None:
                        out.append(got2)
                    else:
                        out.extend(_ig_collect_post_rows_from_raw([node]))
    return out


def _ig_explore_tag_urls_from_meta(raw_items: list) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for r in raw_items:
        if not isinstance(r, dict):
            continue
        u = (r.get("url") or "").strip()
        if not u or "/explore/tags/" not in u.lower():
            continue
        u = u.split("?")[0].rstrip("/") + "/"
        if u not in seen:
            seen.add(u)
            urls.append(u)
    return urls


def _ig_dedupe_post_rows(rows: list) -> list:
    seen: set[str] = set()
    out: list = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        key = (
            str(r.get("shortCode") or r.get("code") or r.get("shortcode") or "")
            .strip()
            or str(r.get("url") or "").strip()
            or str(r.get("pk") or r.get("id") or "").strip()
        )
        if not key:
            key = str(id(r))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _ig_fallback_posts_via_direct_tag_url(tag_url: str, cap: int, crawl_from: str) -> list:
    """Actor 僅回傳 explore/tags 摘要時，用標籤頁 URL + resultsType=posts 再抓一輪貼文。"""
    u = tag_url.split("?")[0].strip().rstrip("/") + "/"
    actor_input: dict = {
        "directUrls": [u],
        "resultsType": "posts",
        "resultsLimit": cap,
        "addParentData": False,
    }
    cf = (crawl_from or "").strip()[:10]
    if cf:
        actor_input["onlyPostsNewerThan"] = cf
    return _run_apify_actor(NEGATIVE_MONITOR_IG_ACTOR_ID, actor_input)


def _ig_warn_if_no_posts_extracted(raw_items: list, kw: str) -> None:
    if not raw_items:
        return
    if any(isinstance(r, dict) and _ig_apify_item_looks_like_post(r) for r in raw_items):
        return
    r0 = raw_items[0]
    if not isinstance(r0, dict):
        print(f"  ⚠️ [IG 負面監測] 關鍵詞 {kw!r}: Apify 回傳 {len(raw_items)} 條但首條非 dict，無法解析")
        return
    if _ig_apify_row_is_error(r0):
        hint = r0.get("errorDescription") or r0.get("error") or r0.get("requestErrorMessages")
        print(f"  ⚠️ [IG 負面監測] 關鍵詞 {kw!r}: Apify 回傳錯誤/阻擋列（非帖文）→ {hint}")
        hs = str(hint).lower()
        if "empty or private" in hs or "private data" in hs:
            print(
                "     提示：此訊息通常表示該 **hashtag** 在 IG 上沒有足夠 **公開** 帖文、標籤過於冷門，"
                "或含日期篩選時範圍內無帖。可嘗試：① 逗號拆成多個較短關鍵詞；② 用瀏覽器打開 "
                "https://www.instagram.com/explore/tags/…（把 … 換成標籤本文）確認是否有公開內容；"
                "③ Run crawl 暫不填 From date（避免 onlyPostsNewerThan 過嚴）；④ 用已知熱門標籤測 Apify。"
            )
        return
    keys = list(r0.keys())[:18]
    sc, u = r0.get("shortCode"), (r0.get("url") or "")[:100]
    print(
        f"  ⚠️ [IG 負面監測] 關鍵詞 {kw!r}: Apify 有 {len(raw_items)} 條但結構不像帖文"
        f"（缺 shortCode 與 /p/、/reel/ 連結）。可能為 hashtag 摘要、空結果或版式變更。"
        f" keys={keys} shortCode={sc!r} url片段={u!r}"
    )


def _ig_post_url_for_comments(raw: dict) -> str:
    u = (raw.get("url") or "").strip()
    if u and "instagram.com" in u:
        return u.split("?")[0].rstrip("/") + "/"
    code = str(raw.get("shortCode") or raw.get("code") or raw.get("shortcode") or "").strip()
    if (not code or "/" in code) and isinstance(raw.get("id"), str):
        alt = raw.get("id")
        if alt and alt.isalnum() and 8 <= len(alt) <= 32:
            code = alt
    if code and "/" not in code:
        return f"https://www.instagram.com/p/{code}/"
    return ""


def _ig_comment_post_url(raw: dict) -> str:
    u = (raw.get("postUrl") or raw.get("url") or "").strip()
    if u and "instagram.com" in u:
        return u.split("?")[0]
    for k in ("shortCode", "mediaShortCode", "parentShortCode", "postShortCode"):
        c = str(raw.get(k) or "").strip()
        if c and "/" not in c:
            return f"https://www.instagram.com/p/{c}/"
    pid = str(raw.get("postId") or "").strip()
    if pid and "/" not in pid and not pid.isdigit():
        return f"https://www.instagram.com/p/{pid}/"
    return ""


def _ig_apify_comment_to_negative_item(raw: dict, source_keyword: str) -> dict:
    text = (raw.get("text") or "").strip()
    cid = str(raw.get("id") or "").strip()
    ts = raw.get("timestamp")
    create_dt = ""
    if isinstance(ts, str) and ts.strip():
        create_dt = ts.strip().replace("Z", "")[:19]
    elif isinstance(ts, (int, float)) and ts > 0:
        sec = float(ts) / 1000.0 if ts > 1e12 else float(ts)
        try:
            create_dt = datetime.datetime.utcfromtimestamp(sec).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    owner = raw.get("ownerUsername")
    if not owner and isinstance(raw.get("owner"), dict):
        owner = (raw.get("owner") or {}).get("username")
    uname = (owner or "").strip()
    prefix = f"[IG評論 @{uname}] " if uname else "[IG評論] "
    body = prefix + text
    title = (body[:80] + "…") if len(body) > 80 else body
    post_hint = str(
        raw.get("postId") or raw.get("mediaId") or raw.get("shortCode") or raw.get("mediaShortCode") or ""
    ).strip()
    if post_hint and cid:
        nid = f"{post_hint}_c_{cid}"
    elif cid:
        nid = f"igc_{cid}"
    else:
        nid = f"igc_{abs(hash(body)) % (10**12)}"
    post_url = _ig_comment_post_url(raw)
    return {
        "note_id": nid[:180],
        "title": title,
        "desc": body,
        "content": body,
        "create_date_time": create_dt,
        "note_url": post_url,
        "source_keyword": source_keyword,
    }


def _ig_apify_item_to_negative_post(raw: dict, source_keyword: str) -> dict:
    """Apify instagram-scraper 帖條目 → 與 XHS 專表兼容的字典。"""
    caption = (
        (raw.get("caption") or raw.get("text") or raw.get("accessibility_caption") or "")
        .strip()
    )
    sid = (
        raw.get("shortCode")
        or raw.get("code")
        or raw.get("shortcode")
        or raw.get("pk")
        or raw.get("id")
        or ""
    )
    url = (raw.get("url") or "").strip()
    sc_for_url = str(raw.get("shortCode") or raw.get("code") or raw.get("shortcode") or "").strip()
    if not url and sc_for_url and "/" not in sc_for_url:
        url = f"https://www.instagram.com/p/{sc_for_url}/"
    ts = raw.get("timestamp")
    create_dt = ""
    if isinstance(ts, str) and ts.strip():
        create_dt = ts.strip().replace("Z", "")[:19]
    elif isinstance(ts, (int, float)) and ts > 0:
        sec = float(ts) / 1000.0 if ts > 1e12 else float(ts)
        try:
            create_dt = datetime.datetime.utcfromtimestamp(sec).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    title = caption[:80] + ("…" if len(caption) > 80 else "")
    return {
        "note_id": str(sid),
        "title": title,
        "desc": caption,
        "create_date_time": create_dt,
        "note_url": url,
        "source_keyword": source_keyword,
    }


def _fb_canonical_post_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u.split("?")[0].rstrip("/")


def _fb_search_item_post_url(raw: dict) -> str:
    return (raw.get("url") or raw.get("postUrl") or raw.get("facebookUrl") or "").strip()


def _fb_comment_apify_row_is_bad(r: dict) -> bool:
    if not isinstance(r, dict):
        return True
    if r.get("error") or r.get("errorMessage") or r.get("errorDescription"):
        return True
    return False


def _fb_comment_item_to_negative_row(raw: dict, source_keyword: str) -> dict:
    """apify/facebook-comments-scraper 單條留言 → 與 fb_negative_monitor 入庫 JSON 兼容。"""
    text = (raw.get("text") or "").strip()
    uname = (raw.get("profileName") or "").strip()
    prefix = f"[FB評論 @{uname}] " if uname else "[FB評論] "
    body = prefix + text
    title = (body[:80] + "…") if len(body) > 80 else body
    cid = str(raw.get("id") or raw.get("feedbackId") or "")
    post_u = (raw.get("inputUrl") or raw.get("facebookUrl") or "").strip()
    post_u = post_u.split("?")[0] if post_u else ""
    nid = (f"fbc_{cid}")[:180] if cid else f"fbc_{abs(hash(body)) % (10**12)}"
    create_dt = ""
    d = raw.get("date")
    if isinstance(d, str) and d.strip():
        create_dt = d.strip().replace("Z", "")[:19]
    return {
        "note_id": nid[:180],
        "title": title,
        "desc": body,
        "content": body,
        "create_date_time": create_dt,
        "note_url": post_u,
        "source_keyword": source_keyword,
    }


def _fb_apify_item_to_negative_post(raw: dict, source_keyword: str) -> dict:
    """scraper_one/facebook-posts-search 帖條目 → 與專表兼容。"""
    text = (raw.get("postText") or raw.get("text") or raw.get("message") or "").strip()
    pid = raw.get("postId") or raw.get("id") or ""
    url = (raw.get("url") or "").strip()
    ts = raw.get("timestamp")
    create_dt = ""
    if isinstance(ts, (int, float)) and ts > 1e11:
        try:
            create_dt = datetime.datetime.utcfromtimestamp(ts / 1000.0).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    elif isinstance(ts, str) and ts.strip():
        create_dt = ts.strip().replace("Z", "")[:19]
    title = text[:80] + ("…" if len(text) > 80 else "")
    return {
        "note_id": str(pid),
        "title": title,
        "desc": text,
        "content": text,
        "create_date_time": create_dt,
        "note_url": url,
        "source_keyword": source_keyword,
    }


def run_ig_negative_monitor_crawl(
    keywords_csv: str | None = None,
    max_notes: int | None = None,
    crawl_from_date: str | None = None,
    crawl_to_date: str | None = None,
) -> dict:
    """
    Instagram：Apify instagram-scraper，hashtag 搜索帖文後再以 directUrls + resultsType=comments 拉留言；
    寫入 data/ig/json 並入庫 ig_negative_monitor。需 APIFY_TOKEN。
    關閉留言：NEGATIVE_MONITOR_FETCH_IG_COMMENTS=0；單次留言上限與 URL 批量見 NEGATIVE_MONITOR_IG_COMMENTS_*。
    """
    if not (os.getenv("APIFY_TOKEN") or "").strip():
        return {"ok": False, "error": "APIFY_TOKEN 未設置，無法調用 Apify", "ingested": 0}

    base_path = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(base_path, "data", "ig", "json")
    os.makedirs(json_dir, exist_ok=True)

    kws = (keywords_csv or "").strip() or ",".join(IG_NEGATIVE_MONITOR_DEFAULT_KEYWORDS)
    parts = [p.strip() for p in kws.split(",") if p.strip()]
    cap = int(max_notes) if max_notes is not None else 50
    cap = max(1, min(cap, 200))
    cf = (crawl_from_date or "").strip()[:10]
    ct = (crawl_to_date or "").strip()[:10]
    strip_spaces = os.getenv("NEGATIVE_MONITOR_IG_HASHTAG_STRIP_SPACES", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )

    all_posts: list = []
    ig_sl = NEGATIVE_MONITOR_IG_HASHTAG_SEARCH_LIMIT
    for kw in parts:
        search_q = kw.replace(" ", "") if strip_spaces else kw
        actor_input: dict = {
            "search": search_q,
            "searchType": "hashtag",
            "searchLimit": ig_sl,
            "resultsType": "posts",
            "resultsLimit": cap,
            "addParentData": False,
        }
        if cf:
            actor_input["onlyPostsNewerThan"] = cf
        print(
            f"\n{'='*50}\n📸 [IG 負面監測] Apify hashtag 搜索: {search_q!r} "
            f"(searchLimit={ig_sl}，每個命中標籤最多 {cap} 條帖)\n"
        )
        try:
            raw_items = _run_apify_actor(NEGATIVE_MONITOR_IG_ACTOR_ID, actor_input)
        except Exception as e:
            print(f"⚠️ IG Apify 失敗 ({kw}): {e}")
            continue

        post_rows = _ig_dedupe_post_rows(_ig_collect_post_rows_from_raw(raw_items))
        if not post_rows:
            merged_fb: list = []
            for tu in _ig_explore_tag_urls_from_meta(raw_items):
                print(f"  🔁 [IG 負面監測] hashtag 結果為標籤頁摘要，改以 directUrls 補抓貼文: {tu}")
                try:
                    extra = _ig_fallback_posts_via_direct_tag_url(tu, cap, cf)
                    merged_fb.extend(_ig_collect_post_rows_from_raw(extra))
                except Exception as e:
                    print(f"⚠️ IG directUrls 補抓失敗 ({kw}): {e}")
                    continue
            post_rows = _ig_dedupe_post_rows(merged_fb)

        if not post_rows:
            _ig_warn_if_no_posts_extracted(raw_items, kw)
        else:
            for r in post_rows:
                all_posts.append(_ig_apify_item_to_negative_post(r, kw))

        if NEGATIVE_MONITOR_FETCH_IG_COMMENTS:
            urls: list[str] = []
            seen: set[str] = set()
            for r in post_rows:
                if not isinstance(r, dict) or not _ig_apify_item_looks_like_post(r):
                    continue
                pu = _ig_post_url_for_comments(r)
                if pu and pu not in seen:
                    seen.add(pu)
                    urls.append(pu)
            batches = _ig_chunked(urls, NEGATIVE_MONITOR_IG_COMMENTS_URL_BATCH)
            for bidx, batch in enumerate(batches):
                if not batch:
                    continue
                comment_input: dict = {
                    "directUrls": batch,
                    "resultsType": "comments",
                    "resultsLimit": NEGATIVE_MONITOR_IG_COMMENTS_RESULTS_LIMIT,
                    "addParentData": False,
                }
                print(
                    f"  💬 [IG 負面監測] 留言 batch {bidx + 1}/{len(batches)}，{len(batch)} 個帖 URL "
                    f"(每帖最多 {NEGATIVE_MONITOR_IG_COMMENTS_RESULTS_LIMIT} 條，見 Apify 上限)"
                )
                try:
                    raw_comments = _run_apify_actor(NEGATIVE_MONITOR_IG_ACTOR_ID, comment_input)
                except Exception as e:
                    print(f"⚠️ IG 留言 Apify 失敗 ({kw}, batch {bidx}): {e}")
                    continue
                for rc in raw_comments:
                    if isinstance(rc, dict) and not _ig_apify_row_is_error(rc):
                        all_posts.append(_ig_apify_comment_to_negative_item(rc, kw))

    out_name = f"search_contents_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    out_path = os.path.join(json_dir, out_name)

    ingested = 0
    if all_posts:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(all_posts, f, ensure_ascii=False, indent=2)
        print(f"\n📥 負面監測入庫 (ig_negative_monitor): {out_path}")
        ingested = int(
            ingest_ig_negative_monitor_json(
                out_path,
                skip_ids=set(),
                max_age_days=NEGATIVE_MONITOR_INGEST_MAX_AGE_DAYS,
                from_date=cf or None,
                to_date=ct or None,
            )
        )
    else:
        print(
            "⚠️ IG 未取得任何帖文，無入庫；未寫入空檔（避免 data/ig/json 只有 [] 誤導）。"
            " 請看控制台「略過」原因，或到 Apify Console 查看該次 run 的 dataset 原始列。"
        )

    return {
        "ok": True,
        "contents_json": out_path if all_posts else None,
        "keywords_used": kws,
        "rows_written": ingested,
        "source": "ig",
    }


def run_fb_negative_monitor_crawl(
    keywords_csv: str | None = None,
    max_notes: int | None = None,
    crawl_from_date: str | None = None,
    crawl_to_date: str | None = None,
) -> dict:
    """
    Facebook 負面監測（兩段式）：
    1) Apify facebook-posts-search：關鍵詞搜公開帖；
    2) Apify facebook-comments-scraper：對搜到的帖 URL 批量抓留言（可關 NEGATIVE_MONITOR_FETCH_FB_COMMENTS）。
    寫入 data/fb/json 並入庫 fb_negative_monitor。需 APIFY_TOKEN。
    """
    if not (os.getenv("APIFY_TOKEN") or "").strip():
        return {"ok": False, "error": "APIFY_TOKEN 未設置，無法調用 Apify", "ingested": 0}

    base_path = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(base_path, "data", "fb", "json")
    os.makedirs(json_dir, exist_ok=True)

    kws = (keywords_csv or "").strip() or ",".join(FB_NEGATIVE_MONITOR_DEFAULT_KEYWORDS)
    parts = [p.strip() for p in kws.split(",") if p.strip()]
    cap = int(max_notes) if max_notes is not None else 50
    cap = max(1, min(cap, 500))
    cf = (crawl_from_date or "").strip()[:10]
    ct = (crawl_to_date or "").strip()[:10]

    all_posts: list = []
    for kw in parts:
        actor_input = {"query": kw, "resultsCount": cap}
        print(f"\n{'='*50}\n📘 [FB 負面監測·1/2] 關鍵詞搜索帖: {kw!r} (最多 {cap} 條)\n")
        try:
            raw_items = _run_apify_actor(NEGATIVE_MONITOR_FB_SEARCH_ACTOR_ID, actor_input)
        except Exception as e:
            print(f"⚠️ FB 關鍵詞搜索失敗 ({kw}): {e}")
            continue

        post_rows = [r for r in raw_items if isinstance(r, dict)]
        for r in post_rows:
            all_posts.append(_fb_apify_item_to_negative_post(r, kw))

        if not NEGATIVE_MONITOR_FETCH_FB_COMMENTS or not post_rows:
            continue

        url_to_kw: dict[str, str] = {}
        urls: list[str] = []
        seen_u: set[str] = set()
        for r in post_rows:
            u = _fb_search_item_post_url(r)
            if not u or "facebook.com" not in u.lower():
                continue
            canon = _fb_canonical_post_url(u)
            if not canon or canon in seen_u:
                continue
            seen_u.add(canon)
            urls.append(canon)
            url_to_kw[canon] = kw

        batches = _ig_chunked(urls, NEGATIVE_MONITOR_FB_COMMENTS_URL_BATCH)
        for bidx, batch in enumerate(batches):
            if not batch:
                continue
            comment_input: dict = {
                "startUrls": [{"url": u} for u in batch],
                "resultsLimit": NEGATIVE_MONITOR_FB_COMMENTS_RESULTS_LIMIT,
                "includeNestedComments": NEGATIVE_MONITOR_FB_COMMENTS_NESTED,
            }
            if cf:
                comment_input["onlyCommentsNewerThan"] = f"{cf}T00:00:00.000Z"
            print(
                f"  💬 [FB 負面監測·2/2] 留言 Actor batch {bidx + 1}/{len(batches)}，{len(batch)} 個帖 URL "
                f"(每帖最多約 {NEGATIVE_MONITOR_FB_COMMENTS_RESULTS_LIMIT} 條)"
            )
            try:
                raw_comments = _run_apify_actor(NEGATIVE_MONITOR_FB_COMMENTS_ACTOR_ID, comment_input)
            except Exception as e:
                print(f"⚠️ FB 留言 Apify 失敗 ({kw}, batch {bidx}): {e}")
                continue
            for rc in raw_comments:
                if _fb_comment_apify_row_is_bad(rc):
                    continue
                pu = _fb_canonical_post_url(
                    (rc.get("inputUrl") or rc.get("facebookUrl") or "").strip()
                )
                parent_kw = url_to_kw.get(pu) or kw
                all_posts.append(_fb_comment_item_to_negative_row(rc, parent_kw))

    out_name = f"search_contents_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    out_path = os.path.join(json_dir, out_name)

    ingested = 0
    if all_posts:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(all_posts, f, ensure_ascii=False, indent=2)
        print(f"\n📥 負面監測入庫 (fb_negative_monitor): {out_path}")
        ingested = int(
            ingest_fb_negative_monitor_json(
                out_path,
                skip_ids=set(),
                max_age_days=NEGATIVE_MONITOR_INGEST_MAX_AGE_DAYS,
                from_date=cf or None,
                to_date=ct or None,
            )
        )
    else:
        print(
            "⚠️ FB 未取得任何帖文／留言，無入庫；未寫入空檔。"
            " 若只要帖不要留言可設 NEGATIVE_MONITOR_FETCH_FB_COMMENTS=0。"
        )

    return {
        "ok": True,
        "contents_json": out_path if all_posts else None,
        "keywords_used": kws,
        "rows_written": ingested,
        "source": "fb",
    }
