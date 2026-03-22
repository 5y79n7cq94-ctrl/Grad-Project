import os, sys, json, subprocess, glob, datetime, time, random, tempfile, requests
from db_manager import ingest_crawler_data, mark_as_crawled

# ── Apify 設定 ─────────────────────────────────────────────
APIFY_TOKEN   = api_token = os.getenv("APIFY_TOKEN")
IG_ACTOR_ID   = "apify~instagram-scraper"
FB_ACTOR_ID   = "apify~facebook-posts-scraper"
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
    return {
        "Authorization": f"Bearer {APIFY_TOKEN}",
        "Content-Type":  "application/json",
    }


def _run_apify_actor(actor_id: str, input_data: dict) -> list:
    """啟動 Apify Actor，等佢完成，返回 dataset items"""
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