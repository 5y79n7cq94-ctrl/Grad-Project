import os, sys, json, subprocess, glob, datetime, time, random
from db_manager import ingest_crawler_data, mark_as_crawled

# ── 官方帳號 ID ────────────────────────────────────────────
OFFICIAL_ACCOUNTS = {
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
        "government": ["5492416329", "5529448477"],  # 旅遊局、喜劇節
    },
}

# ✏️ CHANGED: 只入庫最近幾日嘅帖文，避免爬入歷史舊帖
INGEST_MAX_AGE_DAYS = 90


# ✏️ CHANGED: 新增 helper —— 爬之前記錄 JSON 裡面已有嘅 post ID set
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
    """爬取指定平台的所有運營商"""
    base_path = os.path.dirname(os.path.abspath(__file__))
    main_py   = os.path.join(base_path, "main.py")
    # MediaCrawler儲存路徑用 "weibo"，但CLI參數用 "wb"
    cli_platform = "wb" if platform == "wb" else platform
    data_dir     = "weibo" if platform == "wb" else platform
    json_dir     = os.path.join(base_path, "data", data_dir, "json")
    json_patterns = [
        os.path.join(json_dir, "creator_contents_*.json"),
    ]

    for op in selected_ops:
        creator_ids = OFFICIAL_ACCOUNTS[platform].get(op, [])

        if not creator_ids:
            print(f"⏭️  {op} / {platform.upper()} 冇帳號，跳過")
            mark_as_crawled(op, category)
            continue

        print(f"\n{'='*50}\n🏢 [{platform.upper()}] 開始爬取: {op.upper()} ({len(creator_ids)} 個帳號)")

        # ✏️ CHANGED: 同時記錄爬之前的 mtime 同 post ID snapshot
        # 目的：MediaCrawler 係 append 落同一個日期 JSON，
        # 所以入庫時要區分「今次新爬」vs「之前已有」嘅帖文
        before_mtimes = {}
        before_ids_per_file = {}  # {filepath: set of existing post IDs}
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

        # 所有帳號跑完後一次性入庫
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
                # ✏️ CHANGED: 微博只有一個帳號對一個 operator，hardcode 冇問題
                # XHS 多個 operator 共用同一個日期 JSON，唔 hardcode，
                # 讓 ingest_crawler_data 靠帖文內容自動判斷 operator
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


def run_task_master(keyword, operators="", category=""):
    selected_ops = [op.strip().lower() for op in operators.split(",") if op.strip()]
    if not selected_ops:
        selected_ops = ["wynn", "sands", "galaxy", "mgm", "melco", "sjm"]

    # XHS 同 微博 順序跑
    for platform in ["xhs", "wb"]:
        ops_with_accounts = [op for op in selected_ops if OFFICIAL_ACCOUNTS[platform].get(op)]
        if not ops_with_accounts:
            continue
        print(f"\n🌐 開始爬取平台: {platform.upper()}")
        _crawl_platform(platform, selected_ops, category)

    print(f"\n✅ 全部完成: {selected_ops}")