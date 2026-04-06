"""
heat_analyzer.py — 活動熱度分析（Event-Level，跨平台整合）

熱度公式（每個 event）：
  每個 source post：
    engagement_score  = dot(PCA_weights, log1p([likes, comments, shares, ...]))
    engagement_rate   = engagement_score / log1p(followers)   ← 用 followers 標準化
    post_score        = engagement_rate × log1p(followers)    ← 大 account 仍有觸及優勢

  event_raw_score = sum(post_scores) × sqrt(唔同平台數)       ← 跨平台 bonus
  heat_score      = percentile_rank(event_raw_score) × time_decay

  time_decay = exp(-ln2/7 × days_since_newest_post)           ← 7 日後跌一半

  注：冇 followers 數據嘅 post，改用平台中位數 followers 估算。

用法：
  python heat_analyzer.py                  # 計算全部活動熱度
  python heat_analyzer.py --explain        # 印 PCA loadings + Top 15
  python heat_analyzer.py --half-life 14   # 調整 decay
  python heat_analyzer.py --dry-run        # 只計算唔寫 DB

依賴：
  pip install scikit-learn numpy python-dotenv
"""

import os, json, math, sqlite3, argparse, logging, urllib.request
from datetime import datetime
from typing import Optional

import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from dotenv import load_dotenv

load_dotenv()

# ── 配置 ──────────────────────────────────────────────────────────────────────
DB_PATH           = os.getenv("DB_PATH", "macau_analytics.db")
DEFAULT_HALF_LIFE = 14

PLATFORM_FEATURES = {
    "xhs":   ["likes", "comments", "shares", "collects"],
    "ig":    ["likes", "comments", "views"],
    "fb":    ["likes", "comments", "shares"],
    "weibo": ["likes", "comments", "shares"],
}
PLATFORM_TABLE = {
    "xhs": "posts_xhs", "ig": "posts_ig",
    "fb":  "posts_fb",  "weibo": "posts_weibo",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn(db_path):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def ensure_heat_cols(conn):
    cur = conn.cursor()
    for col, typ in [("heat_score", "REAL"), ("heat_meta", "TEXT")]:
        try:
            cur.execute(f"ALTER TABLE events_deduped ADD COLUMN {col} {typ}")
            log.info(f"✅ 已加 {col} 欄位到 events_deduped")
        except sqlite3.OperationalError:
            pass
    conn.commit()


# ── Step 1: Per-platform PCA weights（從數據學習）────────────────────────────
def fit_platform_weights(conn, explain=False):
    """
    每個平台用全量 engagement 數據跑 PCA。
    PC1 loadings = data-driven weights（唔係人手設定）。
    每次重新跑自動適應最新數據。
    """
    platform_info = {}
    for platform, features in PLATFORM_FEATURES.items():
        table    = PLATFORM_TABLE[platform]
        feat_sql = ", ".join(f"COALESCE({f}, 0)" for f in features)
        cur = conn.cursor()
        cur.execute(f"SELECT {feat_sql} FROM {table}")
        data     = np.array(cur.fetchall(), dtype=float)
        data_fit = data[data.sum(axis=1) > 0]

        if len(data_fit) < 5:
            weights, explained = np.ones(len(features)) / len(features), None
        else:
            X         = StandardScaler().fit_transform(np.log1p(data_fit))
            pca       = PCA(n_components=1)
            pca.fit(X)
            weights   = pca.components_[0]
            explained = pca.explained_variance_ratio_[0]
            if weights.mean() < 0:
                weights = -weights

        # 計平台中位數 followers（供冇 followers 數據嘅 post 用）
        cur.execute(f"SELECT followers FROM {table} WHERE followers IS NOT NULL AND followers > 0")
        fols = [r[0] for r in cur.fetchall()]
        median_followers = float(np.median(fols)) if fols else 10000.0

        platform_info[platform] = {
            "weights":          weights,
            "features":         features,
            "explained":        explained,
            "median_followers": median_followers,
        }

        if explain:
            exp_str = f"{explained:.1%}" if explained else "N/A"
            log.info(f"  [{platform}] PC1={exp_str} | {dict(zip(features, weights.round(4)))} | median_followers={median_followers:,.0f}")

    return platform_info


# ── Step 2: 載入所有 posts ────────────────────────────────────────────────────
def load_all_posts(conn):
    posts = {}
    cur   = conn.cursor()
    for platform, features in PLATFORM_FEATURES.items():
        table    = PLATFORM_TABLE[platform]
        feat_sql = ", ".join(f"COALESCE({f}, 0)" for f in features)
        cur.execute(f"SELECT post_id, {feat_sql}, COALESCE(followers, 0), published_at FROM {table}")
        for row in cur.fetchall():
            pid          = row[0]
            feat_vals    = dict(zip(features, row[1:1+len(features)]))
            followers    = row[1+len(features)]
            published_at = row[2+len(features)]
            posts[pid]   = {
                "platform":     platform,
                "followers":    followers,
                "published_at": published_at,
                **feat_vals,
            }
    log.info(f"✅ 載入 {len(posts)} 條帖文")
    return posts


# ── Step 3: 每個 post 計分 ────────────────────────────────────────────────────
def post_score(post, platform_info):
    """
    post_score = PCA_score × log(followers + 1)
    PCA_score  = dot(PCA_weights, log1p([likes, comments, shares...]))
    冇 followers 數據：fallback 用平台中位數
    """
    info     = platform_info[post["platform"]]
    features = info["features"]
    weights  = info["weights"]

    pca_score = float(np.dot(weights, np.log1p(
        [post.get(f, 0) or 0 for f in features]
    )))

    followers = post.get("followers") or 0
    if followers <= 0:
        followers = info["median_followers"]

    return pca_score * math.log1p(followers)


# ── Step 4: Time decay ────────────────────────────────────────────────────────
def parse_dt(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(s)[:19], fmt)
        except ValueError:
            continue
    return None

def time_decay(dt, half_life):
    if dt is None:
        return 0.5
    days = max((datetime.now() - dt).total_seconds() / 86400, 0)
    return math.exp(-math.log(2) / half_life * days)


# ── Step 5: Event-level 聚合 ──────────────────────────────────────────────────
def compute_event_scores(conn, platform_info, all_posts, half_life):
    cur = conn.cursor()
    cur.execute("SELECT event_id, source_post_ids FROM events_deduped")
    all_events = cur.fetchall()

    # 建立 post → 屬於幾多個 event 嘅 mapping
    # 例如 roundup post 屬於 4 個 event，佢嘅熱度就除以 4
    from collections import defaultdict
    post_event_count = defaultdict(int)
    for event_id, sids_json in all_events:
        try:
            source_ids = json.loads(sids_json or "[]")
        except:
            source_ids = [event_id]
        for pid in source_ids:
            post_event_count[pid] += 1

    results = []
    for event_id, sids_json in all_events:
        try:
            source_ids = json.loads(sids_json or "[]")
        except:
            source_ids = [event_id]

        post_scores = []
        platforms   = set()
        post_dts    = []

        for pid in source_ids:
            post = all_posts.get(pid)
            if not post:
                continue
            # ← 核心改動：熱度按所屬 event 數公平分攤
            # roundup post 講 4 個活動，每個活動只攞 1/4 嘅熱度
            n_events = post_event_count.get(pid, 1)
            score    = post_score(post, platform_info) / n_events
            post_scores.append(score)
            platforms.add(post["platform"])
            dt = parse_dt(post.get("published_at"))
            if dt:
                post_dts.append(dt)

        n         = len(post_scores)
        avg_score = sum(post_scores) / n if n > 0 else 0.0
        raw_score = avg_score * math.log1p(n)
        newest_dt    = max(post_dts) if post_dts else None
        decay_factor = time_decay(newest_dt, half_life)

        # 記錄哪些係 roundup posts（屬於多個 event）供 leaderboard 顯示層過濾
        roundup_pids = [pid for pid in source_ids if post_event_count.get(pid, 1) > 1]

        results.append({
            "event_id":       event_id,
            "raw_score":      raw_score,
            "heat_score":     0.0,
            "platforms":      sorted(platforms),
            "platform_count": len(platforms),
            "source_count":   len(source_ids),
            "newest_dt":      newest_dt,
            "decay_factor":   decay_factor,
            "roundup_post_ids": roundup_pids,
        })

    return results


# ── Step 6: Apply decay → log min-max normalize → 0-100 ─────────────────────
def normalize_and_decay(results):
    # 先乘 time decay
    decayed = np.array([e["raw_score"] * e["decay_factor"] for e in results])

    # log 壓縮極端值
    log_decayed = np.log1p(decayed)

    # min-max normalize 到 0-100
    mn, mx = log_decayed.min(), log_decayed.max()
    if mx > mn:
        normed = (log_decayed - mn) / (mx - mn) * 100
    else:
        normed = np.full(len(results), 50.0)

    for e, score in zip(results, normed):
        e["heat_score"] = round(float(score), 2)
    return results


# ── Step 7: 寫入 DB ───────────────────────────────────────────────────────────
def write_scores(conn, results):
    cur = conn.cursor()
    for e in results:
        meta = {
            "platforms":        e["platforms"],
            "platform_count":   e["platform_count"],
            "source_count":     e["source_count"],
            "raw_score":        round(e["raw_score"], 4),
            "decay_factor":     round(e["decay_factor"], 4),
            "newest_post":      e["newest_dt"].strftime("%Y-%m-%d") if e["newest_dt"] else None,
            "roundup_post_ids": e.get("roundup_post_ids", []),
        }
        cur.execute(
            "UPDATE events_deduped SET heat_score=?, heat_meta=? WHERE event_id=?",
            (e["heat_score"], json.dumps(meta, ensure_ascii=False), e["event_id"])
        )
    conn.commit()
    log.info(f"✅ 已更新 {len(results)} 個 events_deduped heat_score")


# ── 公開接口 ──────────────────────────────────────────────────────────────────
def run_heat_analysis(db_path=DB_PATH, half_life=DEFAULT_HALF_LIFE,
                      dry_run=False, explain=False, top_n=10):
    log.info(f"🔥 開始活動熱度分析 (half_life={half_life}d)")
    conn = get_conn(db_path)

    log.info("\n📐 Fitting per-platform PCA weights...")
    platform_info = fit_platform_weights(conn, explain=explain)

    log.info("\n📥 載入所有帖文...")
    all_posts = load_all_posts(conn)

    log.info("\n🔗 計算活動熱度...")
    results = compute_event_scores(conn, platform_info, all_posts, half_life)
    results = normalize_and_decay(results)

    if not dry_run:
        ensure_heat_cols(conn)
        write_scores(conn, results)

    sorted_r = sorted(results, key=lambda x: -x["heat_score"])
    scores   = [e["heat_score"] for e in results]
    log.info(f"\n📈 熱度分佈：min={min(scores):.1f}  max={max(scores):.1f}  mean={sum(scores)/len(scores):.1f}")

    cur = conn.cursor()
    log.info(f"\n🏆 Top {top_n} 熱門活動：")
    for i, e in enumerate(sorted_r[:top_n], 1):
        cur.execute("SELECT ai_name, content FROM events_deduped WHERE event_id=?", (e["event_id"],))
        row  = cur.fetchone()
        name = (row[0] if row and row[0] else "") or (row[1][:40] if row and row[1] else e["event_id"][:40])
        log.info(
            f"  {i:2}. score={e['heat_score']:5.1f} | "
            f"[{'+'.join(e['platforms']):20}] "
            f"posts={e['source_count']:3} decay={e['decay_factor']:.2f} | {name}"
        )

    conn.close()
    log.info("\n✅ 完成")

    # ── 自動觸發 leaderboard cache 更新 ──────────────────────────────────────
    if not dry_run:
        _refresh_leaderboard_cache()

    return sorted_r


def _refresh_leaderboard_cache(host: str = "127.0.0.1", port: int = 9038):
    """
    Heat analyzer 跑完之後自動 POST /api/heat/leaderboard-ai/refresh
    令 leaderboard 即時反映最新 heat score，唔需要人手按 Refresh。
    bridge.py 必須係跑緊狀態，唔係嘅話只係 log warning，唔會 crash。
    """
    url = f"http://{host}:{port}/api/heat/leaderboard-ai/refresh"
    log.info(f"🔄 觸發 leaderboard cache 更新: POST {url}")
    try:
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode())
            if body.get("success"):
                total = body.get("total", "?")
                log.info(f"✅ Leaderboard cache 更新成功（{total} events）")
            else:
                log.warning(f"⚠️ Leaderboard refresh 返回失敗: {body.get('message')}")
    except Exception as e:
        log.warning(
            f"⚠️ Leaderboard cache 更新失敗（bridge.py 係咪跑緊？）: {e}\n"
            f"   可以之後手動 POST {url}"
        )


# ── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",        default=DB_PATH)
    parser.add_argument("--half-life", type=float, default=DEFAULT_HALF_LIFE)
    parser.add_argument("--top",       type=int,   default=10)
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--explain",   action="store_true")
    args = parser.parse_args()
    run_heat_analysis(args.db, args.half_life, args.dry_run, args.explain, args.top)