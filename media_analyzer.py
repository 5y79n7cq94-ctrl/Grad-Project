"""
media_analyzer.py — 圖片分析工具（Qwen-VL OCR，本地下載→base64）

改動：
  - 圖片改用本地下載→base64，解決XHS防盜鏈問題
  - 移除Paraformer影片語音（澳門連唔上），影片只OCR封面圖
  - 預設 images_only=True

用法：
  python media_analyzer.py              # 分析所有未處理帖文
  python media_analyzer.py --limit 20   # 最多處理 20 條
  python media_analyzer.py --dry-run    # 只睇唔調API
  python media_analyzer.py --reset-failed  # 清除失敗記錄重新跑

依賴：
  pip install openai python-dotenv httpx pillow

.env 檔案：
  DASHSCOPE_API_KEY=sk-xxxx
"""

import os, json, time, sqlite3, argparse, logging, base64, tempfile
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
import httpx
from openai import OpenAI

# ── 配置 ───────────────────────────────────────────────────
DB_PATH           = os.getenv("DB_PATH", "C:/Users/user/MediaCrawler/macau_analytics.db")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
QWEN_VL_MODEL     = "qwen-vl-plus"
MAX_IMAGES        = 4
MAX_IMG_MB        = 10     # 單張圖片大小上限
MAX_RETRIES       = 3
RETRY_DELAY       = 2

# 失敗標記（呢啲唔算有效內容，要重試）
FAILED_MARKERS = ("（無可分析內容）",)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── 客戶端 ─────────────────────────────────────────────────
def get_client() -> OpenAI:
    if not DASHSCOPE_API_KEY:
        raise ValueError("缺少 DASHSCOPE_API_KEY！請喺 .env 加入：DASHSCOPE_API_KEY=sk-xxxx")
    return OpenAI(
        api_key=DASHSCOPE_API_KEY,
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )

# ── 下載圖片→base64 ────────────────────────────────────────
def download_image_base64(url: str) -> Optional[tuple[str, str]]:
    """
    下載圖片，返回 (base64字串, mime_type)。
    失敗返回 None。
    """
    try:
        resp = httpx.get(url, timeout=15, follow_redirects=True, headers={
            # 模擬瀏覽器，繞過防盜鏈
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.xiaohongshu.com/",
        })
        if resp.status_code != 200:
            log.warning(f"    圖片下載失敗 HTTP {resp.status_code}: {url[:60]}")
            return None

        # 大小檢查
        if len(resp.content) > MAX_IMG_MB * 1024 * 1024:
            log.warning(f"    圖片太大 ({len(resp.content)//1024//1024}MB)，跳過")
            return None

        # 判斷格式
        content_type = resp.headers.get("content-type", "image/jpeg")
        if "png" in content_type:
            mime = "image/png"
        elif "webp" in content_type:
            mime = "image/webp"
        elif "gif" in content_type:
            mime = "image/gif"
        else:
            mime = "image/jpeg"

        b64 = base64.b64encode(resp.content).decode("utf-8")
        return b64, mime

    except Exception as e:
        log.warning(f"    圖片下載出錯: {e}")
        return None

# ── 圖片 OCR ───────────────────────────────────────────────
def ocr_image_base64(client: OpenAI, b64: str, mime: str) -> Optional[str]:
    """用base64傳圖片俾Qwen-VL做OCR。"""
    prompt = (
        "請完整提取圖片中所有可見文字，包括中文、英文、數字、日期、地點、活動名稱。"
        "直接輸出文字，唔需要解釋。冇文字就回覆「（無文字）」。"
    )
    data_url = f"data:{mime};base64,{b64}"

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.chat.completions.create(
                model=QWEN_VL_MODEL,
                messages=[{"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": data_url}},
                    {"type": "text", "text": prompt},
                ]}],
                max_tokens=800,
            )
            text = (resp.choices[0].message.content or "").strip()
            return None if text in ("（無文字）", "无文字", "(無文字)", "(无文字)") else (text or None)
        except Exception as e:
            log.warning(f"    OCR 嘗試 {attempt+1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    return None

def ocr_images(client: OpenAI, urls: list) -> str:
    """下載並OCR多張圖片，合併結果。"""
    results = []
    for i, url in enumerate(urls[:MAX_IMAGES]):
        log.info(f"    📷 OCR 圖片 {i+1}/{min(len(urls), MAX_IMAGES)}: {url[:60]}...")
        img = download_image_base64(url)
        if not img:
            log.warning(f"    ⚠️ 下載失敗，跳過呢張圖")
            continue
        b64, mime = img
        text = ocr_image_base64(client, b64, mime)
        if text:
            results.append(text)
        time.sleep(0.2)  # 避免太急
    return "\n---\n".join(results)

# ── 從 raw_json 提取圖片 URL ───────────────────────────────
def extract_image_urls(raw_json: dict, media_urls_str: str = "") -> list:
    urls = []

    # ── FB：圖片 URL 係存喺 posts_fb.media_urls 欄位 ──
    if media_urls_str:
        try:
            media_list = json.loads(media_urls_str) if isinstance(media_urls_str, str) else []
            for u in (media_list if isinstance(media_list, list) else []):
                if isinstance(u, str) and u.startswith("http") and "fbcdn.net" in u:
                    urls.append(u)
        except (json.JSONDecodeError, TypeError):
            pass

    # ── XHS：image_list 係字串或列表 ──
    img_list = raw_json.get("image_list")
    if isinstance(img_list, str) and img_list.startswith("http"):
        urls.append(img_list)
    elif isinstance(img_list, list):
        for item in img_list:
            url = item.get("url") if isinstance(item, dict) else item
            if url and str(url).startswith("http"):
                urls.append(url)

    # ── IG：displayUrl 係封面圖 ──
    for key in ["displayUrl", "image_url", "img_url", "photo_url", "cover"]:
        val = raw_json.get(key)
        if val and str(val).startswith("http"):
            urls.append(val)

    # IG images 列表
    for url in (raw_json.get("images") or []):
        if isinstance(url, str) and url.startswith("http"):
            urls.append(url)

    # ── 微博：pic_ids 拼接 URL，或者 pics 列表 ──
    for pid in (raw_json.get("pic_ids") or []):
        if isinstance(pid, str) and pid:
            urls.append(f"https://wx1.sinaimg.cn/large/{pid}.jpg")

    for p in (raw_json.get("pics") or []):
        u = (p.get("large", {}).get("url") or p.get("url")) if isinstance(p, dict) else p
        if u and str(u).startswith("http"):
            urls.append(u)

    # 通用
    for key in ["media_url", "pic_url"]:
        val = raw_json.get(key)
        if val and str(val).startswith("http"):
            urls.append(val)

    # 去重
    seen, deduped = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); deduped.append(u)
    return deduped

# ── DB 操作 ────────────────────────────────────────────────
def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def ensure_media_text_col(conn: sqlite3.Connection):
    cursor = conn.cursor()
    for table in ["posts_xhs", "posts_ig", "posts_fb", "posts_weibo"]:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN media_text TEXT")
            log.info(f"✅ 已加 media_text 欄位到 {table}")
        except sqlite3.OperationalError:
            pass
    conn.commit()

def reset_failed_records(conn: sqlite3.Connection):
    """清除之前失敗嘅記錄，令佢哋可以重新分析。"""
    cursor = conn.cursor()
    total = 0
    for marker in FAILED_MARKERS:
        for table in ["posts_xhs", "posts_ig", "posts_fb", "posts_weibo"]:
            cursor.execute(
                f"UPDATE {table} SET media_text = NULL WHERE media_text = ?",
                (marker,)
            )
            total += cursor.rowcount
    conn.commit()
    log.info(f"🧹 清除失敗記錄：{total} 條重置為 NULL，下次會重新分析")

def get_posts_to_analyze(conn: sqlite3.Connection, limit: int,
                         post_id: Optional[str] = None,
                         post_ids: Optional[list] = None) -> list:
    cursor  = conn.cursor()
    results = []
    tables  = {"posts_xhs": "xhs", "posts_ig": "ig", "posts_fb": "fb", "posts_weibo": "weibo"}

    for table, platform in tables.items():
        if post_id:
            # 單一指定 post
            cursor.execute(
                f"SELECT post_id, media_type, raw_json, media_urls FROM {table} WHERE post_id=?",
                (post_id,)
            )
        elif post_ids:
            # 批量指定 post_ids（今次新入庫嘅）
            placeholders = ",".join("?" * len(post_ids))
            cursor.execute(f"""
                SELECT post_id, media_type, raw_json, media_urls
                FROM {table}
                WHERE post_id IN ({placeholders})
                  AND (media_text IS NULL OR media_text = '')
                  AND media_type IN ('image','video')
                  AND raw_json IS NOT NULL
            """, post_ids)
        else:
            # 全量補跑模式
            cursor.execute(f"""
                SELECT post_id, media_type, raw_json, media_urls
                FROM {table}
                WHERE (media_text IS NULL OR media_text = '')
                  AND media_type IN ('image','video')
                  AND raw_json IS NOT NULL
                ORDER BY ingested_at DESC
                LIMIT ?
            """, (limit,))

        for pid, media_type, raw_json_str, media_urls_str in cursor.fetchall():
            try:
                rj = json.loads(raw_json_str) if raw_json_str else {}
            except json.JSONDecodeError:
                continue

            image_urls = extract_image_urls(rj, media_urls_str or "")
            if not image_urls:
                continue

            results.append({
                "id":         pid,
                "table":      table,
                "platform":   platform,
                "media_type": media_type,
                "image_urls": image_urls,
            })
    return results

def save_media_text(conn: sqlite3.Connection, table: str, post_id: str, text: str):
    conn.execute(f"UPDATE {table} SET media_text=? WHERE post_id=?", (text, post_id))
    conn.commit()
    log.info(f"    ✅ 儲存 [{post_id}] ({len(text)} 字)")

# ── 主流程 ─────────────────────────────────────────────────
def run(db_path=DB_PATH, limit=100, post_id=None, post_ids=None, dry_run=False, reset_failed=False):
    log.info("🚀 開始媒體分析（圖片 base64 模式）")
    conn = get_conn(db_path)
    ensure_media_text_col(conn)

    if reset_failed:
        reset_failed_records(conn)

    posts = get_posts_to_analyze(conn, limit, post_id, post_ids)
    mode  = f"指定 {len(post_ids)} 個 post_ids" if post_ids else ("全量" if not post_id else f"單一 {post_id}")
    log.info(f"📋 [{mode}] 搵到 {len(posts)} 條帖文需要分析")

    if not posts:
        log.info("✅ 全部已處理"); conn.close(); return

    if dry_run:
        for p in posts:
            log.info(f"   [{p['platform']}] {p['id']} | {p['media_type']} | 圖:{len(p['image_urls'])}張")
        conn.close(); return

    client = get_client()
    ok = fail = skip = 0

    for i, post in enumerate(posts):
        log.info(f"\n[{i+1}/{len(posts)}] {post['platform'].upper()} {post['id']} ({post['media_type']})")

        try:
            text = ocr_images(client, post["image_urls"])
        except Exception as e:
            log.error(f"    ❌ 失敗: {e}")
            fail += 1
            continue

        if not text:
            log.info("    ⚠️ 未提取到文字")
            save_media_text(conn, post["table"], post["id"], "（圖片無文字）")
            skip += 1
            continue

        save_media_text(conn, post["table"], post["id"], f"【圖片文字】\n{text}")
        log.info(f"    📝 預覽: {text[:100]}...")
        ok += 1

        if i < len(posts) - 1:
            time.sleep(0.3)

    conn.close()
    log.info(f"\n✅ 完成！成功:{ok} 失敗:{fail} 跳過(無文字):{skip}")

# ── CLI ────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",           default=DB_PATH)
    parser.add_argument("--limit",        type=int, default=100)
    parser.add_argument("--post-id")
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--reset-failed", action="store_true", help="清除失敗記錄，重新分析")
    args = parser.parse_args()
    run(args.db, args.limit, args.post_id, args.dry_run, args.reset_failed)