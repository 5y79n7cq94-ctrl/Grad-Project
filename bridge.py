import os, sys, json, uvicorn, re, hashlib, sqlite3, glob, time, math
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from openai import OpenAI
from collections import defaultdict
from db_manager import query_db_by_filters, get_ops_needing_crawl, backfill_event_dates, DB_PATH, query_fb_negative_monitor, query_ig_negative_monitor, query_weibo_negative_monitor, query_xhs_negative_monitor
from task_manager import run_task_master, run_fb_negative_monitor_crawl, run_ig_negative_monitor_crawl, run_weibo_negative_monitor_crawl, run_xhs_negative_monitor_crawl
import threading
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pathlib import Path
from fastapi.responses import FileResponse, Response
import joblib  
from full_web_heat_adapter import get_mediacrawler_root, get_project_analytics_service
from full_web_heat_jobs import heat_job_manager

# ── 最先 load .env，確保所有 os.getenv() 都能讀到環境變量 ──────────────────
load_dotenv()

# ✏️ CHANGED: 防止重複爬蟲 thread
# key = operator, value = True 表示而家正在爬緊
_crawling_ops: set = set()
_crawling_lock = threading.Lock()

_neg_monitor_crawl_lock = threading.Lock()
_neg_monitor_crawl_running = False
BRIDGE_ROOT = Path(__file__).resolve().parent
WEBUI_DIR = BRIDGE_ROOT / "webui"
STATIC_DIR = BRIDGE_ROOT / "static"
HEAT_ANALYSIS_WEEK_FLOOR = os.getenv("HEAT_ANALYSIS_WEEK_FLOOR", "2026-03-01").strip()

def _bridge_html_file(filename: str) -> FileResponse:
    p = BRIDGE_ROOT / filename
    if not p.is_file():
        raise HTTPException(status_code=404, detail=f"{filename} not found under {BRIDGE_ROOT}")
    return FileResponse(p, media_type="text/html; charset=utf-8")


# ── DeepSeek analysis cache（per operator per date range）─────────────────────
# 避免同一 operator + 日期範圍重複 call DeepSeek；重啟 bridge.py 先清除 cache
# Analysis cache 已移到 SQLite，見 _get_analysis_cache / _set_analysis_cache

def _analysis_cache_key(op_key: str, from_date: str, to_date: str, keyword: str = "") -> str:
    kw = ",".join(sorted(k.strip().lower() for k in keyword.split(",") if k.strip()))
    if kw:
        return f"{op_key}|{from_date}|{to_date}|{kw}"
    return f"{op_key}|{from_date}|{to_date}"
def _get_analysis_cache(op_key, from_date, to_date, keyword=""):
    try:
        conn = _heat_db_conn()
        base_key = _analysis_cache_key(op_key, from_date, to_date, "")  # 永遠搵全量
        row = conn.execute(
            "SELECT activities FROM analysis_cache WHERE cache_key=?", (base_key,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        
        from datetime import date
        try:
            if row[1] == 0 and date.today() > date.fromisoformat(to_date):
                return None
        except Exception:
            pass
        all_acts = json.loads(row[0])
        
        if not keyword.strip():
            return all_acts  # 無 keyword 直接返全量
        
        # 有 keyword：從全量過濾（加繁簡轉換）
        try:
            from trad_simp import expand_variants as _expand_kw
        except ImportError:
            def _expand_kw(k): return [k]
        
        kw_list = []
        for k in keyword.split(","):
            k = k.strip()
            if k:
                kw_list.extend(v.lower() for v in _expand_kw(k))
        
        return [
            a for a in all_acts
            if any(
                kw in (a.get("name") or "").lower()
                or kw in (a.get("description") or "").lower()
                for kw in kw_list
            )
        ]
    except Exception:
        return None

def _set_analysis_cache(op_key: str, from_date: str, to_date: str, activities: list, keyword: str = ""):
    """寫入 cache"""
    if keyword.strip():
        return
    try:
        conn = _heat_db_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_cache (
                cache_key   TEXT PRIMARY KEY,
                activities  TEXT,
                cached_at   TEXT DEFAULT (datetime('now','localtime')),
                is_complete INTEGER DEFAULT 0
            )
        """)
        try:
            conn.execute("ALTER TABLE analysis_cache ADD COLUMN is_complete INTEGER DEFAULT 0")
            conn.commit()
        except Exception:
            pass  # column 已存在就跳過
        key = _analysis_cache_key(op_key, from_date, to_date, keyword)

        # 判斷個月係咪已經完結
        from datetime import date
        today = date.today()
        try:
            end = date.fromisoformat(to_date)
            is_complete = 1 if today > end else 0
        except Exception:
            is_complete = 0

        conn.execute(
            "INSERT OR REPLACE INTO analysis_cache (cache_key, activities, cached_at, is_complete) VALUES (?,?,datetime('now','localtime'),?)",
            (key, json.dumps(activities, ensure_ascii=False), is_complete)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ analysis_cache 寫入失敗: {e}")

def _invalidate_analysis_cache(op_key: str = None):
    """清除 cache，op_key=None 清全部"""
    try:
        conn = _heat_db_conn()
        conn.execute("CREATE TABLE IF NOT EXISTS analysis_cache (cache_key TEXT PRIMARY KEY, activities TEXT, cached_at TEXT)")
        if op_key:
            conn.execute("DELETE FROM analysis_cache WHERE cache_key LIKE ?", (f"{op_key}|%",))
        else:
            conn.execute("DELETE FROM analysis_cache")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ analysis_cache invalidate 失敗: {e}")

# ── Period Cache (hot_themes / recommendations / leaderboard) ─────────────────
def _period_cache_key(from_date: str, to_date: str) -> str:
    return f"{from_date}|{to_date}"

def _get_period_cache(from_date: str, to_date: str) -> dict | None:
    try:
        conn = _heat_db_conn()
        # 嘗試讀新 columns，fallback 到舊 3-column schema
        try:
            row = conn.execute(
                "SELECT hot_themes, recommendations, leaderboard, events, heatmap_data FROM period_cache WHERE cache_key=?",
                (_period_cache_key(from_date, to_date),)
            ).fetchone()
        except Exception:
            row = conn.execute(
                "SELECT hot_themes, recommendations, leaderboard FROM period_cache WHERE cache_key=?",
                (_period_cache_key(from_date, to_date),)
            ).fetchone()
        conn.close()
        if not row:
            return None
        result = {
            "hot_themes":      json.loads(row[0]) if row[0] else [],
            "recommendations": json.loads(row[1]) if row[1] else [],
            "leaderboard":     json.loads(row[2]) if row[2] else [],
        }
        if len(row) >= 5:
            result["events"]       = json.loads(row[3]) if row[3] else {}
            result["heatmap_data"] = json.loads(row[4]) if row[4] else {}
        return result
    except Exception as e:
        print(f"⚠️ period_cache 讀取失敗: {e}")
        return None

def _set_period_cache(from_date: str, to_date: str, hot_themes: list, recommendations: list, leaderboard: list, events: dict = None, heatmap_data: dict = None):
    try:
        conn = _heat_db_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS period_cache (
                cache_key       TEXT PRIMARY KEY,
                hot_themes      TEXT,
                recommendations TEXT,
                leaderboard     TEXT,
                events          TEXT,
                heatmap_data    TEXT,
                cached_at       TEXT DEFAULT (datetime('now','localtime')),
                is_complete     INTEGER DEFAULT 0
            )
        """)
        # 兼容舊 DB：嘗試加新 column（已存在就跳過）
        for col in ["events", "heatmap_data"]:
            try:
                conn.execute(f"ALTER TABLE period_cache ADD COLUMN {col} TEXT")
            except Exception:
                pass
        key = _period_cache_key(from_date, to_date)
        conn.execute(
            """INSERT OR REPLACE INTO period_cache 
               (cache_key, hot_themes, recommendations, leaderboard, events, heatmap_data, cached_at)
               VALUES (?,?,?,?,?,?,datetime('now','localtime'))""",
            (key,
             json.dumps(hot_themes,       ensure_ascii=False),
             json.dumps(recommendations,  ensure_ascii=False),
             json.dumps(leaderboard,      ensure_ascii=False),
             json.dumps(events      or {}, ensure_ascii=False),
             json.dumps(heatmap_data or {}, ensure_ascii=False))
        )
        conn.commit()
        conn.close()
        print(f"✅ period_cache saved: {from_date}→{to_date} | events cats={len(events or {})} | heatmap cats={len(heatmap_data or {})}")
    except Exception as e:
        print(f"⚠️ period_cache 寫入失敗: {e}")

def _report_insights_key(from_date: str, to_date: str, keyword: str = "") -> str:
    kw = ",".join(sorted(k.strip().lower() for k in str(keyword or "").split(",") if k.strip()))
    return f"{from_date}|{to_date}|{kw}"

def _ensure_report_insights_table():
    conn = _heat_db_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS report_insights_cache (
                cache_key TEXT PRIMARY KEY,
                from_date TEXT NOT NULL,
                to_date TEXT NOT NULL,
                keyword TEXT DEFAULT '',
                hot_themes TEXT DEFAULT '[]',
                recommendations TEXT DEFAULT '[]',
                comparisons TEXT DEFAULT '[]',
                cached_at TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        conn.commit()
    finally:
        conn.close()

def _get_report_insights(from_date: str, to_date: str, keyword: str = ""):
    _ensure_report_insights_table()
    conn = _heat_db_conn()
    try:
        row = conn.execute(
            """SELECT hot_themes, recommendations, comparisons, cached_at
               FROM report_insights_cache WHERE cache_key=?""",
            (_report_insights_key(from_date, to_date, keyword),)
        ).fetchone()
        if not row:
            return None
        return {
            "hotThemes": json.loads(row["hot_themes"] or "[]"),
            "aiRecommendations": json.loads(row["recommendations"] or "[]"),
            "comparisons": json.loads(row["comparisons"] or "[]"),
            "cachedAt": row["cached_at"] or ""
        }
    except Exception as e:
        print(f"⚠️ report_insights 讀取失敗: {e}")
        return None
    finally:
        conn.close()

def _set_report_insights(from_date: str, to_date: str, keyword: str = "", hot_themes=None, recommendations=None, comparisons=None):
    _ensure_report_insights_table()
    conn = _heat_db_conn()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO report_insights_cache
               (cache_key, from_date, to_date, keyword, hot_themes, recommendations, comparisons, cached_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))""",
            (
                _report_insights_key(from_date, to_date, keyword),
                from_date,
                to_date,
                ",".join(k.strip() for k in str(keyword or "").split(",") if k.strip()),
                json.dumps(hot_themes or [], ensure_ascii=False),
                json.dumps(recommendations or [], ensure_ascii=False),
                json.dumps(comparisons or [], ensure_ascii=False),
            )
        )
        conn.commit()
    except Exception as e:
        print(f"⚠️ report_insights 寫入失敗: {e}")
        raise
    finally:
        conn.close()
backfill_event_dates()
app = FastAPI()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

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

def _heat_service():
    service = get_project_analytics_service()
    service.ensure_schema()
    return service


def _heat_jobs():
    return heat_job_manager


def _file_response(path: Path) -> FileResponse:
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Missing file: {path.name}")
    return FileResponse(path)

@app.get("/")
@app.get("/operation-panel")
@app.get("/operation-panel/")
async def operation_panel_page():
    p = BRIDGE_ROOT / "operation_panel.html"
    if not p.is_file():
        return {"error": "operation_panel.html missing", "path": str(p)}
    return FileResponse(p, media_type="text/html; charset=utf-8")


@app.get("/project")
async def serve_market_report_alias():
    return _file_response(BRIDGE_ROOT / "operation_panel.html")


@app.get("/full-web-heat-analysis")
@app.get("/heat-analysis")
async def serve_heat_analysis_page():
    return _file_response(WEBUI_DIR / "full_web_heat_analysis.html")


@app.get("/full-web-heat-analysis/trends")
@app.get("/heat-analysis/trends")
async def serve_heat_trends_page():
    return _file_response(WEBUI_DIR / "full_web_heat_trends.html")


@app.get("/api/full-web-heat-analysis/overview")
@app.get("/api/heat-analysis/overview")
async def get_heat_overview(platform: str = "wb", auto_sync: bool = False):
    try:
        service = _heat_service()
        if auto_sync:
            service.sync(platform=platform)
        payload = service.get_overview(platform=platform)
        payload["mediacrawler_root"] = str(get_mediacrawler_root())
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/full-web-heat-analysis/analysis-windows")
@app.get("/api/heat-analysis/analysis-windows")
async def get_heat_analysis_windows(platform: str = "wb", weeks: int = 24):
    try:
        payload = _heat_service().list_analysis_windows(platform=platform, weeks=weeks)
        items = payload.get("items", [])
        if HEAT_ANALYSIS_WEEK_FLOOR:
            items = [item for item in items if str(item.get("week_start") or "") >= HEAT_ANALYSIS_WEEK_FLOOR]
        return {
            **payload,
            "items": items,
            "total": len(items),
            "week_floor": HEAT_ANALYSIS_WEEK_FLOOR,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/full-web-heat-analysis/event-clusters")
@app.get("/api/heat-analysis/event-clusters")
async def get_heat_event_clusters(
    platform: str = "wb",
    q: str = "",
    dashboard_category: str = "",
    limit: int = 30,
    offset: int = 0,
    week_start: str = "",
    week_end: str = "",
):
    try:
        return _heat_service().list_event_clusters(
            platform=platform,
            q=q,
            dashboard_category=dashboard_category,
            limit=limit,
            offset=offset,
            week_start=week_start,
            week_end=week_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/full-web-heat-analysis/topic-clusters")
@app.get("/api/heat-analysis/topic-clusters")
async def get_heat_topic_clusters(
    platform: str = "wb",
    q: str = "",
    dashboard_category: str = "",
    limit: int = 30,
    offset: int = 0,
    week_start: str = "",
    week_end: str = "",
):
    try:
        return _heat_service().list_topic_clusters(
            platform=platform,
            q=q,
            dashboard_category=dashboard_category,
            limit=limit,
            offset=offset,
            week_start=week_start,
            week_end=week_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/full-web-heat-analysis/event-trend")
@app.get("/api/heat-analysis/event-trend")
async def get_heat_event_trend(
    platform: str = "wb",
    event_family_key: str = "",
    days: int = 7,
    start_date: str = "",
    end_date: str = "",
    week_start: str = "",
    week_end: str = "",
):
    try:
        return _heat_service().get_event_discussion_trend(
            platform=platform,
            event_family_key=event_family_key,
            days=days,
            start_date=start_date,
            end_date=end_date,
            week_start=week_start,
            week_end=week_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/full-web-heat-analysis/run-analysis")
@app.post("/api/heat-analysis/run-analysis")
async def run_heat_analysis(
    platform: str = "wb",
    status: str = "ready",
    replace: bool = True,
    week_start: str = "",
    week_end: str = "",
):
    try:
        if week_start or week_end:
            return _heat_service().extract_events_weekly(
                platform=platform,
                week_start=week_start,
                week_end=week_end,
                status=status,
                replace=replace,
            )
        return _heat_service().extract_events(platform=platform, status=status, replace=replace)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/full-web-heat-analysis/update-week")
@app.post("/api/heat-analysis/update-week")
async def update_heat_week(platform: str = "wb", week_start: str = "", week_end: str = "", db_path: str = ""):
    try:
        return _heat_jobs().start_update_job(
            platform=platform,
            week_start=week_start,
            week_end=week_end,
            db_path=db_path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/full-web-heat-analysis/jobs")
@app.get("/api/heat-analysis/jobs")
async def list_heat_jobs(limit: int = 20):
    return _heat_jobs().list_jobs(limit=limit)


@app.get("/api/full-web-heat-analysis/jobs/{job_id}")
@app.get("/api/heat-analysis/jobs/{job_id}")
async def get_heat_job(job_id: str):
    try:
        return _heat_jobs().get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Heat-analysis job not found: {job_id}") from exc

@app.get("/negative-monitor")
async def negative_monitor_page():
    p = BRIDGE_ROOT / "negative_monitor.html"
    if not p.is_file():
        return {"error": "negative_monitor.html missing", "path": str(p)}
    return FileResponse(p, media_type="text/html; charset=utf-8")

@app.get("/negative_monitor.html")
async def bridge_negative_monitor_html():
    return _bridge_html_file("negative_monitor.html")

@app.get("/login_page.html")
async def bridge_login_page_html():
    return _bridge_html_file("login_page.html")

@app.get("/admin_page.html")
async def bridge_admin_page_html():
    return _bridge_html_file("admin_page.html")

@app.get("/heat_leaderboard_v2.html")
async def bridge_heat_leaderboard_html():
    return _bridge_html_file("heat_leaderboard_v2.html")

@app.get("/archived_report.html")
async def bridge_archived_html():
    return _bridge_html_file("archived_report.html")

@app.get("/download_report.html")
async def bridge_download_report_html():
    return _bridge_html_file("download_report.html")

@app.get("/operation_panel.html")
async def bridge_operation_panel_dot_html():
    return _bridge_html_file("operation_panel.html")

@app.get("/favicon.ico")
@app.get("/apple-touch-icon.png")
@app.get("/apple-touch-icon-precomposed.png")
async def browser_icon_placeholders():
    return Response(status_code=204)


# DeepSeek client（用於帖文分析）
client = OpenAI(
    api_key="sk-ec64f5296ab34389a632b48aa8c28600",
    base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
)
DEEPSEEK_JSON_MODEL = "deepseek-chat"
DEEPSEEK_REASON_MODEL = "deepseek-reasoner"
# Qwen client（用於 Wynn Market Performance & Positioning 推薦）
QWEN_RECOMMENDATION_CLIENT = OpenAI(
    api_key="sk-995dbed7e46548a6992a8e5153628165",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    timeout=240,
)
QWEN_RECOMMENDATION_MODEL = "qwen3.5-plus"

NEGATIVE_MONITOR_LEXICON = [
    "避雷", "踩雷", "翻車", "差評", "差评", "吐槽", "投訴", "维权", "維權", "騙局", "骗局",
    "被坑", "別去", "别去", "不要住", "服務差", "服务差", "態度差", "态度差", "衛生", "卫生",
    "髒", "脏", "吵", "騷擾", "骚扰", "退費", "退费", "退款", "報警", "报警", "凶殺", "凶杀",
    "命案", "事故", "受傷", "受伤", "食物中毒", "發霉", "发霉", "筹码", "公关", "抽成", "偷"
]

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
            model=DEEPSEEK_JSON_MODEL,
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

# ── Footfall──────────────

FOOTFALL_MACAU_VENUE_REFERENCE_ZH = """
你必须把每个活动判到下列两类统计分区之一（JSON 里 region 只能填 cotai 或 nam_van）：

**路氹填海区 (cotai)** — 统计分区名称：路氹填海區。主要酒店/场所（与运营商对应关系供参考）：
- Wynn（永利）：永利皇宫
- Sands（金沙）：澳门威尼斯人、澳门伦敦人、澳门巴黎人、澳门瑞吉酒店、澳门喜来登酒店、澳门康莱德酒店、澳门四季酒店（含四季名荟）、伦敦人御园、伦敦人御匾名汇、巴黎人御匾峰、伦敦人名汇
- MGM（美高梅）：美狮美高梅（含雍华府）
- Galaxy（银河）：丽思卡尔顿、悦榕庄、JW万豪、银河酒店、大仓酒店、百老汇、安达仕酒店、莱佛士、嘉佩乐
- Melco（新濠）：新濠影汇（含映星汇）、君悦、摩珀斯、颐居、迎尚、W酒店
- SJM（澳博）：澳门上葡京、范思哲酒店、卡尔拉格斐酒店

**外港及南湾湖新填海区 (nam_van)** — 统计分区名称：外港及南灣湖新填海區。主要酒店/场所：
- SJM（澳博）：澳门新葡京、澳门葡京（老葡京）、回力酒店
- MGM（美高梅）：澳门美高梅
- Melco（新濠）：澳门新濠锋
- Wynn（永利）：永利澳门
- 注意：**励宫酒店**、**澳门文华东方（文化东方）不属于澳博/SJM**；若活动在这些场所举办，region 仍填 nam_van，primary_venue 填真实酒店名，**不要**把场所写成或归到 SJM/澳博名下。
"""


def _footfall_parse_json_object(text: str) -> dict:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
    text = text.strip()
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("no JSON object in model output")
    return json.loads(m.group())


def _footfall_events_summary_for_date(ds: str) -> str:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        rows = conn.execute(
            """
            SELECT COALESCE(ai_name, ''), COALESCE(category, ''), COALESCE(sub_type, ''), COALESCE(event_date, '')
            FROM events_deduped
            WHERE event_date IS NOT NULL AND length(event_date) >= 10 AND substr(event_date, 1, 10) = ?
            LIMIT 40
            """,
            (ds[:10],),
        ).fetchall()
        conn.close()
        if not rows:
            return ""
        lines = []
        for r in rows:
            name, cat, sub, ed = r
            lines.append(f"- {(name or '')[:120]} | {cat} / {sub} | date={ed}")
        return "\n".join(lines)
    except Exception as e:
        print(f"⚠️ footfall events summary: {e}")
        return ""


def _footfall_ai_continuous_regressors(ds: str, aux_df: pd.DataFrame, events_summary: str) -> dict | None:
    """
    由 DeepSeek 估计目标日 EXCHANGE_RATE、PRICE_INDEX（与训练表量纲一致）。
    失败返回 None，由调用方回退到 finaldata1 查表。
    """
    if not os.getenv("DEEPSEEK_API_KEY"):
        return None
    d = datetime.strptime(ds[:10], "%Y-%m-%d")
    weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][d.weekday()]
    try:
        h = aux_df.copy()
        h["Date"] = pd.to_datetime(h["Date"])
        h = h.sort_values("Date")
        tail = h.tail(60)
        er_s = pd.to_numeric(tail.get("EXCHANGE_RATE"), errors="coerce").dropna()
        pi_s = pd.to_numeric(tail.get("PRICE_INDEX"), errors="coerce").dropna()
        er_last = float(er_s.iloc[-1]) if len(er_s) else None
        pi_last = float(pi_s.iloc[-1]) if len(pi_s) else None
        er_rng = f"{float(er_s.min()):.4f}–{float(er_s.max()):.4f}" if len(er_s) else "—"
        pi_rng = f"{float(pi_s.min()):.2f}–{float(pi_s.max()):.2f}" if len(pi_s) else "—"
    except Exception:
        er_last = pi_last = None
        er_rng = pi_rng = "—"

    prompt = f"""你是澳门旅游宏观经济辅助变量估计员。目标日期：{ds}（{weekday_cn}）。

## 量纲（须与训练表 finaldata1 一致）
训练列 **EXCHANGE_RATE**：人民币/港元等相关汇率口径的数值；历史末值约 {er_last if er_last is not None else '—'} ，近60日范围 {er_rng}。
训练列 **PRICE_INDEX**：物价指数；历史末值约 {pi_last if pi_last is not None else '—'} ，近60日范围 {pi_rng}。

请结合公历与节假日、常识与下方活动摘要，**估计该日用于 Prophet 的 EXCHANGE_RATE 与 PRICE_INDEX（各一个浮点数）**。须与上列**同一数量级**，勿编造离谱数量级。

活动摘要（events_deduped，可能为空）：
{events_summary or "（无）"}

只输出一个 JSON 对象，键必须是：EXCHANGE_RATE, PRICE_INDEX。不要任何其他文字。"""
    try:
        resp = client.chat.completions.create(
            model=DEEPSEEK_JSON_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.25,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = _footfall_parse_json_object(raw)
        er = float(data.get("EXCHANGE_RATE"))
        pi = float(data.get("PRICE_INDEX"))
        if not (math.isfinite(er) and math.isfinite(pi)):
            return None
        return {"EXCHANGE_RATE": er, "PRICE_INDEX": pi}
    except Exception as e:
        print(f"⚠️ footfall AI continuous (EXCHANGE/PRICE) failed: {e}")
        return None


def _footfall_ai_five_regressors(ds: str, events_summary: str, cont: dict) -> dict:
    if not os.getenv("DEEPSEEK_API_KEY"):
        return {}
    d = datetime.strptime(ds[:10], "%Y-%m-%d")
    weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][d.weekday()]
    er = cont.get("EXCHANGE_RATE", "")
    pi = cont.get("PRICE_INDEX", "")
    prompt = f"""你是澳门旅游客流 Prophet 模型的特征标注员。目标日期：{ds}（{weekday_cn}）。

## 当日宏观连续变量（已由上游估计，与 Prophet 使用值一致；供你综合判断，勿在 JSON 里输出这两项）
- EXCHANGE_RATE（训练列名 EXCHANGE_RATE）: {er}
- PRICE_INDEX（训练列名 PRICE_INDEX）: {pi}

## 5 个二值特征（仅能取 0 或 1）
请结合：公历与节假日常识、下方「活动摘要」、以及上面对汇率与物价的背景，判断下列开关；不确定则 0。

- IS_PH_CN：当日是否为中国内地法定节假日（公众休假日）。
- IS_PH_HK：当日是否为香港法定节假日。
- HAS_Concerts：当日澳门是否有大型演唱会或主要音乐演出（可参考活动摘要）。
- HAS_Macau_Big_Events：当日是否有大型节庆、赛事、会展等对客流有明显抬升的活动（非日常小型活动）。
- IS_TYPHOON8910：是否属于台风高发期（8–10 月）且当日有台风或极端天气对澳门有合理影响；否则 0。

活动摘要（来自本系统 events_deduped，可能为空）：
{events_summary or "（无）"}

只输出一个 JSON 对象，键必须是：IS_PH_CN, IS_PH_HK, HAS_Concerts, HAS_Macau_Big_Events, IS_TYPHOON8910，值只能是 0 或 1。不要任何其他文字。"""
    try:
        resp = client.chat.completions.create(
            model=DEEPSEEK_JSON_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = _footfall_parse_json_object(raw)
        keys = ["IS_PH_CN", "IS_PH_HK", "HAS_Concerts", "HAS_Macau_Big_Events", "IS_TYPHOON8910"]
        out = {}
        for k in keys:
            v = data.get(k, 0)
            try:
                out[k] = 1 if int(v) == 1 else 0
            except (TypeError, ValueError):
                out[k] = 0
        return out
    except Exception as e:
        print(f"⚠️ footfall AI regressors failed: {e}")
        return {}


_FOOTFALL_FITTED_CACHE: dict = {}


def _get_footfall_fitted(model_path: Path):
    import joblib

    k = str(model_path.resolve())
    if k not in _FOOTFALL_FITTED_CACHE:
        _FOOTFALL_FITTED_CACHE[k] = joblib.load(model_path)
    return _FOOTFALL_FITTED_CACHE[k]


def _footfall_predict_row_for_date(
    ds: str,
    *,
    model_path: Path,
    zone_csv: Path,
    footfall_dir: Path,
    fast: bool = False,
    continuous_override: dict | None = None,
    no_per_day_continuous_ai: bool = False,
) -> dict:
    """
    fast=True：仅用 finaldata1 连续变量 + 周末日历（不调 DeepSeek）。
    continuous_override：若给定，该日强制使用该组 EXCHANGE_RATE/PRICE_INDEX（少用）。
    no_per_day_continuous_ai：为 True 时不调用「单日」汇率/物价 AI（仅用 CSV 连续变量）。
    非 fast：按自然日 ds 各估一组 EXCHANGE/PRICE；**同一日历日的所有活动**共用当日 zone 预测（见 footfall-event-allocate 按 ds 只算一次）。
    """
    if str(footfall_dir) not in sys.path:
        sys.path.insert(0, str(footfall_dir))

    from load_finaldata import continuous_values_for_date, load_finaldata_df
    from predict_one_day import build_future_row, merge_regressors_for_prediction, predict_total_australia
    from zone_daily_from_total import COTAI_ZH, NAM_VAN_ZH, split_forecast_by_zone_shares

    aux_df = load_finaldata_df(footfall_dir / "finaldata1.csv")
    if continuous_override is not None:
        cont = {
            "EXCHANGE_RATE": float(continuous_override["EXCHANGE_RATE"]),
            "PRICE_INDEX": float(continuous_override["PRICE_INDEX"]),
        }
    else:
        cont = continuous_values_for_date(aux_df, ds)
    five: dict = {}
    if not fast and os.getenv("DEEPSEEK_API_KEY"):
        evs = _footfall_events_summary_for_date(ds)
        if continuous_override is None and not no_per_day_continuous_ai:
            ai_cont = _footfall_ai_continuous_regressors(ds, aux_df, evs)
            if ai_cont is not None:
                cont = ai_cont
        five = _footfall_ai_five_regressors(ds, evs, cont)
    regressors = merge_regressors_for_prediction(ds, five, cont)
    fitted = _get_footfall_fitted(model_path)
    row_df = build_future_row(ds, regressors)
    yhat_o = predict_total_australia(fitted, row_df, inverse_log10=True)
    one = pd.DataFrame({"ds": [pd.to_datetime(ds)], "yhat_original": [yhat_o]})
    df = split_forecast_by_zone_shares(one, zone_csv=zone_csv, yhat_col="yhat_original", ds_col="ds")
    row = df.iloc[0]
    col_c = f"visitation_{COTAI_ZH}"
    col_n = f"visitation_{NAM_VAN_ZH}"
    return {
        "ds": ds,
        "macau_total": float(row["yhat_original"]),
        "zone_cotai": float(row[col_c]),
        "zone_namvan": float(row[col_n]),
        "labels": {"cotai": COTAI_ZH, "namvan": NAM_VAN_ZH},
    }


def _footfall_enumerate_dates(d0: str, d1: str) -> list[str]:
    a = datetime.strptime(d0[:10], "%Y-%m-%d").date()
    b = datetime.strptime(d1[:10], "%Y-%m-%d").date()
    if b < a:
        a, b = b, a
    out = []
    cur = a
    while cur <= b:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _footfall_fallback_assignment(idx: int, ev: dict, allowed_dates: list[str]) -> dict:
    """无 DeepSeek 或解析失败时的粗规则：按运营商/文案关键字猜区域与日期。"""
    text = f"{ev.get('name','')} {ev.get('description','')} {ev.get('location','')}".lower()
    op = (ev.get("operator") or "").lower()
    nam_kw = ["新葡京", "老葡京", "葡京", "文华东方", "文化东方", "美高梅", "新濠锋", "永利澳门", "励宫", "回力", "外港", "南灣", "南湾"]
    cot_kw = ["威尼斯人", "伦敦人", "巴黎人", "美狮", "银河", "影汇", "路氹", "路环", "永利皇", "皇宫", "上葡京", "范思哲", "卡尔拉格斐"]
    region = "nam_van"
    if any(k in text for k in cot_kw) or op in ("sands", "galaxy", "melco") or (
        op == "wynn" and ("皇宫" in text or "皇宮" in text or "路氹" in text)
    ):
        region = "cotai"
    if any(k in text for k in nam_kw) and op in ("sjm", "mgm", "melco", "wynn"):
        region = "nam_van"
    mid = len(allowed_dates) // 2
    ds_pick = allowed_dates[mid] if allowed_dates else ""
    return {
        "id": str(idx),
        "region": region,
        "primary_venue": (ev.get("location") or "")[:80] or "—",
        "active_dates": [ds_pick] if ds_pick else [],
    }


def _footfall_ai_assign_top_events(
    events: list[dict],
    from_date: str,
    to_date: str,
    allowed_dates: list[str],
) -> list[dict]:
    if not os.getenv("DEEPSEEK_API_KEY"):
        return []
    lines = []
    for i, ev in enumerate(events):
        lines.append(
            f"[{i}] name={ev.get('name','')[:200]}\n"
            f"    operator={ev.get('operator','')}\n"
            f"    location={ev.get('location','')[:120]}\n"
            f"    date_hint={ev.get('date','')[:120]}\n"
            f"    heat={ev.get('heat_score')}\n"
            f"    desc={str(ev.get('description',''))[:400]}"
        )
    prompt = f"""你是澳门大型综合度假村活动与地理分析助手。用户查询日期范围：{from_date} 至 {to_date}（含首尾）。

{FOOTFALL_MACAU_VENUE_REFERENCE_ZH}

## 任务
下面最多 {len(events)} 个活动（已按热度优先列出）。请为每个活动判断：
1. **region**：活动主要发生所在统计分区，只能填 **cotai**（路氹填海区）或 **nam_van**（外港及南湾湖新填海区）。
2. **primary_venue**：活动举办场所，尽量用上面名单中的酒店/场所简称（如「澳门威尼斯人」）。
3. **active_dates**：该活动在上述查询范围内**实际举办**的公历日期列表，格式 YYYY-MM-DD。若活动跨多天，列出全部日期且必须 ⊆ 允许日期集合。
   - 允许日期集合（你必须只使用这些日期）：{json.dumps(allowed_dates, ensure_ascii=False)}
   - 若文案未写清日期，可根据 date_hint 推断；仍无法确定则取与 date_hint 最接近日的一条或该范围内中间一日（只能一条）。

只输出一个 JSON 对象，格式严格如下（不要 markdown）：
{{
  "assignments": [
    {{"id": "0", "region": "cotai", "primary_venue": "澳门威尼斯人", "active_dates": ["2026-03-15"]}}
  ]
}}
id 必须与下方 [0]..[{max(0, len(events)-1)}] 对应。"""
    prompt += "\n\n## 活动列表\n" + "\n".join(lines)
    try:
        resp = client.chat.completions.create(
            model=DEEPSEEK_JSON_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.15,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = _footfall_parse_json_object(raw)
        return list(data.get("assignments") or [])
    except Exception as e:
        print(f"⚠️ footfall AI assign failed: {e}")
        return []


def _footfall_allocate_visitors(
    events: list[dict],
    assignments: list[dict],
    zone_by_date: dict[str, dict],
    allowed_dates: set[str],
) -> dict:
    """按 (region, ds) 分组，同区同日多活动按 heat_score 比例分配该区当日预测客流。"""
    # id -> assignment
    by_id: dict = {}
    for a in assignments:
        i = str(a.get("id", "")).strip()
        if i:
            by_id[i] = a

    n = len(events)
    for idx in range(n):
        sid = str(idx)
        if sid not in by_id:
            by_id[sid] = _footfall_fallback_assignment(idx, events[idx], sorted(allowed_dates))

    # 规范化 active_dates
    for sid, a in list(by_id.items()):
        ads = a.get("active_dates") or []
        clean = []
        for x in ads if isinstance(ads, list) else []:
            s = str(x).strip()[:10]
            if len(s) == 10 and s[4] == "-" and s[7] == "-" and s in allowed_dates:
                clean.append(s)
        a["active_dates"] = sorted(set(clean))
        r = (a.get("region") or "").lower().strip()
        if r not in ("cotai", "nam_van"):
            a["region"] = "cotai"
        else:
            a["region"] = r

    alist = sorted(allowed_dates)
    for idx, ev in enumerate(events):
        sid = str(idx)
        a = by_id.get(sid, {})
        if not a.get("active_dates"):
            hint = str(ev.get("date") or "")
            pick = [d for d in re.findall(r"\d{4}-\d{2}-\d{2}", hint) if d in allowed_dates]
            if not pick and alist:
                pick = [alist[len(alist) // 2]]
            a["active_dates"] = pick
            by_id[sid] = a

    # (region, ds) -> list of (idx, heat)
    buckets: dict = defaultdict(list)
    for idx, ev in enumerate(events):
        a = by_id.get(str(idx), {})
        region = a.get("region") or "cotai"
        heat = float(ev.get("heat_score") or 0) or 0.1
        for ds in a.get("active_dates") or []:
            if ds not in zone_by_date:
                continue
            buckets[(region, ds)].append((idx, heat))

    per_event_daily: dict = defaultdict(list)
    totals: dict = defaultdict(float)

    for (region, ds), lst in buckets.items():
        z = zone_by_date.get(ds) or {}
        ztot = float(z.get("cotai" if region == "cotai" else "nam_van") or 0)
        sh = sum(max(h, 0.1) for _, h in lst)
        for idx, h in lst:
            share = ztot * (max(h, 0.1) / sh)
            per_event_daily[idx].append(
                {
                    "ds": ds,
                    "visitors": round(share, 1),
                    "zone_total_that_day": round(ztot, 1),
                    "region": region,
                }
            )
            totals[idx] += share

    out_by_key: dict = {}
    for idx, ev in enumerate(events):
        key = (ev.get("key") or "").strip() or f"{ev.get('name','')}|{ev.get('operator','')}"
        days = per_event_daily.get(idx) or []
        tot = totals.get(idx, 0)
        avg = (tot / len(days)) if days else 0
        a = by_id.get(str(idx), {})
        out_by_key[key] = {
            "region": a.get("region"),
            "primary_venue": a.get("primary_venue") or "—",
            "daily": days,
            "total_visitors": round(tot, 1),
            "avg_daily_visitors": round(avg, 1),
            "days_count": len(days),
        }
    return out_by_key


@app.get("/api/footfall-predict")
async def api_footfall_predict(ds: str, ai: bool = True):
    """单日全澳 Prophet + 两区拆分（5 个 0/1 可由 DeepSeek + finaldata1 连续变量）。"""
    load_dotenv()
    model_path = Path(os.getenv("FOOTFALL_MODEL_PATH", str(BRIDGE_ROOT / "footfall" / "fitted_prophet.joblib")))
    zone_csv = Path(os.getenv("FOOTFALL_ZONE_CSV", str(BRIDGE_ROOT / "footfall" / "zone_table1_monthly_share.csv")))
    ds = (ds or "").strip()[:10]
    if len(ds) < 10 or ds[4] != "-" or ds[7] != "-":
        raise HTTPException(status_code=400, detail="ds 須為 YYYY-MM-DD")
    if not model_path.is_file():
        return {
            "ok": False,
            "error": "model_not_found",
            "message": "未找到 Prophet 模型文件",
            "model_path": str(model_path.resolve()),
        }
    if not zone_csv.is_file():
        return {
            "ok": False,
            "error": "zone_csv_not_found",
            "message": "未找到 zone_table1_monthly_share.csv",
            "zone_csv": str(zone_csv.resolve()),
        }
    footfall_dir = BRIDGE_ROOT / "footfall"
    try:
        row = _footfall_predict_row_for_date(
            ds,
            model_path=model_path,
            zone_csv=zone_csv,
            footfall_dir=footfall_dir,
            fast=not ai,
        )
    except Exception as e:
        return {"ok": False, "error": "predict_failed", "message": str(e)}
    return {"ok": True, **row}


@app.post("/api/footfall-event-allocate")
async def api_footfall_event_allocate(payload: dict):
    """
    对热度 Top 活动：Prophet 给出每日两区客流，DeepSeek 判定分区/场地/日期后，按热度在同区同日拆分。
    payload: {{
      "from_date": "2026-03-01",
      "to_date": "2026-03-31",
      "events": [ {{"key", "name", "description", "location", "operator", "heat_score", "date"}} ]
    }}
    """
    load_dotenv()
    from_date = (payload.get("from_date") or "").strip()[:10]
    to_date = (payload.get("to_date") or "").strip()[:10]
    events_in = payload.get("events") or []
    if not events_in or not from_date or not to_date:
        return {"ok": False, "message": "需要 from_date、to_date 与 events"}

    model_path = Path(os.getenv("FOOTFALL_MODEL_PATH", str(BRIDGE_ROOT / "footfall" / "fitted_prophet.joblib")))
    zone_csv = Path(os.getenv("FOOTFALL_ZONE_CSV", str(BRIDGE_ROOT / "footfall" / "zone_table1_monthly_share.csv")))
    if not model_path.is_file() or not zone_csv.is_file():
        return {"ok": False, "message": "缺少 fitted_prophet.joblib 或 zone CSV"}

    allowed = _footfall_enumerate_dates(from_date, to_date)
    if len(allowed) > 150:
        return {"ok": False, "message": "日期范围过长（最多 150 天）"}
    allowed_set = set(allowed)

    footfall_dir = BRIDGE_ROOT / "footfall"
    # 默認 fast：逐日 CSV 连续变量 + Prophet，不调 DeepSeek。
    # FAST=0：每个自然日各问一次 EXCHANGE/PRICE + 五个 0/1；同日多活动共用 zone_by_date[ds]（按日只算一次）。
    _alloc_fast = os.getenv("FOOTFALL_ALLOCATE_FAST", "1").strip().lower() not in ("0", "false", "no")
    print(
        f"[footfall-allocate] 计算 {len(allowed)} 天 zone 客流（Prophet；FAST={_alloc_fast}；"
        f"按日 EXCHANGE/PRICE={'CSV' if _alloc_fast else 'AI+CSV 回退'}）…"
    )
    zone_by_date: dict = {}
    for i, ds in enumerate(allowed):
        try:
            if (i + 1) % 7 == 1 or i == 0 or i == len(allowed) - 1:
                print(f"[footfall-allocate]   … {i + 1}/{len(allowed)} {ds}")
            row = _footfall_predict_row_for_date(
                ds,
                model_path=model_path,
                zone_csv=zone_csv,
                footfall_dir=footfall_dir,
                fast=_alloc_fast,
                continuous_override=None,
                no_per_day_continuous_ai=False,
            )
            zone_by_date[ds] = {
                "macau_total": row["macau_total"],
                "cotai": row["zone_cotai"],
                "nam_van": row["zone_namvan"],
            }
        except Exception as e:
            print(f"⚠️ footfall zone row {ds}: {e}")
            zone_by_date[ds] = {"macau_total": 0.0, "cotai": 0.0, "nam_van": 0.0}
    print("[footfall-allocate] zone 完成 → DeepSeek 活动分区/拆分")

    events = events_in[:10]
    for ev in events:
        if not (ev.get("key") or "").strip():
            ev["key"] = f"{(ev.get('name') or '').strip()}|{(ev.get('operator') or '').lower()}"

    assigns = _footfall_ai_assign_top_events(events, from_date, to_date, allowed)
    if not assigns:
        assigns = [_footfall_fallback_assignment(i, events[i], allowed) for i in range(len(events))]

    by_key = _footfall_allocate_visitors(events, assigns, zone_by_date, allowed_set)
    return {
        "ok": True,
        "by_key": by_key,
        "zone_totals_by_date": zone_by_date,
        "labels": {"cotai": "路氹填海區", "nam_van": "外港及南灣湖新填海區"},
    }


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

        # ── Cache check：同一 operator + 日期範圍唔重複 call DeepSeek ──────────
        _cached_acts = _get_analysis_cache(op_key, from_date, to_date, keyword)
        if _cached_acts is not None:
            print(f"💾 [{op_key}] 用 cache（{len(_cached_acts)} 個活動），跳過 DeepSeek")
            all_summaries[op_key] = _cached_acts
            for act in _cached_acts:
                n  = act.get("name", "").strip()
                nk = _norm_name(n)
                if n and nk not in globally_extracted:
                    globally_extracted[nk] = act
            continue
        # ── End cache check ───────────────────────────────────────────────────

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
                        
                    # ✏️ 新增：跳過純 roundup row
                    # 判斷：src_ids 只得自己一條，同埋 event_id 唔係 __act 衍生
                    # 即係呢個 deduped row 係直接以一條 roundup post 為代表，唔係拆分出嚟嘅活動
                    if (pid
                            and "__act" not in str(pid)
                            and len(src_ids) == 1
                            and src_ids[0] == pid
                            and p.get("source_count", 1) == 1):
                        # 再檢查佢係咪 roundup（extracted_events > 1 個活動）
                        _is_r = False
                        try:
                            _plat = pid.split("_")[0]
                            _conn_r = __import__('sqlite3').connect(
                                __import__('os').getenv("DB_PATH", "macau_analytics.db"))
                            _row_r = _conn_r.execute(
                                f"SELECT extracted_events FROM posts_{_plat} WHERE post_id=?",
                                (pid,)
                            ).fetchone()
                            _conn_r.close()
                            if _row_r and _row_r[0]:
                                _evts = __import__('json').loads(_row_r[0])
                                _is_r = isinstance(_evts, list) and len(_evts) > 1
                        except Exception:
                            pass
                        if _is_r:
                            continue  # 跳過呢個 roundup row，唔喂 DeepSeek
                    groups.append({
                        "event_ids": [pid] if pid else [],
                        "post_ids": src_ids if src_ids else ([pid] if pid else []),
                    })
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
                    groups.append({
                        "event_ids": list(group),
                        "post_ids": list(group),
                    })
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
            for group_info in groups:
                group_pids = list(group_info.get("post_ids") or [])
                group_event_ids = list(group_info.get("event_ids") or group_pids)
                # 組合呢組嘅所有帖文內容
                snippets = []
                group_post_ids = []
                for i, pid in enumerate(group_pids):
                    p = post_obj.get(pid)
                    if p is None:
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

合併規則（重要）：
- 同一個主題活動嘅不同子內容（例如主題燈光show、配套SPA、限定甜品、應援小卡、打卡攻略），只算**一個活動**，唔好拆開
- 以最大範圍嘅活動名稱及日期代表整個活動
- 唔同帖文講同一活動嘅唔同細節，都算同一個活動

任務：
1. 睇晒所有帖文，識別每一個獨立活動（唔同帖文講同一活動只算一個）
2. 以下活動已提取，**概念上相同嘅唔需要重複**（即使名稱唔同、語言唔同、描述角度唔同，只要係同一個活動就唔輸出）。特別注意：如果新活動與已提取活動係**同一地點、同一時期、同一主題**，即使名稱唔同都視為同一活動，唔好重複輸出：
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
                        model=DEEPSEEK_JSON_MODEL,
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
                        for geid in group_event_ids:
                            if geid not in existing.get("source_event_ids", []):
                                existing.setdefault("source_event_ids", []).append(geid)
                        print(f"🔀 跨 operator 重複，merge: {item_name}")
                        continue
                    # 用 source_indices 反查實際相關嘅 post IDs
                    raw_indices = item.get("source_indices") or []
                    if raw_indices:
                        # source_indices 係 1-based，對應 snippets 入面嘅帖文順序
                        # group_post_ids 係跟 snippets 同一順序 append 落去嘅
                        relevant_post_ids = []
                        for idx in raw_indices:
                            actual_idx = int(idx) - 1  # 轉 0-based
                            if 0 <= actual_idx < len(group_post_ids):
                                relevant_post_ids.append(group_post_ids[actual_idx])
                        # fallback：如果 indices 解析唔到，用全部
                        if not relevant_post_ids:
                            relevant_post_ids = group_post_ids
                    else:
                        relevant_post_ids = group_post_ids

                    # 用 relevant_post_ids 過濾 source_event_ids
                    relevant_event_ids = [
                        eid for eid in group_event_ids
                        if any(eid == pid or eid.startswith(pid + "__act") for pid in relevant_post_ids)
                    ]
                    if not relevant_event_ids:
                        relevant_event_ids = group_event_ids
                    new_act = {
                        "name":             item_name,
                        "description":      item_desc or "暫無描述",
                        "date":             _resolve_overlapping_dates(item_date, post_text=all_snippets_text, activity_name=item_name),
                        "location":         item.get("location") or "",
                        "category":         cat_out,
                        "sub_type":         sub,
                        "source_post_ids":  relevant_post_ids,
                        "source_event_ids": list(dict.fromkeys(relevant_event_ids)),
                    }
                    activities.append(new_act)
                    if item_name:
                        globally_extracted[_norm_name(item_name)] = new_act

        all_summaries[op_key] = activities
        # ── Save to cache ────────────────────────────────────────────────────
        _set_analysis_cache(op_key, from_date, to_date, activities, keyword)
        # ── End cache save ───────────────────────────────────────────────────
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
            # post_id in all tables uses full prefixed format: xhs_xxx, fb_xxx, etc.
            # source_post_ids also uses same format — query directly, no stripping needed
            all_ids_list = list(all_source_ids)
            for table, platform_name in [
                ("posts_xhs", "xhs"), ("posts_ig", "ig"),
                ("posts_fb", "fb"),   ("posts_weibo", "weibo"),
            ]:
                try:
                    placeholders = ",".join("?" * len(all_ids_list))
                    _cur.execute(f"""
                        SELECT post_id, published_at, post_url,
                               substr(content, 1, 500) AS title_preview,
                               extracted_events
                        FROM {table}
                        WHERE post_id IN ({placeholders})
                    """, all_ids_list)
                    for pid, pub_at, post_url, title_preview, extracted_events_raw in _cur.fetchall():
                        # 判斷係咪 roundup post（提及多於一個活動）
                        is_roundup = False
                        try:
                            evts = json.loads(extracted_events_raw or "[]")
                            is_roundup = isinstance(evts, list) and len(evts) > 1
                        except Exception:
                            pass
                        post_details[pid] = {
                            "post_id":      pid,
                            "platform":     platform_name,
                            "url":          post_url or "",
                            "title":        title_preview or "",
                            "published_at": str(pub_at or "")[:10],
                            "is_roundup":   is_roundup,
                        }
                except _sq2.OperationalError:
                    pass
            _conn.close()
            print(f"  📎 source_posts: {len(all_ids_list)} ids queried → {len(post_details)} found")
            print(f"  🔍 post_details keys sample: {list(post_details.keys())[:5]}")
            print(f"  🔍 ig_3841758428616911734 in post_details: {'ig_3841758428616911734' in post_details}")
        except Exception as e:
            print(f"⚠️ source_posts 補充失敗（唔影響主結果）: {e}")

    for acts in cat_summaries.values():
        for act in acts:
            ids = act.get("source_post_ids") or []
            # 過濾掉 roundup posts（提及多個活動），只顯示專屬呢個 event 嘅 posts
            dedicated = [post_details[i] for i in ids if i in post_details and not post_details[i].get("is_roundup")]
            # 如果全部都係 roundup（即冇專屬 posts），fallback 顯示全部
            act["source_posts"] = dedicated if dedicated else [post_details[i] for i in ids if i in post_details]
            print(f"  📎 '{act.get('name','')}': {len(ids)} source_ids → {len(act['source_posts'])} matched posts ({len(ids)-len(dedicated)} roundup filtered)")

    # ── Attach heat_score to every AI activity (aggregate from events_deduped) ─
    # source_event_ids stores events_deduped.event_id values (one deduped group = one row).
    # source_post_ids stays as raw platform post ids for link/details display.
    # For each AI activity we compute a weighted-average heat score across its groups,
    # weighted by source_count (more source posts → more representative).
    try:
        _hconn = _heat_db_conn()
        _all_ev_ids: set = set()
        for _acts in cat_summaries.values():
            for _act in _acts:
                for _sid in (_act.get("source_event_ids") or _act.get("source_post_ids") or []):
                    _all_ev_ids.add(_sid)

        _heat_map: dict = {}  # event_id → {heat_score, decay_factor, newest_post, platforms, source_count}
        if _all_ev_ids:
            _ph = ",".join(["?"] * len(_all_ev_ids))
            for _row in _hconn.execute(
                f"SELECT event_id, heat_score, heat_meta, source_count FROM events_deduped WHERE event_id IN ({_ph})",
                list(_all_ev_ids)
            ).fetchall():
                _eid, _hs, _hm_json, _sc = _row
                _hm = {}
                try: _hm = json.loads(_hm_json or "{}")
                except Exception: pass
                _heat_map[_eid] = {
                    "heat_score":   float(_hs or 0),
                    "decay_factor": float(_hm.get("decay_factor", 1.0)),
                    "newest_post":  _hm.get("newest_post", ""),
                    "platforms":    _hm.get("platforms", []),
                    "source_count": _sc or 1,
                }
        _hconn.close()

        def _agg_heat(act):
            heat_ids = act.get("source_event_ids") or act.get("source_post_ids") or []
            rows = [_heat_map[i] for i in heat_ids if i in _heat_map]
            if not rows: return None
            total_sc  = sum(r["source_count"] for r in rows)
            w_heat    = sum(r["heat_score"] * r["source_count"] for r in rows) / max(total_sc, 1)
            best_decay = max(r["decay_factor"] for r in rows)
            newest    = max((r["newest_post"] for r in rows if r["newest_post"]), default="")
            platforms = sorted({p for r in rows for p in r["platforms"]})
            return {
                "heat_score":    round(w_heat, 1),
                "decay_factor":  round(best_decay, 4),
                "newest_post":   newest,
                "platforms":     platforms,
                "platform_count":len(platforms),
                "source_count":  sum(r["source_count"] for r in rows),
            }

        for _acts in cat_summaries.values():
            for _act in _acts:
                _h = _agg_heat(_act)
                if _h:
                    _act.update(_h)
                else:
                    _act.setdefault("heat_score", None)

        # Sort each category by heat_score descending
        for _cat_key in cat_summaries:
            cat_summaries[_cat_key].sort(key=lambda a: (a.get("heat_score") or 0), reverse=True)

        print("✅ heat_score attached to all AI activities")
    except Exception as _he:
        print(f"⚠️ heat_score attach failed (non-fatal): {_he}")

    return {
        "status":             "success",
        "operator_summaries": all_summaries,
        "category_summaries": dict(cat_summaries),
    }

@app.post("/api/hot-themes")
async def hot_themes(payload: dict):
    """
    接收 event names + descriptions + heat_score，用 DeepSeek 返回 2-3 個 semantic hot themes，
    並帶回對應活動序號，方便前端 click filter。
    payload: { "events": [ {"name": "...", "description": "...", "heat_score": 87.5}, ... ] }
    """
    events = payload.get("events", [])
    if not events:
        return {"themes": []}

    lines = []
    for i, ev in enumerate(events[:200], 1):
        name = (ev.get("name") or "").strip()
        desc = (ev.get("description") or ev.get("desc") or "").strip()[:80]
        heat = ev.get("heat_score")
        try:
            heat_num = float(heat)
        except Exception:
            heat_num = None
        if name:
            heat_prefix = f"[heat {heat_num:.1f}] " if heat_num is not None else ""
            lines.append(f"{i}. {heat_prefix}{name}{'：' + desc if desc else ''}")

    if not lines:
        return {"themes": []}

    event_list = "\n".join(lines)
    prompt = f"""以下係澳門各博企近期嘅活動列表：

{event_list}

以上列表已經大致按 heat score 由高到低排列，heat 越高代表越值得優先參考。

請分析以上活動，識別出 2-3 個最突出嘅市場主題（hot themes）。
主題應該係具體嘅概念，例如「韓星演唱會」、「葡萄酒品鑑」、「沉浸式體驗」、「非遺文化」，而唔係籠統嘅字眼如「活動」、「體驗」、「娛樂」。
每個主題用 3-8 個字表達，繁體中文。
每個主題要帶返對應活動序號 indices，方便前端點擊後篩選相關活動。

【最重要：必須以 heat score 高嘅活動為主】
- Hot themes 代表市場上最受關注嘅趨勢，因此 theme 嘅選擇必須以列表最前面（heat 最高）嘅活動為主
- 如果某個 theme 裡面嘅活動 heat 都偏低（例如全部都係 30 分以下），呢個唔係熱門主題，唔應該選佢
- 每個 theme 嘅 indices 所對應活動嘅平均 heat 要盡量高

【重要：indices 只能包含真正屬於該主題嘅活動】
- 每個 index 必須係該 theme 嘅直接相關活動，唔相關嘅一律唔包含
- 如果唔確定某個活動係咪屬於某個 theme，唔好包含，寧少勿錯

【國籍主題規則——極嚴格】
- 如果 theme 係關於某個國家/地區嘅藝人（例如「韓流演唱會」「韓團見面會」），indices 裡面每一個活動都必須明確係嗰個國家嘅藝人，缺乏明確國籍線索嘅活動一律排除
- 韓國藝人線索：aespa、EXO、GOT7、MARK段宜恩、BLACKPINK、BTS、FANCON、FANMEETING、韓團、韓星、K-pop、UPPOOM 等
- 香港藝人（唔可以歸入韓流主題）：Anson Lo、Edan、張天賦、TYSON YOSHI、Kiri T、炎明熹、姜濤、Mirror、MUSIC UNBOUNDED LIVE MACAU 等香港歌手/組合或相關活動
- 台灣藝人（唔可以歸入韓流主題）：周杰倫、五月天、張惠妹等台灣藝人
- 只有活動名或描述中有明確韓國藝人名、韓語、K-pop、韓團相關詞，先可以入韓流主題

只返回 JSON array，例如：
[
  {{"theme":"韓流演唱會熱潮","indices":[1,2,4]}},
  {{"theme":"澳門當代藝術推廣","indices":[3,6]}},
  {{"theme":"經典音樂會重溫","indices":[5,7]}}
]
唔需要任何解釋，只輸出 JSON array。"""

    try:
        resp = client.chat.completions.create(
            model=DEEPSEEK_JSON_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
        themes = json.loads(raw)
        if not isinstance(themes, list):
            themes = []
        try:
            from trad_simp import to_trad as _to_trad
            normalized = []
            for item in themes[:3]:
                if isinstance(item, str):
                    normalized.append({"theme": _to_trad(item), "indices": []})
                    continue
                if not isinstance(item, dict):
                    continue
                theme = _to_trad(str(item.get("theme") or "").strip())
                indices = item.get("indices") or []
                if not theme:
                    continue
                normalized.append({
                    "theme": theme,
                    "indices": [int(i) for i in indices if str(i).isdigit()]
                })
            themes = normalized
        except Exception:
            themes = themes[:3]
        print(f"🔥 Hot themes: {themes}")
        return {"themes": themes}
    except Exception as e:
        print(f"⚠️ hot_themes 出錯: {e}")
        return {"themes": []}


@app.post("/api/wynn-recommendations")
async def wynn_recommendations(payload: dict):
    categories = payload.get("categories") or []
    selected_operators = payload.get("selected_operators") or []
    strengths = payload.get("strengths") or []
    improvements = payload.get("improvements") or []
    peak_gaps = payload.get("peak_gaps") or []
    if not categories:
        return {"recommendations": []}

    def _fmt_heat(v):
        try:
            return f"{float(v):.1f}"
        except Exception:
            return "0.0"

    # Per-category competitor structural insight — injected inline into each block
    CAT_COMPETITOR_INSIGHT = {
        "concert":       "🎤 Concert策略：Wynn場地1,200座限制，2028年前無法辦大型演唱會，唔應該投入資源追大型演唱會。但Wynn heat領先（如有）屬小型精品優勢。建議方向：mark低目前市場上高熱度歌手，積極建立關係，2028年新場館落成後優先邀請。Galaxy銀河2024年辦460場，係結構性霸主，無需正面競爭。type應為keep_up（若Wynn heat領先）。",
        "food":          "✅ 餐飲係Wynn絕對優勢：2025年米芝蓮5粒星（Wing Lei兩星16年連獎、Chef Tam's Seasons澳門唯一兩星、Mizumi一星），Forbes全球最多五星餐廳。最需警惕係Melco（Jade Dragon三星）。若Wynn領先，type=keep_up，深化Fine Dining+會員禮遇。",
        "accommodation": "✅ 住宿係Wynn絕對優勢：Forbes 63粒星澳門最多，永利皇宮連3年全球最大Forbes五星度假村。type=keep_up，深化個人化禮賓服務差異化。",
        "experience":    "⚠️ Experience唔係Wynn最強項。MGM有《澳門2049》張藝謀駐場+The Spectacle，Melco有水舞間。Wynn有表演湖+SkyCab但規模較小。若Wynn heat落後，type=avoid，唔需要集中資源喺呢個category。",
        "exhibition":    "⚠️ Exhibition唔係Wynn強項。MGM有保利藝術博物館，Sands有藝術展覽。若Wynn heat明顯落後或count=0，type=avoid，建議Wynn唔需要focus呢個category，集中資源喺餐飲、住宿等強項。",
        "sport":         "⚠️ Sport結構性限制：Galaxy有UFC（16,000座），Sands有NBA季前賽（14,000座），Melco heat最高。Wynn場地唔夠，type=avoid，唔需要追大型體育IP。",
        "crossover":     "Melco跨界最強（heat 68.3），Wynn可走奢侈品+Fine Dining+藝術的高端crossover。若Wynn落後，type=improve。",
        "shopping":      "✅ Wynn名店街頂級品牌有優勢（heat 56.4）。Galaxy零售規模最大但Wynn走奢侈品精品路線，方向不同。type=keep_up，深化會員專屬購物體驗。",
        "gaming":        "Wynn gaming定位premium mass+VIP，heat領先。type=keep_up，配合住宿+餐飲禮遇作轉化。",
        "government":    "政府活動代表城市級文化節慶，熱度往往較高（如澳門美食之都嘉年華、格蘭披治大賽車等）。若Government活動熱度高於Wynn，建議Wynn參考該活動的策展主題或形式，以奢華包裝呼應政府文化方向，爭取成為官方合作博企夥伴。",
    }

    blocks = []
    for idx, cat in enumerate(categories[:12], 1):
        label = str(cat.get("label") or cat.get("catKey") or "").strip()
        cat_key = str(cat.get("catKey") or "").strip()
        wynn = cat.get("wynn") or {}
        top_opp = cat.get("topOpponent") or {}
        opponents = cat.get("opponents") or []
        events = cat.get("events") or []

        def _fmt_event(ev):
            if not isinstance(ev, dict):
                return ""
            name = str(ev.get("name") or "").strip()
            operator = str(ev.get("operator") or "").strip()
            desc = str(ev.get("description") or "").strip()[:80]
            if not name:
                return ""
            return f"- {operator} | {name} | heat {_fmt_heat(ev.get('heat'))}" + (f" | {desc}" if desc else "")

        opponent_lines = [
            f"- {opp.get('opTitle')}: count {opp.get('count', 0)}, avg heat {_fmt_heat(opp.get('avg'))}"
            for opp in opponents[:3]
        ]
        event_lines = [_fmt_event(ev) for ev in events[:6]]
        event_lines = [line for line in event_lines if line]
        wynn_top = wynn.get("topEvent") or {}
        opp_top = top_opp.get("topEvent") or {}
        cat_insight = CAT_COMPETITOR_INSIGHT.get(cat_key, "")

        blocks.append(
            f"""[{idx}] {label} ({cat_key})
{f"策略洞察：{cat_insight}" if cat_insight else ""}
Wynn:
- count {wynn.get('count', 0)}, avg heat {_fmt_heat(wynn.get('avg'))}
""" + (
                f"- top event: {wynn_top.get('name', '')} | heat {_fmt_heat(wynn_top.get('heat'))}\n"
                if wynn_top else ""
            ) + (
                "Opponents:\n" + ("\n".join(opponent_lines) if opponent_lines else "- none") + "\n"
            ) + (
                f"- opponent top event: {opp_top.get('name', '')} | heat {_fmt_heat(opp_top.get('heat'))}\n"
                if opp_top else ""
            ) + (
                "Relevant events:\n" + ("\n".join(event_lines) if event_lines else "- none")
            )
        )

    n_cats = len(categories[:12])
    n_recs = min(n_cats, 3)  # 1-2 categories → same count; 3+ categories → 3

    prompt = f"""你係 Wynn 嘅策略顧問。根據以下市場數據，從已選 categories 中選出最重要的【{n_recs} 個】，各出一條精準建議。

目前對比 organiser:
{", ".join(str(op) for op in selected_operators) if selected_operators else "Wynn only"}

已確立嘅前置結論（建議不可同以下內容矛盾）：
第1點 Wynn 優勢：
{json.dumps(strengths, ensure_ascii=False)}

第2點 Wynn 改進空間：
{json.dumps(improvements, ensure_ascii=False)}

以下係每個 category 嘅對比資料（已附策略洞察）：

{chr(10).join(blocks)}

選擇原則（必須遵守）：
- 只可從以上已列出的 categories 中選，絕對不可加入未列出的 category
- 優先選 Wynn 有真實優勢嘅 category（food、accommodation、shopping 等），type=keep_up
- Concert 若 Wynn heat 領先：type=keep_up，建議積極與市場上單場熱度達60分以上的藝人建立長線合作關係，並列舉數個來自 Relevant events 中熱度達60+ 的具體活動名作例子（格式：如「張天賦永利音樂會」🔥70.6），待2028年新場館落成後優先邀請
- Exhibition 或 Experience 若對手熱度明顯高於 Wynn（差距 ≥ 10分）：type=avoid，建議 Wynn 將資源投放至自身強項（F&B、住宿、購物），而非「避免集中資源」——措辭應積極，例如「建議將資源集中於永利的核心優勢」
- 若 Wynn 與對手熱度相近（差距 < 10分），不應用 avoid，改用 improve 並給出具體提升方向
- Government 的活動必須納入對比：若某 category 中 Government 的活動熱度高於 Wynn，應在建議中提及可參考政府活動的主題或形式
- 選出【剛好 {n_recs} 個】最具策略價值嘅 category

輸出要求：
1. 只返回【剛好 {n_recs} 個】建議，不多不少
2. 每條建議1-2句，書面繁體中文，有具體行動
3. 建議內容只講 Wynn 應該做咩；Concert 類別可引用 Relevant events 中的 Wynn 活動名作例子；其他對手活動禁止點名，但可提及熱度數字
4. type 字段：keep_up / improve / avoid
5. avoid 類別的措辭必須前後一致，格式為「永利毋須大力投入 [該類別]，但可輕量參考 [具體方向]，以奢華包裝試行，將資源重心保持在 [核心強項]」；禁止在同一句先說「避免集中資源」再說「作輕量嘗試」——兩者矛盾
6. 絕對禁止出現「count=0」、「heat 0」、「熱度偏低 0.0」等技術性表達
7. 若 Wynn 在某 category 沒有活動，用自然書面語：「永利目前並未舉辦任何 Exhibition 活動」
8. 描述領先關係時，用「進一步拉開與對手的差距」或「持續擴大優勢」，禁止使用「鞏固對 X 的領先地位」

請只返回 JSON array（{n_recs} 項）：
[
  {{"catKey":"food","type":"keep_up","text":"建議永利延續米芝蓮Fine Dining優勢，推出Chef Tam's Seasons主廚晚宴配合Wynn Insider會員專屬預覽禮遇。"}},
  {{"catKey":"concert","type":"keep_up","text":"建議永利延續精品演唱會路線，同時積極與市場上單場熱度達60分以上的藝人建立長線合作關係，待2028年新場館落成後優先邀請。"}}
]
唔需要任何解釋。"""

    try:
        resp = QWEN_RECOMMENDATION_CLIENT.chat.completions.create(
            model=QWEN_RECOMMENDATION_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=900,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
        try:
            recs = json.loads(raw)
        except Exception:
            match = re.search(r'(\[\s*\{.*\}\s*\])', raw, re.S)
            if match:
                recs = json.loads(match.group(1))
            else:
                print(f"⚠️ wynn_recommendations raw parse failed: {raw[:500]}")
                raise
        if not isinstance(recs, list):
            recs = []
        try:
            from trad_simp import to_trad as _to_trad
            selected_cat_keys = {str(c.get("catKey") or "").strip().lower() for c in categories}
            normalized = []
            for item in recs:
                if not isinstance(item, dict):
                    continue
                cat_key = str(item.get("catKey") or "").strip().lower()
                if cat_key not in selected_cat_keys:
                    continue
                text = _to_trad(str(item.get("text") or "").strip())
                rec_type = str(item.get("type") or "improve").strip().lower()
                if cat_key and text:
                    normalized.append({"catKey": cat_key, "type": rec_type, "text": text})
            recs = normalized[:n_recs]
        except Exception:
            recs = [item for item in recs if isinstance(item, dict)][:n_recs]
        return {"recommendations": recs}
    except Exception as e:
        print(f"⚠️ wynn_recommendations 出錯: {e}")
        return {"recommendations": []}

@app.get("/api/report-insights")
async def get_report_insights(from_date: str = "", to_date: str = "", keyword: str = ""):
    if not from_date or not to_date:
        return {"status": "error", "message": "from_date and to_date are required"}
    data = _get_report_insights(from_date, to_date, keyword)
    if not data:
        return {"status": "not_found", "hotThemes": [], "aiRecommendations": [], "comparisons": []}
    return {"status": "success", **data}

@app.post("/api/report-insights")
async def save_report_insights(payload: dict):
    from_date = str(payload.get("from_date") or "").strip()
    to_date = str(payload.get("to_date") or "").strip()
    keyword = str(payload.get("keyword") or "").strip()
    if not from_date or not to_date:
        return {"status": "error", "message": "from_date and to_date are required"}
    hot_themes = payload.get("hotThemes") or []
    recommendations = payload.get("aiRecommendations") or []
    comparisons = payload.get("comparisons") or []
    try:
        _set_report_insights(from_date, to_date, keyword, hot_themes, recommendations, comparisons)
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@app.post("/api/analysis-cache/invalidate")
async def invalidate_analysis_cache(operator: str = ""):
    """db_manager 入庫後 call 呢個清 cache"""
    _invalidate_analysis_cache(operator or None)
    return {"success": True, "operator": operator or "all"}

@app.get("/api/archive/months")
async def get_archive_months():
    """返回所有 is_complete=1 嘅月份列表，供 Archive Report 用"""
    try:
        conn = _heat_db_conn()
        rows = conn.execute(
            "SELECT DISTINCT cache_key FROM analysis_cache WHERE is_complete = 1"
        ).fetchall()
        conn.close()
        months = set()
        for (key,) in rows:
            parts = key.split("|")
            if len(parts) >= 3:
                months.add(parts[1][:7])  # 抽出 "2025-02"
        return {"months": sorted(months, reverse=True)}
    except Exception as e:
        return {"months": [], "error": str(e)}

@app.get("/api/archive/report")
async def get_archive_report(month: str = "", from_date: str = "", to_date: str = "", operators: str = ""):
    """
    month = '2025-02'
    operators = 'wynn,sands' （唔傳就返全部）
    優先讀 period_cache（Panel Apply 後存落嘅完整數據），
    冇 cache 先 fallback 去 events_deduped / analysis_cache
    """
    try:
        from datetime import date, timedelta
        from calendar import monthrange
        if from_date and to_date:
            from_str = from_date
            to_str = to_date
            if not month:
                month = from_date[:7]
        else:
            year, mon = int(month[:4]), int(month[5:7])
            from_str = f"{year}-{mon:02d}-01"
            last_day = monthrange(year, mon)[1]
            to_str = f"{year}-{mon:02d}-{last_day:02d}"

        target_ops = [o.strip() for o in operators.split(",") if o.strip()] or ['wynn','sands','galaxy','mgm','melco','sjm','government']

        # ── 優先讀 period_cache（Panel Apply 後存落嘅完整數據）──────────────
        period_data = _get_period_cache(from_str, to_str)
        cached_events   = (period_data or {}).get("events",       {})
        cached_heatmap  = (period_data or {}).get("heatmap_data", {})

        if cached_events:
            # cached_events 係 by-category 格式 {catKey: [{name, organiserKey, ...}]}
            # archived_report.html 期望 by-operator 格式 {opKey: [{name, category, ...}]}
            # 需要轉換
            by_op = {}
            for cat_key, acts in cached_events.items():
                for act in (acts or []):
                    op_key = (act.get("organiserKey") or act.get("operator") or "").lower()
                    if not op_key:
                        continue
                    if op_key not in by_op:
                        by_op[op_key] = []
                    # 確保 category field 存在
                    act_copy = dict(act)
                    if not act_copy.get("category"):
                        act_copy["category"] = cat_key
                    by_op[op_key].append(act_copy)

            print(f"✅ archive/report: 用 period_cache 數據 ({from_str}~{to_str}), ops={list(by_op.keys())}")
            return {
                "status":  "success",
                "data":    by_op,
                "period":  period_data,
                "start":   from_str,
                "end":     to_str,
                "heatmap": cached_heatmap,
                "month":   month,
                "source":  "period_cache"
            }

        # ── Fallback：period_cache 冇數據，舊方式讀 analysis_cache + events_deduped ──
        print(f"⚠️ archive/report: period_cache 冇數據，fallback 去 analysis_cache ({from_str}~{to_str})")
        results = {}
        missing = []
        for op in target_ops:
            conn = _heat_db_conn()
            row = conn.execute(
                "SELECT activities FROM analysis_cache WHERE cache_key=? AND is_complete=1",
                (f"{op}|{from_str}|{to_str}",)
            ).fetchone()
            conn.close()
            if row:
                results[op] = json.loads(row[0])
            else:
                missing.append(op)
        # 喺 results 計完之後，讀 period_cache
        period_data = _get_period_cache(from_str, to_str)

        # ── 從 events_deduped 查真實 heatmap（heat_score + event count）──────
        heatmap = {}
        try:
            conn = _heat_db_conn()
            ent_subs = {"concert", "sport", "crossover"}
            rows = conn.execute("""
                SELECT operator, category, sub_type,
                       COUNT(*) as n,
                       AVG(CASE WHEN heat_score IS NOT NULL THEN heat_score END) as avg_heat
                FROM events_deduped
                WHERE published_at >= ? AND published_at <= ?
                  AND operator IN ({})
                GROUP BY operator, category, sub_type
            """.format(",".join(["?"] * len(target_ops))),
                [from_str + " 00:00:00", to_str + " 23:59:59"] + target_ops
            ).fetchall()
            conn.close()
            for row in rows:
                op, cat, sub, n, avg_h = row
                # entertainment → use sub_type as key
                cat_key = sub if (cat == "entertainment" and sub in ent_subs) else cat
                if not cat_key:
                    continue
                if cat_key not in heatmap:
                    heatmap[cat_key] = {}
                if op not in heatmap[cat_key]:
                    heatmap[cat_key][op] = {"n": 0, "h": 0}
                heatmap[cat_key][op]["n"] += n
                heatmap[cat_key][op]["h"] = round(avg_h, 1) if avg_h else 0
        except Exception as he:
            print(f"⚠️ archive heatmap query failed: {he}")

        if missing:
            return {
                "status": "incomplete",
                "message": f"以下 operator 冇 archive 數據：{', '.join(missing)}。請喺 Panel 開曬所有 filter Apply 一次先。",
                "data": results,
                "period": period_data,
                "heatmap": heatmap,
                "month": month
            }

        return {"status": "success", "data": results, "period": period_data, "heatmap": heatmap, "month": month}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ── Auth Helpers ────────────────────────────────────────────────────────────
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
# /api/heat/leaderboard  — Live heat score leaderboard
# ══════════════════════════════════════════════════════════════════════════════

def _heat_db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def _fmt_event(row, rank: int) -> dict:
    import json as _json
    meta = {}
    try:
        if row["heat_meta"]:
            meta = _json.loads(row["heat_meta"])
    except Exception:
        pass
    cats = [c.strip() for c in (row["category"] or "").split("|") if c.strip()]
    # Derive display name from content if ai_name missing
    content = row["content"] or ""
    name = row["ai_name"] or ""
    if not name:
        import re
        m = re.search(r'【([^】]{4,40})】', content)
        name = m.group(1) if m else content[:50]
    return {
        "rank":          rank,
        "event_id":      row["event_id"],
        "name":          name,
        "content":       content[:150],
        "category":      cats,
        "operator":      row["operator"] or "",
        "platform":      row["platform"] or "",
        "source_count":  row["source_count"] or 1,
        "heat_score":    float(row["heat_score"]),
        "platforms":     meta.get("platforms", [row["platform"]]),
        "platform_count":meta.get("platform_count", 1),
        "decay_factor":  meta.get("decay_factor", 1.0),
        "newest_post":   meta.get("newest_post", ""),
        "published_at":  row["published_at"] or "",
    }

def get_heat_score_map(conn, event_ids):
    if not event_ids: return {}
    placeholders = ",".join(["?"] * len(event_ids))
    rows = conn.execute(f"SELECT event_id, heat_score FROM events_deduped WHERE event_id IN ({placeholders})", event_ids).fetchall()
    return {r[0]: (r[1] or 0) for r in rows}

@app.get("/api/heat/leaderboard")
async def heat_leaderboard(top: int = 10):
    """
    Returns top N events overall + top 3 per atomic category.
    Requires heat_analyzer.py to have been run first.
    """
    ATOMIC_CATS = [
        "accommodation","concert","crossover","entertainment",
        "exhibition","experience","food","gaming","shopping","sport"
    ]
    conn = _heat_db_conn()
    try:
        # Check heat_score exists
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(events_deduped)").fetchall()}
        if "heat_score" not in cols:
            return {"success": False, "message": "heat_score 欄位唔存在，請先跑 heat_analyzer.py"}

        # Top overall
        rows = conn.execute("""
            SELECT event_id, platform, operator, content, category,
                   source_count, heat_score, heat_meta, ai_name, published_at
            FROM events_deduped
            WHERE heat_score IS NOT NULL
            ORDER BY heat_score DESC
            LIMIT ?
        """, (top,)).fetchall()
        top_overall = [_fmt_event(r, i+1) for i, r in enumerate(rows)]

        # Per-category top 3
        by_category = {}
        for cat in ATOMIC_CATS:
            cat_rows = conn.execute("""
                SELECT event_id, platform, operator, content, category,
                       source_count, heat_score, heat_meta, ai_name, published_at
                FROM events_deduped
                WHERE heat_score IS NOT NULL AND category LIKE ?
                ORDER BY heat_score DESC
                LIMIT 3
            """, (f"%{cat}%",)).fetchall()
            by_category[cat] = [_fmt_event(r, i+1) for i, r in enumerate(cat_rows)]

        total = conn.execute("SELECT COUNT(*) FROM events_deduped WHERE heat_score IS NOT NULL").fetchone()[0]
        return {
            "success":     True,
            "total":       total,
            "categories":  ATOMIC_CATS,
            "top_overall": top_overall,
            "by_category": by_category,
        }
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# /api/heat/leaderboard-ai  — AI-activity-level leaderboard with DB cache
#
# Architecture:
#   • Results are cached in heat_leaderboard_cache table (JSON blob per cache_key)
#   • GET  /api/heat/leaderboard-ai          → serve from cache (instant)
#   • POST /api/heat/leaderboard-ai/refresh  → re-run AI extraction, update cache
#   • Cache key = "operators|categories"     → different filter combos cached separately
#   • Cache TTL enforced client-side via `cached_at` timestamp in response
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_cache_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS heat_leaderboard_cache (
            cache_key   TEXT PRIMARY KEY,
            cached_at   TEXT NOT NULL,
            payload     TEXT NOT NULL
        )
    """)
    conn.commit()

def _cache_key(operators: str, category: str) -> str:
    ops  = ",".join(sorted(o.strip() for o in operators.split(",") if o.strip()))
    cats = ",".join(sorted(c.strip() for c in category.split(",")  if c.strip()))
    return f"{ops}|{cats}"

def _read_cache(conn, key: str):
    row = conn.execute(
        "SELECT cached_at, payload FROM heat_leaderboard_cache WHERE cache_key=?", (key,)
    ).fetchone()
    if not row: return None, None
    try:
        return row[0], json.loads(row[1])
    except Exception:
        return None, None

def _write_cache(conn, key: str, payload: dict):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        "INSERT OR REPLACE INTO heat_leaderboard_cache (cache_key, cached_at, payload) VALUES (?,?,?)",
        (key, now, json.dumps(payload, ensure_ascii=False))
    )
    conn.commit()


async def _build_leaderboard_ai_payload(operators: str, category: str, top: int,
                                         from_date: str, to_date: str) -> dict:
    """
    Core logic: call analyze(), flatten + sort AI activities by heat_score.
    Returns the full response payload dict (without cache metadata).
    """
    default_ops = "wynn,sands,galaxy,mgm,melco,sjm"
    ops_param   = operators or default_ops

    result = await analyze(
        keyword   = "",
        operators = ops_param,
        category  = category,
        from_date = from_date,
        to_date   = to_date,
    )
    if result.get("status") != "success":
        return {"success": False, "message": result.get("message", "analyze failed")}

    cat_data = result.get("category_summaries") or {}

    ATOMIC_CATS = [
        "accommodation", "concert", "crossover", "exhibition",
        "experience", "food", "gaming", "shopping", "sport",
    ]

    # Flatten and global-sort for top_overall
    all_acts = []
    for cat_key, acts in cat_data.items():
        for act in acts:
            if act.get("heat_score") is not None:
                all_acts.append({**act, "_cat_key": cat_key})
    all_acts.sort(key=lambda a: (a.get("heat_score") or 0), reverse=True)

    top_overall = []
    for i, act in enumerate(all_acts[:top]):
        cat_key = act.pop("_cat_key", "")
        top_overall.append({
            "rank":          i + 1,
            "name":          act.get("name") or "",
            "description":   act.get("description") or "",
            "date":          act.get("date") or "",
            "location":      act.get("location") or "",
            "category":      act.get("category") or cat_key,
            "sub_type":      act.get("sub_type") or "",
            "operator":      act.get("operator") or "",
            "heat_score":    act.get("heat_score"),
            "decay_factor":  act.get("decay_factor"),
            "newest_post":   act.get("newest_post") or "",
            "platforms":     act.get("platforms") or [],
            "platform_count":act.get("platform_count") or 0,
            "source_count":  act.get("source_count") or 0,
            "source_posts":  act.get("source_posts") or [],
        })

    # Per-category top 3 (already sorted by heat inside each cat)
    by_category = {}
    for cat_key in ATOMIC_CATS:
        acts = cat_data.get(cat_key) or []
        by_category[cat_key] = [
            {
                "rank":          j + 1,
                "name":          a.get("name") or "",
                "description":   a.get("description") or "",
                "date":          a.get("date") or "",
                "location":      a.get("location") or "",
                "category":      a.get("category") or cat_key,
                "sub_type":      a.get("sub_type") or "",
                "operator":      a.get("operator") or "",
                "heat_score":    a.get("heat_score"),
                "decay_factor":  a.get("decay_factor"),
                "newest_post":   a.get("newest_post") or "",
                "platforms":     a.get("platforms") or [],
                "platform_count":a.get("platform_count") or 0,
                "source_count":  a.get("source_count") or 0,
                "source_posts":  a.get("source_posts") or [],
            }
            for j, a in enumerate(acts[:3])
            if a.get("heat_score") is not None
        ]

    total = sum(len(v) for v in cat_data.values())
    return {
        "success":     True,
        "total":       total,
        "categories":  ATOMIC_CATS,
        "top_overall": top_overall,
        "by_category": by_category,
    }


@app.get("/api/heat/leaderboard-ai")
async def heat_leaderboard_ai(
    operators: str = "",
    category:  str = "",
    top:       int = 10,
    from_date: str = "",
    to_date:   str = "",
):
    """
    Serve AI-activity-level heat leaderboard from DB cache.
    If no cache exists yet, runs the full analysis and caches it.
    Use POST /api/heat/leaderboard-ai/refresh to force a rebuild.
    """
    conn = _heat_db_conn()
    try:
        _ensure_cache_table(conn)
        key = _cache_key(operators, category)
        cached_at, payload = _read_cache(conn, key)

        if payload:
            # ── 24-hour TTL check ─────────────────────────────────────────────
            # If cache is older than 24 hours, rebuild in background and serve stale
            try:
                from datetime import timezone as _tz
                cached_dt = datetime.strptime(cached_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz.utc)
                age_hours = (datetime.now(_tz.utc) - cached_dt).total_seconds() / 3600
                payload["cache_age_hours"] = round(age_hours, 1)
                if age_hours > 24:
                    payload["stale"] = True
                    # Trigger async rebuild (non-blocking — serve stale now)
                    import asyncio as _aio
                    _aio.create_task(_build_and_write_cache(operators, category, top, from_date, to_date, conn_factory=_heat_db_conn, key=key))
            except Exception:
                pass
            payload["cached_at"]   = cached_at
            payload["from_cache"]  = True
            return payload

        # No cache yet → build it now (first-time, blocking)
        print(f"[leaderboard-ai] No cache for '{key}', building now…")
        payload = await _build_leaderboard_ai_payload(operators, category, top, from_date, to_date)
        if payload.get("success"):
            _write_cache(conn, key, payload)
        payload["from_cache"] = False
        return payload

    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        conn.close()


async def _build_and_write_cache(operators, category, top, from_date, to_date, conn_factory, key):
    """Background task: rebuild stale cache without blocking the response."""
    try:
        payload = await _build_leaderboard_ai_payload(operators, category, top, from_date, to_date)
        if payload.get("success"):
            conn = conn_factory()
            _ensure_cache_table(conn)
            _write_cache(conn, key, payload)
            conn.close()
            print(f"[leaderboard-ai] Background cache refresh done for '{key}'")
    except Exception as e:
        print(f"[leaderboard-ai] Background cache refresh failed: {e}")


@app.post("/api/heat/leaderboard-ai/refresh")
async def heat_leaderboard_ai_refresh(
    operators: str = "",
    category:  str = "",
    top:       int = 10,
    from_date: str = "",
    to_date:   str = "",
):
    """
    Force re-run AI activity extraction and update the DB cache.
    Call this after heat_analyzer.py runs, or when you want fresh activity names.
    Returns the new payload immediately.
    """
    conn = _heat_db_conn()
    try:
        _ensure_cache_table(conn)
        key = _cache_key(operators, category)
        print(f"[leaderboard-ai/refresh] Rebuilding cache for '{key}'…")
        payload = await _build_leaderboard_ai_payload(operators, category, top, from_date, to_date)
        if payload.get("success"):
            _write_cache(conn, key, payload)
            payload["from_cache"] = False
            payload["refreshed"]  = True
        return payload
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        conn.close()

@app.post("/api/period-cache/save")
async def save_period_cache(payload: dict):
    """前端生成完所有數據後 call 呢個儲存"""
    from_date       = payload.get("from_date", "")
    to_date         = payload.get("to_date", "")
    hot_themes      = payload.get("hot_themes", [])
    recommendations = payload.get("recommendations", [])
    leaderboard     = payload.get("leaderboard", [])
    events          = payload.get("events", {})
    heatmap_data    = payload.get("heatmap_data", {})
    if not from_date or not to_date:
        return {"success": False, "message": "from_date / to_date 必填"}
    _set_period_cache(from_date, to_date, hot_themes, recommendations, leaderboard, events, heatmap_data)
    return {"success": True}

@app.get("/api/period-cache/get")
async def get_period_cache(from_date: str = "", to_date: str = ""):
    """Archive report 讀取 period-level cache"""
    if not from_date or not to_date:
        return {"success": False, "message": "from_date / to_date 必填"}
    data = _get_period_cache(from_date, to_date)
    if not data:
        return {"success": False, "message": "冇 cache"}
    return {"success": True, **data}

# ══════════════════════════════════════════════════════════════════════════════
# 負面監測（XHS / 微博 / IG / FB 關鍵字專表 + 兩階段 AI；IG·FB 經 Apify）
# ══════════════════════════════════════════════════════════════════════════════


def _xhs_json_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "xhs", "json")


def _weibo_json_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "weibo", "json")


def load_search_comments_index(json_dir: str) -> dict:
    """合併 search_comments_*.json → { note_id: [comment_text, ...] }。"""
    by_note: dict[str, list] = defaultdict(list)
    if not json_dir or not os.path.isdir(json_dir):
        return {}
    for path in glob.glob(os.path.join(json_dir, "search_comments_*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for c in data:
            if not isinstance(c, dict):
                continue
            nid = str(c.get("note_id") or "").strip()
            txt = (c.get("content") or "").strip()
            if nid and txt:
                by_note[nid].append(txt)
    return dict(by_note)


def load_xhs_search_comments_index(json_dir: str | None = None) -> dict:
    return load_search_comments_index(json_dir or _xhs_json_dir())


def _note_id_from_posts_row(row: dict) -> str:
    nid0 = row.get("note_id")
    if nid0 is not None and str(nid0).strip():
        return str(nid0).strip()
    rj = row.get("raw_json")
    if rj:
        try:
            raw = json.loads(rj) if isinstance(rj, str) else rj
            nid = raw.get("note_id")
            if nid:
                return str(nid)
        except Exception:
            pass
    pid = str(row.get("post_id") or "")
    if pid.startswith("xhs_"):
        return pid[4:]
    if pid.startswith("weibo_"):
        return pid[7:]
    if pid.startswith("ig_"):
        return pid[3:]
    if pid.startswith("fb_"):
        return pid[3:]
    return pid


def _lexicon_hits(text: str) -> list:
    if not text:
        return []
    t = str(text)
    found = [w for w in NEGATIVE_MONITOR_LEXICON if w in t]
    return list(dict.fromkeys(found))[:20]


def _parse_llm_json_array(raw: str) -> list:
    if not raw:
        return []
    s = raw.strip().replace("\ufeff", "")
    s = re.sub(r"^```(?:json|JSON)?\s*", "", s)
    s = re.sub(r"\s*```\s*$", "", s).strip()
    try:
        out = json.loads(s)
        return out if isinstance(out, list) else []
    except json.JSONDecodeError:
        pass
    i = s.find("[")
    j = s.rfind("]")
    if i != -1 and j != -1 and j > i:
        chunk = s[i : j + 1]
        try:
            out = json.loads(chunk)
            return out if isinstance(out, list) else []
        except json.JSONDecodeError:
            pass
    k = s.find("{")
    m = s.rfind("}")
    if k != -1 and m != -1 and m > k:
        try:
            obj = json.loads(s[k : m + 1])
            if isinstance(obj, dict):
                for key in ("items", "results", "data", "list"):
                    v = obj.get(key)
                    if isinstance(v, list):
                        return v
        except json.JSONDecodeError:
            pass
    return []


def _deepseek_score_negative_monitor(items: list[dict]) -> list[dict]:
    if not items:
        return []
    lines = []
    for it in items:
        pid = it.get("id") or ""
        blob = f"{it.get('title') or ''}\n{it.get('body') or ''}\n【評論摘錄】{it.get('comments_sample') or '無'}"[:1200]
        lines.append(f"### post_id={pid}\n{blob}")
    prompt = """你是澳門博企公關風險分析助手。以下每條均為社交媒體（微博、小紅書等）上與「永利／永利皇宮／Wynn」相關的貼文摘要（含部分評論）。
請逐條判斷是否對「永利 Wynn」品牌有明顯負面影響或潛在輿情風險，例如：避雷吐槽、服務/衛生投訴、差評、惡性事件傳聞、可能造謠需警惕等。
注意：單純打卡分享、中性攻略、正面種草、無關抱怨（未指向永利）應判為非負面。

只輸出一段 JSON：要麼是數組，要麼是對象且含 "items" 數組，不要其它文字、不要 Markdown。示例數組：
[{"post_id":"與上文一致","negative":false,"severity":0,"reason":"繁體短句","triggers":[]}]
其中 severity: 0=無負面 1=輕微情緒 2=明確負面 3=嚴重/安全法律敏感
post_id 必須與 ### 行完全一致。

貼文列表：
""" + "\n".join(lines)

    try:
        resp = client.chat.completions.create(
            model=DEEPSEEK_JSON_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=min(4096, 350 * len(items) + 400),
        )
        raw = (resp.choices[0].message.content or "").strip()
        out = _parse_llm_json_array(raw)
        return out
    except Exception as e:
        print(f"⚠️ _deepseek_score_negative_monitor: {e}")
        return []


NEGATIVE_MONITOR_FETCH_CAP = max(1, min(20000, int(os.environ.get("NEGATIVE_MONITOR_FETCH_CAP", "300"))))
NEGATIVE_MONITOR_AI_BATCH = max(2, min(12, int(os.environ.get("NEGATIVE_MONITOR_AI_BATCH", "8"))))
NEGATIVE_MONITOR_PHASE2_RECENT = max(1, min(500, int(os.environ.get("NEGATIVE_MONITOR_PHASE2_RECENT", "100"))))


def _nm_run_ai_batches(ai_candidates: list, bs: int) -> list:
    ai_flat: list[dict] = []
    if not ai_candidates:
        return ai_flat
    for start in range(0, len(ai_candidates), bs):
        chunk = ai_candidates[start : start + bs]
        payload = [
            {
                "id": c["post_id"],
                "title": c["title"],
                "body": c["content_preview"],
                "comments_sample": c["comments_sample"][:1800],
            }
            for c in chunk
        ]
        part = _deepseek_score_negative_monitor(payload)
        by_pid = {
            str(h.get("post_id")): h
            for h in part
            if isinstance(h, dict) and h.get("post_id")
        }
        for c in chunk:
            hit = by_pid.get(str(c["post_id"]))
            if not hit:
                continue
            ai_flat.append({
                "post_id": c["post_id"],
                "note_id": c["note_id"],
                "negative": bool(hit.get("negative")),
                "severity": int(hit.get("severity") or 0),
                "reason": hit.get("reason") or "",
                "triggers": hit.get("triggers") or [],
            })
    return ai_flat


NEGATIVE_MONITOR_SOURCES = ("xhs", "weibo", "ig", "fb")
# 進程啟動時刻（Unix 秒）；重啟 bridge 即變，供前端清空「以為延續上一次分析」的本地狀態
NEGATIVE_MONITOR_BRIDGE_BOOT_TS = time.time()


def _normalize_negative_monitor_source(s: str) -> str | None:
    v = (s or "").strip().lower()
    if v in ("", "xhs", "xiaohongshu", "redbook"):
        return "xhs"
    if v in ("weibo", "wb"):
        return "weibo"
    if v in ("ig", "instagram", "ins"):
        return "ig"
    if v in ("fb", "facebook"):
        return "fb"
    return None


@app.post("/api/v2/negative-monitor/crawl")
@app.post("/api/v2/wynn-negative/crawl")
async def negative_monitor_crawl(
    max_comments: int = 40,
    headless: str = "0",
    keywords: str = "",
    get_comments: int = 1,
    max_notes: int = 0,
    source: str = "xhs",
    from_date: str = "",
    to_date: str = "",
):
    src = _normalize_negative_monitor_source(source)
    if not src:
        return {
            "status": "error",
            "message": f"不支持的 source，可選：{', '.join(NEGATIVE_MONITOR_SOURCES)}",
        }
    global _neg_monitor_crawl_running
    with _neg_monitor_crawl_lock:
        if _neg_monitor_crawl_running:
            return {"status": "busy", "message": "已有負面監測採集任務在執行"}
        _neg_monitor_crawl_running = True

    kw_csv = keywords.strip() or None
    mn = int(max_notes) if int(max_notes) > 0 else None
    gc = bool(int(get_comments))
    cf = (from_date or "").strip()[:10] or None
    ct = (to_date or "").strip()[:10] or None

    def _job(mc, hl, kws, do_comments, notes_cap, platform: str, crawl_from: str | None, crawl_to: str | None):
        global _neg_monitor_crawl_running
        try:
            if platform == "weibo":
                run_weibo_negative_monitor_crawl(
                    max_comments_per_note=mc,
                    headless=hl,
                    keywords_csv=kws,
                    get_comments=do_comments,
                    max_notes=notes_cap,
                    crawl_from_date=crawl_from,
                    crawl_to_date=crawl_to,
                )
            elif platform == "ig":
                run_ig_negative_monitor_crawl(
                    keywords_csv=kws,
                    max_notes=notes_cap,
                    crawl_from_date=crawl_from,
                    crawl_to_date=crawl_to,
                )
            elif platform == "fb":
                run_fb_negative_monitor_crawl(
                    keywords_csv=kws,
                    max_notes=notes_cap,
                    crawl_from_date=crawl_from,
                    crawl_to_date=crawl_to,
                )
            else:
                run_xhs_negative_monitor_crawl(
                    max_comments_per_note=mc,
                    headless=hl,
                    keywords_csv=kws,
                    get_comments=do_comments,
                    max_notes=notes_cap,
                    crawl_from_date=crawl_from,
                    crawl_to_date=crawl_to,
                )
        finally:
            with _neg_monitor_crawl_lock:
                _neg_monitor_crawl_running = False

    _storage_map = {
        "weibo": "weibo_negative_monitor",
        "xhs": "xhs_negative_monitor",
        "ig": "ig_negative_monitor",
        "fb": "fb_negative_monitor",
    }
    storage = _storage_map.get(src, "xhs_negative_monitor")
    threading.Thread(
        target=_job,
        args=(max_comments, headless, kw_csv, gc, mn, src, cf, ct),
        daemon=True,
    ).start()
    return {
        "status": "started",
        "message": "後台採集中；完成後請 GET /api/v2/negative-monitor/analyze",
        "storage": storage,
        "source": src,
    }


@app.get("/api/v2/negative-monitor/status")
async def negative_monitor_status():
    with _neg_monitor_crawl_lock:
        busy = _neg_monitor_crawl_running
    return {
        "crawl_running": busy,
        "supported_sources": list(NEGATIVE_MONITOR_SOURCES),
        "bridge_boot_ts": NEGATIVE_MONITOR_BRIDGE_BOOT_TS,
    }


@app.get("/api/v2/negative-monitor/analyze")
@app.get("/api/v2/wynn-negative/analyze")
async def negative_monitor_analyze(
    from_date: str = "",
    to_date: str = "",
    phase: int = 1,
    phase2_offset: int = 0,
    limit: int = 300,
    use_ai: int = 1,
    lexicon_only_for_ai: int = 0,
    ai_max: int = 60,
    batch_size: int = 8,
    source: str = "xhs",
):
    src = _normalize_negative_monitor_source(source)
    if not src:
        return {
            "status": "error",
            "hint": f"不支持的 source，可選：{', '.join(NEGATIVE_MONITOR_SOURCES)}",
            "supported_sources": list(NEGATIVE_MONITOR_SOURCES),
        }

    ph = int(phase)
    cap = min(int(limit), NEGATIVE_MONITOR_FETCH_CAP) if int(limit) > 0 else NEGATIVE_MONITOR_FETCH_CAP
    bs = NEGATIVE_MONITOR_AI_BATCH
    mod = {
        "weibo": "weibo_negative_monitor",
        "xhs": "xhs_negative_monitor",
        "ig": "ig_negative_monitor",
        "fb": "fb_negative_monitor",
    }.get(src, "xhs_negative_monitor")

    if src == "weibo":
        df = query_weibo_negative_monitor(
            from_date or None,
            to_date or None,
            limit=cap,
        )
        comments_idx = load_search_comments_index(_weibo_json_dir())
    elif src == "ig":
        df = query_ig_negative_monitor(
            from_date or None,
            to_date or None,
            limit=cap,
        )
        comments_idx = {}
    elif src == "fb":
        df = query_fb_negative_monitor(
            from_date or None,
            to_date or None,
            limit=cap,
        )
        comments_idx = {}
    else:
        df = query_xhs_negative_monitor(
            from_date or None,
            to_date or None,
            limit=cap,
        )
        comments_idx = load_xhs_search_comments_index()

    empty = {
        "status": "success",
        "module": mod,
        "source": src,
        "phase": ph,
        "total_posts": 0,
        "items": [],
        "ai_updates": [],
        "ai_flagged": [],
        "hint": "無數據。可先 POST /api/v2/negative-monitor/crawl（帶 source=xhs|weibo|ig|fb），或調整時間範圍。",
    }
    if df.empty:
        return empty

    records = df.to_dict(orient="records")
    built: list[dict] = []
    for row in records:
        nid = _note_id_from_posts_row(row)
        coms = comments_idx.get(nid, [])
        com_sample = " | ".join(coms[:25])[:2500]
        title = (row.get("title") or "").strip()
        body = row.get("content") or ""
        if not title and body:
            title = (body[:100] + "…") if len(body) > 100 else body
        lex_h = _lexicon_hits(f"{title}\n{body}\n{com_sample}")
        built.append({
            "post_id": row.get("post_id"),
            "note_id": nid,
            "title": title,
            "content_preview": (body or "")[:500],
            "published_at": row.get("published_at"),
            "post_url": row.get("post_url") or "",
            "source_keyword": row.get("source_keyword") or "",
            "comment_count_file": len(coms),
            "lexicon_hits": lex_h,
            "comments_sample": com_sample,
        })

    if ph == 2:
        def _pub_key(it):
            return (it.get("published_at") or "")[:19] or ""

        ranked = sorted(built, key=_pub_key, reverse=True)
        eligible = [x for x in ranked if not x.get("lexicon_hits")]
        n = NEGATIVE_MONITOR_PHASE2_RECENT
        off = max(0, int(phase2_offset))

        if not eligible:
            return {
                "status": "success",
                "module": mod,
                "source": src,
                "phase": 2,
                "total_posts": len(built),
                "fetch_cap": cap,
                "phase2_recent_n": n,
                "phase2_offset": 0,
                "phase2_next_offset": 0,
                "phase2_exhausted": True,
                "phase2_eligible_total": 0,
                "ai_scanned": 0,
                "items_batch": [],
                "ai_updates": [],
                "ai_flagged": [],
                "message_en": "Step 2 skipped: every loaded post had lexicon hits (Step 1 already sent those to AI).",
            }

        recent = eligible[off : off + n]
        next_off = off + len(recent)
        exhausted = next_off >= len(eligible) or len(recent) == 0

        if not recent:
            return {
                "status": "success",
                "module": mod,
                "source": src,
                "phase": 2,
                "total_posts": len(built),
                "fetch_cap": cap,
                "phase2_recent_n": n,
                "phase2_offset": off,
                "phase2_next_offset": off,
                "phase2_exhausted": True,
                "phase2_eligible_total": len(eligible),
                "ai_scanned": 0,
                "items_batch": [],
                "ai_updates": [],
                "ai_flagged": [],
                "message_en": "No more Step-2-eligible posts (no lexicon hits) in this range for the current offset.",
            }

        if int(use_ai):
            ai_flat = _nm_run_ai_batches(recent, bs)
        else:
            ai_flat = []
        by_pid_ai = {a["post_id"]: dict(a) for a in ai_flat}
        items_batch = []
        for c in recent:
            row = {k: v for k, v in c.items() if k != "ai"}
            row["ai"] = by_pid_ai.get(c["post_id"])
            items_batch.append(row)
        ai_updates = [{"post_id": a["post_id"], "ai": dict(a)} for a in ai_flat]
        return {
            "status": "success",
            "module": mod,
            "source": src,
            "phase": 2,
            "total_posts": len(built),
            "posts_fetched": len(built),
            "fetch_cap": cap,
            "phase2_recent_n": n,
            "phase2_offset": off,
            "phase2_next_offset": next_off,
            "phase2_exhausted": exhausted,
            "phase2_eligible_total": len(eligible),
            "ai_scanned": len(recent),
            "items_batch": items_batch,
            "ai_updates": ai_updates,
            "ai_flagged": [a for a in ai_flat if a.get("negative")],
            "message_en": (
                f"Full AI on {len(recent)} posts with no lexicon hits (Step 1 never sent them), "
                f"batch rank #{off + 1}–{off + len(recent)} of {len(eligible)} eligible (newest first). "
                f"{'No further Step-2 batches.' if exhausted else 'Click Step 2 again for the next 100 eligible posts.'}"
            ),
        }

    ai_candidates = [x for x in built if x["lexicon_hits"]]
    if int(use_ai):
        ai_flat = _nm_run_ai_batches(ai_candidates, bs)
    else:
        ai_flat = []
    neg_by_pid = {a["post_id"]: a for a in ai_flat}
    for item in built:
        item["ai"] = neg_by_pid.get(item["post_id"])

    lex_rows = [x for x in built if x.get("lexicon_hits")]
    if not lex_rows:
        plat_zh = {"weibo": "微博", "xhs": "小紅書", "ig": "Instagram", "fb": "Facebook"}.get(src, "小紅書")
        return {
            "status": "success",
            "module": mod,
            "source": src,
            "phase": 1,
            "posts_fetched": len(built),
            "total_posts": len(built),
            "fetch_cap": cap,
            "ai_scanned": 0,
            "lexicon_only_for_ai": True,
            "items": [],
            "ai_flagged": [],
            "hint": (
                f"載入 {len(built)} 條{plat_zh}帖，但沒有任何帖命中監測用負面詞表（lexicon）。"
                f"表格僅顯示有詞表命中的帖。可用 Step 2 對「無詞表命中」的帖做全文 AI（與 {plat_zh} 同邏輯）。"
            ),
            "message_en": (
                f"Loaded {len(built)} {src} posts in range; none hit the monitoring lexicon. "
                "Table lists lexicon hits only — empty. Use Step 2 for full AI on posts without lexicon hits."
            ),
        }

    return {
        "status": "success",
        "module": mod,
        "source": src,
        "phase": 1,
        "posts_fetched": len(built),
        "total_posts": len(built),
        "fetch_cap": cap,
        "ai_scanned": len(ai_candidates),
        "lexicon_only_for_ai": True,
        "items": lex_rows,
        "ai_flagged": [a for a in ai_flat if a.get("negative")],
        "message_en": (
            f"Step 1 ({src}): {len(built)} posts loaded in range, {len(lex_rows)} with lexicon hits (table shows these only). "
            "Step 2: full AI on posts with no lexicon hits, up to 100 per click (same logic as XHS)."
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════

def _find_free_listen_port(host: str, start: int, attempts: int = 32) -> tuple[int, bool]:
    """在本機 host 上從 start 起試綁定，返回 (端口, 是否與 start 不同)。全滿則 SystemExit。"""
    import socket

    for p in range(start, start + attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, p))
            return p, p != start
        except OSError:
            continue
    raise SystemExit(
        f"❌ {host} 在 {start}–{start + attempts - 1} 均無可用端口；請關閉佔用進程或 export BRIDGE_PORT=其它埠"
    )


if __name__ == "__main__":
    _host = "127.0.0.1"
    _preferred = int(os.environ.get("BRIDGE_PORT", "9038"))
    _access_log = os.environ.get("BRIDGE_ACCESS_LOG", "1").strip().lower() not in ("0", "false", "no")
    _port, _bumped = _find_free_listen_port(_host, _preferred)
    if _bumped:
        print(
            f"⚠️  端口 {_preferred} 已被佔用，已改用 {_port}。"
            f"（若需固定 9038，請先關掉舊的 bridge：`lsof -iTCP:{_preferred} -sTCP:LISTEN`）"
        )
    print(f"🌐 http://{_host}:{_port}  （強制指定埠：export BRIDGE_PORT=…）")
    print(f"📋 負面監測: http://{_host}:{_port}/negative-monitor")
    if not _access_log:
        print("ℹ️  HTTP access log 已關閉（BRIDGE_ACCESS_LOG=0），終端唔再逐條打印 GET/POST")
    uvicorn.run(app, host=_host, port=_port, access_log=_access_log)

# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9038)
