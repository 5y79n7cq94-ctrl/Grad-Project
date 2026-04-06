"""
分類測試工具 - 本地 server
用法: python classifier_tester.py
然後瀏覽器開 http://localhost:8765
"""
import sqlite3, json, re, os, sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from openai import OpenAI

# ── 設定 ──────────────────────────────────────────────────
DB_PATH      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "macau_analytics.db")
DEEPSEEK_KEY = "sk-06452010eeff43f59e36f4d86d4d5076"  # 同 bridge.py 一樣
PORT         = 8765

client = OpenAI(api_key=DEEPSEEK_KEY, base_url="https://api.deepseek.com")

# ── 你現有嘅規則分類器（搬自 bridge.py）─────────────────────
CAT_RULES = [
    ("entertainment", "concert", [
        "演唱會","音樂會","FANMEETING","見面會","CONCERT","FANCON","SHOWCASE",
        "演唱会","音乐会","见面会","巡演","世巡","开唱",
        "LIVE TOUR","LIVE IN","IN MACAU","IN MACAO",
        "抢票","開始售票","票務開售","即將開售","开始售票","即将开售",
        "演出季","音樂劇","歌劇","話劇","舞劇","京劇","粵劇",
        "音乐剧","歌剧","话剧","舞剧","京剧","粤剧",
    ]),
    ("entertainment","sport",[
        "馬拉松","MARATHON","長跑","长跑",
        "十公里","10公里","10K","5K","半馬","半马",
        "乒乓球","羽毛球","籃球","足球","網球","排球","跑步","篮球","网球",
        "F1大獎賽","格蘭披治大賽","格兰披治大赛",
        "全運會","全运会","奧運","奥运",
        "UFC","格鬥賽","格斗赛","拳擊賽","拳击赛",
        "高爾夫球賽","高爾夫賽事","高尔夫球赛","高尔夫赛事","GOLF TOURNAMENT","GOLF OPEN",
        "WTT","FISE",
    ]),
    ("entertainment","crossover",[
        "聯名","快閃","POP-UP","POPUP","泡泡瑪特","POPMART","主題展覽",
        "联名","快闪","泡泡玛特",
    ]),
    ("experience",None,[
        "VR","SANDBOX","沉浸式","體驗館","水舞間","主題樂園","常駐",
        "体验馆","主题乐园","天浪淘园","星动银河","ILLUMINARIUM","幻影空間",
    ]),
    ("exhibition",None,[
        "展覽","展出","藝術展","TEAMLAB","EXPO","球拍珍品","博物館","展示館","紀念館",
        "展览","艺术展","艺荟","博物馆","展示馆","纪念馆",
    ]),
    ("food",None,[
        "美食","餐廳","餐飲","自助餐","下午茶","食評","扒房","點心","茶餐廳",
        "火鍋","煲仔","葡萄酒","品酒","美酒","佳釀","評酒","酒宴","餐酒",
        "大師班","品鑑","晚宴","宴席","春茗",
        "BUFFET","RESTAURANT","DINING","STEAKHOUSE","WINE","DEGUSTATION",
        "餐厅","餐饮","茶餐厅","美食地图","火锅","品鉴",
        "酒吧","調酒","雞尾酒","特調","微醺","BAR","COCKTAIL",
        "调酒","鸡尾酒","特调",
    ]),
    ("accommodation",None,[
        "酒店優惠","住宿套票","HOTEL PACKAGE","住宿","度假套","住宿禮遇","酒店住客",
    ]),
    ("shopping",None,[
        "購物","折扣","優惠券","購物返現",
        "购物","优惠券","购物返现","时尚汇","旗舰店",
    ]),
    ("gaming",None,[
        "博彩","賭場","CASINO","積分兌換","貴賓",
        "赌场","积分","贵宾",
    ]),
]

def rule_classify(title, desc):
    text = (str(title or '') + ' ' + str(desc or '')).upper()
    for cat, sub, kws in CAT_RULES:
        if any(k.upper() in text for k in kws):
            return cat, sub
    return "experience", None


# ── DB 讀取 ───────────────────────────────────────────────
def get_posts(limit=50, operator="", platform=""):
    if not os.path.exists(DB_PATH):
        return [], f"DB 搵唔到: {DB_PATH}"
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    where = ["1=1"]
    params = []
    if operator:
        where.append("operator = ?"); params.append(operator)
    if platform:
        where.append("platform = ?"); params.append(platform)
    c.execute(f"""
        SELECT id, platform, operator, title, description
        FROM macau_events
        WHERE {' AND '.join(where)}
        ORDER BY RANDOM()
        LIMIT ?
    """, params + [limit])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows, None

def get_filter_options():
    if not os.path.exists(DB_PATH):
        return [], []
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT operator FROM macau_events WHERE operator IS NOT NULL ORDER BY operator")
    ops = [r[0] for r in c.fetchall()]
    c.execute("SELECT DISTINCT platform FROM macau_events WHERE platform IS NOT NULL ORDER BY platform")
    plats = [r[0] for r in c.fetchall()]
    conn.close()
    return ops, plats


# ── DeepSeek 分類 ─────────────────────────────────────────
def ai_classify(posts):
    snippets = []
    for i, p in enumerate(posts):
        t = (p.get('title') or '').strip()
        d = (p.get('description') or '').strip()[:200]
        snippets.append(f"[{i}] 標題: {t}\n內容: {d}")

    prompt = f"""你係澳門活動分類助手。以下係社交媒體帖文，請為每條帖文分類。

分類選項：
- concert（演唱會/音樂會/話劇等演出）
- sport（體育賽事）
- crossover（聯名/快閃）
- experience（沉浸式體驗/常駐節目）
- exhibition（展覽/博物館）
- food（餐飲/美食活動）
- accommodation（住宿優惠）
- shopping（購物優惠）
- gaming（博彩/賭場）
- other（唔屬於以上任何一類）

帖文：
{chr(10).join(snippets)}

以 JSON array 格式返回，每個 object 含 index（整數）同 category（字串）同 reason（一句話解釋點解咁分，15字內）：
[{{"index": 0, "category": "food", "reason": "提及餐廳同美食活動"}}]
只返回 JSON，唔需要任何前言："""

    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000, # Increased to handle more posts
            temperature=0.1  # Keep it precise
        )
        
        content = resp.choices[0].message.content or ""
        
        # ── Robust JSON Extraction ──
        # This looks for the first '[' and the last ']' to ignore any AI "chit-chat"
        match = re.search(r'\[.*\]', content, re.DOTALL)
        if match:
            clean_json = match.group(0)
            return json.loads(clean_json), None
        else:
            # If regex fails, try the old strip method as a backup
            raw = re.sub(r'^```[a-z]*\n?', '', content.strip()).rstrip('`').strip()
            return json.loads(raw), None

    except json.JSONDecodeError as je:
        return None, f"JSON Error at line {je.lineno}: Check if AI output was truncated."
    except Exception as e:
        return None, str(e)


# ── HTTP Handler ──────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # 靜音 log

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if parsed.path == '/':
            self.serve_html()

        elif parsed.path == '/api/options':
            ops, plats = get_filter_options()
            self.send_json({"operators": ops, "platforms": plats})

        elif parsed.path == '/api/posts':
            limit    = int(qs.get('limit', ['50'])[0])
            operator = qs.get('operator', [''])[0]
            platform = qs.get('platform', [''])[0]
            posts, err = get_posts(limit, operator, platform)
            if err:
                self.send_json({"error": err}, 500)
                return
            # 先用規則分類
            results = []
            for p in posts:
                rc, rs = rule_classify(p['title'], p['description'])
                results.append({**p, "rule_cat": rc, "rule_sub": rs, "ai_cat": None, "ai_reason": None})
            self.send_json({"posts": results})

        elif parsed.path == '/api/classify':
            limit = int(qs.get('limit', ['0'])[0]) # 0 means all
            operator = qs.get('operator', [''])[0]
            platform = qs.get('platform', [''])[0]
            
            # 1. Get the data
            all_posts, err = get_posts(limit if limit > 0 else 5000, operator, platform)
            if err:
                self.send_json({"error": err}, 500); return

            # 2. Batch Processing (Send 20 at a time to avoid AI cutoff)
            batch_size = 20
            final_results = []
            ai_err_accumulated = None

            for i in range(0, len(all_posts), batch_size):
                batch = all_posts[i : i + batch_size]
                # We need to pass the current offset so the AI 'index' matches our loop
                ai_results, ai_err = ai_classify(batch) 
                
                if ai_err:
                    ai_err_accumulated = ai_err
                    break # Stop if AI fails

                # Map AI results back to our posts
                for j, p in enumerate(batch):
                    rc, rs = rule_classify(p['title'], p['description'])
                    # Find matching index in AI response (AI returns 0-19, so we match by batch position)
                    ai_data = next((item for item in ai_results if item['index'] == j), {})
                    ac = ai_data.get('category', '?')
                    ar = ai_data.get('reason', '')
                    
                    final_results.append({
                        **p,
                        "rule_cat": rc, "rule_sub": rs,
                        "ai_cat": ac, "ai_reason": ar,
                        "match": (rc == ac or rs == ac)
                    })

            mismatch = sum(1 for r in final_results if not r['match'])
            self.send_json({
                "posts": final_results,
                "ai_error": ai_err_accumulated,
                "total": len(final_results),
                "mismatch": mismatch,
            })

    def serve_html(self):
        html = get_html()
        body = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)


# ── HTML 介面 ─────────────────────────────────────────────
def get_html():
    return '''<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<title>分類誤判測試工具</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Noto+Sans+TC:wght@400;500;700&display=swap');
  :root {
    --bg: #0f0f11; --surface: #18181c; --border: #2a2a32;
    --accent: #e8ff47; --red: #ff4d6d; --green: #4dffb4;
    --yellow: #ffd166; --muted: #666680; --text: #e8e8f0;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Noto Sans TC', sans-serif; min-height: 100vh; }

  header {
    border-bottom: 1px solid var(--border);
    padding: 20px 32px;
    display: flex; align-items: center; gap: 16px;
  }
  header .logo { font-family: 'IBM Plex Mono', monospace; font-size: 13px; color: var(--accent); letter-spacing: 2px; }
  header h1 { font-size: 18px; font-weight: 700; }

  .toolbar {
    padding: 16px 32px;
    display: flex; gap: 12px; align-items: center; flex-wrap: wrap;
    border-bottom: 1px solid var(--border);
  }
  select, input[type=number] {
    background: var(--surface); border: 1px solid var(--border);
    color: var(--text); padding: 8px 12px; border-radius: 6px;
    font-size: 13px; font-family: inherit;
  }
  select:focus, input:focus { outline: none; border-color: var(--accent); }

  .btn {
    padding: 8px 20px; border-radius: 6px; border: none;
    font-family: 'IBM Plex Mono', monospace; font-size: 13px;
    cursor: pointer; font-weight: 600; transition: all .15s;
  }
  .btn-primary { background: var(--accent); color: #0f0f11; }
  .btn-primary:hover { background: #d4eb30; }
  .btn-secondary { background: var(--surface); color: var(--text); border: 1px solid var(--border); }
  .btn-secondary:hover { border-color: var(--accent); color: var(--accent); }
  .btn:disabled { opacity: .4; cursor: not-allowed; }

  .stats {
    padding: 12px 32px;
    display: flex; gap: 24px; align-items: center;
    font-family: 'IBM Plex Mono', monospace; font-size: 12px;
    border-bottom: 1px solid var(--border);
  }
  .stat { display: flex; gap: 6px; align-items: center; }
  .stat-val { font-weight: 600; }
  .stat-val.red { color: var(--red); }
  .stat-val.green { color: var(--green); }

  .filter-bar {
    padding: 10px 32px;
    display: flex; gap: 8px; align-items: center;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
  }
  .filter-chip {
    padding: 4px 10px; border-radius: 20px; cursor: pointer;
    border: 1px solid var(--border); font-size: 12px;
    transition: all .12s; font-family: 'IBM Plex Mono', monospace;
  }
  .filter-chip:hover { border-color: var(--accent); color: var(--accent); }
  .filter-chip.active { background: var(--accent); color: #0f0f11; border-color: var(--accent); }

  .table-wrap { padding: 0 32px 32px; overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 13px; }
  th {
    text-align: left; padding: 10px 12px;
    font-family: 'IBM Plex Mono', monospace; font-size: 11px;
    color: var(--muted); border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  td { padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }
  tr:hover td { background: var(--surface); }

  .title-cell { max-width: 220px; }
  .title-text { font-weight: 500; margin-bottom: 4px; }
  .desc-text { color: var(--muted); font-size: 12px; line-height: 1.4; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }

  .tag {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 600;
  }
  .tag-op { background: #1e1e2e; border: 1px solid #3a3a5c; color: #9090c0; }
  .tag-plat { background: #1a1e2a; border: 1px solid #2a3a5c; color: #6090d0; }

  .cat-tag {
    display: inline-block; padding: 3px 10px; border-radius: 4px;
    font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 600;
  }
  .cat-concert    { background: #2a1a3a; color: #d080ff; }
  .cat-sport      { background: #1a2a1a; color: #60d060; }
  .cat-food       { background: #2a1a1a; color: #ff8060; }
  .cat-exhibition { background: #1a2a2a; color: #60c0d0; }
  .cat-experience { background: #2a2a1a; color: #d0c060; }
  .cat-crossover  { background: #1a1a2a; color: #6080ff; }
  .cat-accommodation { background: #2a2a2a; color: #c0c0c0; }
  .cat-shopping   { background: #2a1a2a; color: #ff60c0; }
  .cat-gaming     { background: #1a2a1a; color: #40ff80; }
  .cat-other      { background: #222; color: #888; }

  .match-icon { font-size: 16px; }
  .mismatch-row td { background: rgba(255,77,109,.04); }
  .mismatch-row:hover td { background: rgba(255,77,109,.08) !important; }

  .reason-text { color: var(--muted); font-size: 11px; margin-top: 3px; font-style: italic; }

  .empty { text-align: center; padding: 60px; color: var(--muted); font-family: 'IBM Plex Mono', monospace; }
  .loading { text-align: center; padding: 60px; color: var(--accent); font-family: 'IBM Plex Mono', monospace; }
  .error { background: rgba(255,77,109,.1); border: 1px solid var(--red); border-radius: 8px; padding: 12px 16px; margin: 16px 32px; color: var(--red); font-size: 13px; }
</style>
</head>
<body>

<header>
  <div class="logo">MACAU ANALYTICS</div>
  <h1>分類誤判測試工具</h1>
</header>

<div class="toolbar">
  <select id="selOperator"><option value="">所有運營商</option></select>
  <select id="selPlatform"><option value="">所有平台</option></select>
  <label style="font-size:13px;color:var(--muted)">抽樣數量</label>
  <input type="number" id="numLimit" value="30" min="5" max="100" style="width:70px">
  <button class="btn btn-secondary" onclick="loadPosts()">只看規則分類</button>
  <button class="btn btn-primary" id="btnAI" onclick="runAI()">▶ AI 對比分析</button>
</div>

<div class="stats" id="statsBar" style="display:none">
  <div class="stat">共 <span class="stat-val" id="statTotal">0</span> 條</div>
  <div class="stat">✅ 一致 <span class="stat-val green" id="statMatch">0</span></div>
  <div class="stat">❌ 差異 <span class="stat-val red" id="statMismatch">0</span></div>
  <div class="stat" id="aiErrStat" style="display:none">⚠️ AI錯誤: <span class="stat-val red" id="statAiErr"></span></div>
</div>

<div class="filter-bar" id="filterBar" style="display:none">
  <span style="color:var(--muted);margin-right:4px">篩選:</span>
  <span class="filter-chip active" onclick="setFilter('all',this)">全部</span>
  <span class="filter-chip" onclick="setFilter('mismatch',this)">只看差異</span>
  <span class="filter-chip" onclick="setFilter('match',this)">只看一致</span>
</div>

<div id="errorMsg"></div>
<div class="table-wrap">
  <div class="empty" id="emptyMsg">請選擇條件後點擊按鈕載入數據</div>
  <table id="mainTable" style="display:none">
    <thead>
      <tr>
        <th>#</th>
        <th>運營商 / 平台</th>
        <th>帖文標題 / 內容</th>
        <th>規則分類</th>
        <th>AI 分類</th>
        <th>結果</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>

<script>
let allData = [];
let currentFilter = 'all';

async function loadOptions() {
  try {
    const r = await fetch('/api/options');
    const d = await r.json();
    const opSel = document.getElementById('selOperator');
    const plSel = document.getElementById('selPlatform');
    d.operators.forEach(o => { opSel.innerHTML += `<option value="${o}">${o}</option>`; });
    d.platforms.forEach(p => { plSel.innerHTML += `<option value="${p}">${p}</option>`; });
  } catch(e) {}
}

async function loadPosts() {
  setLoading(true);
  const params = buildParams();
  try {
    const r = await fetch(`/api/posts?${params}`);
    const d = await r.json();
    if (d.error) { showError(d.error); setLoading(false); return; }
    allData = d.posts;
    renderTable(false);
  } catch(e) { showError(e.toString()); }
  setLoading(false);
}

async function runAI() {
  setLoading(true);
  document.getElementById('btnAI').disabled = true;
  const params = buildParams();
  try {
    const r = await fetch(`/api/classify?${params}`);
    const d = await r.json();
    if (d.error) { showError(d.error); setLoading(false); document.getElementById('btnAI').disabled = false; return; }
    allData = d.posts;
    updateStats(d.total, d.mismatch, d.ai_error);
    renderTable(true);
    if (d.ai_error) showError('AI 出錯: ' + d.ai_error);
  } catch(e) { showError(e.toString()); }
  setLoading(false);
  document.getElementById('btnAI').disabled = false;
}

function buildParams() {
  const op = document.getElementById('selOperator').value;
  const pl = document.getElementById('selPlatform').value;
  const lm = document.getElementById('numLimit').value;
  return new URLSearchParams({operator:op, platform:pl, limit:lm});
}

function setFilter(f, el) {
  currentFilter = f;
  document.querySelectorAll('.filter-chip').forEach(c => c.classList.remove('active'));
  el.classList.add('active');
  renderRows(allData.length > 0 && allData[0].ai_cat !== undefined);
}

function renderTable(hasAI) {
  document.getElementById('mainTable').style.display = 'table';
  document.getElementById('emptyMsg').style.display = 'none';
  document.getElementById('filterBar').style.display = hasAI ? 'flex' : 'none';
  if (!hasAI) document.getElementById('statsBar').style.display = 'none';
  renderRows(hasAI);
}

function renderRows(hasAI) {
  const tbody = document.getElementById('tableBody');
  tbody.innerHTML = '';
  let filtered = allData;
  if (hasAI) {
    if (currentFilter === 'mismatch') filtered = allData.filter(p => !p.match);
    if (currentFilter === 'match')    filtered = allData.filter(p =>  p.match);
  }
  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:40px;color:var(--muted)">冇符合條件嘅結果</td></tr>';
    return;
  }
  filtered.forEach((p, i) => {
    const ruleLabel = p.rule_sub || p.rule_cat || '?';
    const aiLabel   = p.ai_cat || '—';
    const isMismatch = hasAI && !p.match;
    const tr = document.createElement('tr');
    if (isMismatch) tr.className = 'mismatch-row';
    tr.innerHTML = `
      <td style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:var(--muted)">${i+1}</td>
      <td>
        <span class="tag tag-op">${p.operator||'—'}</span><br>
        <span class="tag tag-plat" style="margin-top:4px">${p.platform||'—'}</span>
      </td>
      <td class="title-cell">
        <div class="title-text">${esc(p.title||'(無標題)')}</div>
        <div class="desc-text">${esc(p.description||'')}</div>
      </td>
      <td><span class="cat-tag cat-${ruleLabel}">${ruleLabel}</span></td>
      <td>
        ${hasAI ? `<span class="cat-tag cat-${aiLabel}">${aiLabel}</span>
        <div class="reason-text">${esc(p.ai_reason||'')}</div>` : '<span style="color:var(--muted)">—</span>'}
      </td>
      <td class="match-icon">${hasAI ? (isMismatch ? '❌' : '✅') : '—'}</td>
    `;
    tbody.appendChild(tr);
  });
}

function updateStats(total, mismatch, aiErr) {
  document.getElementById('statsBar').style.display = 'flex';
  document.getElementById('statTotal').textContent = total;
  document.getElementById('statMatch').textContent = total - mismatch;
  document.getElementById('statMismatch').textContent = mismatch;
  if (aiErr) {
    document.getElementById('aiErrStat').style.display = 'flex';
    document.getElementById('statAiErr').textContent = aiErr.substring(0,40);
  }
}

function setLoading(on) {
  document.getElementById('emptyMsg').style.display = on ? 'block' : 'none';
  if (on) {
    document.getElementById('emptyMsg').className = 'loading';
    document.getElementById('emptyMsg').textContent = '⏳ 載入中...';
    document.getElementById('mainTable').style.display = 'none';
  }
}

function showError(msg) {
  document.getElementById('errorMsg').innerHTML = `<div class="error">⚠️ ${esc(msg)}</div>`;
  setTimeout(() => document.getElementById('errorMsg').innerHTML = '', 8000);
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

loadOptions();
</script>
</body>
</html>'''


# ── 啟動 ──────────────────────────────────────────────────
if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        print(f"⚠️  搵唔到 DB: {DB_PATH}")
        print(f"   請將 macau_analytics.db 放喺同一資料夾，或修改腳本頂部嘅 DB_PATH")
        sys.exit(1)

    print(f"✅ DB: {DB_PATH}")
    print(f"🚀 Server 啟動中... http://localhost:{PORT}")
    print(f"   按 Ctrl+C 停止")
    HTTPServer(('localhost', PORT), Handler).serve_forever()
