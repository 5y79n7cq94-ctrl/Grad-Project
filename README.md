# 澳門活動監察系統

爬取澳門六大運營商（永利、金沙、銀河、美高梅、新濠、葡京）及政府旅遊局的 XHS、微博、Instagram、Facebook 官方帳號，自動分類活動並透過介面展示，支援熱度排行與 AI 活動摘要。

---

## 模組說明

本項目目前有兩條不同的 heat 能力，命名上已刻意區分：

- `heat_analyzer.py` / `heat_leaderboard_v2.html`
  - 原有的活動級 heat 排行能力
- `Full-Web Heat Analysis`
  - 使用獨立的 `social_media_analytics.db` 做 weekly / monthly 全網熱度面板
  - Full-Web 後端程式已內嵌在 `Grad-Project`，不再依賴外部 `project_analytics.py`
  - 詳細說明見 [docs/full_web_heat.md](./docs/full_web_heat.md)

---

## 前置條件

- 已安裝並設定好 [MediaCrawler](https://github.com/NanmiCoder/MediaCrawler)（XHS / 微博）
- 已在 MediaCrawler 完成 XHS 同微博掃碼登入
- 擁有有餘額的 [DeepSeek API Key](https://platform.deepseek.com)（活動分類與摘要）
- 擁有 [Apify API Token](https://apify.com)（IG / FB 爬取）
- 擁有 [阿里雲 DashScope API Key](https://dashscope.console.aliyun.com)（圖片 OCR 及文字 Embedding）

---

## 安裝步驟

### 1. Clone repo

```bash
git clone https://github.com/FHLam727/Grad-Project.git
```

### 2. 將核心檔案複製入 MediaCrawler 根目錄

```bash
cp bridge.py db_manager.py task_manager.py post_normalizer.py \
   media_analyzer.py heat_analyzer.py process_events.py \
   trad_simp.py classifier_tester.py \
   operation_panel.html heat_leaderboard_v2.html \
   admin_page.html login_page.html \
   macau_analytics.db negative_monitor.html \
   /path/to/MediaCrawler/

cp -R footfall /path/to/MediaCrawler/
```

### 3. 覆蓋 MediaCrawler 修改過的檔案

```bash
cp mediacrawler_patches/config/base_config.py /path/to/MediaCrawler/config/
cp mediacrawler_patches/config/weibo_config.py /path/to/MediaCrawler/config/
cp mediacrawler_patches/config/xhs_config.py /path/to/MediaCrawler/config/
cp mediacrawler_patches/media_platform/weibo/client.py /path/to/MediaCrawler/media_platform/weibo/
cp mediacrawler_patches/media_platform/weibo/core.py /path/to/MediaCrawler/media_platform/weibo/
cp mediacrawler_patches/media_platform/xhs/login.py /path/to/MediaCrawler/media_platform/xhs/
cp mediacrawler_patches/media_platform/xhs/core.py /path/to/MediaCrawler/media_platform/xhs/
cp mediacrawler_patches/tools/negative_monitor_date.py /path/to/MediaCrawler/tools/
```

**改動摘要：**

| 檔案 | 改動內容 |
|------|----------|
| `config/base_config.py` | `CRAWLER_MAX_NOTES_COUNT` 由 `15` 改為 `50`；`ENABLE_GET_IMAGES` 改為 `True` |
| `media_platform/weibo/client.py` | `get_all_notes_by_creator()` 加入 `max_count` 上限 |
| `media_platform/weibo/core.py` | 爬取時傳入 `config.CRAWLER_MAX_NOTES_COUNT`，改用固定 sleep interval |

### 4. 安裝額外依賴

```bash
pip install -r requirements_extra.txt
pip install openai python-dotenv httpx pillow opencc-python-reimplemented scikit-learn numpy
```

### 5. 設定 API Keys

在 MediaCrawler 根目錄建立 `.env` 檔案：

```
DASHSCOPE_API_KEY=sk-xxxx        # 阿里雲 DashScope（圖片 OCR、Embedding）
DEEPSEEK_API_KEY=sk-xxxx         # DeepSeek（活動分析與摘要）
DEEPSEEK_BASE_URL=https://api.deepseek.com
DB_PATH=C:/Users/user/MediaCrawler/macau_analytics.db
```

打開 `task_manager.py`，換成自己的 Apify Token：

```python
APIFY_TOKEN = "你的TOKEN"
```

---

## 使用方式

### 啟動主服務

```bash
python bridge.py
```

服務預設運行於 `http://127.0.0.1:9038`，然後直接用瀏覽器打開 `operation_panel.html`。

### 圖片 OCR 分析（獨立執行）

```bash
# 分析所有未處理帖文
python media_analyzer.py --limit 100

# 只做 dry-run 查看待處理列表
python media_analyzer.py --dry-run

# 清除失敗記錄並重新執行
python media_analyzer.py --reset-failed --limit 100
```

建議每次爬取新帖後執行一次 `media_analyzer.py`，補充圖片文字資料。

### 活動去重與聚類（獨立執行）

```bash
# 對所有帖文按內容相似度去重，寫入 events_deduped 表
python process_events.py --db macau_analytics.db

# 只處理特定運營商
python process_events.py --db macau_analytics.db --operator wynn

# 調整時間窗口（預設 90 天）
python process_events.py --db macau_analytics.db --window 60
```

### 熱度分析（獨立執行）

```bash
# 計算所有活動熱度並寫入資料庫
python heat_analyzer.py

# 查看 PCA 權重說明及 Top 15
python heat_analyzer.py --explain --top 15

# 調整半衰期（預設 14 天）
python heat_analyzer.py --half-life 7

# 只計算不寫入資料庫
python heat_analyzer.py --dry-run
```

熱度分析完成後會自動觸發 `POST /api/heat/leaderboard-ai/refresh`，無需手動刷新排行榜。

### 單獨重爬某個平台

```bash
# 只重爬微博（唔影響其他平台）
python -c "
from task_manager import _crawl_platform
_crawl_platform('wb', ['wynn','sands','galaxy','mgm','melco','sjm','government'], '')
"

# 重爬單一帳號（例如微博 UID 5577774461）
python main.py --platform wb --type creator --creator_id 5577774461 --headless 0
# 爬完後手動入庫
python -c "
from db_manager import ingest_crawler_data
ingest_crawler_data('data/weibo/json/creator_contents_5577774461.json', 'weibo', '', operator='melco')
"
```

### 分類誤判測試工具

```bash
python classifier_tester.py
```

瀏覽器打開 `http://localhost:8765`

---

## 檔案說明

| 檔案 | 功能 |
|------|------|
| `bridge.py` | FastAPI 服務（端口 9038），處理前端請求、調用 DeepSeek 分析、提供活動卡片及熱度排行 API |
| `db_manager.py` | 所有資料庫操作：入庫、查詢、日期解析、backfill |
| `task_manager.py` | 控制 MediaCrawler（XHS/微博）及 Apify（IG/FB）爬蟲 |
| `post_normalizer.py` | 將各平台原始帖文標準化入 `posts_*` 表（去 emoji、繁體化、清洗）|
| `media_analyzer.py` | 圖片 OCR 工具，用 Qwen-VL 提取圖片文字存入 `media_text` 欄位（base64 本地下載模式，繞過防盜鏈）|
| `process_events.py` | 基於帖文內容相似度（DashScope Embedding + SequenceMatcher fallback）去重聚類，寫入 `events_deduped` 表 |
| `heat_analyzer.py` | 活動熱度分析：per-platform PCA 權重 + 時間衰減 + 跨平台加成，寫入 `heat_score` 欄位 |
| `trad_simp.py` | 繁簡字符互轉工具（基於 opencc），支援搜尋時自動擴展繁簡變體 |
| `classifier_tester.py` | 本地測試工具，對比規則分類同 AI 分類結果 |
| `operation_panel.html` | 主前端介面，按運營商／類別／日期範圍查看活動卡片 |
| `heat_leaderboard_v2.html` | 熱度排行榜介面，展示 AI 活動級別排名及各分類 Top 3 |
| `admin_page.html` | 管理員介面，管理用戶帳號 |
| `login_page.html` | 用戶登入頁面 |
| `macau_analytics.db` | SQLite 資料庫，儲存所有爬取帖文、活動分組及熱度數據 |
| `mediacrawler_patches/` | 修改過的 MediaCrawler 原始檔案，需覆蓋到對應路徑 |

---

## 資料庫結構

```
macau_events              — 原始爬取數據（raw_json、event_date）
    ↓ post_normalizer.py
posts_xhs                 — XHS 標準化帖文
posts_ig                  — Instagram 標準化帖文
posts_fb                  — Facebook 標準化帖文
posts_weibo               — 微博標準化帖文
    ↓ process_events.py
events_deduped            — 去重後的活動分組（含 heat_score、heat_meta）
    ↓ bridge.py
analysis_cache            — DeepSeek 分析結果緩存（per operator × 日期範圍）
heat_leaderboard_cache    — AI 活動熱度排行緩存（24 小時 TTL，自動後台更新）
crawl_log                 — 記錄各運營商最後爬取時間（7 天有效期）
users                     — 用戶帳號（登入鑑權）
```

### 重要欄位說明

| 欄位 | 所在表 | 說明 |
|------|--------|------|
| `content` | `posts_*` | 清洗後純文字（去 emoji、去 hashtag、繁體化） |
| `media_urls` | `posts_*` | 圖片／影片 URL（JSON array） |
| `media_text` | `posts_*` | Qwen-VL OCR 從圖片提取的文字 |
| `embedding` | `posts_*` | DashScope text-embedding-v3 向量（用於去重相似度計算） |
| `raw_json` | `macau_events` | 原始爬取 JSON |
| `source_post_ids` | `events_deduped` | 組成該活動分組的所有帖文 ID（JSON array） |
| `heat_score` | `events_deduped` | 歸一化熱度分數（0–100） |
| `heat_meta` | `events_deduped` | 熱度計算元數據（平台列表、衰減系數、最新帖文日期） |

---

## AI 分析流程

```
posts_* (content + media_text)
    ↓ process_events.py：Embedding 相似度去重聚類
events_deduped
    ↓ heat_analyzer.py：PCA 熱度計算 → heat_score
    ↓ bridge.py /analyze：DeepSeek 提取活動資訊 → analysis_cache
    ↓ bridge.py /api/heat/leaderboard-ai：AI 活動排行 → heat_leaderboard_cache
前端 operation_panel.html / heat_leaderboard_v2.html 顯示
```

**API 端點說明（bridge.py，端口 9038）：**

| 端點 | 說明 |
|------|------|
| `GET /analyze` | 按運營商／類別／日期查詢活動，調用 DeepSeek 生成摘要 |
| `GET /api/heat/leaderboard` | 基於 `heat_score` 欄位的活動熱度排行（需先執行 `heat_analyzer.py`）|
| `GET /api/heat/leaderboard-ai` | AI 活動級別熱度排行（從資料庫緩存即時回傳）|
| `POST /api/heat/leaderboard-ai/refresh` | 強制重建 AI 排行緩存（`heat_analyzer.py` 完成後自動觸發）|

**圖片 OCR 整合邏輯：**
- `media_text` 由 Qwen-VL 從圖片自動 OCR 提取，可能含雜訊
- 只有當中同時出現明確活動名稱同日期時，DeepSeek 才參考，否則忽略
- 有助捕捉描述較短但海報資訊豐富的帖文

**分析緩存機制：**
- `analysis_cache`：同一運營商 + 日期範圍的 DeepSeek 結果緩存至 SQLite，避免重複調用
- `heat_leaderboard_cache`：AI 排行結果緩存 24 小時，逾期後自動後台更新並先回傳舊緩存

---

## 平台覆蓋

| 運營商 | XHS | 微博 | Instagram | Facebook |
|--------|-----|------|-----------|----------|
| 永利 Wynn | ✅ | ✅ | ✅ | ✅ |
| 金沙 Sands | ✅ | ✅ | ✅ | ✅ |
| 銀河 Galaxy | ✅ | ✅ | ✅ | ✅ |
| 美高梅 MGM | ✅ | ✅ | ✅ | ✅ |
| 新濠 Melco | ✅ | ✅ | ✅ | ✅ |
| 葡京 SJM | ✅ | ✅ | ✅ | ✅ |
| 政府旅遊局 | ✅ | ✅ | ✅ | ✅ |

---

## 活動分類

系統按以下分類自動識別帖文內容：

| 大類 | 子類 | 說明 |
|------|------|------|
| `entertainment` | `concert` | 演唱會、音樂會、舞台劇、音樂劇等 |
| `entertainment` | `sport` | 馬拉松、UFC、球類賽事、F1 等 |
| `entertainment` | `crossover` | 聯名、快閃、Pop-up 展覽 |
| `experience` | — | 沉浸式體驗、主題樂園、常駐表演 |
| `exhibition` | — | 藝術展、博物館、特展 |
| `food` | — | 餐廳、自助餐、品酒、雞尾酒活動 |
| `accommodation` | — | 酒店優惠、住宿套票 |
| `shopping` | — | 折扣、購物優惠 |
| `gaming` | — | 博彩、貴賓禮遇、積分兌換 |

---

## 常見問題

**DeepSeek 出現 402 錯誤**
→ 帳戶餘額不足，前往 [platform.deepseek.com](https://platform.deepseek.com) 充值

**爬不到 XHS 或微博**
→ 登入狀態過期，重新在 MediaCrawler 掃碼登入

**`media_analyzer.py` 圖片 OCR 失敗（400 Bad Request）**
→ 代碼已改用 base64 本地下載模式處理防盜鏈問題，重新執行即可
→ 如仍失敗，用 `--reset-failed` 清除記錄重試

**`process_events.py` 執行很慢**
→ 首次執行需為所有帖文調用 DashScope Embedding API，屬正常現象
→ Embedding 結果會緩存至各 `posts_*` 表的 `embedding` 欄位，之後執行速度會快很多

**熱度排行榜沒有數據**
→ 需先執行 `python heat_analyzer.py` 計算並寫入 `heat_score`
→ 之後 `bridge.py` 會自動觸發排行榜緩存更新

**`heat_leaderboard_v2.html` 顯示舊緩存**
→ 緩存超過 24 小時後系統會後台自動重建，稍後重新整理頁面即可
→ 或手動執行：`POST http://127.0.0.1:9038/api/heat/leaderboard-ai/refresh`

**分類全都是 experience**
→ 正常，`experience` 係預設分類（帖文沒有符合任何關鍵字時）
→ 可用 `classifier_tester.py` 測試及調整分類規則

**`posts_*` 表的 `media_text` 全部是 NULL**
→ 需要執行 `python media_analyzer.py` 填充圖片 OCR 結果

**Paraformer 影片語音轉錄 Connection error**
→ 阿里雲 Paraformer `/audio/transcriptions` 端點澳門連接不上，已移除此功能
→ 影片帖文改為 OCR 封面圖提取文字
