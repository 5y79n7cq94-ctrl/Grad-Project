# 澳門活動監察系統

這個 repo 現在包含兩條並列能力：

- `Market Report`
  - 原有的官方帳號活動監察與 AI 摘要面板
- `Full-Web Heat Analysis`
  - 基於 MediaCrawler 已跑好的 `wb + fb` 全網熱度分析面板
  - 支援 weekly Sunday-to-Saturday snapshot、`Update Database`、`Run Analysis`、Leaderboard 與 Trend Analysis

---

## 前置條件

- 已安裝並設定好 [MediaCrawler](https://github.com/NanmiCoder/MediaCrawler)
- 已在 MediaCrawler 完成 XHS 同微博掃碼登入
- Full-Web Heat Analysis 需要 MediaCrawler 內現成的熱度分析資料庫：
  - 預設使用 `MediaCrawler/data/project/social_media_analytics.db`
- 如要使用原本的 AI 摘要功能，仍然需要有餘額的 [DeepSeek API Key](https://platform.deepseek.com)

---

## 安裝步驟

### 1. Clone repo

```bash
git clone https://github.com/FHLam727/Grad-Project.git
```

### 2. 覆蓋 MediaCrawler 修改過的檔案

這個項目對 MediaCrawler 有三個檔案改動，需要覆蓋：

```bash
cp mediacrawler_patches/config/base_config.py /path/to/MediaCrawler/config/
cp mediacrawler_patches/media_platform/weibo/client.py /path/to/MediaCrawler/media_platform/weibo/
cp mediacrawler_patches/media_platform/weibo/core.py /path/to/MediaCrawler/media_platform/weibo/
```

**改動摘要：**

| 檔案 | 改動內容 |
|------|----------|
| `config/base_config.py` | `CRAWLER_MAX_NOTES_COUNT` 由 `15` 改成 `50` |
| `media_platform/weibo/client.py` | `get_all_notes_by_creator()` 加 `max_count` 上限，到上限就停止爬蟲 |
| `media_platform/weibo/core.py` | 爬取時傳入 `config.CRAWLER_MAX_NOTES_COUNT`，改用固定 sleep interval |

### 3. 安裝額外依賴

在 Grad-Project 或 MediaCrawler 可用的 Python 環境安裝：

```bash
pip install -r requirements_extra.txt
```

如果要完整支援 Full-Web Heat Analysis，環境亦需要：

- `jieba`
- MediaCrawler 既有 `.venv` 中已安裝的分析依賴

最簡單做法是直接使用 MediaCrawler 的虛擬環境啟動本專案。

### 4. 設定 DeepSeek API Key

如果你要使用原本的 AI 摘要分析，打開 [`bridge.py`](./bridge.py) 內的 API key 設定改成自己的 key。

如果你只使用 `Full-Web Heat Analysis`，即使沒有 `openai` 套件，頁面也可以正常啟動和使用。

---

## 啟動方式

### 推薦方式：直接用啟動腳本

在本 repo 根目錄執行：

```bash
./start_panel.sh
```

這個腳本會：

- 自動尋找 `MediaCrawler`
- 使用 `MediaCrawler/.venv/bin/python`
- 啟動 FastAPI server
- 預設監聽 `http://127.0.0.1:9038`

如果你的 MediaCrawler 不在預設位置，先設定：

```bash
export MEDIACRAWLER_ROOT=/path/to/MediaCrawler
./start_panel.sh
```

如果你想改主項目自己的 SQLite 路徑，可以額外設定：

```bash
export MACAU_ANALYTICS_DB_PATH=/path/to/macau_analytics.db
```

### 傳統方式

```bash
python bridge.py
```

但這種方式需要你當前 Python 環境已經裝齊：

- `fastapi`
- `uvicorn`
- `pandas`
- `openpyxl`
- `jieba`
- 如要用 AI 摘要則還要有 `openai`

---

## 如何真正打開

server 啟動後，直接用瀏覽器打開：

- 主面板：
  - `http://127.0.0.1:9038/`
- Market Report 別名：
  - `http://127.0.0.1:9038/project`
- Full-Web Heat Analysis：
  - `http://127.0.0.1:9038/full-web-heat-analysis`
- Trend Analysis：
  - `http://127.0.0.1:9038/full-web-heat-analysis/trends`

你也可以先進主面板，再點新增的 `Full-Web Heat Analysis` 按鈕。

---

## Full-Web Heat Analysis 資料來源

Full-Web Heat Analysis 現時直接讀取 MediaCrawler 已跑好的主分析庫：

```text
MediaCrawler/data/project/social_media_analytics.db
```

目前已接通的能力：

- weekly snapshot 狀態
  - `To Be Updated`
  - `To Be Analyzed`
  - `Completed`
  - `Future`
- `Update Database`
  - 會呼叫 MediaCrawler 既有 pipeline
- `Run Analysis`
  - 會對選中的 Sunday-to-Saturday week 生成 cluster
- Event / Topic leaderboard
- Weekly trend analysis

---

### 分類誤判測試工具

```bash
python classifier_tester.py
```

瀏覽器開 `http://localhost:8765`

- **只看規則分類** — 查看現有關鍵字規則點分類每條帖文，不 call AI
- **AI 對比分析** — 同時用 DeepSeek 獨立分類，對比差異，紅色高亮顯示誤判

---

## 檔案說明

| 檔案 | 功能 |
|------|------|
| `bridge.py` | 主 FastAPI server，現在同時提供 `Market Report` 同 `Full-Web Heat Analysis` |
| `db_manager.py` | 所有 DB 操作：入庫、查詢、日期解析、backfill |
| `task_manager.py` | 控制 MediaCrawler 爬蟲，管理爬取順序同入庫邏輯 |
| `operation_panel.html` | 前端介面，選擇運營商/類別/日期範圍查看活動 |
| `webui/heat_analysis.html` | Full-Web Heat Analysis leaderboard 頁面 |
| `webui/heat_trends.html` | Full-Web Heat Analysis trend 頁面 |
| `static/heat_analysis.js` | Full-Web Heat Analysis 前端邏輯 |
| `static/heat_trends.js` | Trend Analysis 前端邏輯 |
| `static/heat_analysis.css` | Full-Web Heat Analysis / Trend Analysis 共用樣式 |
| `heat_analysis_adapter.py` | 將 Grad-Project 橋接到 MediaCrawler 熱度分析 backend |
| `heat_analysis_jobs.py` | `Update Database` 的 weekly background jobs |
| `start_panel.sh` | 推薦啟動方式，會自動使用 MediaCrawler 的 `.venv` |
| `macau_analytics.db` | SQLite 資料庫，儲存所有爬取帖文同政府活動數據 |
| `mediacrawler_patches/` | 修改過的 MediaCrawler 原始檔案，需覆蓋到對應路徑 |

---

## 常見問題

**DeepSeek 出現 402 錯誤**
→ DeepSeek 帳戶餘額不足，前往 [platform.deepseek.com](https://platform.deepseek.com) 充值

**爬不到 XHS 或微博**
→ 登入狀態過期，重新在 MediaCrawler 掃碼登入

**bridge.py 跑不了**
→ 最穩陣係直接用 `./start_panel.sh`

**Full-Web Heat Analysis 打不開**
→ 確認 `MEDIACRAWLER_ROOT` 指向正確的 MediaCrawler 根目錄

**Full-Web Heat Analysis 冇數據**
→ 確認 `MediaCrawler/data/project/social_media_analytics.db` 已經有跑好的 `wb / fb` cluster 數據

**分類全都是 experience**
→ 正常，`experience` 係預設分類（帖文冇 match 任何關鍵字時）

---

## 運營商帳號覆蓋

| 運營商 | XHS | 微博 |
|--------|-----|------|
| 永利 Wynn | ✅ | ✅ |
| 金沙 Sands | ✅ | ✅ |
| 銀河 Galaxy | ✅ | ✅ |
| 美高梅 MGM | ✅ | ✅ |
| 新濠 Melco | ✅ | ✅ |
| 葡京 SJM | ✅ | ✅ |
| 政府旅遊局 | ✅ | ✅ |
