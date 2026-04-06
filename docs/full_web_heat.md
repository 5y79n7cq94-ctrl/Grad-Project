# Full-Web Heat Analysis

這個 repo 目前有兩條並列能力：

- `Market Report`
  - 原有官方帳號活動監察與 AI 摘要面板
- `Full-Web Heat Analysis`
  - 你的 `wb + fb` weekly / monthly 全網熱度分析面板
  - 支援 Sunday-to-Saturday weekly snapshot、calendar-month monthly snapshot、`Run Analysis`、Leaderboard 與 Trend Analysis

## 現在的交接方式

Full-Web Heat Analysis 的**後端程式碼已經完整內嵌在 `Grad-Project`**：

- 不再依賴外部 repo 的 `project_analytics.py`
- 不再需要啟動時先找到另一份 MediaCrawler 程式碼
- 啟動只需要本 repo 自己的 Python 環境

但要明確區分兩件事：

1. `程式碼`
   - 現在已經在主 repo 內
2. `分析資料庫`
   - 不在 GitHub 內建檔案裡

Full-Web 預設會讀取：

```text
./data/social_media_analytics.db
```

也可以透過環境變數改路徑：

```bash
export PROJECT_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db
```

## 為什麼資料庫沒有直接跟 repo 一起放

完整的 `social_media_analytics.db` 太大，超過一般 GitHub 單檔直接版本控制的實際交付上限。  
我已經實測過，把 Full-Web 所需主要表抽成精簡版後，檔案仍然大約 **423 MB**，不適合直接當普通 Git 檔案推進 repo。

所以目前最穩的交接方式是：

- `Grad-Project` 負責完整程式碼與頁面
- `social_media_analytics.db` 作為單獨交付件提供

這樣隊友 pull repo 之後，只要把資料庫放到 `data/`，就能直接使用 Full-Web。

## 前置條件

- Python 3
- 安裝 `requirements_extra.txt`
- 一份可用的 `social_media_analytics.db`

如果要用原本的 AI 摘要功能，另外仍然需要自己的 API key；  
但 **只使用 Full-Web Heat Analysis** 時，即使沒有 `openai` 套件，也不會在 import 階段直接炸掉。

## 安裝與啟動

### 1. Clone repo

```bash
git clone https://github.com/FHLam727/Grad-Project.git
cd Grad-Project
```

### 2. 安裝依賴

```bash
python3 -m pip install -r requirements_extra.txt
```

### 3. 放入 Full-Web 分析資料庫

把你的資料庫檔案放到：

```text
data/social_media_analytics.db
```

或設定：

```bash
export PROJECT_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db
```

### 4. 啟動

```bash
./start_panel.sh
```

這個腳本會優先使用：

- `./.venv/bin/python`
- 否則退回系統 `python3`

並在啟動前檢查：

- `fastapi`
- `uvicorn`
- `pandas`
- `python-dotenv`
- `jieba`

## 進入頁面

啟動後打開：

- 主面板：
  - `http://127.0.0.1:9038/`
- Full-Web Heat Analysis：
  - `http://127.0.0.1:9038/full-web-heat-analysis`
- Full-Web Trend Analysis：
  - `http://127.0.0.1:9038/full-web-heat-analysis/trends`

## 目前已自包含的能力

- Full-Web weekly / monthly windows
- `To Be Updated / To Be Analyzed / Completed / Future` 狀態
- Event / Topic leaderboard
- Weekly / monthly trend analysis
- `Run Analysis`

## 目前仍然需要額外配置的部分

`Update Database` 是 crawler 入口，不是純讀庫功能。  
如果你希望這個按鈕真的能一鍵更新，仍要另外設定實際抓取命令：

```bash
export FULL_WEB_WB_UPDATE_COMMAND='...'
export FULL_WEB_FB_UPDATE_COMMAND='...'
```

如果沒有設這兩個環境變數：

- 頁面仍然可以正常打開
- 已有數據仍可正常查看與 `Run Analysis`
- 但 `Update Database` 會回傳清楚的設定提示，而不是去找不存在的外部腳本

## 主要檔案

| 檔案 | 功能 |
|------|------|
| `bridge.py` | 主 FastAPI server，同時提供原本 Market Report 與 Full-Web Heat Analysis |
| `full_web_backend/` | Full-Web 內嵌分析 backend |
| `full_web_heat_adapter.py` | Full-Web 相容入口，已不再橋接外部 repo |
| `full_web_heat_jobs.py` | Full-Web weekly update job 相容 wrapper |
| `webui/full_web_heat_analysis.html` | Full-Web leaderboard 頁面 |
| `webui/full_web_heat_trends.html` | Full-Web trend 頁面 |
| `static/full_web_heat_analysis.js` | Full-Web leaderboard 前端邏輯 |
| `static/full_web_heat_trends.js` | Full-Web trend 前端邏輯 |
| `static/full_web_heat.css` | Full-Web 共用樣式 |
| `data/social_media_analytics.db` | Full-Web 分析資料庫預設路徑 |

## 風險說明

這次我已經把「程式碼依賴另一個 repo」這個問題解掉了。  
現在剩下的唯一主要交接風險是：

- **Full-Web 的資料庫檔案太大，無法自然地跟 repo 一起當普通 Git 檔案交付**

所以如果隊友 pull repo 後沒有另外拿到 `social_media_analytics.db`：

- 頁面可以打開
- 但不會自帶你現在這套 Full-Web 分析結果
