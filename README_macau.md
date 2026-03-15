# 澳門活動監察系統

爬取澳門六大運營商（永利、金沙、銀河、美高梅、新濠、葡京）及政府旅遊局的 XHS 同微博官方帳號，自動分類活動並透過介面展示。

---

## 前置條件

- 已安裝並設定好 [MediaCrawler](https://github.com/NanmiCoder/MediaCrawler)
- 已在 MediaCrawler 完成 XHS 同微博掃碼登入
- 擁有有餘額的 [DeepSeek API Key](https://platform.deepseek.com)

---

## 安裝步驟

### 1. Clone repo

```bash
git clone https://github.com/你的帳號/macau-analytics.git
```

### 2. 將核心檔案複製入 MediaCrawler 根目錄

```bash
cp bridge.py db_manager.py task_manager.py operation_panel.html classifier_tester.py macau_analytics.db /path/to/MediaCrawler/
```

### 3. 覆蓋 MediaCrawler 修改過的檔案

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

### 4. 安裝額外依賴

在 MediaCrawler 根目錄執行：

```bash
pip install -r requirements_extra.txt
```

### 5. 設定 DeepSeek API Key

打開 `bridge.py`，第 27 行換成自己的 Key：

```python
client = OpenAI(api_key="你的KEY", base_url="https://api.deepseek.com")
```

---

## 使用方式

### 主介面

```bash
python bridge.py
```

然後直接用瀏覽器打開 `operation_panel.html`（不需要額外 server）

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
| `bridge.py` | FastAPI server，處理前端請求、call DeepSeek、返回活動 cards |
| `db_manager.py` | 所有 DB 操作：入庫、查詢、日期解析、backfill |
| `task_manager.py` | 控制 MediaCrawler 爬蟲，管理爬取順序同入庫邏輯 |
| `operation_panel.html` | 前端介面，選擇運營商/類別/日期範圍查看活動 |
| `classifier_tester.py` | 本地測試工具，對比規則分類同 AI 分類結果 |
| `macau_analytics.db` | SQLite 資料庫，儲存所有爬取帖文同政府活動數據 |
| `mediacrawler_patches/` | 修改過的 MediaCrawler 原始檔案，需覆蓋到對應路徑 |

---

## 常見問題

**DeepSeek 出現 402 錯誤**
→ DeepSeek 帳戶餘額不足，前往 [platform.deepseek.com](https://platform.deepseek.com) 充值

**爬不到 XHS 或微博**
→ 登入狀態過期，重新在 MediaCrawler 掃碼登入

**bridge.py 跑不了**
→ 確認已安裝 `requirements_extra.txt` 所有依賴，並在 MediaCrawler 根目錄執行

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
