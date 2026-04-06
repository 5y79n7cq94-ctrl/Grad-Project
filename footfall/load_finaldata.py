"""
加载 finaldata1：支持真 CSV / xlsx；若扩展名为 .csv 实为 Apple Numbers 压缩包，则用 numbers-parser。
numbers_parser 要求文件以 .numbers 为后缀，故会复制到临时文件再读。
成功解析后可写出 finaldata1_exported.csv 便于 Excel 编辑。
"""

from __future__ import annotations

import io
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

FOOTFALL_DIR = Path(__file__).resolve().parent

def _is_zip_file(path: Path) -> bool:
    return path.is_file() and zipfile.is_zipfile(path)

def _load_numbers(path: Path) -> pd.DataFrame:
    try:
        from numbers_parser import Document
    except ImportError as e:
        raise ImportError(
            "finaldata1 为 Apple Numbers 格式（zip），需要安装：pip install numbers-parser\n"
            "或在 Numbers 中「文件 → 导出到 → CSV」得到真 CSV 后覆盖 footfall/finaldata1.csv"
        ) from e

    path = Path(path)
    if path.suffix.lower() != ".numbers":
        fd, tmp = tempfile.mkstemp(suffix=".numbers")
        os.close(fd)
        tmp_path = Path(tmp)
        try:
            shutil.copy2(path, tmp_path)
            doc = Document(str(tmp_path))
        finally:
            tmp_path.unlink(missing_ok=True)
    else:
        doc = Document(str(path))
    rows: list[list] = []
    for t in doc.sheets[0].tables:
        for r in range(t.num_rows):
            rows.append([t.cell(r, c).value for c in range(t.num_cols)])
    if not rows:
        raise ValueError("Numbers 文件无表格数据")
    header = [str(x).strip() if x is not None else "" for x in rows[0]]
    data = rows[1:]
    return pd.DataFrame(data, columns=header)

def _strip_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lstrip("\ufeff").strip() for c in df.columns]
    return df

def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_column_names(df)
    if "Date" in df.columns:
        return df
    lower = {str(c).strip().lower(): c for c in df.columns}
    if "date" in lower:
        return df.rename(columns={lower["date"]: "Date"})
    for c in df.columns:
        if str(c).strip() in ("日期", "时间", "日期時間"):
            return df.rename(columns={c: "Date"})
    raise ValueError(
        "数据需包含 Date 列（当前列名: "
        + ", ".join(map(str, list(df.columns)[:12]))
        + ("…" if len(df.columns) > 12 else "")
        + "）。若用 Numbers 导出 CSV，请删除首行表名单元格或让第二行以 Date 开头。"
    )

def _read_plain_csv(path: Path) -> pd.DataFrame:
    """
    Numbers 导出时常在第一行写表名（如 finaldata1），整行无逗号；第二行才是表头 Date,...
    """
    raw = path.read_bytes()
    text = None
    for enc in ("utf-8-sig", "utf-8", "gbk"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = raw.decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        raise ValueError("CSV 为空")
    skiprows = 0
    first = lines[0].strip()
    # 首行无逗号 → 视为表题，跳过
    if "," not in first:
        skiprows = 1
    else:
        low = first.lower()
        if not low.startswith("date") and "date" not in low.split(",")[0].lower():
            # 首行有逗号但第一列不是 Date，仍尝试跳过一行（兼容杂项标题行）
            peek = lines[1] if len(lines) > 1 else ""
            if peek.strip().lower().startswith("date"):
                skiprows = 1
    return pd.read_csv(io.StringIO("\n".join(lines)), skiprows=skiprows)

def load_finaldata_df(
    path: Path | None = None,
    *,
    export_csv: Path | None = None,
) -> pd.DataFrame:
    """
    默认读取 footfall/finaldata1.csv（或同目录 finaldata1.xlsx）。
    Apple Numbers 误存为 .csv 时自动用 numbers-parser。
    export_csv 若给定，将把规范化后的表写出（utf-8-sig）。
    """
    if path is None:
        for name in ("finaldata1.csv", "finaldata1.xlsx", "finaldata1.numbers"):
            p = FOOTFALL_DIR / name
            if p.is_file():
                path = p
                break
        if path is None:
            raise FileNotFoundError(f"在 {FOOTFALL_DIR} 未找到 finaldata1.csv / .xlsx / .numbers")

    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)

    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    elif _is_zip_file(path):
        try:
            df = pd.read_excel(path, engine="openpyxl")
        except Exception:
            df = _load_numbers(path)
    else:
        df = _read_plain_csv(path)

    df = _ensure_date_column(df)
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)
    df = df.sort_values("Date").reset_index(drop=True)
    # 宏观列允许个别日期空缺：用阶梯前向/后向填满，避免 Prophet 回归项出现 NaN
    for c in ("EXCHANGE_RATE", "PRICE_INDEX"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df[c] = df[c].ffill().bfill()

    if export_csv is not None:
        export_csv = Path(export_csv)
        export_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(export_csv, index=False, encoding="utf-8-sig")
    return df


def continuous_values_for_date(
    df: pd.DataFrame,
    ds: str,
    *,
    cols: tuple[str, ...] = ("EXCHANGE_RATE", "PRICE_INDEX"),
) -> dict[str, float]:
    """对每个连续变量，在 Date≤ds 的范围内取「该列最后一个非缺失值」（避免 Prophet 报 NaN）。"""
    ts = pd.to_datetime(ds).normalize()
    d = df.copy()
    d["Date"] = pd.to_datetime(d["Date"]).dt.normalize()
    past = d[d["Date"] <= ts].sort_values("Date")
    if past.empty:
        raise ValueError(f"没有 {ts.date()} 及之前的汇率/物价指数数据")
    out: dict[str, float] = {}
    for c in cols:
        if c not in d.columns:
            raise KeyError(f"数据缺少列: {c}")
        ser = pd.to_numeric(past[c], errors="coerce")
        valid = ser.dropna()
        if valid.empty:
            raise ValueError(f"列 {c} 在 {ts.date()} 及之前无有效数值")
        out[c] = float(valid.iloc[-1])
    return out
