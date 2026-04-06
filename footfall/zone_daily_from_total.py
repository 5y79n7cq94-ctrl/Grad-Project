from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

COTAI_ZH = "路氹填海區"
NAM_VAN_ZH = "外港及南灣湖新填海區"


def _load_share_lookup(csv_path: str | Path):
    long = pd.read_csv(csv_path)
    long["月份"] = long["月份"].astype(str).str.strip()
    mcol = long["月份"]

    if mcol.str.match(r"^\d{4}-\d{2}$").all():
        long["period"] = pd.to_datetime(mcol + "-01")
        wide = (
            long.pivot_table(index="period", columns="统计分区", values="游客占比", aggfunc="first")
            .reset_index()
        )
        wide.columns.name = None
        wide = wide.rename(columns={COTAI_ZH: "share_cotai", NAM_VAN_ZH: "share_namvan"})
        return wide[["period", "share_cotai", "share_namvan"]], "period"

    long["month"] = long["月份"].astype(int)
    wide = (
        long.pivot_table(index="month", columns="统计分区", values="游客占比", aggfunc="first")
        .reset_index()
    )
    wide.columns.name = None
    wide = wide.rename(columns={COTAI_ZH: "share_cotai", NAM_VAN_ZH: "share_namvan"})
    return wide[["month", "share_cotai", "share_namvan"]], "month"


def split_forecast_by_zone_shares(
    forecast_df: pd.DataFrame,
    zone_csv: str | Path,
    *,
    yhat_col: str = "yhat_original",
    ds_col: str = "ds",
) -> pd.DataFrame:
    lookup, how = _load_share_lookup(zone_csv)
    out = forecast_df.copy()
    out[ds_col] = pd.to_datetime(out[ds_col])

    if how == "period":
        out["period"] = out[ds_col].dt.to_period("M").dt.to_timestamp()
        out = out.merge(lookup, on="period", how="left")
        out["share_cotai"] = out["share_cotai"].ffill().bfill()
        out["share_namvan"] = out["share_namvan"].ffill().bfill()
        out = out.drop(columns=["period"])
    else:
        out["month"] = out[ds_col].dt.month
        out = out.merge(lookup, on="month", how="left")
        out = out.drop(columns=["month"])
        out["share_cotai"] = out["share_cotai"].ffill().bfill()
        out["share_namvan"] = out["share_namvan"].ffill().bfill()

    out[f"visitation_{COTAI_ZH}"] = out[yhat_col] * out["share_cotai"]
    out[f"visitation_{NAM_VAN_ZH}"] = out[yhat_col] * out["share_namvan"]
    return out


def save_district_daily_csv(
    forecast_with_zones: pd.DataFrame,
    out_path: str | Path,
    *,
    ds_col: str = "ds",
) -> None:
    """写出日期、全澳预测、两区预测，便于 Excel。"""
    cols = [ds_col, "share_cotai", "share_namvan", f"visitation_{COTAI_ZH}", f"visitation_{NAM_VAN_ZH}"]
    extra = [c for c in forecast_with_zones.columns if c in ("yhat_original", "yhat") and c not in cols]
    use = [c for c in [ds_col] + extra + cols[1:] if c in forecast_with_zones.columns]
    forecast_with_zones[use].to_csv(out_path, index=False, encoding="utf-8-sig")
