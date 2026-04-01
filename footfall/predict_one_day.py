from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from feature_config import AI_BINARY_KEYS, CONTINUOUS_COLUMNS, REGRESSOR_COLUMNS
from zone_daily_from_total import split_forecast_by_zone_shares


def _default_zone_csv() -> Path:
    return Path(__file__).resolve().parent / "zone_table1_monthly_share.csv"


def weekday_regressor_flags(ds: str | pd.Timestamp) -> dict[str, int]:
    d = pd.to_datetime(ds)
    wd = int(d.weekday())
    return {
        "IS_SATURDAY": 1 if wd == 5 else 0,
        "IS_SUNDAY": 1 if wd == 6 else 0,
    }


def merge_regressors_for_prediction(
    ds: str | pd.Timestamp,
    partial_ai_five: dict[str, int] | None,
    continuous: dict[str, float],
) -> dict[str, float | int]:
    partial_ai_five = partial_ai_five or {}
    out: dict[str, float | int] = {}
    for k in AI_BINARY_KEYS:
        out[k] = int(partial_ai_five.get(k, 0)) & 1
    out.update(weekday_regressor_flags(ds))
    for k in CONTINUOUS_COLUMNS:
        out[k] = float(continuous[k])
    ordered: dict[str, float | int] = {c: out[c] for c in REGRESSOR_COLUMNS}
    return ordered


def build_future_row(
    ds: str | pd.Timestamp,
    regressors: dict[str, Any] | None = None,
    *,
    order: list[str] | None = None,
) -> pd.DataFrame:
    """构造 Prophet predict 所需的一行"""
    cols = order or REGRESSOR_COLUMNS
    reg = regressors or {}
    row: dict[str, Any] = {"ds": pd.to_datetime(ds)}
    for c in cols:
        if c not in reg:
            raise ValueError(f"缺少回归列: {c}")
        if c in CONTINUOUS_COLUMNS:
            row[c] = float(reg[c])
        else:
            v = int(reg[c]) & 1
            row[c] = v
    return pd.DataFrame([row])


def predict_total_australia(
    fitted_model: Any,
    future_row: pd.DataFrame,
    *,
    inverse_log10: bool = True,
) -> float:
    fc = fitted_model.predict(future_row)
    yhat = float(fc["yhat"].iloc[0])
    if inverse_log10:
        return float(10**yhat)
    return yhat


def predict_one_day(
    ds: str | pd.Timestamp,
    regressors: dict[str, Any] | None,
    *,
    model_path: Path | str | None,
    zone_csv: Path | str | None = None,
    y_total_override: float | None = None,
    inverse_log10: bool = True,
) -> pd.DataFrame:
    zone_csv = Path(zone_csv or _default_zone_csv())
    if y_total_override is not None:
        yhat_o = float(y_total_override)
    else:
        if not model_path:
            raise ValueError("未提供 y_total_override 时必须指定 model_path")
        model_path = Path(model_path)
        if not model_path.is_file():
            raise FileNotFoundError(f"找不到模型文件: {model_path.resolve()}")
        fitted = joblib.load(model_path)
        row = build_future_row(ds, regressors)
        yhat_o = predict_total_australia(fitted, row, inverse_log10=inverse_log10)

    one = pd.DataFrame({"ds": [pd.to_datetime(ds)], "yhat_original": [yhat_o]})
    return split_forecast_by_zone_shares(
        one, zone_csv=zone_csv, yhat_col="yhat_original", ds_col="ds"
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="单日全澳 Prophet 预测并按 zone CSV 拆两区"
    )
    p.add_argument("--ds", required=True, help="日期，如 2026-03-15")
    p.add_argument("--model", "-m", type=Path, default=None)
    p.add_argument("--zone-csv", type=Path, default=None)
    p.add_argument("--y-total", type=float, default=None, dest="y_total")
    p.add_argument("--no-log10-inverse", action="store_true")
    p.add_argument("--json", action="store_true")
    p.add_argument("--is-ph-cn", type=int, choices=[0, 1], default=0, dest="is_ph_cn")
    p.add_argument("--is-ph-hk", type=int, choices=[0, 1], default=0, dest="is_ph_hk")
    p.add_argument("--has-concerts", type=int, choices=[0, 1], default=0, dest="has_concerts")
    p.add_argument(
        "--has-macau-big-events",
        type=int,
        choices=[0, 1],
        default=0,
        dest="has_macau_big_events",
    )
    p.add_argument("--is-typhoon8910", type=int, choices=[0, 1], default=0, dest="is_typhoon8910")
    p.add_argument(
        "--exchange-rate",
        type=float,
        default=None,
        help="人民币/港元等汇率数值；省略则从 finaldata1 按日 lookup",
    )
    p.add_argument(
        "--price-index",
        type=float,
        default=None,
        help="物价指数；省略则从 finaldata1 按日 lookup",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    zone_csv = Path(args.zone_csv) if args.zone_csv else None
    try:
        reg: dict[str, Any] | None
        if args.y_total is not None:
            reg = None
        else:
            five_cli = {
                "IS_PH_CN": args.is_ph_cn,
                "IS_PH_HK": args.is_ph_hk,
                "HAS_Concerts": args.has_concerts,
                "HAS_Macau_Big_Events": args.has_macau_big_events,
                "IS_TYPHOON8910": args.is_typhoon8910,
            }
            if args.exchange_rate is not None and args.price_index is not None:
                cont = {
                    "EXCHANGE_RATE": float(args.exchange_rate),
                    "PRICE_INDEX": float(args.price_index),
                }
            else:
                from load_finaldata import continuous_values_for_date, load_finaldata_df

                foot = Path(__file__).resolve().parent
                df = load_finaldata_df(foot / "finaldata1.csv")
                cont = continuous_values_for_date(df, args.ds)
            reg = merge_regressors_for_prediction(args.ds, five_cli, cont)

        out = predict_one_day(
            args.ds,
            reg,
            model_path=args.model,
            zone_csv=zone_csv,
            y_total_override=args.y_total,
            inverse_log10=not args.no_log10_inverse,
        )
    except (FileNotFoundError, ValueError) as e:
        print(e, file=sys.stderr)
        return 1
    if args.json:
        rec = json.loads(out.head(1).to_json(orient="records", date_format="iso"))[0]
        print(json.dumps(rec, ensure_ascii=False))
    else:
        print(out.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
