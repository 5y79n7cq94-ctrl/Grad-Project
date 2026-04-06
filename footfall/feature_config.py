from __future__ import annotations

REGRESSOR_COLUMNS: list[str] = [
    "IS_PH_CN",
    "IS_PH_HK",
    "HAS_Concerts",
    "HAS_Macau_Big_Events",
    "IS_SATURDAY",
    "IS_SUNDAY",
    "IS_TYPHOON8910",
    "EXCHANGE_RATE",
    "PRICE_INDEX",
]

CONTINUOUS_COLUMNS: tuple[str, ...] = ("EXCHANGE_RATE", "PRICE_INDEX")

AI_BINARY_KEYS: tuple[str, ...] = (
    "IS_PH_CN",
    "IS_PH_HK",
    "HAS_Concerts",
    "HAS_Macau_Big_Events",
    "IS_TYPHOON8910",
)
