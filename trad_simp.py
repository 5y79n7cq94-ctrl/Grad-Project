"""
trad_simp.py — 繁簡字符互轉工具（基於 opencc）

用法：
    from trad_simp import to_simp, to_trad, expand_variants

    to_simp('張天賦')      → '张天赋'
    to_trad('张天赋')      → '張天賦'
    expand_variants('張天') → ['張天', '张天']
"""

import opencc

_t2s = opencc.OpenCC('t2s')  # 繁體 → 簡體
_s2t = opencc.OpenCC('s2t')  # 簡體 → 繁體


def to_simp(text: str) -> str:
    """繁體轉簡體"""
    return _t2s.convert(text)


def to_trad(text: str) -> str:
    """簡體轉繁體"""
    return _s2t.convert(text)


def expand_variants(keyword: str) -> list[str]:
    """
    返回 keyword 嘅所有繁簡變體（去重）。
    例：'張天賦' → ['張天賦', '张天赋']
        '张天赋' → ['张天赋', '張天賦']
        'BLACKPINK' → ['BLACKPINK']
    """
    variants = {keyword, to_simp(keyword), to_trad(keyword)}
    return list(variants)


