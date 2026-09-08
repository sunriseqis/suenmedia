# -*- coding: utf-8 -*-
"""
normalize/category.py —— 分类归一化（薄封装，规则仍以 `taxonomy.py` 为准）

本模块**不复制** taxonomy 的词表，只做三件事：

1. `normalize_category()`：把源站原始分类 / 子分类 / 刮削权威信号，
   收敛到四个规范分类 `movies | tv | anime | variety`。
2. `resolve_region()` / `resolve_group()`：直接转发 taxonomy 的判定函数，
   让调用方只 import `normalize.*` 一个包即可。
3. `category_from_sub_category()`：无刮削命中时的纯词表回退。

taxonomy 不可用（导入失败 / config.json 缺失）时退化为内置最小词表，
保证归一化层在任何环境都能 import 与运行。
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

# taxonomy 依赖 common.load_config()，在极端环境可能不可用；做软依赖。
try:  # pragma: no cover - 依赖存在性分支
    import taxonomy as _taxonomy
except Exception:  # pragma: no cover
    _taxonomy = None

# ---------------------------------------------------------------- 常量

CANON_CATEGORIES: Tuple[str, ...] = ("movies", "tv", "anime", "variety")
DEFAULT_CATEGORY = "tv"

# 原始分类 / 常见别名 -> 规范分类
CATEGORY_ALIASES: Dict[str, str] = {
    # movies
    "movies": "movies", "movie": "movies", "电影": "movies", "电影片": "movies",
    "院线": "movies", "片": "movies", "film": "movies",
    # tv
    "tv": "tv", "tvseries": "tv", "teleplay": "tv", "电视剧": "tv", "剧集": "tv",
    "国产剧": "tv", "港剧": "tv", "台剧": "tv", "日韩剧": "tv", "美剧": "tv",
    "海外剧": "tv", "泰剧": "tv", "网剧": "tv", "短剧": "tv", "short_tv": "tv",
    "shorttv": "tv", "微短剧": "tv", "竖屏短剧": "tv", "series": "tv",
    # anime
    "anime": "anime", "动漫": "anime", "动画": "anime", "动画片": "anime",
    "番剧": "anime", "国漫": "anime", "animation": "anime", "cartoon": "anime",
    # variety
    "variety": "variety", "综艺": "variety", "综艺节目": "variety", "真人秀": "variety",
    "脱口秀": "variety", "talkshow": "variety", "talk_show": "variety",
    "reality": "variety", "晚会": "variety", "音乐节目": "variety",
}

# 子分类关键词 -> 规范分类（按序匹配，前面优先）
_SUB_CATEGORY_RULES: Sequence[Tuple[str, Tuple[str, ...]]] = (
    ("variety", ("综艺", "真人秀", "脱口秀", "访谈", "晚会", "音乐节目", "选秀", "搞笑")),
    ("anime", ("动漫", "动画", "卡通", "番剧", "国漫", "二次元")),
    ("movies", ("电影", "院线", "大片", "动作片", "喜剧片", "爱情片", "科幻片",
                "恐怖片", "剧情片", "战争片", "悬疑片", "惊悚片", "犯罪片", "片")),
    ("tv", ("电视剧", "国产剧", "港剧", "台剧", "日剧", "韩剧", "美剧", "泰剧",
            "网剧", "短剧", "剧集", "连续剧", "海外剧")),
)


# ---------------------------------------------------------------- 分类


def _norm_key(text: str) -> str:
    """把原始分类串压成小写无分隔的查找键。"""
    s = "" if text is None else str(text).strip().lower()
    s = re.sub(r"[\s_\-/.·、,，|]+", "", s)
    return s


def normalize_category(raw: Any, provider: str = "", meta: Optional[Dict[str, Any]] = None) -> str:
    """原始分类 → 规范分类（movies / tv / anime / variety）。

    优先级：
    1. 刮削权威信号（taxonomy.authoritative_category，需 `provider` + `meta`）
    2. 原始分类别名表
    3. 子分类关键词（`meta["sub_category"]` 或 `raw` 本身）
    4. 兜底 `tv`

    Args:
        raw: 源站给的 category / sub_category 字符串。
        provider: 刮削来源（TMDB / 豆瓣 / OMDb / Bilibili / TheTVDB）。
        meta: 刮削元数据，用于权威校正。

    Returns:
        四个规范分类之一。
    """
    if provider and meta and _taxonomy is not None:
        try:
            corrected, _reason = _taxonomy.authoritative_category(provider, meta)
            if corrected in CANON_CATEGORIES:
                return corrected
        except Exception:  # pragma: no cover - 防御性
            pass

    key = _norm_key(raw)
    if key in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[key]

    sub = ""
    if isinstance(meta, dict):
        sub = str(meta.get("sub_category") or "")
    candidate = sub or ("" if raw is None else str(raw))
    resolved = category_from_sub_category(candidate)
    if resolved:
        return resolved
    return DEFAULT_CATEGORY


def category_from_sub_category(sub_category: Any) -> str:
    """纯词表回退：从子分类串里找分类关键词；找不到返回空串（由调用方兜底）。"""
    text = "" if sub_category is None else str(sub_category)
    if not text.strip():
        return ""
    for category, words in _SUB_CATEGORY_RULES:
        for word in words:
            if word in text:
                return category
    return ""


# ---------------------------------------------------------------- 地区 / 题材（转发 taxonomy）


def resolve_region(category: str, item: Optional[Dict[str, Any]] = None) -> str:
    """地区桶判定（转发 taxonomy.resolve_region）。

    taxonomy 不可用时退化为按分类给固定桶。
    """
    item = item or {}
    if _taxonomy is not None:
        try:
            return _taxonomy.resolve_region(category, item)
        except Exception:  # pragma: no cover - 防御性
            pass
    fallback = {"movies": "其他", "anime": "日韩剧", "variety": "国产剧", "tv": "其他剧"}
    return fallback.get(category, "其他剧")


def resolve_group(category: str, item: Optional[Dict[str, Any]] = None) -> str:
    """题材判定（转发 taxonomy.resolve_group）。

    taxonomy 不可用时退化为未分类。
    """
    item = item or {}
    if _taxonomy is not None:
        try:
            return _taxonomy.resolve_group(category, item)
        except Exception:  # pragma: no cover - 防御性
            pass
    return "未分类"


def normalize_genre(word: str) -> str:
    """题材词规范化（转发 taxonomy.normalize_genre）。"""
    if _taxonomy is not None:
        try:
            return _taxonomy.normalize_genre(word)
        except Exception:  # pragma: no cover - 防御性
            pass
    return ""


def normalize_item_taxonomy(item: Dict[str, Any], provider: str = "") -> Dict[str, Any]:
    """一次性把条目的 category / region / group_name 归一化。

    Args:
        item: 至少含 `category`（或 `sub_category`）的条目 dict。
        provider: 刮削来源，用于权威校正。

    Returns:
        新 dict（不改动入参），含 `category` / `region` / `group_name` 三个键。
    """
    item = item or {}
    meta = item if isinstance(item, dict) else {}
    category = normalize_category(
        item.get("category") or item.get("sub_category") or "",
        provider=provider,
        meta=meta if provider else None,
    )
    return {
        "category": category,
        "region": resolve_region(category, item),
        "group_name": resolve_group(category, item),
    }
