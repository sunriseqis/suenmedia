# -*- coding: utf-8 -*-
"""
normalize —— 归一化层（设计文档 §11 / T02）

子模块：
    normalize.title     标题归一化（粗犷合并核心，§4）
    normalize.episode   集名规范化（§5）
    normalize.category  分类归一化（薄封装 taxonomy 词表）

设计红线（来自 §4.3）：
    归一化之后一律使用 **精确字符串匹配** 做合并键，
    不做模糊匹配、不做编辑距离。合并不够只会导致重复刮削（可容忍），
    误并会污染数据（不可接受）。

典型用法::

    from normalize import normalize_title, parse_episode, normalize_category

    parts = normalize_title("無間道Ⅱ", category="movies")
    parts.norm_title   # '无间道'
    parts.seq          # 2
    parts.merge_key    # 'movies|无间道|2'

    parse_episode("第20260822期")
    # {'ep_number': 20260822, 'ep_title': '2026-08-22',
    #  'air_date': '2026-08-22', 'kind': 'air_date', ...}
"""
from __future__ import annotations

from normalize.title import (
    TitleParts,
    normalize_title,
    title_merge_key,
    title_merge_group,
    extract_seq,
    chinese_to_int,
    to_simplified,
)
from normalize.episode import (
    KIND_AIR_DATE,
    KIND_AIR_DATE_EXTRA,
    KIND_EPISODE,
    KIND_EXTRA,
    KIND_UNKNOWN,
    KIND_VALUES,
    EpisodeParts,
    parse_episode,
    parse_episode_parts,
    normalize_episode_list,
    align_episodes,
    alignment_key,
)
from normalize.category import (
    CANON_CATEGORIES,
    DEFAULT_CATEGORY,
    category_from_sub_category,
    normalize_category,
    resolve_group,
    resolve_region,
)

__all__ = [
    # title
    "TitleParts",
    "normalize_title",
    "title_merge_key",
    "title_merge_group",
    "extract_seq",
    "chinese_to_int",
    "to_simplified",
    # episode
    "KIND_EPISODE",
    "KIND_AIR_DATE",
    "KIND_AIR_DATE_EXTRA",
    "KIND_EXTRA",
    "KIND_UNKNOWN",
    "KIND_VALUES",
    "EpisodeParts",
    "parse_episode",
    "parse_episode_parts",
    "normalize_episode_list",
    "align_episodes",
    "alignment_key",
    # category
    "CANON_CATEGORIES",
    "DEFAULT_CATEGORY",
    "normalize_category",
    "category_from_sub_category",
    "resolve_region",
    "resolve_group",
]

__version__ = "1.0.0"
