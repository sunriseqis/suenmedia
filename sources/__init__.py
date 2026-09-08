# -*- coding: utf-8 -*-
"""sources —— 刮削适配层（设计文档 §2 P4 / T04）

分层位置（设计文档 §1.1）：入口层 main.py → 管道层 pipeline/ → 适配层 sources/
（本包）+ crawlers/ → 领域层 normalize/ taxonomy.py schema.py → 基础设施 core/。

只提供**与业务无关**的源适配能力，不 import 上层 pipeline；依赖方向：
sources → core（http/config/cache）+ normalize + taxonomy。

包导出：
    SourceProvider / 各源类 / REGISTRY 源优先级链 / build_sources() 工厂。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sources.base import (
    ST_CATEGORY_DISCARD,
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    ProviderError,
    RetryableError,
    SourceProvider,
    empty_meta,
)
from sources.scorer import Scored, choose_best, score_candidate
from sources.tmdb import TmdbSource
from sources.douban import DoubanSource
from sources.tvdb import TvdbSource
from sources.bilibili import BilibiliSource
from sources.omdb import OmdbSource

__all__ = [
    # 状态常量
    "ST_HIT", "ST_MISS", "ST_RETRYABLE", "ST_CATEGORY_DISCARD",
    # 异常
    "ProviderError", "RetryableError",
    # 契约与打分
    "SourceProvider", "empty_meta", "Scored", "score_candidate", "choose_best",
    # 源类
    "TmdbSource", "DoubanSource", "TvdbSource", "BilibiliSource", "OmdbSource",
    # 注册表 / 工厂
    "REGISTRY", "build_sources",
]

#: 源优先级链（设计文档 §P4：TMDB 主 → TheTVDB 剧集 → 豆瓣兜底 →
#: Bilibili anime → OMDb 评分兜底）。`build_sources` 按此序实例化。
REGISTRY: List[str] = [
    "tmdb",
    "tvdb",
    "douban",
    "bilibili",
    "omdb",
]


def _factory(name: str, settings: Optional[Dict[str, Any]] = None,
             client: Optional[Any] = None) -> Optional[SourceProvider]:
    """按注册名构造源实例；未知名返回 None。"""
    cls_by_name = {
        "tmdb": TmdbSource,
        "tvdb": TvdbSource,
        "douban": DoubanSource,
        "bilibili": BilibiliSource,
        "omdb": OmdbSource,
    }
    cls = cls_by_name.get(str(name).lower())
    if cls is None:
        return None
    try:
        return cls(client=client, settings=settings)
    except TypeError:  # 部分源构造签名差异（如缺 client 参数时的兼容）
        try:
            return cls(settings=settings)
        except TypeError:
            return cls()


def build_sources(settings: Optional[Dict[str, Any]] = None,
                  client: Optional[Any] = None,
                  enabled_only: bool = True) -> Dict[str, SourceProvider]:
    """实例化源链（按 REGISTRY 顺序），返回 {注册名: 实例}。

    Args:
        settings: settings dict（缺省 None → 源用自带默认）。
        client: 可复用的 core.http.HttpClient（缺省各源自建）。
        enabled_only: True（默认）时跳过配置关闭的源。
    """
    out: Dict[str, SourceProvider] = {}
    for name in REGISTRY:
        inst = _factory(name, settings=settings, client=client)
        if inst is None:
            continue
        if enabled_only and not inst.enabled:
            continue
        out[name] = inst
    return out