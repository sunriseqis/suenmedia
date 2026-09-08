# -*- coding: utf-8 -*-
"""sources/base.py —— 刮削源公共契约（设计文档 §2 P4 / T04）

源优先级链（§P4）：TMDB（主）→ TheTVDB（剧集）→ 豆瓣（正式兜底，
限额 300-500/轮）→ Bilibili（anime）→ OMDb（评分兜底）。

每个 Provider 实现 `search(item)`：返回 `(meta, status)`。

- meta 为统一元数据 dict（见下方 ProviderMeta 契约），None 表示未产出；
- status ∈ hit / miss / retryable / category_discard：
  * hit：搜到可打分候选（是否采用由 scorer + 缓存键决策）；
  * miss：真未命中（TMDB 200 空结果等，可写负缓存）；
  * retryable：429 / 5xx / 超时 / 连接失败 / 反爬 —— 绝不写负缓存，进 retry_queue；
  * category_discard：刮削判定非四类（taxonomy 权威校正），直接丢审计。

分层注意：sources 属于适配层，不 import pipeline（管道层），
只依赖 core（http / cache / config）+ normalize + taxonomy。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

__all__ = [
    "ST_HIT",
    "ST_MISS",
    "ST_RETRYABLE",
    "ST_CATEGORY_DISCARD",
    "ProviderError",
    "RetryableError",
    "SourceProvider",
    "empty_meta",
]

#: 源查询三态 + 丢弃态
ST_HIT: str = "hit"
ST_MISS: str = "miss"
ST_RETRYABLE: str = "retryable"
ST_CATEGORY_DISCARD: str = "category_discard"


class ProviderError(Exception):
    """不可重试的源错误（业务性失败，勿重试）。"""


class RetryableError(ProviderError):
    """可重试错误（限流 / 网络 / 反爬）：由调用方进 retry_queue。"""


def empty_meta() -> Dict[str, Any]:
    """空元数据骨架（各源填充一致字段）。"""
    return {}


class SourceProvider(ABC):
    """刮削源协议。

    Attributes:
        name: 源显示名（TMDB / 豆瓣 / TheTVDB / Bilibili / OMDb）。
        media_kinds: 支持的媒体类型集合（{"movie"} / {"tv"} / {"movie", "tv"}）。
        enabled: 配置开关（读 settings）。
    """

    name: str = "source"
    media_kinds: frozenset = frozenset({"movie", "tv"})

    def __init__(self, settings: Any = None) -> None:
        self._settings = settings

    @property
    def enabled(self) -> bool:
        """配置开关（子类覆盖）。"""
        return True

    def media_type_for(self, category: str) -> Optional[str]:
        """按作品分类推断 TMDB 端点类型。"""
        kind = "movie" if category == "movies" else "tv"
        return kind if kind in self.media_kinds else None

    @abstractmethod
    def search(self, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """按条目搜索元数据。

        Returns:
            (meta, status)；status=hit 时 meta 为候选（供 scorer 打分），
            status=miss 时 meta 为 None（真未命中），
            status=retryable 时 meta 为 None（可重试错误）。
        """