# -*- coding: utf-8 -*-
"""sources/omdb.py —— OMDb (IMDb) 评分兜底源（设计文档 §2 P4 / T04）

定位（子任务 8 已收窄）：OMDb **只做「评分 + 票数」兜底**，不参与标题/简介
主链路 —— 中文产物契约下英文 title/overview/cast 无意义，评分才是唯一价值。

搜索策略：
- `search(item)`（Tier A）：按标题 s= 搜列表，`type=movie|series` 按入口分类，
  返回 200 空结果 → miss；无 apikey / 5xx / 429 → retryable。
- `by_id(imdb_id)`：按 IMDb ID 精确取评分（评分兜底回调，1 req）。

错误分类与其它源一致：429/5xx/超时/连接失败 → retryable（绝不写负缓存）；
401 → ProviderError（业务性失败）；200 空 → miss。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from core.http import FetchResult, HttpClient
from sources.base import (
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    ProviderError,
    SourceProvider,
)

__all__ = ["OmdbSource"]

#: OMDb 官方 API 端点
_OMDB_BASE: str = "https://www.omdbapi.com"


class OmdbSource(SourceProvider):
    """OMDb 搜索 / 按 IMDb ID 取评分。"""

    name: str = "OMDb"
    media_kinds: frozenset = frozenset({"movie", "tv"})

    def __init__(self, client: Optional[HttpClient] = None,
                 settings: Any = None) -> None:
        super().__init__(settings)
        self._api_key: str = str(
            (settings.get("omdb_api_key") if settings is not None else "") or "")
        self._interval: float = max(
            float((settings.get("omdb_min_interval") if settings is not None else 0) or 0.5), 0.0)
        self._client: HttpClient = client if client is not None else HttpClient()
        self._owns_client: bool = client is None
        self._last_ts: float = 0.0
        self._throttle_lock = threading.Lock()

    # ---------------------------------------------------------- 节流

    def _throttle(self) -> None:
        if self._interval <= 0:
            return
        with self._throttle_lock:
            wait = self._last_ts + self._interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_ts = time.monotonic()

    # ---------------------------------------------------------- 请求

    def _get(self, params: Dict[str, Any]) -> FetchResult:
        """发一次带节流的 OMDb 请求。"""
        query: Dict[str, Any] = dict(params or {})
        query["apikey"] = self._api_key
        self._throttle()
        result = self._client.get(
            _OMDB_BASE, params=query, timeout=8.0, retries=2, as_json=True)
        if result.status_code in (401, 403):
            raise ProviderError(f"OMDb 鉴权失败: HTTP {result.status_code}")
        return result

    # ---------------------------------------------------------- search（Tier A）

    def search(self, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """按标题搜索（评分兜底优先），返回候选包装 dict。

        候选统一 meta 含 imdb_id / type / rating / vote_count / year；
        title / original_title 也带上（供 scorer 关联，但主要评分链路不依赖）。
        """
        title = str(item.get("search_title") or item.get("title") or "").strip()
        if not title:
            return None, ST_MISS
        category = str(item.get("category") or "movies")
        otype = "movie" if category == "movies" else "series"

        result = self._get({"s": title, "type": otype})
        if not result.ok:
            if result.is_retryable:
                return None, ST_RETRYABLE
            if result.status_code == 404:
                return None, ST_MISS
            raise ProviderError(f"OMDb search 失败: HTTP {result.status_code}")

        payload = result.data if isinstance(result.data, dict) else {}
        if str(payload.get("Response") or "") == "False":
            return None, ST_MISS  # 真未命中（api 明确返回 False）
        results = payload.get("Search") or []
        if not results:
            return None, ST_MISS

        candidates: List[Dict[str, Any]] = []
        for row in results[:10]:
            imdb = str(row.get("imdbID") or "").strip()
            cand_title = str(row.get("Title") or "").strip()
            year = str(row.get("Year") or "").strip()[:4]
            cand_type = str(row.get("Type") or otype)
            if not imdb or not cand_title:
                continue
            candidates.append({
                "provider": self.name,
                "title": cand_title,
                "original_title": cand_title,  # OMDb 无单独原名，评分链路不依赖 S2
                "imdb_id": imdb,
                "media_type": "movie" if cand_type == "movie" else "tv",
                "year": year if year.isdigit() else "",
                "rating": None,   # 列表页无评分，需 by_id 补（评分兜底回调）
                "vote_count": None,
                "overview": "",
            })
        if not candidates:
            return None, ST_MISS
        return {"_candidates": candidates, "provider": self.name}, ST_HIT

    # ---------------------------------------------------------- by_id（评分兜底）

    def by_id(self, imdb_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """按 IMDb ID 精确取评分（1 req）。用于主源命中但 rating 缺失时的兜底。"""
        if not str(imdb_id or "").strip():
            return None, ST_MISS
        result = self._get({"i": str(imdb_id).strip()})
        if not result.ok:
            if result.is_retryable:
                return None, ST_RETRYABLE
            if result.status_code == 404:
                return None, ST_MISS
            raise ProviderError(f"OMDb by_id 失败: HTTP {result.status_code}")
        payload = result.data if isinstance(result.data, dict) else {}
        if str(payload.get("Response") or "") == "False":
            return None, ST_MISS
        rating = payload.get("imdbRating")
        votes = payload.get("imdbVotes")
        try:
            rating_f = float(rating) if rating not in (None, "", "N/A") else None
        except (TypeError, ValueError):
            rating_f = None
        votes_int = None
        if votes not in (None, "", "N/A"):
            try:
                votes_int = int(str(votes).replace(",", ""))
            except (TypeError, ValueError):
                votes_int = None
        meta: Dict[str, Any] = {
            "provider": self.name,
            "imdb_id": str(payload.get("imdbID") or imdb_id),
            "rating": rating_f,
            "vote_count": votes_int,
            "rating_source": "IMDb",
            "year": str(payload.get("Year") or "").strip()[:4],
            "title": str(payload.get("Title") or "").strip(),
        }
        if rating_f is None and votes_int is None:
            return meta, ST_HIT  # 结构可用但无评分 → 交由编排层决定（通常视作弱命中）
        return meta, ST_HIT

    # ---------------------------------------------------------- 生命周期

    def close(self) -> None:
        if self._owns_client:
            try:
                self._client.close()
            except Exception:  # pylint: disable=broad-except
                pass

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)