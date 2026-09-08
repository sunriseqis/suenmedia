# -*- coding: utf-8 -*-
"""sources/tmdb.py —— TMDB 刮削源（设计文档 §2 P4 / §6.4 / T04）

职责：search（Tier A 1 req）→ 候选列表；detail（Tier B +1 req）→ 深度字段；
external_ids（Tier C / S8，按需 +1 req）。

请求限速：`settings.tmdb_min_interval`（默认 0.25s = 4 req/s，修 429 元凶），
在源内部用互斥节流实现（跨线程安全，配合调用层 HttpClient 重试退避）。

错误分类：429/5xx/超时/连接失败 → (None, "retryable")（绝不写负缓存）；
HTTP 401 → 业务性失败 ProviderError；200 且空结果 → (None, "miss")。

分页：search 只取第 1 页 top-N（candidate_limit 打分候选由编排层截断）。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from core.http import FetchResult, HttpClient
from sources.base import (
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    ProviderError,
    SourceProvider,
)

__all__ = ["TmdbSource"]

#: Tier B 深度字段映射（detail 响应 → 统一 meta 键）
_CREDITS_CAST_LIMIT: int = 10


class TmdbSource(SourceProvider):
    """TMDB 搜索 / 详情 / external_ids。"""

    name: str = "TMDB"
    media_kinds: frozenset = frozenset({"movie", "tv"})

    def __init__(self, client: Optional[HttpClient] = None,
                 settings: Any = None) -> None:
        super().__init__(settings)
        if settings is not None:
            self._api_base: str = str(settings.get("tmdb_api_base") or "https://api.themoviedb.org/3")
            self._api_key: str = str(settings.get("tmdb_api_key") or "")
            self._image_base: str = str(
                settings.get("tmdb_image_base") or "https://image.tmdb.org/t/p/w500")
            self._interval: float = max(
                float(settings.get("tmdb_min_interval") or 0.25), 0.0)
        else:
            self._api_base = "https://api.themoviedb.org/3"
            self._api_key = ""
            self._image_base = "https://image.tmdb.org/t/p/w500"
            self._interval = 0.25
        self._client: HttpClient = client if client is not None else HttpClient()
        self._owns_client: bool = client is None
        self._last_ts: float = 0.0
        self._throttle_lock = threading.Lock()

    # ---------------------------------------------------------- 节流

    def _throttle(self) -> None:
        """跨线程全局节流：两次请求间隔 ≥ tmdb_min_interval。"""
        interval = self._interval
        if interval <= 0:
            return
        with self._throttle_lock:
            wait = self._last_ts + interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_ts = time.monotonic()

    # ---------------------------------------------------------- 请求

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> FetchResult:
        """发一次带节流的 TMDB 请求。"""
        if not self._api_key:
            raise ProviderError("TMDB_API_KEY 未配置")
        self._throttle()
        query: Dict[str, Any] = dict(params or {})
        query["api_key"] = self._api_key
        query["language"] = query.get("language", "zh-CN")
        url = f"{self._api_base.rstrip('/')}/{path.lstrip('/')}"
        result = self._client.get(url, params=query, timeout=8.0, retries=3,
                                  as_json=True)
        if result.status_code in (401, 403):
            raise ProviderError(f"TMDB 鉴权失败: HTTP {result.status_code}")
        return result

    # ---------------------------------------------------------- search（Tier A）

    def search(self, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """按条目搜索：返回**候选包装**（candidates 列表放 meta["_candidates"]）。

        返回 meta 恒为包装 dict（供 scorer 逐候选打分）；没有候选时
        返回 (None, "miss") 或 (None, "retryable")。
        """
        title = str(item.get("search_title") or item.get("title") or "").strip()
        if not title:
            return None, ST_MISS
        category = str(item.get("category") or "movies")
        media_type = self.media_type_for(category) or "movie"
        params: Dict[str, Any] = {"query": title, "include_adult": "false"}
        year = str(item.get("year") or "").strip()
        if year.isdigit():
            if media_type == "movie":
                params["year"] = year
            else:
                params["first_air_date_year"] = year

        result = self._get(f"search/{media_type}", params)
        if not result.ok:
            if result.is_retryable:
                return None, ST_RETRYABLE
            if result.status_code == 404:
                return None, ST_MISS
            raise ProviderError(f"TMDB search 失败: HTTP {result.status_code}")

        payload = result.data if isinstance(result.data, dict) else {}
        results = payload.get("results") or []
        if not results:
            return None, ST_MISS  # 200 且空 → soft_miss 可写负缓存

        candidates: List[Dict[str, Any]] = []
        for row in results[:20]:
            cand = self._candidate(row, media_type)
            if cand:
                candidates.append(cand)
        if not candidates:
            return None, ST_MISS
        return {"_candidates": candidates, "provider": self.name}, ST_HIT

    def _candidate(self, row: Dict[str, Any], media_type: str) -> Optional[Dict[str, Any]]:
        """TMDB search 结果行 → 统一候选 meta。"""
        try:
            rid = str(row.get("id") or "")
        except (TypeError, ValueError):
            return None
        if not rid:
            return None
        got_type = str(row.get("media_type") or media_type)
        title = str(row.get("name") or row.get("title") or "").strip()
        if not title:
            return None
        orig = str(row.get("original_name") or row.get("original_title") or "").strip()
        date = str(row.get("first_air_date") or row.get("release_date") or "").strip()
        year = date[:4] if len(date) >= 4 else ""
        poster = row.get("poster_path")
        backdrop = row.get("backdrop_path")
        return {
            "provider": self.name,
            "title": title,
            "original_title": orig,
            "media_type": got_type,
            "tmdb_id": rid,
            "overview": str(row.get("overview") or "").strip(),
            "poster": f"{self._image_base}{poster}" if poster else "",
            "backdrop": f"https://image.tmdb.org/t/p/w1280{backdrop}" if backdrop else "",
            "year": year,
            "first_air_date": date,
            "rating": row.get("vote_average"),
            "vote_count": row.get("vote_count"),
            "popularity": row.get("popularity"),
            "original_language": str(row.get("original_language") or ""),
            "country": (row.get("origin_country") or [""])[0] if row.get("origin_country") else "",
            "number_of_episodes": row.get("number_of_episodes") if got_type == "tv" else None,
            "number_of_seasons": row.get("number_of_seasons") if got_type == "tv" else None,
        }

    # ---------------------------------------------------------- detail（Tier B）

    def detail(self, tmdb_id: str, media_type: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """Tier B：一次拉深字段（cast / director / runtime / 集季数 / imdb_id）。"""
        result = self._get(f"{media_type}/{tmdb_id}",
                           {"append_to_response": "credits,external_ids"})
        if not result.ok:
            if result.is_retryable:
                return None, ST_RETRYABLE
            if result.status_code == 404:
                return None, ST_MISS
            raise ProviderError(f"TMDB detail 失败: HTTP {result.status_code}")
        data = result.data if isinstance(result.data, dict) else {}
        credits = data.get("credits") or {}
        cast = [str(c.get("name") or "") for c in (credits.get("cast") or [])][:_CREDITS_CAST_LIMIT]
        cast = [c for c in cast if c]
        directors = [
            str(c.get("name") or "")
            for c in (credits.get("crew") or [])
            if str(c.get("job") or "") == "Director"
        ]
        ext = (data.get("external_ids") or {}) if isinstance(data.get("external_ids"), dict) else {}
        meta: Dict[str, Any] = {
            "provider": self.name,
            "tmdb_id": str(data.get("id") or tmdb_id),
            "media_type": media_type,
            "cast": cast,
            "director": [d for d in directors if d],
            "runtime": data.get("runtime"),
            "number_of_seasons": data.get("number_of_seasons"),
            "number_of_episodes": data.get("number_of_episodes"),
            "homepage": str(data.get("homepage") or ""),
            "status": str(data.get("status") or ""),
            "imdb_id": str(ext.get("imdb_id") or ""),
            "genres": [str(g.get("name") or "") for g in (data.get("genres") or []) if g.get("name")],
            "logo": (f"https://image.tmdb.org/t/p/w500"
                     f"{data.get('images', {}).get('logos', [{}])[0].get('file_path', '')}"
                     if data.get("images", {}).get("logos") else ""),
        }
        return meta, ST_HIT

    # ---------------------------------------------------------- external_ids（Tier C / S8）

    def external_ids(self, tmdb_id: str, media_type: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """Tier C：external_ids（S8 不确定区校验用）。"""
        result = self._get(f"{media_type}/{tmdb_id}/external_ids")
        if not result.ok:
            if result.is_retryable:
                return None, ST_RETRYABLE
            if result.status_code == 404:
                return None, ST_MISS
            raise ProviderError(f"TMDB external_ids 失败: HTTP {result.status_code}")
        data = result.data if isinstance(result.data, dict) else {}
        return {"provider": self.name, "tmdb_id": str(data.get("id") or tmdb_id),
                "imdb_id": str(data.get("imdb_id") or "")}, ST_HIT

    # ---------------------------------------------------------- 生命周期

    def close(self) -> None:
        """关闭自建连接池（外部注入的 client 由调用方管理）。"""
        if self._owns_client:
            try:
                self._client.close()
            except Exception:  # pylint: disable=broad-except
                pass

    @property
    def enabled(self) -> bool:
        return bool(self._api_key) and bool(
            (self._settings.get("enable_tmdb", True) if self._settings is not None else True))


def _quote(text: str) -> str:  # pragma: no cover - 备用
    return quote(str(text), safe="")