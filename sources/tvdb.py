# -*- coding: utf-8 -*-
"""sources/tvdb.py —— TheTVDB 刮削源（剧集兜底：TMDB 未命中且条目为 tv/anime/variety）

TheTVDB v4 API：login 换 token → search → （翻译名可选，Tier B）。
search 只发 1 次 /v4/search（Tier A）；token 缓存至过期（15 天）。

错误分类：429/5xx/超时/连接 → retryable；401（token 失效）→ 刷新后重试一次；
其余业务失败 → ProviderError。
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

__all__ = ["TvdbSource"]

_API_BASE: str = "https://api4.thetvdb.com/v4"
_TOKEN_TTL: float = 7 * 24 * 3600  # token 有效期（v4 约 30 天，保守 7 天刷新）


class TvdbSource(SourceProvider):
    """TheTVDB v4 搜索源。"""

    name: str = "TheTVDB"
    media_kinds: frozenset = frozenset({"tv"})

    def __init__(self, client: Optional[HttpClient] = None,
                 settings: Any = None) -> None:
        super().__init__(settings)
        self._api_key: str = str(
            settings.get("tvdb_api_key") if settings is not None else "") or ""
        self._client: HttpClient = client if client is not None else HttpClient()
        self._owns_client: bool = client is None
        self._lock = threading.RLock()
        self._token: str = ""
        self._token_ts: float = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)

    # ---------------------------------------------------------- token

    def _ensure_token(self) -> str:
        """获取/复用 token（带锁，防并发重复 login）。"""
        with self._lock:
            if self._token and (time.monotonic() - self._token_ts) < _TOKEN_TTL:
                return self._token
            result: FetchResult = self._client.post_json(
                f"{_API_BASE}/login", json_body={"apikey": self._api_key})
            if not result.ok:
                if result.is_retryable:
                    raise ProviderError("tvdb login retryable")
                raise ProviderError(f"tvdb login 失败: HTTP {result.status_code}")
            data = result.data if isinstance(result.data, dict) else {}
            token = str((data.get("data") or {}).get("token") or "" if isinstance(data.get("data"), dict) else "")
            if not token:
                raise ProviderError("tvdb login: token 为空")
            self._token = token
            self._token_ts = time.monotonic()
            return token

    # ---------------------------------------------------------- search

    def search(self, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        title = str(item.get("search_title") or item.get("title") or "").strip()
        category = str(item.get("category") or "tv")
        if category == "movies":
            return None, ST_MISS  # 剧集源不服务电影
        if not title:
            return None, ST_MISS
        try:
            token = self._ensure_token()
        except ProviderError as exc:
            if "retryable" in str(exc):
                return None, ST_RETRYABLE
            raise

        result = self._client.get(
            f"{_API_BASE}/search",
            params={"query": title, "type": "series"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=8.0, retries=2, as_json=True)
        if not result.ok:
            if result.status_code == 401 and result.retryable:  # token 失效 → 刷新重试
                with self._lock:
                    self._token = ""
                try:
                    token = self._ensure_token()
                except ProviderError:
                    return None, ST_RETRYABLE
                result = self._client.get(
                    f"{_API_BASE}/search",
                    params={"query": title, "type": "series"},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=8.0, retries=1, as_json=True)
            if not result.ok:
                if result.is_retryable:
                    return None, ST_RETRYABLE
                if result.status_code == 404:
                    return None, ST_MISS
                raise ProviderError(f"tvdb search 失败: HTTP {result.status_code}")

        payload = result.data if isinstance(result.data, dict) else {}
        rows = payload.get("data") or []
        if not isinstance(rows, list) or not rows:
            return None, ST_MISS
        candidates: List[Dict[str, Any]] = []
        for row in rows[:20]:
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            year = (str(row.get("first_air_time") or row.get("year") or "")[:4])
            candidates.append({
                "provider": self.name,
                "title": name,
                "original_title": str(row.get("originalName") or "").strip() or name,
                "media_type": "tv",
                "tvdb_id": str(row.get("tvdb_id") or row.get("id") or ""),
                "overview": str(row.get("overview") or "").strip(),
                "poster": str(row.get("image_url") or ""),
                "year": year,
                "first_air_date": str(row.get("first_air_time") or "").strip(),
                "original_language": "",
                "country": "",
                "number_of_episodes": None,
                "number_of_seasons": None,
            })
        if not candidates:
            return None, ST_MISS
        return {"_candidates": candidates, "provider": self.name}, ST_HIT

    def close(self) -> None:
        if self._owns_client:
            try:
                self._client.close()
            except Exception:  # pylint: disable=broad-except
                pass