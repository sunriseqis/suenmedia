# -*- coding: utf-8 -*-
"""sources/bilibili.py —— Bilibili 番剧源（设计文档 §2 P4 / T04）

定位：anime 类目的中文兜底源（免 Key）。B 站搜索 `media_bangumi` 需 wbi
签名（前端公开同款算法，prop_table 置换 + md5 w_rid，实测 2026-09-07 可用），
命中后调 pgc 详情拉全字段（全中文，与中文产物契约一致）。

请求策略：
- `search(item)`（Tier A = 2 req：nav 取 wbi 密钥 + search/type；命中再 +1 pgc 详情。
  密钥 6h 缓存，实际每 6h 才多 1 req，可视为稳态 1 req/条）。
- 仅服务 anime：movies/tv/variety 直接 miss。

错误分类：nav 不可达 / 无密钥 → retryable（无法签名，属源暂时不可用）；
搜索接口 429/5xx/业务码非 0（含 -412 风控）→ retryable；200 且无 season_id → miss。

注：B 站搜索常返回多个季，season_id 取首个；候选打包供 scorer 按
source_title/seq 打分，避免 B 站"同名季"错配。
"""

from __future__ import annotations

import hashlib
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from core.http import FetchResult, HttpClient
from sources.base import (
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    ProviderError,
    SourceProvider,
)

__all__ = ["BilibiliSource"]

_NAV_URL: str = "https://api.bilibili.com/x/web-interface/nav"
_SEARCH_URL: str = "https://api.bilibili.com/x/web-interface/wbi/search/type"
_PGC_URL: str = "https://api.bilibili.com/pgc/view/web/season"

#: wbi 置换表（B 站前端公开算法）
_MIXIN_TAB: Tuple[int, ...] = (
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52)

_UA: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
_HEADERS: Dict[str, str] = {"User-Agent": _UA, "Referer": "https://www.bilibili.com/"}
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(s: Any) -> str:
    return _TAG_RE.sub("", str(s or "")).strip()


class BilibiliSource(SourceProvider):
    """Bilibili 番剧搜索 / pgc 详情。"""

    name: str = "Bilibili"
    media_kinds: frozenset = frozenset({"anime"})

    def __init__(self, client: Optional[HttpClient] = None,
                 settings: Any = None) -> None:
        super().__init__(settings)
        self._enable: bool = bool(
            (settings.get("bilibili_enable") if settings is not None else True))
        self._interval: float = max(
            float((settings.get("bilibili_min_interval") if settings is not None else 0) or 1.0), 0.0)
        self._client: HttpClient = client if client is not None else HttpClient()
        self._owns_client: bool = client is None
        self._last_ts: float = 0.0
        self._throttle_lock = threading.Lock()
        self._keys: Tuple[str, str] = ("", "")
        self._keys_ts: float = 0.0

    # ---------------------------------------------------------- 节流 / wbi

    def _throttle(self) -> None:
        if self._interval <= 0:
            return
        with self._throttle_lock:
            wait = self._last_ts + self._interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_ts = time.monotonic()

    def _wbi_keys(self) -> Tuple[str, str]:
        """nav 接口取 wbi 密钥（内存缓存 6h）。失败返回 ("","")。"""
        now = time.time()
        if self._keys[0] and self._keys[1] and now < self._keys_ts + 6 * 3600:
            return self._keys
        self._throttle()
        try:
            result = self._client.get(_NAV_URL, headers=_HEADERS, timeout=8.0,
                                      retries=2, as_json=True)
        except Exception:  # pylint: disable=broad-except
            return ("", "")
        if not result.ok or not (isinstance(result.data, dict) and result.data.get("code") == 0):
            return ("", "")
        wbi = ((result.data.get("data") or {}).get("wbi_img") or {})
        img_url, sub_url = str(wbi.get("img_url") or ""), str(wbi.get("sub_url") or "")
        keys = (
            str(wbi.get("img_key") or (img_url.rsplit("/", 1)[-1].split(".")[0] if img_url else "")),
            str(wbi.get("sub_key") or (sub_url.rsplit("/", 1)[-1].split(".")[0] if sub_url else "")),
        )
        if keys[0] and keys[1]:
            self._keys, self._keys_ts = keys, now
        return keys

    @staticmethod
    def _wbi_sign(params: Dict[str, Any], img_key: str, sub_key: str) -> Dict[str, Any]:
        """wbi 签名：key 排序拼接 + 置换 mixin key → md5 w_rid。"""
        raw = img_key + sub_key
        mixin = "".join(raw[i] for i in _MIXIN_TAB)[:32]
        signed: Dict[str, str] = {k: str(v) for k, v in sorted(params.items())}
        signed["wts"] = str(int(time.time()))
        query = urlencode(signed)
        signed["w_rid"] = hashlib.md5((query + mixin).encode("utf-8")).hexdigest()
        return signed

    # ---------------------------------------------------------- search（Tier A）

    def search(self, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """anime 类目搜索：命中返回候选包装 dict（含候选列表 + 详细 meta）。"""
        if not self._enable:
            return None, ST_MISS
        if str(item.get("category") or "") != "anime":
            return None, ST_MISS  # 仅服务 anime（movies/tv/variety 直接 miss）
        title = str(item.get("search_title") or item.get("title") or "").strip()
        if not title:
            return None, ST_MISS

        img_key, sub_key = self._wbi_keys()
        if not img_key or not sub_key:
            return None, ST_RETRYABLE  # nav 不可达，源暂时不可用

        self._throttle()
        params = self._wbi_sign(
            {"keyword": title, "search_type": "media_bangumi", "page": 1},
            img_key, sub_key)
        try:
            result = self._client.get(_SEARCH_URL, params=params, headers=_HEADERS,
                                      timeout=8.0, retries=2, as_json=True)
        except Exception:  # pylint: disable=broad-except
            return None, ST_RETRYABLE
        if not result.ok:
            return None, ST_RETRYABLE
        payload = result.data if isinstance(result.data, dict) else {}
        if payload.get("code") != 0:
            # -412 风控等业务码异常 → 源暂时不可用，不判真未命中
            return None, ST_RETRYABLE
        results = (payload.get("data") or {}).get("result") or []
        targets = [r for r in results if r.get("season_id")]
        if not targets:
            return None, ST_MISS  # 200 正常返回但 B 站未收录

        candidates: List[Dict[str, Any]] = []
        for t in targets[:10]:
            cand = self._candidate_from_search(t)
            if cand:
                candidates.append(cand)
        # 取首个 season_id 拉 pgc 详情（全中文深度字段）
        detail_meta, status = self._pgc_detail(str(targets[0].get("season_id") or ""))
        if status == ST_RETRYABLE:
            return None, ST_RETRYABLE
        if detail_meta:
            detail_meta["_candidates"] = candidates
            detail_meta["provider"] = self.name
            return detail_meta, ST_HIT
        if candidates:
            return {"_candidates": candidates, "provider": self.name}, ST_HIT
        return None, ST_MISS

    def _candidate_from_search(self, t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """搜索行 → 统一候选 meta（供 scorer 打分）。"""
        title = _clean(t.get("title") or "")
        if not title:
            return None
        # B 站搜索行 css 样式名在独立字段，title 其它字段含 RT 标签时剔除
        return {
            "provider": self.name,
            "title": title,
            "original_title": str(t.get("org_title") or "").strip() or None,
            "media_type": "tv",
            "bilibili_season_id": str(t.get("season_id") or ""),
            "year": str(t.get("pubdate") or "")[:4],
            "overview": _clean(t.get("desc") or ""),
            "rating": None,
            "vote_count": None,
            "areas": [a.get("name") or "" for a in (t.get("areas") or []) if isinstance(a, dict)],
            "genres": [g.strip() for g in str(t.get("styles") or "").split("/") if g.strip()],
            "number_of_episodes": None,
        }

    # ---------------------------------------------------------- pgc 详情

    def _pgc_detail(self, season_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """pgc/view/web/season 详情（全中文深度字段）。失败返回 (None, retryable/miss)。"""
        self._throttle()
        try:
            result = self._client.get(_PGC_URL, params={"season_id": season_id},
                                      headers=_HEADERS, timeout=8.0, retries=2,
                                      as_json=True)
        except Exception:  # pylint: disable=broad-except
            return None, ST_RETRYABLE
        if not result.ok:
            return None, ST_RETRYABLE
        payload = result.data if isinstance(result.data, dict) else {}
        if payload.get("code") != 0:
            return None, ST_RETRYABLE
        d = payload.get("result") or {}
        if not str(d.get("title") or "").strip():
            return None, ST_MISS

        rating = d.get("rating") or {}
        pub_time = str((d.get("publish") or {}).get("pub_time") or "")[:10]
        cast: List[str] = []
        for line in str(d.get("staff") or "").splitlines():
            if "：" not in line:
                continue
            name = re.sub(r"[（(].*?[)）]", "", line.split("：", 1)[1]).strip()
            if name and name not in cast:
                cast.append(name)
            if len(cast) >= 10:
                break
        styles = d.get("styles")
        if isinstance(styles, list):
            genres = [str(g).strip() for g in styles if str(g).strip()]
        else:
            genres = [g.strip() for g in str(styles or "").split("/") if g.strip()]
        areas = d.get("areas") or []
        try:
            vote_count = int(rating.get("count") or 0) or None
        except (TypeError, ValueError):
            vote_count = None
        return {
            "provider": self.name,
            "title": _clean(d.get("title") or ""),
            "original_title": str(d.get("jp_title") or "").strip() or None,
            "overview": _clean(d.get("evaluate") or "")[:2000],
            "poster": str(d.get("cover") or "").strip(),
            "backdrop": str(d.get("cover") or "").strip(),   # B 站仅一张正片封面
            "rating": rating.get("score"),
            "vote_count": vote_count,
            "rating_source": "哔哩哔哩",
            "year": pub_time[:4] or None,
            "first_air_date": pub_time or None,
            "genres": genres or None,
            "number_of_episodes": d.get("total"),
            "cast": cast or None,
            "director": None,
            "country": (str(areas[0].get("name") or "") if areas and isinstance(areas[0], dict) else ""),
            "bilibili_season_id": season_id,
            "media_type": "tv",
        }, ST_HIT

    # ---------------------------------------------------------- 生命周期

    def close(self) -> None:
        if self._owns_client:
            try:
                self._client.close()
            except Exception:  # pylint: disable=broad-except
                pass

    @property
    def enabled(self) -> bool:
        return self._enable