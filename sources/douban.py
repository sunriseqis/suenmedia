# -*- coding: utf-8 -*-
"""sources/douban.py —— 豆瓣刮削源（正式兜底：TMDB 未命中时启用，限额 300-500/轮）

反爬治理（T04 验收④）：不再「连续 5 次失败整轮熔断」，改为滚动失败率
`> 30%（样本 ≥ 5）→ 本轮降级跳过`，并带半开重试（冷却 15 分钟到期试 1 次）。

豆瓣 j/search 返回 JSON（items 为 HTML 片段），用 stdlib `html.parser`
抽取 subject id / 标题 / 评分 / 简介（年份与类型从简介文本正则提取）。

不可达/403/反爬 → (None, "retryable")；200 但无结果 → (None, "miss")。
"""

from __future__ import annotations

import html as _html
import re
import threading
import time
from collections import deque
from html.parser import HTMLParser
from typing import Any, Deque, Dict, List, Optional, Tuple

from core.http import FetchResult, HttpClient
from sources.base import (
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    ProviderError,
    SourceProvider,
)

__all__ = ["DoubanSource"]

_SEARCH_CAT: Dict[str, int] = {"movies": 1001, "tv": 1002, "anime": 1002, "variety": 1002}
_ABSTRACT_RE = re.compile(r"类型[:：]\s*([^/]+?)(?:/|$)")

#: 简介里常见字段分隔（年份/地区/类型提取用）
_ABSTRACT_FIELDS = ("导演", "编剧", "主演", "类型", "上映日期", "片长", "语言")


class _DoubanItemParser(HTMLParser):
    """从豆瓣 j/search item HTML 抽条目（subject_id / title / rating / abstract）。"""

    def __init__(self) -> None:
        super().__init__()
        self.items: List[Dict[str, Any]] = []
        self._cur: Optional[Dict[str, Any]] = None
        self._in_title = False
        self._in_rating = False
        self._in_abstract = False
        self._buf: List[str] = []

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        lower_tag = str(tag).lower()
        attrs_d = {k.lower(): v for k, v in (attrs or [])}

        if lower_tag == "a" and self._cur is None:
            href = attrs_d.get("href") or ""
            m = re.search(r"subject[/]?(\d+)", href)
            if m:
                self._cur = {"id": m.group(1), "title": "", "rating": "", "abstract": ""}
                self._in_title = True
                self._buf = []
        elif lower_tag == "span" and self._cur is not None:
            cls = attrs_d.get("class") or ""
            if "rating" in cls:
                self._in_rating = True
                self._in_title = False
                self._buf = []
        elif lower_tag == "p" and self._cur is not None:
            cls = attrs_d.get("class") or ""
            if "abstract" in cls:
                self._in_abstract = True
                self._in_title = False
                self._in_rating = False
                self._buf = []

    def handle_endtag(self, tag: str) -> None:
        lower_tag = str(tag).lower()
        if self._cur is None:
            return
        if lower_tag == "a" and self._in_title:
            self._cur["title"] = "".join(self._buf).strip()
            self._in_title = False
        elif lower_tag == "span" and self._in_rating:
            self._cur["rating"] = "".join(self._buf).strip()
            self._in_rating = False
        elif lower_tag == "p" and self._in_abstract:
            text = "".join(self._buf).strip()
            self._cur["abstract"] = _html.unescape(text.replace("\n", ""))
            self._in_abstract = False
            self.items.append(self._cur)
            self._cur = None

    def handle_data(self, data: str) -> None:
        if self._in_title or self._in_rating or self._in_abstract:
            self._buf.append(data)


def _abstract_to_meta(abstract: str) -> Dict[str, Any]:
    """从豆瓣简介解析 year / genres / region（"导演:… 类型:… 自有年份" 等）。"""
    out: Dict[str, Any] = {}
    date_m = re.search(r"(\d{4})[-/年](\d{1,2})?[-/月]?(\d{1,2})?", abstract)
    if date_m:
        out["year"] = date_m.group(1)
    type_m = _ABSTRACT_RE.search(abstract)
    if type_m:
        genres = [g.strip() for g in type_m.group(1).split("/") if g.strip()]
        out["genres"] = genres[:6]
    return out


class DoubanSource(SourceProvider):
    """豆瓣搜索源（兜底，限流 + 失败率降级）。"""

    name: str = "豆瓣"
    media_kinds: frozenset = frozenset({"movie", "tv"})

    def __init__(self, client: Optional[HttpClient] = None,
                 settings: Any = None) -> None:
        super().__init__(settings)
        from core.config import Settings  # 循环安全惰性导入
        # settings 可能是 dict（ScrapeOrchestrator 传 dict 下来）或 Settings 实例，
        # 本类依赖 Settings.rate_limit()，统一归一化避免 AttributeError。
        if isinstance(settings, Settings):
            self._settings = settings
        elif isinstance(settings, dict):
            self._settings = Settings.from_dict(settings)
        else:
            self._settings = settings or Settings()
        interval = 1.0 / max(float(self._settings.rate_limit("douban") or 1.0), 0.5)
        self._interval: float = interval
        self._client: HttpClient = client if client is not None else HttpClient()
        self._owns_client: bool = client is None
        self._last_ts: float = 0.0
        self._lock = threading.RLock()
        self._window: Deque[bool] = deque(maxlen=200)
        self._threshold: float = 0.30
        self._min_samples: int = 5
        self._cooldown: float = 900.0
        self._disabled_until: float = 0.0

    # ---------------------------------------------------------- 失败率闸门

    @property
    def enabled(self) -> bool:
        """降级门：失败率 ≤ 30% 或半开到期（允许试 1 次）。"""
        with self._lock:
            if time.monotonic() >= self._disabled_until:
                return True
            return False

    def _record(self, ok: bool) -> None:
        with self._lock:
            self._window.append(ok)
            if len(self._window) >= self._min_samples:
                rate = 1.0 - (sum(1 for x in self._window if x) / len(self._window))
                if rate > self._threshold:
                    self._disabled_until = time.monotonic() + self._cooldown

    def _throttle(self) -> None:
        interval = self._interval
        if interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            if now < self._last_ts + interval:
                time.sleep(self._last_ts + interval - now)
            self._last_ts = time.monotonic()

    # ---------------------------------------------------------- search

    def search(self, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        title = str(item.get("search_title") or item.get("title") or "").strip()
        if not title:
            return None, ST_MISS
        category = str(item.get("category") or "movies")
        cat = _SEARCH_CAT.get(category, 1002)
        self._throttle()

        result: FetchResult = self._client.get(
            "https://www.douban.com/j/search",
            params={"q": title, "cat": cat, "limit": 10},
            headers={
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"),
                "Referer": "https://www.douban.com/",
            },
            timeout=8.0, retries=2, as_json=True)
        if not result.ok:
            ok_flag = False
            self._record(False)
            # 403 属反爬拦截，按本模块 docstring 约定降级为 retryable
            # （进 RetryStore 冷却重试），不 raise 以免逐条刷 source_err。
            if result.is_retryable or result.status_code == 403:
                return None, ST_RETRYABLE
            raise ProviderError(f"豆瓣搜索失败: HTTP {result.status_code}")
        self._record(True)

        payload = result.data if isinstance(result.data, dict) else {}
        raw_items = payload.get("items") or []
        candidates: List[Dict[str, Any]] = []
        for raw in raw_items:
            parser = _DoubanItemParser()
            try:
                parser.feed(str(raw))
            except Exception:  # pylint: disable=broad-except
                continue
            for it in parser.items:
                did = it.get("id")
                t = it.get("title") or ""
                if not did or not t:
                    continue
                title_txt = re.sub(r"<[^>]+>", "", t).strip()
                if not title_txt:
                    continue
                ab = it.get("abstract") or ""
                extra = _abstract_to_meta(ab)
                rating = it.get("rating") or ""
                candidates.append({
                    "provider": self.name,
                    "title": title_txt.split("/")[0].strip(),
                    "original_title": "",
                    "media_type": "movie" if category == "movies" else "tv",
                    "douban_id": did,
                    "rating": _safe_float(rating) or None,
                    "rating_source": "豆瓣",
                    "poster": "",
                    "overview": ab[:300],
                    "year": extra.get("year", ""),
                    "genres": extra.get("genres", []),
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default