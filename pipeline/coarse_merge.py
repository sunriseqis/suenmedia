# -*- coding: utf-8 -*-
"""pipeline/coarse_merge.py —— P3 粗合并（设计文档 §2 P3 / T00·素材库建库）

职责：`list[RawItem]` → CoarseEntity 批量追加进素材库（raw_library 表）。

CoarseEntity（§P3 契约）：
    {merge_key, category, norm_title, seq, year, weight, last_updated,
     primary: {site, raw_id, priority, raw_title, search_title, sub_category,
               episode, remarks, poster, douban_id, douban_score, actor,
               director, overview, update_time, lines[]},
     siblings: [同片其它线路条目]}

- **归并键**：`RawLibrary.merge_key_of(category, norm_title, seq)`，
  （与 §P3 merge_key 一致：category|norm_title|seq）。
- **主条目**：priority 最优（数值小者优先）且线路数最多的站点；
  其余同 key 条目进 siblings（fine_merge 时做线路合并，不重复刮削）。
- **weight**：按类目加权（variety ×2，§7.2 让冷通道 variety 优先补样本）。
- **幂等**：enqueue_many UPSERT —— 同 merge_key 已存在只刷新 last_updated/payload，
  不重计 first_seen（游标 FIFO 稳定）。

分层注意：本模块属于管道层，只依赖 core / normalize / common，不 import sources。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.cache import RawLibrary
from core.logging import event, log_event

__all__ = ["CoarseMerger", "coarse_merge_items", "DEFAULT_CATEGORY_WEIGHTS"]

#: 类目权重缺省（§13 library.category_weights；variety 翻倍推优先消费）
DEFAULT_CATEGORY_WEIGHTS: Dict[str, float] = {
    "movies": 1.0,
    "tv": 1.0,
    "anime": 1.0,
    "variety": 2.0,
}


def _weight_for(category: str, weights: Dict[str, float]) -> float:
    return float(weights.get(str(category), 1.0) or 1.0)


def _safe_year(item: Dict[str, Any]) -> str:
    """year 纯 4 位数字才保留，否则交给后续归一。"""
    y = str(item.get("year") or "").strip()
    return y if y.isdigit() and len(y) == 4 else ""


def _digest_url(url: Any) -> str:
    """线路去重用：去掉 scheme/末尾斜杠的规范化 URL。"""
    u = str(url or "").strip().rstrip("/")
    return u.split("://")[-1] if "://" in u else u


def _union_lines(primary: Dict[str, Any],
                 siblings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """汇合 primary + 全部 siblings 的线路（按 URL 去重，保原序）。

    P5 精合并只消费实体顶层 `lines`，必须把同片多站点线路全部带上，
    否则 `_expand_episodes` 拿到的只是主站线路，alt_urls 合并形同虚设。
    """
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for node in [primary] + (siblings or []):
        for line in (node or {}).get("lines") or []:
            if not isinstance(line, dict) or not line.get("url"):
                continue
            dk = _digest_url(line["url"])
            if dk in seen:
                continue
            seen.add(dk)
            out.append(line)
    return out


def _parse_update_time(raw: Any) -> int:
    """update_time（"%Y-%m-%d %H:%M:%S" 等）→ Unix 时间戳；解析失败返回 0。"""
    text = str(raw or "").strip()
    if not text:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S",
                "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return int(datetime.strptime(text[:19], fmt).timestamp())
        except (ValueError, TypeError):
            continue
    try:
        v = int(float(text))
        return v if v > 10_000_000_000 or v > 10_000_000 else 0
    except (TypeError, ValueError):
        return 0


class CoarseMerger:
    """粗合并器：RawItem 列表 → 按 merge_key 分组 → 素材库 CoarseEntity。"""

    def __init__(self, library: Optional[RawLibrary] = None,
                 weights: Optional[Dict[str, float]] = None) -> None:
        self._library: RawLibrary = library if library is not None else RawLibrary()
        self._weights: Dict[str, float] = dict(DEFAULT_CATEGORY_WEIGHTS)
        if weights:
            self._weights.update({str(k): float(v) for k, v in weights.items()})

    # ---------------------------------------------------------- 分组

    def group(self, items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """RawItem 列表 → CoarseEntity 列表（同 key 合并主/从）。

        Returns:
            CoarseEntity dict 列表（含 merge_key / primary / siblings）。
        """
        groups: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []

        def _bucket(r: Dict[str, Any]) -> str:
            return RawLibrary.merge_key_of(
                str(r.get("category") or "movies"),
                str(r.get("norm_title") or r.get("search_title") or ""),
                r.get("season") or r.get("seq") or 1)

        for raw in items or []:
            if not raw or not (raw.get("line_count") or raw.get("lines")):
                continue
            bk = _bucket(raw)
            ent = groups.get(bk)
            if ent is None:
                groups[bk] = {
                    "merge_key": bk,
                    "category": str(raw.get("category") or "movies"),
                    "norm_title": str(raw.get("norm_title") or raw.get("search_title") or ""),
                    "seq": int(raw.get("season") or raw.get("seq") or 1),
                    "year": _safe_year(raw),
                    "weight": _weight_for(raw.get("category"), self._weights),
                    "last_updated": _parse_update_time(raw.get("update_time")),
                    "primary": None,
                    "siblings": [],
                }
                order.append(bk)
                ent = groups[bk]
            self._absorb(ent, raw)
        out = [groups[k] for k in order]
        for ent in out:
            # 顶层 lines = 主条目 + 全部兄弟线路（去重），P4/P5 消费路径自包含
            ent["lines"] = _union_lines(ent.get("primary"), ent.get("siblings"))
            # 提升主条目的搜索字段到顶层：scorer/tmdb 都读 item["search_title"|"title"]，
            # 缺了 S1 中文名相似度恒为 0 → 全 miss（T04/T05 修复）
            node = ent.get("primary") or {}
            for k in ("title", "search_title", "douban_id", "sub_category",
                      "episode", "remarks"):
                if not ent.get(k) and node.get(k):
                    ent[k] = node[k]
        return out

    def _absorb(self, ent: Dict[str, Any], raw: Dict[str, Any]) -> None:
        """把一条 RawItem 并入实体（主条目取 priority 最优 + 线路多者）。"""
        priority = int(raw.get("priority", 10) or 10)
        line_count = len(raw.get("lines") or [])
        cur = ent.get("primary")
        if cur is None:
            ent["primary"] = self._shallow(raw, line_count)
            ent["line_count"] = line_count
            return
        cur_prio = int(cur.get("priority", 10) or 10)
        cur_lines = int(cur.get("_line_count", 0) or 0)
        # 同片多站点：优先级更低（更可靠）或同级但线路更多 → 提升为主条目
        if (priority < cur_prio) or (priority == cur_prio and line_count > cur_lines):
            ent["siblings"].append(cur)
            ent["primary"] = self._shallow(raw, line_count)
            ent["line_count"] = line_count
        else:
            ent["siblings"].append(self._shallow(raw, line_count))

    @staticmethod
    def _shallow(raw: Dict[str, Any], line_count: int) -> Dict[str, Any]:
        """RawItem → CoarseEntity.primary 精简副本（保留刮削所需全字段 + 线路）。"""
        ent: Dict[str, Any] = dict(raw or {})
        # lines 必须保留：P5 精合并按线路展开分集（§5.3），丢了整片无播放地址。
        # 顶层 ent["lines"] 由 group() 统一汇合（primary + siblings 去重）。
        ent["_line_count"] = int(line_count)
        ent.pop("_from_retry_queue", None)
        return ent

    # ---------------------------------------------------------- 入库

    def merge(self, items: Iterable[Dict[str, Any]],
              hot_window_days: float = 7.0) -> Dict[str, Any]:
        """粗合并 + 素材库入库。

        Args:
            items: RawItem 列表（已过 prefilter 放行）。
            hot_window_days: 热通道判定窗口（§13 library.hot_window_days）。

        Returns:
            {"entities": n, "inserted": n, "skipped": n}。
        """
        entities = self.group(items)
        inserted = self._library.enqueue_many(entities, hot_window_days=hot_window_days)
        return {"entities": len(entities), "inserted": inserted,
                "skipped": max(0, len(entities) - inserted)}

    def close(self) -> None:
        """无自建资源（library 由调用方共享），兼容接口。"""


def coarse_merge_items(items: Iterable[Dict[str, Any]],
                       library: Optional[RawLibrary] = None,
                       hot_window_days: float = 7.0) -> Dict[str, Any]:
    """便捷函数：RawItem 列表 → 素材库。"""
    return CoarseMerger(library=library).merge(items, hot_window_days=hot_window_days)