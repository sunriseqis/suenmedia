# -*- coding: utf-8 -*-
"""pipeline/fine_merge.py —— P5 精细合并（设计文档 §2 P5 / §8 / §9 / T05）

**输入**：刮削产物列表（EnrichedEntity / 缓存命中条目）
**输出**：v3 契约 FinalEntity（见 schema.py），未达门槛的进 unmatched 审计。

归并键优先级（§P5）：`bangou(=provider_id + 季)` > `tmdb_id` > `douban_id` > `merge_key`。

规则：
1. **同键归并**：多个条目共享元数据（只补空不覆盖，`if not entity.get(x) and item.get(x)`）。
2. **线路合并**：同一 `line_name` 维度合并 episodes；同一 `ep_number`/`air_date`
   的多线路 → 主 `url`（priority 最高且域名 alive） + `alt_urls`。
3. **分类闸门**：`taxonomy.authoritative_category()` 权威校正，非四类 → 丢弃。
4. **入库门槛（§9.3，精而准）**：
   `matched == true AND cover 非空 AND overview 非空 AND confidence >= 55`
   不满足 → `unmatched.json`（带 reason：all_sources_miss / low_confidence /
   no_valid_line / not_in_whitelist）。
5. **bangou 构造**：`tmdb_123 / douban_456 / bili_101 / tvdb_789 / imdb_tt123`，
   季 > 1 时追加 `_sN`（§9.1 约定）。

**产物契约 v3（§9，消费端 suenplayer 硬要求）**：
- `cover` 是唯一封面字段（`poster` 不被识别 → 产出即违规）；
- `seasons[].season_number` 必须 int、`episodes[].ep_number` 必须 int；
- 多线路 → `url` + `alt_urls[{source,url,label,resolution,url_type}]`。
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import taxonomy
from core.logging import event, log_event
from normalize.episode import parse_episode
from schema import (
    blank_item,
    gate_reason,
    make_bangou,
    meets_gate,
    new_episode,
    new_season,
)

__all__ = ["FineMerger", "fine_merge_items", "build_unmatched_item", "MIN_CONFIDENCE"]

#: 最低入库置信度（§9.3，与 schema.MIN_CONFIDENCE 保持同步）
MIN_CONFIDENCE: int = 55

#: OMDb/其它源的 media_type 修正与 category 的映射（归一用）
_TYPE_TO_CATEGORY: Dict[str, str] = {"movie": "movies", "tv": "tv", "series": "tv"}


def _digest_url(url: Any) -> str:
    """线路去重用：去掉 scheme/末尾斜杠的规范化 URL。"""
    u = str(url or "").strip().rstrip("/")
    return u.split("://")[-1] if "://" in u else u


class FineMerger:
    """精细合并器：list[EnrichedEntity] → (final_items, unmatched[])。

    Args:
        settings: settings dict（prefilter / match 配置）。
        prefilter: 可复用的 Prefilter 实例（分类闸门前置，缺省内部建）。
    """

    def __init__(self, settings: Optional[Dict[str, Any]] = None,
                 prefilter: Any = None) -> None:
        self._settings: Dict[str, Any] = dict(settings or {})
        self._prefilter = prefilter
        self._items: Dict[str, Dict[str, Any]] = {}   # key → FinalEntity
        self._unmatched: List[Dict[str, Any]] = []

    # ---------------------------------------------------------- 归并键

    def _bucket_key(self, ent: Dict[str, Any]) -> str:
        """归并键优先级：bangou > tmdb_id > douban_id > merge_key。"""
        bangou = str(ent.get("bangou") or "").strip()
        if bangou:
            return f"b:{bangou}"
        tmdb = str(ent.get("tmdb_id") or "").strip()
        if tmdb:
            return f"t:{tmdb}"
        douban = str(ent.get("douban_id") or "").strip()
        if douban:
            return f"d:{douban}"
        return f"m:{ent.get('merge_key') or ''}"

    # ---------------------------------------------------------- 吸收条目

    def add_item(self, ent: Dict[str, Any]) -> None:
        """单条 EnrichedEntity → 归并 / 分流 unmatched。"""
        if not isinstance(ent, dict) or not ent:
            return
        # 分类闸门（§P5）：权威校正非四类 → 直接丢审计
        provider = str(ent.get("source_provider") or ent.get("provider") or "")
        try:
            corrected, reason = taxonomy.authoritative_category(provider, ent)
        except Exception:  # pylint: disable=broad-except
            corrected, reason = "", ""
        if corrected is None:
            self._unmatched.append(build_unmatched_item(ent, reason or "not_in_whitelist"))
            return

        # 入库门槛（§9.3）
        if not meets_gate(ent):
            reason = gate_reason(ent) or "no_valid_line"
            self._unmatched.append(build_unmatched_item(ent, reason))
            return

        bk = self._bucket_key(ent)
        existing = self._items.get(bk)
        if existing is None:
            self._items[bk] = self._finalize(ent, corrected)
        else:
            self._merge_into(existing, ent, corrected)

    def _finalize(self, ent: Dict[str, Any], corrected: str) -> Dict[str, Any]:
        """构造 v3 契约 FinalEntity（单条目独立入库）。"""
        # 类型推断：series（有 seasons）vs video（单季平面条目）
        has_seasons = _requires_seasons(ent)
        item = blank_item(
            bangou=self._bangou(ent),
            title=str(ent.get("title") or ent.get("search_title") or ""),
            item_type="series" if has_seasons else "video",
        )
        self._populate(item, ent, corrected)
        # 集数展开 / 影片线路提升（§5 跨线路对齐；v3 video 分支要求顶层 url）
        if has_seasons:
            self._expand_episodes(item, ent)
        else:
            _promote_video_lines(item, ent)
        return item

    def _merge_into(self, item: Dict[str, Any], ent: Dict[str, Any],
                    corrected: str) -> None:
        """多条同键条目共享元数据（只补空不覆盖）+ 线路合并。"""
        self._populate(item, ent, corrected, merge_only=True)
        if item.get("type") == "series":
            self._expand_episodes(item, ent)
        else:
            _promote_video_lines(item, ent)

    # ---------------------------------------------------------- 字段填充

    def _populate(self, item: Dict[str, Any], ent: Dict[str, Any],
                  corrected: str, merge_only: bool = False) -> None:
        """元数据只补空不覆盖（§P5）。"""
        src = {k: v for k, v in ent.items() if v not in (None, "", [], 0.0, 0)}
        if not merge_only:
            item["matched"] = True
            item["confidence"] = int(ent.get("confidence") or 55)
        if corrected and corrected not in ("movies", "tv", "anime", "variety"):
            corrected = _TYPE_TO_CATEGORY.get(str(corrected), corrected)
        if corrected:
            item["category"] = corrected
        for k in ("source_provider", "provider", "canonical_title", "original_title",
                  "year", "first_air_date", "overview", "rating", "rating_source",
                  "vote_count", "runtime", "genres", "cast", "director", "country",
                  "original_language", "studio", "logo", "certification",
                  "popularity", "number_of_seasons", "number_of_episodes"):
            if k in src and not item.get(k):
                item[k] = src[k]
        # cover 唯一封面字段（poster → cover，杜绝 poster 出现）
        if not item.get("cover"):
            cover = str(ent.get("cover") or ent.get("poster") or "").strip()
            if cover:
                item["cover"] = cover
        if not item.get("status"):
            item["status"] = "ongoing" if ent.get("status") not in ("completed",) else "completed"
        # 标签
        tags = str(ent.get("sub_category") or ent.get("remarks") or "").strip()
        if tags and not item.get("tags"):
            item["tags"] = tags[:200]

    # ---------------------------------------------------------- 集数展开

    def _expand_episodes(self, item: Dict[str, Any], ent: Dict[str, Any]) -> None:
        """线路 → season/episodes 展开（§5.3 跨线路对齐）。"""
        raw_lines = [l for l in (ent.get("lines") or []) if isinstance(l, dict)]
        lines = _normalize_lines(raw_lines)
        if not lines:
            return
        season_num = int(ent.get("season") or 1) or 1
        # 找/建对应季
        season = None
        for s in item.get("seasons") or []:
            if int(s.get("season_number") or 0) == season_num:
                season = s
                break
        if season is None:
            season = new_season(season_num)
            item["seasons"].append(season)
        ep_map: Dict[Any, Dict[str, Any]] = {}
        for s in season.get("episodes") or []:
            ep_map[s.get("_align_key", s.get("ep_number"))] = s
        for idx, line in enumerate(lines):
            parsed = parse_episode(str(line.get("name") or ""))
            if parsed.get("kind") == "unknown":
                ep_number = idx + 1
            else:
                ep_number = int(parsed.get("ep_number") or idx + 1)
            align_key = parsed.get("air_date") or (parsed.get("kind") if parsed.get("kind") in ("extra", "air_date_extra") else ep_number)
            align_key = parsed.get("_raw_name") or align_key
            if align_key is None:
                align_key = f"{ep_number}"
            # 对 air_date 类：ep_number 需重排为连续编号（§5.3），稍后统一处理
            ep = ep_map.get(align_key)
            line_ep: Dict[str, Any] = {
                "ep_number": ep_number,
                "ep_title": parsed.get("ep_title") or str(line.get("name") or ""),
                "air_date": parsed.get("air_date"),
                "kind": parsed.get("kind"),
                "url": str(line.get("url") or ""),
                "url_type": str(line.get("url_type") or _url_type(line.get("url"))),
                "alt_urls": [],
                "_priority": int(line.get("_priority", 10) or 10),
            }
            if ep is None:
                new_ep = new_episode(ep_number, url=line_ep["url"],
                                     ep_title=line_ep["ep_title"])
                new_ep["_align_key"] = align_key
                # 记录主线路优先级：后续 _merge_line 依赖它做"更优转正/其余进 alt"
                new_ep["_priority"] = int(line_ep.get("_priority", 10) or 10)
                if parsed.get("air_date"):
                    new_ep["air_date"] = parsed["air_date"]
                if parsed.get("kind"):
                    new_ep["kind"] = parsed["kind"]
                if line_ep["url_type"]:
                    new_ep["url_type"] = line_ep["url_type"]
                new_ep["alt_urls"] = []
                ep_map[align_key] = new_ep
                season["episodes"].append(new_ep)
                ep = new_ep
            else:
                # 同键多线路 → 主 url 保留第一条，其余进 alt_urls（§5.3）
                _merge_line(ep, line_ep)
        # 清理对齐辅助字段 + 重排（air_date 类按日期升序重编号 1..N）
        _post_process_season(season)
        item["number_of_episodes"] = len(season.get("episodes") or [])

    # ---------------------------------------------------------- bangou 构造

    def _bangou(self, ent: Dict[str, Any]) -> str:
        """按采用源构造 bangou（provider 前缀 + external_id + 季后缀）。

        优先级：调用方显式 bangou > external_id > provider+rid 构造。
        """
        explicit = str(ent.get("bangou") or "").strip()
        if explicit and not str(explicit).startswith(("v_", "n_")):
            return explicit  # 已构造的稳定 bangou 直接复用
        ext_id = str(ent.get("external_id") or "").strip()
        if ext_id:
            return ext_id  # 已是 tmdb_123 形式（external_id_of 构造）
        provider = str(ent.get("source_provider") or ent.get("provider") or "").lower()
        season_num = int(ent.get("season") or 1) or 1
        if provider in ("tmdb", "themoviedb"):
            rid = ent.get("tmdb_id")
        elif provider in ("douban", "豆瓣"):
            rid = ent.get("douban_id")
        elif provider in ("bilibili",):
            rid = ent.get("bilibili_season_id")
        elif provider in ("tvdb", "thetvdb"):
            rid = ent.get("tvdb_id")
        elif provider in ("omdb", "imdb"):
            rid = ent.get("imdb_id")
        else:
            # 无 provider 信号 → 用豆瓣兜底（源站自带 douban_id）
            rid = ent.get("douban_id") or ent.get("tmdb_id")
            provider = "douban" if ent.get("douban_id") else "tmdb"
        if not rid:
            return make_bangou("null", str(ent.get("title") or ent.get("bangou") or ""))
        if provider in ("omdb", "imdb"):
            provider = "imdb"
        elif provider == "thetvdb":
            provider = "tvdb"
        return make_bangou(provider, rid, season=season_num)

    # ---------------------------------------------------------- 汇总

    def finish(self) -> Dict[str, Any]:
        """结束合并：返回 (final_items, unmatched)。"""
        final = list(self._items.values())
        for f in final:
            _cleanup_markers(f)
        return {"items": final, "unmatched": self._unmatched}


# ---------------------------------------------------------------- 工具

def _requires_seasons(ent: Dict[str, Any]) -> bool:
    """判断是否需 seasons 容器（tv/anime/variety 或源站有分集）。"""
    return str(ent.get("category") or "") in ("tv", "anime", "variety")


def _url_type(url: Any) -> str:
    u = str(url or "").strip().lower()
    if u.endswith(".m3u8") or "m3u8" in u:
        return "m3u8"
    if u.endswith(".mp4") or "mp4" in u:
        return "mp4"
    return "m3u8"


def _normalize_lines(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """标准化线路行：去重（同 URL）价保原序。"""
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for l in lines:
        url = str(l.get("url") or "").strip()
        if not url:
            continue
        dk = _digest_url(url)
        if dk in seen:
            continue
        seen.add(dk)
        out.append({"name": str(l.get("name") or ""),
                    "url": url,
                    "url_type": str(l.get("url_type") or _url_type(url)),
                    "_priority": int(l.get("priority", 10) or 10)})
    out.sort(key=lambda x: x["_priority"])
    return out


def _promote_video_lines(item: Dict[str, Any], ent: Dict[str, Any]) -> None:
    """v3 video 分支：把线路提升为顶层 `url`，其余进 `alt_urls`（§9.1）。

    电影类没有分集结构，播放地址必须落在 item.url —— 缺了就违反契约
    （suenplayer video 分支直接读 url，空则无法播放）。
    """
    lines = [l for l in (ent.get("lines") or []) if isinstance(l, dict) and l.get("url")]
    if not lines:
        return
    lines = sorted(lines, key=lambda l: int(l.get("priority", 10) or 10))
    first = lines[0]
    item["url"] = str(first["url"])
    item["url_type"] = str(first.get("url_type") or _url_type(first["url"]))
    rest = [{
        "source": str(l.get("url_type") or ""),
        "url": str(l["url"]),
        "label": str(l.get("name") or ""),
        "resolution": "",
        "url_type": str(l.get("url_type") or _url_type(l["url"])),
    } for l in lines[1:]]
    if rest:
        item["alt_urls"] = rest
    if not item.get("site") and first.get("site"):
        item["site"] = str(first["site"])


def _merge_line(ep: Dict[str, Any], line_ep: Dict[str, Any]) -> None:
    """同 ep 多线路 → 主 url（priority 更优者转正）+ alt_urls 保留其余全部线路。

    语义（§5.3）：多线路都要进产物——最优线路成为 `url`，其余（无论优先级
    同级还是更差）一律并入 `alt_urls` 作为可播放备选，不丢任何源。
    """
    main_prio = int(ep.get("_priority", 10) or 10)
    line_prio = int(line_ep.get("_priority", 10) or 10)
    if line_prio < main_prio:
        # 更优线路成为主 url，原主降级进 alt_urls
        ep.setdefault("alt_urls", []).append({
            "source": str(ep.get("url_type") or ""),
            "url": ep.get("url", ""),
            "label": str(ep.get("ep_title") or ""),
            "resolution": "",
            "url_type": str(ep.get("url_type") or ""),
        })
        ep["url"] = line_ep["url"]
        ep["url_type"] = line_ep["url_type"]
        ep["_priority"] = line_prio
    elif str(line_ep["url"]) != str(ep.get("url")):
        # 同级 / 更差：并入 alt_urls（同 URL 已在 _normalize_lines 去重过）
        ep.setdefault("alt_urls", []).append({
            "source": str(line_ep.get("url_type") or ""),
            "url": line_ep["url"],
            "label": str(line_ep.get("ep_title") or ""),
            "resolution": "",
            "url_type": str(line_ep.get("url_type") or ""),
        })


def _post_process_season(season: Dict[str, Any]) -> None:
    """重排 + 清理：air_date 类按日期升序重编号 1..N；去掉辅助字段。"""
    eps = season.get("episodes") or []
    # 抽 alignment 键（_align_key → _seq；缺失给 0，保证类型稳定）
    for ep in eps:
        al = ep.pop("_align_key", None)
        try:
            ep["_seq"] = int(al or 0)
        except (TypeError, ValueError):
            ep["_seq"] = 0
    kind = str(ep.get("kind") or "") if eps else ""  # noqa: F841 - 类型参考
    # 排序：air_date 优先（无日期按 _seq 源码序）
    eps.sort(key=lambda e: (str(e.get("air_date") or "") or chr(0x10FFFF),
                            int(e.get("_seq", 0) or 0)))
    # 重编号（extra 类保留 0，普通集 1..N）
    counter = 0
    for ep in eps:
        ep_kind = str(ep.get("kind") or "episode")
        if ep_kind in ("episode", "air_date", "air_date_extra", ""):
            counter += 1
            ep["ep_number"] = counter
        else:
            ep["ep_number"] = 0
        ep.pop("_seq", None)
        ep.pop("_priority", None)


def _cleanup_markers(item: Dict[str, Any]) -> None:
    for season in item.get("seasons") or []:
        _post_process_season(season)


def build_unmatched_item(ent: Dict[str, Any], reason: str) -> Dict[str, Any]:
    """构造 unmatched 审计条目（§9 unmatched.json 契约）。"""
    lines = [{"name": l.get("name", ""), "url": l.get("url", ""),
              "url_type": l.get("url_type", "")}
             for l in (ent.get("lines") or []) if isinstance(l, dict) and l.get("url")]
    return {
        "raw_title": str(ent.get("raw_title") or ent.get("title") or ""),
        "search_title": str(ent.get("search_title") or ent.get("title") or ""),
        "category": str(ent.get("category") or ""),
        "season": int(ent.get("season") or 1),
        "year": str(ent.get("year") or ""),
        "site": str(ent.get("site") or ""),
        "raw_id": str(ent.get("raw_id") or ""),
        "reason": reason,
        "provider": str(ent.get("provider") or ent.get("source_provider") or ""),
        "confidence": ent.get("confidence"),
        "lines_count": len(lines),
        "ts": __import__("time").time(),
    }


def fine_merge_items(items: Iterable[Dict[str, Any]],
                     settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """便捷函数：EnrichedEntity 列表 → {"items", "unmatched"}。"""
    merger = FineMerger(settings=settings)
    for ent in items or []:
        merger.add_item(ent)
    return merger.finish()