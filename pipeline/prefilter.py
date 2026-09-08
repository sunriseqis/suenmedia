# -*- coding: utf-8 -*-
"""pipeline/prefilter.py —— L1-L5 前置过滤（设计文档 §2 P1 / §13 prefilter）

过滤发生在**入库前**：任何 RawItem 必须先过 `keep()` 才能进入探活 / 粗合并 / 素材库。
规则按序短路（越靠前越廉价），全部命中才放行：

| 级别 | 规则 | 依据 |
|---|---|---|
| L1 | 分类白名单：category ∈ {movies, tv, anime, variety} | 用户核心需求：产物只保留四类 |
| L2 | 分类黑名单：category ∈ skip_categories（short_tv / discard） | settings.prefilter.skip_categories ∪ crawl_skip_categories |
| L3 | 子分类黑名单：sub_category 命中原站黑词（短剧/解说/微电影/爽文...） | settings.prefilter.sub_category_blacklist ∪ crawl_skip_type_keywords ∪ config.sub_category_blacklist |
| L4 | 标题黑名单：raw_title 命中正则 / 关键词 | settings.prefilter.title_regex_blacklist ∪ crawl_skip_title_keywords |
| L5 | 内容有效性：标题非空 + 至少一条有效线路（可选 CJK 校验） | 结构校验，空线路没有任何价值 |

实现要点：
- **纯函数、无 IO、可缓存**：规则表在实例化时从 settings/config 加载一次。
- `keep()` 返回 `(bool, reason)`，reason 形如 `L3|子分类黑词:短剧`，便于审计与报表。
- **幂等**：同一 item 重复调用结果一致，主流程二道防线复用无副作用。
- 分类缺失时按 sub_category 实时归一（防御），但依赖 RawItem 自带 category 为常态。

分层注意：本模块属于管道层，只依赖 core / normalize / common，不 import crawlers / sources。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Pattern, Tuple, Union

from core.config import Config, Settings, load_config, load_settings
from normalize import CANON_CATEGORIES, normalize_category

__all__ = ["Prefilter", "PrefilterResult", "WHITELIST_CATEGORIES"]

#: L1 白名单（用户核心需求，与旧 crawl_maccms.WHITELIST_CATEGORIES 一致）
WHITELIST_CATEGORIES: Tuple[str, ...] = tuple(CANON_CATEGORIES)

#: CJK 校验（L5 可选；独立实现避免拖入 legacy common 的 requests 依赖链）
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def _has_cjk(text: str) -> bool:
    return bool(_CJK_RE.search(text or ""))


class PrefilterResult:
    """过滤判定结果：是否放行 + 拦截原因。"""

    __slots__ = ("ok", "reason")

    def __init__(self, ok: bool, reason: str = "") -> None:
        self.ok: bool = bool(ok)
        self.reason: str = reason or ("pass" if ok else "")

    def __bool__(self) -> bool:
        return self.ok

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"PrefilterResult(ok={self.ok}, reason={self.reason!r})"


class Prefilter:
    """L1-L5 前置过滤器。

    Args:
        settings: Settings 实例；None 时惰性加载。
        config: Config 实例；None 时惰性加载。
        require_cjk: L5 是否要求标题含 CJK（默认 False：不误伤纯外文标题）。
    """

    def __init__(self, settings: Optional[Settings] = None,
                 config: Optional[Config] = None,
                 require_cjk: bool = False) -> None:
        self._settings: Settings = settings if settings is not None else load_settings()
        self._config: Config = config if config is not None else load_config()
        self._require_cjk: bool = bool(require_cjk)
        self._rules: Dict[str, Any] = {}
        self.reload()

    # ---------------------------------------------------------- 规则加载

    def reload(self) -> None:
        """（重新）加载规则表：实例化后 / 配置热更新时调用。"""
        settings = self._settings
        prefilter_cfg = settings.section("prefilter") or {}
        config = self._config

        skip_categories = list(prefilter_cfg.get("skip_categories") or [])
        skip_categories.extend(list(settings.get("crawl_skip_categories") or []))
        self._rules["skip_categories"] = sorted({str(x) for x in skip_categories if x})

        sub_blacklist = list(prefilter_cfg.get("sub_category_blacklist") or [])
        sub_blacklist.extend(list(settings.get("crawl_skip_type_keywords") or []))
        sub_blacklist.extend(list(config.sub_category_blacklist or []))
        self._rules["sub_blacklist"] = sorted(
            {str(x) for x in sub_blacklist if x}, key=len, reverse=True)

        title_regexes = list(prefilter_cfg.get("title_regex_blacklist") or [])
        title_keywords = list(prefilter_cfg.get("title_keywords") or [])
        title_keywords.extend(list(settings.get("crawl_skip_title_keywords") or []))
        self._rules["title_keywords"] = sorted({str(x) for x in title_keywords if x},
                                               key=len, reverse=True)

        compiled: List[Pattern[str]] = []
        for pattern in title_regexes:
            try:
                compiled.append(re.compile(str(pattern)))
            except re.error:  # pragma: no cover - 配置容错
                continue
        self._rules["title_regexes"] = compiled

        enabled = prefilter_cfg.get("enable")
        self._rules["enable"] = True if enabled is None else bool(enabled)

    # ---------------------------------------------------------- 判定

    def keep(self, item: Union[Dict[str, Any], None]) -> "PrefilterResult":
        """对单个 RawItem 做 L1-L5 判定。

        Args:
            item: RawItem dict（见 §2 P1 契约）；分类缺失时按 sub_category 归一。

        Returns:
            PrefilterResult(ok, reason)。
        """
        if not self._rules["enable"]:
            return PrefilterResult(True)
        if not isinstance(item, dict) or not item:
            return PrefilterResult(False, "L5|empty_item")

        raw_title = str(item.get("raw_title") or "").strip()
        title = str(item.get("title") or "").strip()
        sub_category = str(item.get("sub_category") or "").strip()

        # ---- L1 分类白名单（分类缺失时按子分类实时归一） ----
        category = str(item.get("category") or "").strip()
        if not category:
            category = normalize_category(sub_category or raw_title)
        result = self._l1(category)
        if not result.ok:
            return result

        # ---- L2 分类黑名单 ----
        result = self._l2(category, sub_category)
        if not result.ok:
            return result

        # ---- L3 子分类黑名单 ----
        result = self._l3(sub_category)
        if not result.ok:
            return result

        # ---- L4 标题黑名单 ----
        result = self._l4(raw_title or title)
        if not result.ok:
            return result

        # ---- L5 内容有效性 ----
        return self._l5(raw_title or title, item)

    # ---------------------------------------------------------- 各层规则

    def _l1(self, category: str) -> PrefilterResult:
        if category not in WHITELIST_CATEGORIES:
            return PrefilterResult(False, f"L1|分类白名单外:{category}")
        return PrefilterResult(True)

    def _l2(self, category: str, sub_category: str) -> PrefilterResult:
        if category in self._rules["skip_categories"]:
            return PrefilterResult(False, f"L2|分类黑名单:{category}")
        return PrefilterResult(True)

    def _l3(self, sub_category: str) -> PrefilterResult:
        for word in self._rules["sub_blacklist"]:
            if word and word in sub_category:
                return PrefilterResult(False, f"L3|子分类黑词:{word}")
        return PrefilterResult(True)

    def _l4(self, title: str) -> PrefilterResult:
        for regex in self._rules["title_regexes"]:
            if regex.search(title):
                return PrefilterResult(False, f"L4|标题命中正则:{regex.pattern}")
        for word in self._rules["title_keywords"]:
            if word and word in title:
                return PrefilterResult(False, f"L4|标题黑词:{word}")
        return PrefilterResult(True)

    def _l5(self, title: str, item: Dict[str, Any]) -> PrefilterResult:
        if not title:
            return PrefilterResult(False, "L5|空标题")
        if self._require_cjk and not _has_cjk(title):
            return PrefilterResult(False, "L5|无中文字符")
        lines = item.get("lines")
        if not isinstance(lines, list) or not lines:
            return PrefilterResult(False, "L5|无有效线路")
        for line in lines:
            episodes = line.get("episodes") if isinstance(line, dict) else None
            if not isinstance(episodes, list):
                continue
            for ep in episodes:
                url = ep.get("url") if isinstance(ep, dict) else None
                if isinstance(url, str) and url.startswith(("http://", "https://")):
                    return PrefilterResult(True)
        return PrefilterResult(False, "L5|线路无有效地址")

    # ---------------------------------------------------------- 批量

    def keep_many(self, items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """批量过滤：返回 (放行列表, 各 reason 计数)。

        Args:
            items: RawItem 列表。

        Returns:
            (kept, reasons)：reasons 用于报表（如 `{"L3|子分类黑词:短剧": 123}`）。
        """
        kept: List[Dict[str, Any]] = []
        reasons: Dict[str, int] = {}
        for item in items:
            result = self.keep(item)
            if result.ok:
                kept.append(item)
            else:
                reasons[result.reason] = reasons.get(result.reason, 0) + 1
        return kept, reasons

    # ---------------------------------------------------------- 统计

    def rule_stats(self) -> Dict[str, Any]:
        """规则快照（报表 / 调试用）。"""
        return {
            "enable": self._rules["enable"],
            "require_cjk": self._require_cjk,
            "skip_categories": list(self._rules["skip_categories"]),
            "sub_blacklist_count": len(self._rules["sub_blacklist"]),
            "title_keywords_count": len(self._rules["title_keywords"]),
            "title_regex_count": len(self._rules["title_regexes"]),
        }