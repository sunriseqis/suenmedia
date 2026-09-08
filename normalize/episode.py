# -*- coding: utf-8 -*-
"""
normalize/episode.py —— 集名规范化（设计文档 §5）

输入是源站给的集名（如 `第01集` / `第20260822期` / `EP00` / `正片`），
输出统一的 `EpisodeParts`：

    {"ep_number": int,      # 排序 / 对齐主键；预告与花絮为 0
     "ep_title":  str,      # 规范名
     "air_date":  str|None, # 综艺日期语义，ISO YYYY-MM-DD
     "kind":      "episode|air_date|air_date_extra|extra|unknown"}

**综艺日期语义必须保留**（实测：variety 分类中 58.9% 的集名是 `第20260822期`
这类日期期号，占全库 4.3%）。把它们压成"第20260822集"是荒谬的——
`ep_number` 取 `YYYYMMDD` 整型（可自然排序且唯一），语义落在 `air_date` 上，
最终由 `align_episodes()` 按日期升序重排为 1..N。

解析规则（§5.1，按序匹配）:

    1  第N集 / 第N话（阿拉伯）          -> episode
    2  第一集（中文数字）                -> episode
    3  纯数字 `03`                      -> episode
    4  N集（`121集`）                   -> episode
    5  EP?N（`EP12`）                   -> episode；EP00 -> extra
    6  日期期号 `第20260822期`           -> air_date
    7  日期期号 + 后缀 `第20260905期纯享版` -> air_date_extra
    8  第N期（非日期，综艺集号）         -> episode
    9  区间 `第1-20集`                  -> episode（ep_number=起点，ep_end=终点）
   10  预告 / 花絮 / 彩蛋 / 番外 / SP / OVA -> extra（ep_number=0）
   11  正片 / 全集 / 合全集 / HD / 中字 … -> episode（ep_number=1，统一收敛为"正片"）
   12  无法解析                          -> unknown（ep_number=出现顺序）

第 8/9/11 条是 §5.1 之外的新增规则，依据是 `json/raw/` 全库实测分布
（详见 `normalize/bench_real_data.py`）：不补这三条，全库解析成功率只有 ~92%，
其中 `第N期` 一项就占 3.7%+。
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from normalize.title import chinese_to_int, to_simplified

# ---------------------------------------------------------------- 常量

KIND_EPISODE = "episode"
KIND_AIR_DATE = "air_date"
KIND_AIR_DATE_EXTRA = "air_date_extra"
KIND_EXTRA = "extra"
KIND_UNKNOWN = "unknown"

KIND_VALUES: Tuple[str, ...] = (
    KIND_EPISODE,
    KIND_AIR_DATE,
    KIND_AIR_DATE_EXTRA,
    KIND_EXTRA,
    KIND_UNKNOWN,
)

_AIR_DATE_KINDS = frozenset({KIND_AIR_DATE, KIND_AIR_DATE_EXTRA})

# 排序用的 kind 权重：正片 < 日期 < 花絮 < 无法解析
_KIND_ORDER = {
    KIND_EPISODE: 0,
    KIND_AIR_DATE: 1,
    KIND_AIR_DATE_EXTRA: 1,
    KIND_EXTRA: 2,
    KIND_UNKNOWN: 3,
}

# ---------------------------------------------------------------- 正则

_CN_NUM_CLASS = "一二三四五六七八九十百零〇两兩"

# 1) 第01集 / 第1话（允许尾缀：完结 / 上 / 下 / 中 / 版 …）
_RE_EP_ARABIC = re.compile(r"第\s*(\d{1,4})\s*([集话話])(?!\s*[集话話])")
# 2) 第一集
_RE_EP_CN = re.compile(r"第\s*([" + _CN_NUM_CLASS + r"]{1,5})\s*([集话話])")
# 3) 纯数字
_RE_EP_PLAIN = re.compile(r"^\s*(\d{1,4})\s*$")
# 4) 121集
_RE_EP_N_EP = re.compile(r"^\s*(\d{1,4})\s*[集话話]\s*$")
# 5) EP12 / E12
_RE_EP_EP = re.compile(r"^\s*(?:ep|e)\s*(\d{1,4})\s*$", re.IGNORECASE)
# 9) 第1-20集
_RE_EP_RANGE = re.compile(r"第?\s*(\d{1,4})\s*[-~－—]\s*(\d{1,4})\s*[集话話]")
# 6/7) 日期期号
_RE_DATE_PERIOD = re.compile(r"(19\d{2}|20\d{2})\s*[-/.]?\s*(\d{1,2})\s*[-/.]?\s*(\d{1,2})(?!\d)")
# 8) 第N期（非日期）
_RE_PERIOD_ARABIC = re.compile(r"第\s*(\d{1,4})\s*期")
_RE_PERIOD_CN = re.compile(r"第\s*([" + _CN_NUM_CLASS + r"]{1,5})\s*期")

# 尾缀语义（上 / 下 / 完结 / 大结局 …），不参与 ep_number，只进 note
_RE_TAIL_NOTE = re.compile(r"(完结|已完结|大结局|结局|最终回|上|下|中|版)$")

# 10) 预告 / 花絮 / 彩蛋 / 衍生
_EXTRA_WORDS = (
    "预告片", "预告花絮", "花絮特辑", "幕后花絮", "先导篇", "先导片", "加更版",
    "预告", "花絮", "彩蛋", "幕后", "特辑", "先导", "番外", "加更", "合集",
    "发布会", "直播回放", "直播", "回放", "抢鲜", "抢先", "精编", "高光",
    "陪看", "探班", "衍生", "专享", "纯享", "先导", "收官", "看点",
    "trailer", "preview", "teaser", "ova", "oad", "sp",
)
_RE_EXTRA = re.compile(
    r"(?:预告片|预告花絮|花絮特辑|幕后花絮|先导篇|先导片|加更版|预告|花絮|彩蛋|幕后|"
    r"特辑|先导|番外|加更|合集|发布会|直播回放|直播|回放|抢鲜|抢先|精编|高光|"
    r"陪看|探班|衍生|专享|纯享|收官|看点|trailer|preview|teaser|ova|oad)",
    re.IGNORECASE,
)
# SP01 / 番外02 / OVA 这类带编号的衍生
_RE_EXTRA_NUMBERED = re.compile(
    r"^\s*(?:sp|ova|oad|番外|特别篇|番外篇)\s*(\d{1,3})\s*$", re.IGNORECASE
)

# 11) 正片 / 全集 / 纯质量词（单集可播放条目，电影与短剧的典型形态）
# 长词在前，避免 `全集` 抢掉 `合全集` / `全集完结`
_FULL_MARKERS = (
    "合全集", "全集完结", "全集已完结", "正片", "全集", "完整版", "完结",
    "已完结", "剧场版", "电影版",
)
_QUALITY_ONLY_WORDS = (
    "2160p", "1080p", "720p", "480p", "4k", "8k", "web-dl", "webrip", "bluray",
    "bdrip", "bd", "hd", "ts", "tc", "dvd", "hdr", "sd", "tc版", "ts版",
    "中文字幕", "双语字幕", "国语中字", "粤语中字", "中英字幕", "中文字",
    "普通话", "蓝光", "高清", "超清", "枪版", "国语", "粤语", "中字", "双字",
    "双语", "原声", "外挂", "内嵌", "独家", "全网", "独播", "臻彩", "高码率",
    "修复版", "重制版", "加长版", "典藏版", "标准版", "会员版",
)
_RE_QUALITY_ONLY = re.compile(
    "|".join(re.escape(w) for w in sorted(_QUALITY_ONLY_WORDS, key=len, reverse=True)),
    re.IGNORECASE,
)
_BRACKET_CHARS = re.compile(r"[()（）\[\]【】{}《》〈〉「」『』]")
_SEPARATORS = re.compile(r"[\s_\-\.:：·—～~、,，/|]+")

# ---------------------------------------------------------------- 输出结构


@dataclass(frozen=True)
class EpisodeParts:
    """集名规范化结果（§5.2）。"""

    ep_number: int = 0
    ep_title: str = ""
    air_date: Optional[str] = None
    kind: str = KIND_UNKNOWN
    raw: str = ""
    note: str = ""
    ep_end: Optional[int] = None

    def as_dict(self, include_raw: bool = True) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "ep_number": self.ep_number,
            "ep_title": self.ep_title,
            "air_date": self.air_date,
            "kind": self.kind,
        }
        if self.ep_end is not None:
            out["ep_end"] = self.ep_end
        if self.note:
            out["note"] = self.note
        if include_raw:
            out["raw_name"] = self.raw
        return out

    @property
    def is_valid_date(self) -> bool:
        return self.kind in _AIR_DATE_KINDS and bool(self.air_date)


# ---------------------------------------------------------------- 内部工具


def _clean(text: str) -> str:
    """NFKC + 繁简 + 去首尾空白。用于集名预处理。"""
    if not text:
        return ""
    return to_simplified(unicodedata.normalize("NFKC", str(text))).strip()


def _valid_date(year: int, month: int, day: int) -> Optional[str]:
    """校验日期合法性，返回 ISO 字符串或 None。"""
    if not (1900 <= year <= 2199):
        return None
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _strip_note(text: str) -> Tuple[str, str]:
    """剥离集名尾缀语义（完结 / 上 / 下 …），返回 (剩余, note)。"""
    match = _RE_TAIL_NOTE.search(text)
    if not match:
        return text, ""
    note = match.group(1)
    rest = text[: match.start()]
    if not rest.strip():
        return text, ""
    return rest, note


def _collapse(text: str) -> str:
    """压缩空白：去首尾分隔符，内部空白折叠为单空格。"""
    cleaned = _BRACKET_CHARS.sub("", text)
    cleaned = _SEPARATORS.sub(" ", cleaned).strip()
    return re.sub(r"\s{2,}", " ", cleaned)


def _extract_date_suffix(name: str) -> Tuple[Optional[str], str, str]:
    """从日期期号集名中拆出 (air_date, prefix, suffix)。

    返回 `air_date=None` 表示没找到合法日期。
    """
    match = _RE_DATE_PERIOD.search(name)
    if not match:
        return None, "", ""
    iso = _valid_date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    if iso is None:
        return None, "", ""

    prefix = name[: match.start()]
    suffix = name[match.end():]
    # `第20260822期` —— 去掉紧邻的 `第` 与 `期`
    prefix = re.sub(r"第\s*$", "", prefix)
    suffix = re.sub(r"^\s*期", "", suffix)
    # 前缀若只剩纯数字（如 `220250409` 里的前导 2），是脏数据，丢弃
    if prefix.strip().isdigit():
        prefix = ""
    prefix = _collapse(prefix)
    suffix = _collapse(suffix)
    # 后缀里残留的 `第N期` 已被日期取代，剥掉避免重复
    # （必须先 collapse 去掉括号，否则 `(第1期下)` 匹配不到行首）
    suffix = _collapse(re.sub(r"^第?\s*\d{1,3}\s*期", "", suffix))
    return iso, prefix, suffix


# ---------------------------------------------------------------- 主解析


def parse_episode_parts(name: str, order: int = 1) -> EpisodeParts:
    """把集名解析成 `EpisodeParts`。

    Args:
        name: 源站集名，如 `第01集`、`第20260822期`、`EP00`、`正片`。
        order: 无法解析时的兜底序号（1 基，按出现顺序）。

    Returns:
        `EpisodeParts`。永不抛异常。
    """
    raw = "" if name is None else str(name)
    text = _clean(raw)
    if not text:
        return EpisodeParts(
            ep_number=max(1, order), ep_title=raw, kind=KIND_UNKNOWN, raw=raw
        )

    # --- 规则 1：第01集 / 第1话（先试区间，避免 `第1-20集` 被吃掉起点）
    range_match = _RE_EP_RANGE.search(text)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        if start > 0 and end >= start:
            return EpisodeParts(
                ep_number=start,
                ep_title=f"第{start}-{end}集",
                kind=KIND_EPISODE,
                raw=raw,
                ep_end=end,
            )

    arabic_match = _RE_EP_ARABIC.search(text)
    if arabic_match:
        number = int(arabic_match.group(1))
        unit = arabic_match.group(2)
        if number == 0:
            return EpisodeParts(
                ep_number=0, ep_title=text, kind=KIND_EXTRA, raw=raw, note="预告/花絮"
            )
        rest, note = _strip_note(text[arabic_match.end():])
        title = f"第{number}{'集' if unit in ('集',) else '话'}"
        return EpisodeParts(
            ep_number=number,
            ep_title=title,
            kind=KIND_EPISODE,
            raw=raw,
            note=note or _collapse(rest),
        )

    cn_match = _RE_EP_CN.search(text)
    if cn_match:
        number = chinese_to_int(cn_match.group(1))
        if number is not None and number > 0:
            unit = cn_match.group(2)
            rest, note = _strip_note(text[cn_match.end():])
            return EpisodeParts(
                ep_number=number,
                ep_title=f"第{number}{'集' if unit == '集' else '话'}",
                kind=KIND_EPISODE,
                raw=raw,
                note=note or _collapse(rest),
            )

    # --- 规则 3：纯数字
    plain_match = _RE_EP_PLAIN.match(text)
    if plain_match:
        number = int(plain_match.group(1))
        if number == 0:
            return EpisodeParts(
                ep_number=0, ep_title=text, kind=KIND_EXTRA, raw=raw, note="预告/花絮"
            )
        return EpisodeParts(
            ep_number=number, ep_title=f"第{number}集", kind=KIND_EPISODE, raw=raw
        )

    # --- 规则 4：121集
    n_ep_match = _RE_EP_N_EP.match(text)
    if n_ep_match:
        number = int(n_ep_match.group(1))
        if number > 0:
            return EpisodeParts(
                ep_number=number, ep_title=f"第{number}集", kind=KIND_EPISODE, raw=raw
            )
        return EpisodeParts(
            ep_number=0, ep_title=text, kind=KIND_EXTRA, raw=raw, note="预告/花絮"
        )

    # --- 规则 5：EP12 / E12
    ep_match = _RE_EP_EP.match(text)
    if ep_match:
        number = int(ep_match.group(1))
        if number == 0:
            return EpisodeParts(
                ep_number=0, ep_title=text, kind=KIND_EXTRA, raw=raw, note="预告/花絮"
            )
        return EpisodeParts(
            ep_number=number, ep_title=f"第{number}集", kind=KIND_EPISODE, raw=raw
        )

    # --- 规则 6 / 7：日期期号（综艺 22.6%）
    iso, prefix, suffix = _extract_date_suffix(text)
    if iso:
        compact = int(iso.replace("-", ""))
        extra = _collapse(f"{prefix} {suffix}".strip())
        if extra:
            return EpisodeParts(
                ep_number=compact,
                ep_title=f"{iso} {extra}",
                air_date=iso,
                kind=KIND_AIR_DATE_EXTRA,
                raw=raw,
                note=extra,
            )
        return EpisodeParts(
            ep_number=compact, ep_title=iso, air_date=iso, kind=KIND_AIR_DATE, raw=raw
        )

    # --- 规则 8：第N期（非日期，综艺集号）
    period_match = _RE_PERIOD_ARABIC.search(text) or _RE_PERIOD_CN.search(text)
    if period_match:
        raw_num = period_match.group(1)
        number = int(raw_num) if raw_num.isdigit() else (chinese_to_int(raw_num) or 0)
        if number > 0:
            # 保留前后缀语义：`第1期上` / `第1期下` 必须能被区分开，
            # 否则上下两半会被误并成同一集（不可接受的误并）。
            head = _collapse(text[: period_match.start()])
            tail = _collapse(text[period_match.end():])
            title = f"{head}第{number}期{tail}"
            return EpisodeParts(
                ep_number=number,
                ep_title=title,
                kind=KIND_EPISODE,
                raw=raw,
                note=_collapse(f"{head} {tail}".strip()),
            )

    # --- 规则 10：预告 / 花絮 / 衍生 / SP / OVA
    numbered_extra = _RE_EXTRA_NUMBERED.match(text)
    if numbered_extra:
        return EpisodeParts(
            ep_number=0,
            ep_title=text,
            kind=KIND_EXTRA,
            raw=raw,
            note=f"衍生#{int(numbered_extra.group(1))}",
        )
    if _RE_EXTRA.search(text):
        return EpisodeParts(
            ep_number=0, ep_title=text, kind=KIND_EXTRA, raw=raw, note="预告/花絮/衍生"
        )

    # --- 规则 11：正片 / 全集 / 纯质量词（单集可播放，统一收敛为 ep 1「正片」）
    stripped = _RE_QUALITY_ONLY.sub(" ", text)
    for marker in _FULL_MARKERS:
        stripped = stripped.replace(marker, " ")
    residue = _collapse(stripped)
    if residue in ("", "全集", "合全集", "正片", "全集完结"):
        return EpisodeParts(
            ep_number=1, ep_title="正片", kind=KIND_EPISODE, raw=raw, note="full"
        )

    # --- 规则 12：无法解析
    return EpisodeParts(
        ep_number=max(1, order), ep_title=text, kind=KIND_UNKNOWN, raw=raw
    )


def parse_episode(name: str, order: int = 1) -> Dict[str, Any]:
    """`parse_episode_parts` 的 dict 版（对齐 §5.2 输出结构）。"""
    return parse_episode_parts(name, order).as_dict()


def normalize_episode_list(names: Iterable[str], start_order: int = 1) -> List[Dict[str, Any]]:
    """批量解析一个线路下的集名列表，自动按出现顺序给 unknown 兜底。"""
    results: List[Dict[str, Any]] = []
    for index, name in enumerate(names or [], start=start_order):
        results.append(parse_episode(name, order=index))
    return results


# ---------------------------------------------------------------- 跨线路对齐（§5.3）


def alignment_key(parts: Dict[str, Any]) -> Tuple[Any, ...]:
    """跨线路对齐键（§5.3 第 1 条）。

    用 `(kind_group, ep_number, ep_title)` 三元组做**精确**匹配：
      * `第01集` 与 `第1集`          -> 同为 `(episode, 1, '第1集')`  ✅ 合并
      * `超前营业第1期` 与 `超前彩蛋第1期` -> ep_title 不同            ✅ 不合并
      * 所有 `正片 / HD中字 / 国语`  -> 同为 `(episode, 1, '正片')`   ✅ 合并
    """
    kind = str(parts.get("kind") or KIND_UNKNOWN)
    kind_group = 1 if kind in _AIR_DATE_KINDS else (0 if kind == KIND_EPISODE else 2)
    title = str(parts.get("ep_title") or "")
    if kind == KIND_UNKNOWN:
        title = str(parts.get("raw_name") or title)
    return (kind_group, int(parts.get("ep_number") or 0), title)


def align_episodes(
    episodes: Sequence[Dict[str, Any]],
    alive_domains: Optional[Iterable[str]] = None,
    renumber_air_date: bool = True,
) -> List[Dict[str, Any]]:
    """把多线路的拍平集列表对齐成 suenplayer 的 `episodes[]`（§5.3）。

    Args:
        episodes: 拍平后的集列表。每条至少含 `ep_number` / `ep_title` / `kind`
            与线路信息 `url` / `url_type`；可选 `source` / `priority`。
        alive_domains: 已知存活的域名集合；为 None 时不做存活过滤。
        renumber_air_date: 当**全部**条目都是日期期号时，按 `air_date` 升序
            重排 `ep_number = 1..N`（§5.3 第 3 条）。

    Returns:
        升序的对齐后列表，主 `url` 取 priority 最高且域名存活的线路，
        其余进 `alt_urls`。
    """
    alive = set(alive_domains) if alive_domains is not None else None
    buckets: Dict[Tuple[Any, ...], List[Tuple[int, int, Dict[str, Any]]]] = {}
    seen_urls: Dict[Tuple[Any, ...], set] = {}

    for index, ep in enumerate(episodes or []):
        if not isinstance(ep, dict):
            continue
        url = str(ep.get("url") or "").strip()
        key = alignment_key(ep)
        if url:
            bucket_seen = seen_urls.setdefault(key, set())
            if url in bucket_seen:
                continue
            bucket_seen.add(url)
        try:
            priority = int(ep.get("priority") or 0)
        except (TypeError, ValueError):
            priority = 0
        buckets.setdefault(key, []).append((priority, -index, ep))

    merged: List[Dict[str, Any]] = []
    for key, candidates in buckets.items():
        # priority 高者优先；priority 相同时先出现者优先（用 -index 保证稳定）
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        main = _pick_main(candidates, alive)
        out: Dict[str, Any] = {
            "ep_number": int(main.get("ep_number") or 0),
            "ep_title": main.get("ep_title") or "",
            "air_date": main.get("air_date"),
            "kind": main.get("kind") or KIND_UNKNOWN,
            "url": main.get("url") or "",
            "url_type": main.get("url_type") or "",
        }
        if main.get("ep_end") is not None:
            out["ep_end"] = main["ep_end"]
        if main.get("source"):
            out["source"] = main["source"]
        alt_urls: List[Dict[str, str]] = []
        for _priority, _neg_index, cand in candidates:
            if cand is main:
                continue
            cand_url = str(cand.get("url") or "").strip()
            if not cand_url or cand_url == out["url"]:
                continue
            alt_urls.append(
                {
                    "source": str(cand.get("source") or ""),
                    "url": cand_url,
                    "url_type": str(cand.get("url_type") or ""),
                }
            )
        if alt_urls:
            out["alt_urls"] = alt_urls
        merged.append(out)

    merged.sort(
        key=lambda item: (
            _KIND_ORDER.get(str(item.get("kind")), 3),
            item.get("air_date") or "",
            int(item.get("ep_number") or 0),
            str(item.get("ep_title") or ""),
        )
    )

    if renumber_air_date and merged:
        if all(str(item.get("kind")) in _AIR_DATE_KINDS for item in merged):
            for position, item in enumerate(merged, start=1):
                item["ep_number"] = position
    return merged


def _pick_main(
    candidates: Sequence[Tuple[int, int, Dict[str, Any]]],
    alive: Optional[set] = None,
) -> Dict[str, Any]:
    """在候选线路中挑主线路：域名存活优先，其次 priority，最后出现顺序。

    抽成独立函数便于 T05 接入真实探活结果（§5.3 第 2 条）。
    """
    if not candidates:
        return {}

    def _is_alive(ep: Dict[str, Any]) -> bool:
        if alive is None:
            return True
        url = str(ep.get("url") or "")
        domain = _domain_of(url)
        return domain in alive if domain else True

    for _priority, _neg_index, ep in candidates:
        if _is_alive(ep):
            return ep
    return candidates[0][2]


def _domain_of(url: str) -> str:
    """从 URL 取主机名（小写），失败返回空串。"""
    match = re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://([^/:?#]+)", url or "")
    if not match:
        return ""
    return match.group(1).lower()
