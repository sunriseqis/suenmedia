# -*- coding: utf-8 -*-
"""产物契约 v3（设计文档 §9）+ `validate_item()` 校验。

消费端事实（已核对 suenplayer `app.py`）：
- `bangou` 是 UNIQUE 主键，缺失时 `md5(title+url)` 兜底；
- **`cover` 是唯一封面字段**，`poster` 不被识别 → 全链路统一为 `cover`（修 B5），
  因此产物里出现 `poster` 属契约违规；
- `type=="series"` 或存在 `seasons`/`episodes` → series 分支；否则 video 分支；
- `seasons[].season_number` 必须是 int，否则整季被跳过（app.py:3498）；
- `episodes[].ep_number` 必须是 int 才写入排序；
- 多线路 → `url`(主源) + `alt_urls[{source,url,label,resolution,url_type}]`；
- `genres` 会被 merge 进 `tags`。

本模块只依赖标准库，可被导出层、报表层、测试三方共用。

用法：
    from schema import validate_item, meets_gate, make_bangou
    result = validate_item(item)
    if not result.ok:
        print(result.errors)
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

__all__ = [
    "CONTRACT_VERSION",
    "TYPE_VIDEO",
    "TYPE_SERIES",
    "TYPES",
    "STATUSES",
    "REGIONS",
    "UNMATCHED_REASONS",
    "MIN_CONFIDENCE",
    "ValidationResult",
    "validate_item",
    "validate_items",
    "meets_gate",
    "gate_reason",
    "make_bangou",
    "coerce_item",
    "blank_item",
    "new_season",
    "new_episode",
]

# ---------------------------------------------------------------- 常量

CONTRACT_VERSION: str = "v3"

TYPE_VIDEO: str = "video"
TYPE_SERIES: str = "series"
TYPES: Tuple[str, ...] = (TYPE_VIDEO, TYPE_SERIES)

#: 完结状态枚举
STATUSES: Tuple[str, ...] = ("completed", "ongoing")

#: 地区枚举（§9.1 region 允许值；不在枚举内只告警不报错）
REGIONS: Tuple[str, ...] = ("电影", "国产剧", "日韩剧", "欧美剧", "其他剧", "动漫", "综艺")

#: 入库门槛（§9.3）
MIN_CONFIDENCE: int = 55

#: 隔离区原因（§9.3）
UNMATCHED_REASONS: Tuple[str, ...] = (
    "all_sources_miss", "low_confidence", "not_in_whitelist", "no_valid_line",
)

#: URL 类型
URL_TYPES: Tuple[str, ...] = ("m3u8", "mp4", "webdav", "magnet", "other")

#: bangou provider 前缀映射
BANGOU_PREFIX: Dict[str, str] = {
    "tmdb": "tmdb",
    "TMDB": "tmdb",
    "豆瓣": "douban",
    "douban": "douban",
    "Douban": "douban",
    "thetvdb": "tvdb",
    "tvdb": "tvdb",
    "TheTVDB": "tvdb",
    "bilibili": "bili",
    "bili": "bili",
    "Bilibili": "bili",
    "omdb": "omdb",
    "OMDb": "omdb",
    "imdb": "imdb",
}

_YEAR_RE = re.compile(r"^(19|20)\d{2}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------- 结果类型

class ValidationResult(NamedTuple):
    """`validate_item()` 返回：本身是 tuple，可 `ok, errors, warnings = result`。

    Attributes:
        ok: 是否通过（errors 为空）。
        errors: 硬性契约违规（必须修）。
        warnings: 软性提示（不阻断入库，但建议修）。
    """

    ok: bool
    errors: List[str]
    warnings: List[str]

    def __bool__(self) -> bool:
        """bool(result) 等价于 result.ok。"""
        return self.ok

    def formatted(self) -> str:
        """格式化为可读文本（日志/报表用）。"""
        lines: List[str] = []
        if self.errors:
            lines.append("错误：" + "；".join(self.errors))
        if self.warnings:
            lines.append("警告：" + "；".join(self.warnings))
        return "\n".join(lines) if lines else "ok"


# ---------------------------------------------------------------- 小工具

def _is_nonempty_str(value: Any) -> bool:
    """是否非空字符串。"""
    return isinstance(value, str) and value.strip() != ""


def _is_number(value: Any) -> bool:
    """是否为数字（bool 不算数字）。"""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_int(value: Any) -> bool:
    """是否为 int（bool 不算）。"""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_str_list(value: Any) -> bool:
    """是否为字符串列表（允许空列表）。"""
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


def _digests(*parts: Any) -> str:
    """对若干片段做 md5（兜底 bangou 用）。"""
    text = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def make_bangou(provider: str, external_id: Any, season: Any = None,
                title: str = "", url: str = "") -> str:
    """构造 bangou（§9.1）。

    规则：`{provider}_{external_id}`，带季追加 `_s{n}`；provider 或 external_id
    缺失时用 `v_{md5(title|url)}` 兜底（对齐 app.py 的 `md5(title+url)` 兜底逻辑）。

    约定：季后缀只在 `season > 1` 时追加 —— 这样单季作品的 bangou 保持 `tmdb_123`
    稳定（与历史产物一致），多季作品为 `tmdb_123_s2`，不会出现同 ID 冲突。

    Args:
        provider: tmdb / douban / tvdb / bilibili / omdb / imdb。
        external_id: 外部 ID。
        season: 季序号（>1 时追加后缀）。
        title: 兜底用的标题。
        url: 兜底用的地址。

    Returns:
        bangou 字符串。
    """
    prefix = BANGOU_PREFIX.get(str(provider or "").strip())
    ext = "" if external_id in (None, "") else str(external_id).strip()
    if prefix and ext:
        season_int = 0
        try:
            season_int = int(season) if season not in (None, "") else 0
        except (TypeError, ValueError):
            season_int = 0
        if season_int > 1:
            return f"{prefix}_{ext}_s{season_int}"
        return f"{prefix}_{ext}"
    return f"v_{_digests(title, url)}"


# ---------------------------------------------------------------- 模板

def blank_item(bangou: str = "", title: str = "",
               item_type: str = TYPE_VIDEO) -> Dict[str, Any]:
    """返回一个符合 v3 契约骨架的空条目（字段齐全、类型正确）。

    Returns:
        v3 条目 dict。
    """
    return {
        "bangou": bangou,
        "type": item_type if item_type in TYPES else TYPE_VIDEO,
        "title": title,
        "original_title": "",
        "cover": "",
        "backdrop": "",
        "overview": "",
        "region": "",
        "group_name": "",
        "year": "",
        "date": "",
        "site": "",
        "tags": "",
        "genres": [],
        "status": "completed",
        "rating": None,
        "rating_source": "",
        "vote_count": 0,
        "first_air_date": "",
        "runtime": 0,
        "original_language": "",
        "homepage": "",
        "certification": "",
        "country": "",
        "studio": "",
        "logo": "",
        "popularity": 0.0,
        "cast": [],
        "director": [],
        "url": "",
        "url_type": "",
        "alt_urls": [],
        "seasons": [],
        "number_of_seasons": 0,
        "number_of_episodes": 0,
    }


def new_season(season_number: int, title: str = "", cover: str = "",
               overview: str = "", date: str = "") -> Dict[str, Any]:
    """构造一个合规 Season 对象（season_number 保证为 int）。"""
    return {
        "season_number": int(season_number),
        "season_title": title or f"第 {int(season_number)} 季",
        "season_cover": cover,
        "season_overview": overview,
        "season_date": date,
        "episodes": [],
    }


def new_episode(ep_number: int, url: str = "", ep_title: str = "",
                url_type: str = "", air_date: Optional[str] = None) -> Dict[str, Any]:
    """构造一个合规 Episode 对象（ep_number 保证为 int）。"""
    return {
        "ep_id": "",
        "ep_number": int(ep_number),
        "ep_title": ep_title or f"第{int(ep_number)}集",
        "air_date": air_date,
        "duration": None,
        "ep_overview": None,
        "ep_rating": None,
        "ep_rating_source": None,
        "ep_still": None,
        "url": url,
        "url_type": url_type or ("m3u8" if ".m3u8" in url else ""),
        "alt_urls": [],
    }


# ---------------------------------------------------------------- 校验

def _validate_alt_urls(alt_urls: Any, path: str, errors: List[str]) -> None:
    """校验 alt_urls（§9.1 / §9.2）。"""
    if alt_urls is None:
        return
    if not isinstance(alt_urls, list):
        errors.append(f"{path} 必须是数组")
        return
    for index, alt in enumerate(alt_urls):
        if not isinstance(alt, dict):
            errors.append(f"{path}[{index}] 必须是对象")
            continue
        if not _is_nonempty_str(alt.get("url")):
            errors.append(f"{path}[{index}].url 必须为非空字符串")


def _validate_episodes(episodes: Any, path: str, errors: List[str],
                       warnings: List[str]) -> None:
    """校验 episodes[]（ep_number 必须 int，否则消费端不写排序）。"""
    if not isinstance(episodes, list):
        errors.append(f"{path} 必须是数组")
        return
    for index, episode in enumerate(episodes):
        here = f"{path}[{index}]"
        if not isinstance(episode, dict):
            errors.append(f"{here} 必须是对象")
            continue
        if not _is_int(episode.get("ep_number")):
            errors.append(f"{here}.ep_number 必须是 int（当前 {episode.get('ep_number')!r}）")
        if not _is_nonempty_str(episode.get("url")):
            errors.append(f"{here}.url 必须为非空字符串")
        air_date = episode.get("air_date")
        if air_date is not None and air_date != "" and not (
                isinstance(air_date, str) and _DATE_RE.match(air_date)):
            warnings.append(f"{here}.air_date 建议为 YYYY-MM-DD（当前 {air_date!r}）")
        _validate_alt_urls(episode.get("alt_urls"), f"{here}.alt_urls", errors)


def _validate_seasons(seasons: Any, errors: List[str], warnings: List[str]) -> None:
    """校验 seasons[]（season_number 必须 int，否则整季被跳过）。"""
    if not isinstance(seasons, list):
        errors.append("seasons 必须是数组")
        return
    seen: set = set()
    for index, season in enumerate(seasons):
        here = f"seasons[{index}]"
        if not isinstance(season, dict):
            errors.append(f"{here} 必须是对象")
            continue
        number = season.get("season_number")
        if not _is_int(number):
            errors.append(f"{here}.season_number 必须是 int（当前 {number!r}）")
        else:
            if number in seen:
                errors.append(f"{here}.season_number 重复：{number}")
            seen.add(number)
        episodes = season.get("episodes")
        if episodes is None:
            warnings.append(f"{here}.episodes 缺失（空季）")
        else:
            _validate_episodes(episodes, f"{here}.episodes", errors, warnings)


def validate_item(item: Any, require_gate: bool = False,
                  strict: bool = False) -> ValidationResult:
    """校验单个 v3 产物条目（§9.1 / §9.2）。

    必填字段（缺失或类型错误 → errors）：
    `bangou` `type` `title` `cover` `overview` `region` `group_name` `year`
    `site` `tags` `genres` `status` `rating` `rating_source` `vote_count`
    `first_air_date` `runtime`；`type=="video"` 还需 `url`；`type=="series"`
    还需非空 `seasons`。

    Args:
        item: 待校验条目。
        require_gate: True 时额外校验入库门槛（§9.3：cover + overview +
            confidence >= 55），并把未达标的写入 errors。
        strict: True 时 warnings 也计入 errors（导出前的严格模式）。

    Returns:
        ValidationResult(ok, errors, warnings)。
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not isinstance(item, dict):
        return ValidationResult(False, [f"条目必须是 dict（当前 {type(item).__name__}）"], [])

    # --- B5：poster 不得出现（cover 是唯一封面字段）
    if "poster" in item:
        errors.append("禁止出现 poster 字段（全链路统一为 cover，修 B5）")

    # --- 主键与类型
    if not _is_nonempty_str(item.get("bangou")):
        errors.append("bangou 必须为非空字符串")
    item_type = item.get("type")
    if item_type not in TYPES:
        errors.append(f"type 必须是 {'/'.join(TYPES)}（当前 {item_type!r}）")

    # --- 文本必填
    for key in ("title", "cover", "overview", "region", "group_name", "site"):
        if not _is_nonempty_str(item.get(key)):
            errors.append(f"{key} 必须为非空字符串")

    # --- year / date：至少一个合法 4 位年份
    year = item.get("year")
    date_value = item.get("date")
    if _is_nonempty_str(year):
        if not _YEAR_RE.match(str(year).strip()):
            errors.append(f"year 必须是 4 位年份（当前 {year!r}）")
    elif _is_nonempty_str(date_value) and _YEAR_RE.match(str(date_value).strip()[:4]):
        warnings.append("year 缺失，已由 date 推断（建议显式写 year）")
    else:
        errors.append("year 必须为 4 位年份字符串（date 可作为兜底）")

    # --- 列表/枚举
    tags = item.get("tags")
    if tags is None:
        warnings.append("tags 缺失（按空串处理）")
    elif not isinstance(tags, str):
        errors.append(f"tags 必须是字符串（当前 {type(tags).__name__}）")

    genres = item.get("genres")
    if genres is None:
        warnings.append("genres 缺失（按空数组处理）")
    elif not _is_str_list(genres):
        errors.append("genres 必须是字符串数组")

    status = item.get("status")
    if status not in STATUSES:
        errors.append(f"status 必须是 {'/'.join(STATUSES)}（当前 {status!r}）")

    # --- 数值字段：缺失给 0，但类型必须正确
    rating = item.get("rating")
    if rating is None:
        pass  # 允许 null（无评分不污染）
    elif not _is_number(rating):
        errors.append(f"rating 必须是数字或 null（当前 {rating!r}）")
    elif float(rating) == 0.0:
        warnings.append("rating 为 0.0，无评分应写 null（避免污染评分排序）")
    elif not (0.0 <= float(rating) <= 10.0):
        warnings.append(f"rating 超出 [0,10] 区间（当前 {rating}）")

    for key in ("vote_count", "runtime"):
        value = item.get(key)
        if value is None:
            continue
        if not _is_int(value):
            errors.append(f"{key} 必须是 int（当前 {value!r}）")
        elif int(value) < 0:
            errors.append(f"{key} 不能为负数（当前 {value}）")

    for key in ("rating_source", "first_air_date"):
        value = item.get(key)
        if value is not None and not isinstance(value, str):
            errors.append(f"{key} 必须是字符串（当前 {type(value).__name__}）")

    # --- 地区枚举（软校验）
    region = item.get("region")
    if _is_nonempty_str(region) and region not in REGIONS:
        warnings.append(f"region 不在推荐枚举内（当前 {region!r}）")

    # --- 线路
    if item_type == TYPE_VIDEO:
        if not _is_nonempty_str(item.get("url")):
            errors.append("type=video 时 url 必须为非空字符串")
    if item_type == TYPE_SERIES:
        seasons = item.get("seasons")
        if not seasons:
            errors.append("type=series 时 seasons 必须为非空数组")
        else:
            _validate_seasons(seasons, errors, warnings)
        for key in ("number_of_seasons", "number_of_episodes"):
            value = item.get(key)
            if value is not None and not _is_int(value):
                errors.append(f"{key} 必须是 int（当前 {value!r}）")

    _validate_alt_urls(item.get("alt_urls"), "alt_urls", errors)

    # --- 入库门槛（§9.3）
    if require_gate:
        reason = gate_reason(item)
        if reason:
            errors.append(f"未达入库门槛：{reason}")

    if strict and warnings:
        errors.extend(warnings)
        warnings = []

    return ValidationResult(not errors, errors, warnings)


def validate_items(items: Iterable[Any], require_gate: bool = False,
                   strict: bool = False) -> Dict[str, Any]:
    """批量校验。

    Returns:
        {"total": n, "ok": n, "failed": n, "errors": [(index, [msg...])],
         "warnings": [(index, [msg...])]}
    """
    total = 0
    ok = 0
    error_rows: List[Tuple[int, List[str]]] = []
    warning_rows: List[Tuple[int, List[str]]] = []
    for index, item in enumerate(items):
        total += 1
        result = validate_item(item, require_gate=require_gate, strict=strict)
        if result.ok:
            ok += 1
        else:
            error_rows.append((index, result.errors))
        if result.warnings:
            warning_rows.append((index, result.warnings))
    return {
        "total": total,
        "ok": ok,
        "failed": total - ok,
        "errors": error_rows,
        "warnings": warning_rows,
    }


# ---------------------------------------------------------------- 入库门槛

def gate_reason(item: Dict[str, Any]) -> str:
    """判断未达入库门槛的原因（§9.3）；达标返回空串。

    门槛：matched（有 provider 命中）非空 且 cover 非空 且 overview 非空
    且 confidence >= 55。

    Args:
        item: v3 条目。

    Returns:
        原因字符串（"low_confidence" 等），达标返回 ""。
    """
    if not isinstance(item, dict):
        return "not_in_whitelist"
    if not _is_nonempty_str(item.get("cover")):
        return "no_cover"
    if not _is_nonempty_str(item.get("overview")):
        return "no_overview"
    matched = item.get("matched", item.get("confidence") is not None)
    if matched is False:
        return "all_sources_miss"
    confidence = item.get("confidence")
    try:
        confidence_int = int(confidence) if confidence is not None else MIN_CONFIDENCE
    except (TypeError, ValueError):
        confidence_int = 0
    if confidence_int < MIN_CONFIDENCE:
        return "low_confidence"
    return ""


def meets_gate(item: Dict[str, Any]) -> bool:
    """是否满足入库门槛（cover + overview + confidence >= 55）。"""
    return gate_reason(item) == ""


# ---------------------------------------------------------------- 类型修复

_INT_KEYS: Tuple[str, ...] = (
    "vote_count", "runtime", "number_of_seasons", "number_of_episodes", "confidence")
_STR_KEYS: Tuple[str, ...] = (
    "bangou", "title", "original_title", "cover", "backdrop", "overview", "region",
    "group_name", "year", "date", "site", "tags", "status", "rating_source",
    "first_air_date", "original_language", "homepage", "certification", "country",
    "studio", "logo", "url", "url_type")


def coerce_item(item: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """就地修复可安全推断的类型问题（导出前调用，避免消费端静默丢数据）。

    修复项：
    - int 字段：字符串数字 → int；空值 → 0；
    - str 字段：None → ""；非 str → str()；
    - `seasons[].season_number` / `episodes[].ep_number` 强制转 int（消费端硬要求）；
    - `rating` 为 0.0 且 `rating_source` 为空 → 置 None；
    - `poster` → 迁移为 `cover`（cover 为空时）。

    Args:
        item: 待修复条目（**原地修改**）。

    Returns:
        (item, 修复说明列表)。
    """
    changes: List[str] = []
    if not isinstance(item, dict):
        return item, ["条目不是 dict，跳过修复"]

    # poster → cover（B5 兜底迁移）
    if "poster" in item:
        poster = item.pop("poster")
        if not _is_nonempty_str(item.get("cover")) and _is_nonempty_str(poster):
            item["cover"] = poster
            changes.append("poster → cover")
        else:
            changes.append("移除 poster 字段")

    for key in _INT_KEYS:
        if key not in item:
            continue
        value = item[key]
        if _is_int(value):
            continue
        if value in (None, ""):
            item[key] = 0
            changes.append(f"{key}: 空值 → 0")
            continue
        try:
            item[key] = int(float(value))
            changes.append(f"{key}: {value!r} → int")
        except (TypeError, ValueError):
            item[key] = 0
            changes.append(f"{key}: {value!r} 无法转 int → 0")

    for key in _STR_KEYS:
        if key not in item:
            continue
        value = item[key]
        if isinstance(value, str):
            continue
        item[key] = "" if value is None else str(value)
        changes.append(f"{key}: {value!r} → str")

    if item.get("rating") is not None and not _is_number(item.get("rating")):
        try:
            item["rating"] = float(item["rating"])
            changes.append("rating: → float")
        except (TypeError, ValueError):
            item["rating"] = None
            changes.append("rating: 无法转数字 → None")

    # seasons / episodes 的 int 主键
    seasons = item.get("seasons")
    if isinstance(seasons, list):
        for season in seasons:
            if not isinstance(season, dict):
                continue
            number = season.get("season_number")
            if not _is_int(number):
                try:
                    season["season_number"] = int(float(number))
                    changes.append(f"season_number: {number!r} → int")
                except (TypeError, ValueError):
                    season["season_number"] = 0
                    changes.append(f"season_number: {number!r} 无法转 int → 0")
            episodes = season.get("episodes")
            if isinstance(episodes, list):
                for episode in episodes:
                    if not isinstance(episode, dict):
                        continue
                    ep_number = episode.get("ep_number")
                    if not _is_int(ep_number):
                        try:
                            episode["ep_number"] = int(float(ep_number))
                            changes.append(f"ep_number: {ep_number!r} → int")
                        except (TypeError, ValueError):
                            episode["ep_number"] = 0
                            changes.append(f"ep_number: {ep_number!r} 无法转 int → 0")

    return item, changes
