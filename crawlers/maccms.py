# -*- coding: utf-8 -*-
"""crawlers/maccms.py —— MacCMS v10 采集器（httpx async，设计文档 §2 P1 / T03）

迁移自根目录 crawl_maccms.py，核心变更：

1. **httpx async + 页级并发**：不再"单页串行 × 12s 超时 × 4 次重试"，
   由外部传入 `AsyncHttpClient` + 每站 `asyncio.Semaphore(4)` 约束并发页数（§2 P1）。
2. **纯采集，过滤权上移**：本模块只做"翻页 → vod dict → RawItem"，不做过滤。
   L1-L5 前置过滤在管道层 `pipeline.prefilter`（main.py 编排时调用），职责单一。
3. **两种模式**：
   - `full`（阶段 0 建素材库）：不设 max_pages，翻到站点尽头（空页 / 条数 < 页大小 /
     pagecount 边界）置 `done=true`，可跨多次派发断点续跑；
   - `incremental`（日常）：按 `update_time` 时间窗（默认 24h）停止，
     `h=hours` 交给服务端裁窗 + 客户端逐条复核，硬超时 `deadline` 到点停派新页。
4. **断点续采**：每页完成后回调 `on_page(site, page)`，由编排层写 progress.json；
   重启后从 `start_page` 继续。

数据契约（RawItem，§2 P1）：字段与旧实现一致，`title/search_title/season/episode/year`
由 `normalize.title` 归一化产出；`lines` 由 `parse_play_urls` 解析。

分层注意：本模块属于适配层（crawlers），只依赖 core / normalize（stdlib）；
**禁止 import pipeline（管道层）**，过滤由入口层编排。
"""

from __future__ import annotations

import asyncio
import html as _html
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.config import Settings, load_settings
from core.http import AsyncHttpClient, FetchResult
from core.logging import event, log_event
from normalize import normalize_category, normalize_title

__all__ = [
    "parse_play_urls",
    "build_item",
    "fetch_page",
    "MaccmsSite",
    "MaccmsResult",
    "parse_vod_time",
    "clean_overview",
]

# ---------------------------------------------------------------- 常量

DEFAULT_PAGE_SIZE: int = 20      # MacCMS provide/vod 默认 limit
CONCURRENT_PAGES: int = 4        # 每站页级并发（§2 P1 semaphore(4)）
FETCH_TIMEOUT: float = 12.0      # 单页总超时（对齐旧实现）
FETCH_RETRIES: int = 2           # 单镜像尝试次数
FALLBACK_SLEEP: float = 0.5      # 镜像轮换前的退避
FAIL_THRESHOLD: int = 5          # 连续失败页数达阈值 → 中止该站（对齐旧实现）
BACKOFF_AFTER_PAGE_FAIL: float = 0.3
PAGE_BEAT_EVERY: int = 200        # 每完成 N 页打一行 crawl.site.page 心跳日志（GA 日志实时可见）
PAGE_BEAT_INTERVAL: float = 60.0  # 或距上次心跳 ≥ 该秒数也打一行（页数少/慢站兜底）


def clean_overview(text: Any) -> str:
    """简介清洗：HTML 实体解码 → 去标签 → 去 &nbsp;/零宽字符 → 压缩空白。

    内联自 legacy common.clean_overview（T05 收敛 common 后移除重复）；
    仅依赖 stdlib，避免把适配层拖进 requests 依赖链。
    """
    if not text:
        return ""
    t = _html.unescape(str(text))
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\xa0", " ").replace("\u200b", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t

#: 集数提取（RawItem.episode 用；集名规范化由 normalize.episode 负责，这里只做条目级估算）
_EPISODE_RE = re.compile(r"(?:更新至|更新到|至|第)\s*(\d{1,4})\s*(?:集|话|期)", re.I)

#: vod_time 常见格式（MacCMS 为 "%Y-%m-%d %H:%M:%S"，兼容 date-only / 时间戳）
_VOD_TIME_FMTS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d")


def parse_vod_time(text: Any) -> Optional[datetime]:
    """把源站更新时间解析成 datetime；无法解析返回 None。

    Args:
        text: vod_time 字段（MacCMS 通常为 `"2026-09-08 12:33:45"`，
            也可能是 date-only / Unix 时间戳 / 空）。
    """
    s = str(text or "").strip()
    if not s:
        return None
    for fmt in _VOD_TIME_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    if s.isdigit():
        try:
            ts = int(s)
            if ts > 10_000_000_000:  # 毫秒时间戳
                ts //= 1000
            return datetime.fromtimestamp(ts)
        except (ValueError, OSError, OverflowError):  # pragma: no cover - 防御
            return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:  # pragma: no cover - 源站脏数据
        return None


# ---------------------------------------------------------------- 线路解析

def parse_play_urls(play_from: str, play_url: str,
                    default_line_name: str = "默认线路") -> List[Dict[str, Any]]:
    """解析 MacCMS 的 vod_play_from / vod_play_url 字符串为线路结构。

    格式:
        play_from: `lzm3u8$$$ffm3u8`
        play_url:  `第01集$url1#第02集$url2$$$第01集$url3#第02集$url4`

    返回:
        [{"line_name": "默认线路", "from": "lzm3u8",
          "episodes": [{"name": "第01集", "url": "https://..."}]}, ...]

    仅保留 http(s) 视频地址（m3u8/mp4）；无有效分集的线路不产出。
    """
    if not play_url:
        return []

    from_list = [f.strip() for f in str(play_from or "").split('$$$') if f.strip()]
    url_groups = [g for g in str(play_url or "").split('$$$')]

    lines: List[Dict[str, Any]] = []
    for idx, group_str in enumerate(url_groups):
        group_str = (group_str or "").strip()
        if not group_str:
            continue
        from_code = from_list[idx] if idx < len(from_list) else f"line_{idx + 1}"
        line_name = default_line_name if idx == 0 else f"{default_line_name}-{idx + 1}"

        episodes: List[Dict[str, str]] = []
        for ep_str in group_str.split('#'):
            ep_str = ep_str.strip()
            if not ep_str:
                continue
            if '$' in ep_str:
                name, url = ep_str.split('$', 1)
                name = name.strip()
                url = url.strip()
            else:
                name = f"第{len(episodes) + 1}集"
                url = ep_str.strip()
            if url.startswith(('http://', 'https://')):
                episodes.append({"name": name, "url": url})

        if episodes:
            lines.append({
                "line_name": line_name,
                "from": from_code,
                "episodes": episodes,
            })
    return lines


# ---------------------------------------------------------------- RawItem 构造

def _parse_episode_count(raw_title: str) -> int:
    """从原始标题提取"更新至 N 集 / 第 N 集"的集数；无则 0。"""
    match = _EPISODE_RE.search(raw_title or "")
    if not match:
        return 0
    try:
        return int(match.group(1))
    except (TypeError, ValueError):  # pragma: no cover - 正则已保证数字
        return 0


def _resolve_year(vod: Dict[str, Any], parts_year: Optional[int]) -> Optional[str]:
    """先取源站 vod_year；为空时回落到归一化提取的年份。"""
    raw_year = str(vod.get("vod_year") or "").strip()[:4]
    if raw_year.isdigit():
        return raw_year
    return str(parts_year) if parts_year else None


def build_item(vod: Dict[str, Any], site: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """单条 vod dict → RawItem（§2 P1 契约）；无有效线路 / 空标题返回 None。

    Args:
        vod: MacCMS list 接口的单个条目 dict。
        site: 站点配置（name / priority / line_name）。

    Returns:
        RawItem dict（含过滤前全字段；L1-L5 过滤由 pipeline.prefilter 负责）。
    """
    raw_title = str(vod.get("vod_name") or "").strip()
    if not raw_title:
        return None

    raw_type = str(vod.get("type_name") or "").strip()
    category = normalize_category(raw_type)
    parts = normalize_title(raw_title, category=category)

    line_name = str(site.get("line_name") or site.get("name") or "默认线路")
    lines = parse_play_urls(
        vod.get("vod_play_from", ""), vod.get("vod_play_url", ""),
        default_line_name=line_name)
    if not lines:
        return None

    item: Dict[str, Any] = {
        "raw_id": str(vod.get("vod_id", "")),
        "site": str(site.get("name") or "未命名源"),
        "priority": int(site.get("priority", 10) or 10),
        "raw_title": raw_title,
        "title": parts.norm_title,
        "search_title": parts.norm_title,
        "category": category,
        "sub_category": raw_type,
        "season": parts.seq,
        "episode": _parse_episode_count(raw_title),
        "year": _resolve_year(vod, parts.year),
        "poster": str(vod.get("vod_pic") or "").strip(),
        "overview": clean_overview(vod.get("vod_content", "")),
        "douban_id": str(vod.get("vod_douban_id") or "").strip(),
        "douban_score": str(vod.get("vod_douban_score") or "").strip(),
        "actor": str(vod.get("vod_actor") or "").strip(),
        "director": str(vod.get("vod_director") or "").strip(),
        "remarks": str(vod.get("vod_remarks") or "").strip(),
        "update_time": str(vod.get("vod_time") or "").strip(),
        "lines": lines,
    }
    return item


# ---------------------------------------------------------------- 页抓取

async def fetch_page(client: "AsyncHttpClient", api_urls: List[str], params: Dict[str, Any],
                     timeout: float = FETCH_TIMEOUT,
                     retries: int = FETCH_RETRIES) -> Tuple[Optional[Dict[str, Any]], str]:
    """带镜像轮换 + 退避的页抓取；永不抛异常。

    Args:
        client: 异步 HTTP 客户端（鸭子类型：`request()` 返回 FetchResult）。
        api_urls: 主镜像 + fallbacks。
        params: query 参数（ac=detail & pg=N & [h=hours]）。

    Returns:
        (data, used_url)：成功时 data 为服务端 JSON dict；全部失败返回 (None, "")。
    """
    urls = [u for u in (api_urls or []) if u]
    if not urls:
        return None, ""
    for url in urls:
        result: FetchResult = await client.get(
            url, params=params, timeout=timeout, retries=retries, as_json=True)
        if result.ok and isinstance(result.data, dict):
            return result.data, url
        if result.is_retryable and url is not urls[-1]:
            await asyncio.sleep(FALLBACK_SLEEP)
    return None, ""


# ---------------------------------------------------------------- 站点状态机

@dataclass
class MaccmsResult:
    """单站采集结果。"""

    site: str
    items: List[Dict[str, Any]] = field(default_factory=list)
    pages: int = 0
    done: bool = False
    reason: str = ""          # exhausted | deadline | failures | empty | aborted
    error: str = ""

    def to_report(self) -> Dict[str, Any]:
        """站点报表（harvest 汇总用）。"""
        return {
            "site": self.site,
            "pages": self.pages,
            "items": len(self.items),
            "done": self.done,
            "reason": self.reason,
            "error": self.error,
        }


class MaccmsSite:
    """单站 MacCMS 采集状态机：批量翻页（每站 4 页并发）+ 停止判定 + 断点回调。

    Args:
        site: 站点配置 dict（name / api_url / fallbacks / line_name / priority）。
        client: 共享 AsyncHttpClient（鸭子类型）。
        mode: "full"（阶段 0）| "incremental"（日常）。
        hours: 增量窗口小时数（服务端 h 参数 + 客户端时间窗复核）；full 忽略。
        start_page: 断点起始页（从该页继续）。
        deadline: 单调时钟硬超时（time.monotonic 值）；到点停止派发新页，已派发页照常完成。
        semaphore: 每站页级并发信号量；None 时内部创建 Semaphore(4)。
        on_page: 每完成一页回调 `on_page(site_name, page)`（编排层写断点游标）。
        settings: Settings 实例；None 时惰性加载。
    """

    def __init__(self, site: Dict[str, Any], client: "AsyncHttpClient", *,
                 mode: str = "incremental",
                 hours: Optional[int] = None,
                 start_page: int = 1,
                 deadline: Optional[float] = None,
                 semaphore: Optional[asyncio.Semaphore] = None,
                 on_page: Optional[Callable[[str, int], None]] = None,
                 settings: Optional[Settings] = None) -> None:
        self.site: Dict[str, Any] = site
        self.client: AsyncHttpClient = client
        self.mode: str = mode if mode in ("full", "incremental") else "incremental"
        self._settings: Settings = settings if settings is not None else load_settings()
        self.hours: int = max(int(hours if hours is not None
                                  else self._settings.get("crawl_hours", 24)), 0)
        self.start_page: int = max(int(start_page), 1)
        self.deadline: Optional[float] = deadline
        self._sem: asyncio.Semaphore = semaphore if semaphore is not None \
            else asyncio.Semaphore(CONCURRENT_PAGES)
        self.on_page: Optional[Callable[[str, int], None]] = on_page

        self.site_name: str = str(site.get("name") or "未命名源")
        self.api_urls: List[str] = [str(site.get("api_url") or "").rstrip('/')]
        self.api_urls += [str(u).rstrip('/') for u in (site.get("fallbacks") or []) if u]
        self.line_name: str = str(site.get("line_name") or self.site_name)

        self._cutoff: Optional[datetime] = None
        if self.mode == "incremental" and self.hours > 0:
            self._cutoff = datetime.now() - timedelta(hours=self.hours)

        # 心跳节流状态（crawl.site.page 事件）
        self._beat_page: int = 0
        self._beat_at: float = time_now()

    # ---------------------------------------------------------- 页抓取（受信号量）

    async def _guarded_fetch(self, page: int) -> Tuple[Optional[Dict[str, Any]], str]:
        """在 semaphore(4) 内抓取一页；永不抛异常。"""
        async with self._sem:
            params: Dict[str, Any] = {"ac": "detail", "pg": page}
            if self.mode == "incremental" and self._settings.get("crawl_hours", 24) and self.hours > 0:
                params["h"] = self.hours
            try:
                return await fetch_page(self.client, self.api_urls, params)
            except asyncio.CancelledError:  # pragma: no cover - 取消传播
                raise
            except Exception as exc:  # pragma: no cover - 防御：任何异常按失败页处理
                return None, f"{type(exc).__name__}: {exc}"

    # ---------------------------------------------------------- 停止判定

    def _can_issue(self, page: int, total_pages: Optional[int]) -> bool:
        """是否允许继续派发第 page 页。"""
        if self.deadline is not None and time_now() >= self.deadline:
            return False
        if total_pages is not None and page > total_pages:
            return False
        return True

    def _stop_reason(self, data: Optional[Dict[str, Any]], page: int,
                     total_pages: Optional[int]) -> Tuple[bool, str]:
        """按一页数据判定该站是否到尽头 / 越窗。

        Returns:
            (stop, reason)；reason ∈ exhausted / empty / out_of_window / ""。
        """
        if data is None:
            return False, ""
        vod_list = data.get("list") or []
        page_size = int(data.get("limit") or 0) or DEFAULT_PAGE_SIZE
        pagecount = int(data.get("pagecount") or 0) or 0
        if not vod_list:
            return True, "exhausted"
        # "条目数 < 页大小 = 站点尽头" 仅对全量模式成立：
        # 增量模式的最后一页通常不满页，只按窗口/空页判定。
        if self.mode == "full" and len(vod_list) < page_size:
            return True, "exhausted"
        # 增量：首页第一条越窗 → 按窗口停止（先于 pagecount 边界判定，
        # 保证越窗语义优先，报表 reason=out_of_window 而非 exhausted）。
        if self.mode == "incremental" and self._cutoff is not None:
            first_time = parse_vod_time((vod_list[0] or {}).get("vod_time"))
            if first_time is not None and first_time < self._cutoff:
                return True, "out_of_window"
        if pagecount and page >= pagecount:
            return True, "exhausted"
        return False, ""

    def _in_window(self, vod: Dict[str, Any]) -> bool:
        """增量模式客户端逐条复核是否在时间窗内；无法解析视为窗口内（宽容）。"""
        if self._cutoff is None:
            return True
        parsed = parse_vod_time(vod.get("vod_time"))
        if parsed is None:
            return True
        return parsed >= self._cutoff

    # ---------------------------------------------------------- 主循环

    async def run(self) -> MaccmsResult:
        """翻页主循环（full 模式翻到尽头；incremental 模式按窗口/超时停止）。

        Returns:
            MaccmsResult（页内条目已构造为 RawItem，去重由编排层负责）。
        """
        result = MaccmsResult(site=self.site_name)
        total_pages: Optional[int] = None
        page: int = self.start_page
        consecutive_failures: int = 0
        run_start = time_now()
        event("crawl.site.start", site=self.site_name, mode=self.mode,
              start_page=page, hours=self.hours)

        while self._can_issue(page, total_pages):
            # 断点续跑（start_page>1）且尚未拿到 pagecount 时，只先抓批内首页，
            # 用其 pagecount 校准 total_pages，避免对已回归的站点盲目多发越界页
            # （如从第 2 页续跑而全站仅 3 页，批内 4/5 页是纯浪费请求）；
            # 已知 pagecount 后（fresh 全量首批也在此后）再走满批并发。
            if self.start_page > 1 and total_pages is None:
                batch = [page]
            else:
                batch = [p for p in range(page, page + CONCURRENT_PAGES)
                         if self._can_issue(p, total_pages)]
            if not batch:
                break

            tasks = {p: asyncio.create_task(self._guarded_fetch(p)) for p in batch}
            fetched = dict(zip(batch, await asyncio.gather(*tasks.values())))
            stop_now = False
            reason_now = ""

            for p in sorted(fetched):  # 按页序落盘，保证断点游标单调
                data, err = fetched[p]
                if data is None:
                    consecutive_failures += 1
                    if consecutive_failures >= FAIL_THRESHOLD:
                        event("crawl.site.abort", site=self.site_name,
                              reason="failures", page=p)
                        stop_now, reason_now = True, "failures"
                        break
                    await asyncio.sleep(BACKOFF_AFTER_PAGE_FAIL)
                    continue
                consecutive_failures = 0

                stop, reason = self._stop_reason(data, p, total_pages)
                window_breach = False  # 增量：页内出现越窗记录（首个之后的）→ 处理完本页即停
                vod_list = data.get("list") or []
                for vod in vod_list:
                    if self.mode == "incremental" and not self._in_window(vod):
                        window_breach = True
                        continue
                    item = build_item(vod, self.site)
                    if item is not None:
                        result.items.append(item)

                result.pages += 1
                if self.on_page is not None:
                    try:
                        self.on_page(self.site_name, p)
                    except Exception:  # pragma: no cover - 断点写失败不中断采集
                        log_event("crawl.progress.write_error", "WARNING", None,
                                  site=self.site_name, page=p)

                # 页级心跳：每 N 页或距上次 ≥ interval 秒打一行，让 GA 日志随采集滚动
                if p - self._beat_page >= PAGE_BEAT_EVERY \
                        or time_now() - self._beat_at >= PAGE_BEAT_INTERVAL:
                    event("crawl.site.page", site=self.site_name, page=p,
                          pages=result.pages, items=len(result.items),
                          elapsed=round(time_now() - run_start, 1))
                    self._beat_page = p
                    self._beat_at = time_now()

                if stop:
                    stop_now, reason_now = True, (reason or "exhausted")
                    break
                if window_breach:
                    stop_now, reason_now = True, "out_of_window"
                    break

                if data.get("pagecount"):
                    total_pages = max(int(total_pages or 0), int(data["pagecount"]))

            if stop_now:
                # done 仅表示"采到自然尽头"（耗尽/越窗）；临时中止（失败/超时）
                # 不置 done，下一轮按断点游标续跑。
                result.done = reason_now in ("exhausted", "out_of_window")
                result.reason = reason_now
                break
            page = batch[-1] + 1

        if not result.reason:
            # while 条件退出（deadline / 页边界），无显式 stop 原因
            if self._over_deadline():
                result.reason = "deadline"
            elif total_pages is not None and page > total_pages:
                result.reason = "exhausted"
                result.done = True
            elif page < 0:  # pragma: no cover - 不可达防御
                result.reason = "aborted"
            else:
                result.reason = "exhausted"
                result.done = True

        event("crawl.site.end", site=self.site_name, pages=result.pages,
              items=len(result.items), done=result.done, reason=result.reason)
        return result

    def _over_deadline(self) -> bool:
        """是否已过硬超时。"""
        return self.deadline is not None and time_now() >= self.deadline


#: 模块级可替换时钟（测试注入用）
def time_now() -> float:
    """单调时钟（模块级可替换）。"""
    return _MONOTONIC()


_MONOTONIC = __import__("time").monotonic


def _set_monotonic(fn: Callable[[], float]) -> None:  # pragma: no cover - 测试辅助
    """替换单调时钟（仅测试用）。"""
    global _MONOTONIC
    _MONOTONIC = fn