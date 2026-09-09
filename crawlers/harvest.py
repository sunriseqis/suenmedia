# -*- coding: utf-8 -*-
"""crawlers/harvest.py —— 采集编排（httpx async，设计文档 §2 P0/P1 / T03）

迁移自根目录 harvest.py，核心变更：

1. **async 编排**：`asyncio.gather` 跨站并发（10 站），每站内部
   `asyncio.Semaphore(4)` 页级并发翻页（§2 P1 / T03 验收②）。
2. **采集与刮削彻底解耦**：本模块只做 P1 采集，返回 `list[RawItem]`。
   P2 探活 / P3 粗合并 / P4 刮削由入口层（main.py，T05）经 `pipeline.*` 编排。
3. **两种模式**：
   - `full`（阶段 0 建素材库）：各站翻到尽头置 `done=true`，可跨多次派发断点续跑；
   - `incremental`（日常）：按 24h 时间窗停止 + 硬超时 T_CRAWL_MAX=420s 保护。
4. **断点续采**（验收⑦）：
   - `RawSeen` 记 `(site, raw_id)` + 内容哈希 —— 同 id 无变化跳过、有变化（老剧加集）重入管线；
   - `progress.json` 每站独立 page 游标，kill -9 后重启从断点继续，无重复无丢失。
5. **CRAWLER_DISPATCHER**：`maccms_v10` 走新异步实现；未知类型回落动态插件
   （兼容旧 `crawlers/{plugin}.crawl` 约定，sync/async 皆可）。

入口：
    python -m crawlers.harvest                # incremental（默认）
    python -m crawlers.harvest full           # full（阶段 0 全量建库）
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import inspect
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.cache import CacheDB, RawSeen
from core.config import Config, Settings, load_config, load_settings
from core.http import AsyncHttpClient
from core.logging import event, log_event
from crawlers.maccms import MaccmsResult, MaccmsSite

__all__ = [
    "harvest",
    "harvest_all",
    "HarvestResult",
    "ProgressStore",
    "item_content_hash",
    "CRAWLER_DISPATCHER",
]

#: 默认断点游标文件（设计文档 §2 P1）
DEFAULT_PROGRESS_PATH: str = os.path.join("product", "progress.json")

#: 硬超时默认值 = settings.budget.crawl_max（420s）
DEFAULT_CRAWL_TIMEOUT: int = 420

#: 采集器分发表（站点 type → 异步采集函数）；maccms_v10 走 crawlers.maccms 新实现
async def _crawl_maccms_v10(site: Dict[str, Any], *, client: AsyncHttpClient,
                            mode: str, hours: int, start_page: int,
                            deadline: Optional[float],
                            on_page: Optional[Callable[[str, int], None]],
                            settings: Settings,
                            concurrency: int) -> MaccmsResult:
    """单个 MacCMS 站点采集（每站 semaphore 页级并发）。"""
    sem = asyncio.Semaphore(max(int(concurrency), 1))
    crawler = MaccmsSite(
        site, client, mode=mode, hours=hours,
        start_page=start_page, deadline=deadline,
        semaphore=sem, on_page=on_page, settings=settings)
    return await crawler.run()


CRAWLER_DISPATCHER: Dict[str, Callable[..., Any]] = {
    "maccms_v10": _crawl_maccms_v10,
}


def item_content_hash(item: Dict[str, Any]) -> str:
    """RawItem 内容哈希：仅对"影响刮削结果"的稳定字段求哈希。

    `update_time`（vod_time 每分钟都在变）**不参与**哈希，否则导致"老剧无变化
    也每轮重采"。线路/标题/简介/海报/豆瓣字段变化（如老剧加集）会改变哈希，
    从而被重新拾取进入管线。
    """
    stable = {
        "raw_id": item.get("raw_id"),
        "title": item.get("title"),
        "season": item.get("season"),
        "episode": item.get("episode"),
        "year": item.get("year"),
        "lines": item.get("lines"),
        "poster": item.get("poster"),
        "overview": item.get("overview"),
        "douban_id": item.get("douban_id"),
        "douban_score": item.get("douban_score"),
    }
    blob = json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


class ProgressStore:
    """每站独立断点游标（设计文档 §2 P1 / T03 验收⑦）。

    文件格式（`product/progress.json`）::

        {
          "索尼资源": {"page": 137, "done": true,  "updated_at": "2026-09-08 15:00:00"},
          "红牛资源": {"page": 42,  "done": false, "updated_at": "2026-09-08 15:01:00"}
        }

    `page` 语义 = **最后完成页**；断点续跑从 `page + 1` 开始。
    """

    def __init__(self, path: Optional[str] = None,
                 settings: Optional[Settings] = None) -> None:
        self.path: str = path or DEFAULT_PROGRESS_PATH
        self._settings: Settings = settings if settings is not None else load_settings()
        self._lock = threading.RLock()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._loaded: bool = False

    # ---------------------------------------------------------- 读写

    def load(self) -> Dict[str, Dict[str, Any]]:
        """加载游标文件（存在才读；损坏时重置为空）。"""
        if self._loaded:
            return self._data
        with self._lock:
            if self._loaded:
                return self._data
            data: Dict[str, Dict[str, Any]] = {}
            if os.path.exists(self.path):
                try:
                    with open(self.path, "r", encoding="utf-8") as handle:
                        raw = json.load(handle)
                    if isinstance(raw, dict):
                        data = {str(k): dict(v) for k, v in raw.items() if isinstance(v, dict)}
                except (OSError, ValueError):  # pragma: no cover - 损坏重置
                    data = {}
            self._data = data
            self._loaded = True
            return self._data

    def start_page(self, site: str) -> int:
        """断点起始页：最后完成页 + 1；无记录返回 1。"""
        entry = self.load().get(site, {})
        try:
            return max(int(entry.get("page", 0)), 0) + 1
        except (TypeError, ValueError):  # pragma: no cover - 脏数据防御
            return 1

    def is_done(self, site: str) -> bool:
        """该站是否已标记完成（full 模式跳过已完成站点）。"""
        entry = self.load().get(site, {})
        return bool(entry.get("done"))

    def mark_page(self, site: str, page: int) -> None:
        """记录最后完成页（page 语义：**已完成**的页号，幂等取最大）。"""
        with self._lock:
            entry = self.load().setdefault(site, {})
            try:
                prev = max(int(entry.get("page", 0)), 0)
            except (TypeError, ValueError):  # pragma: no cover
                prev = 0
            if int(page) > prev:
                entry["page"] = int(page)
                entry["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                self._save_locked()

    def mark_done(self, site: str, done: bool = True) -> None:
        """标记站点是否采到尽头（full 模式 done=true 后下一轮跳过）。"""
        with self._lock:
            entry = self.load().setdefault(site, {})
            entry["done"] = bool(done)
            entry["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._save_locked()

    def _save_locked(self) -> None:
        """落盘（调用方持锁）。"""
        try:
            parent = os.path.dirname(os.path.abspath(self.path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            tmp = f"{self.path}.tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(self._data, handle, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except OSError as exc:  # pragma: no cover - 写失败不致命
            log_event("crawl.progress_write_error", "WARNING", None,
                      path=self.path, error=str(exc))

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """当前快照。"""
        return {k: dict(v) for k, v in self.load().items()}


@dataclass
class HarvestResult:
    """一轮采集的汇总结果。"""

    items: List[Dict[str, Any]] = field(default_factory=list)
    reports: List[Dict[str, Any]] = field(default_factory=list)
    progress: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    elapsed: float = 0.0
    mode: str = "incremental"

    @property
    def total_raw(self) -> int:
        """各站原始条目合计（未去重）。"""
        return sum(int(r.get("items", 0)) for r in self.reports)

    @property
    def done_sites(self) -> List[str]:
        """采到尽头的站点。"""
        return [r["site"] for r in self.reports if r.get("done")]


# ---------------------------------------------------------------- 去重

class _SeenDeduper:
    """RawSeen 去重 + 内容哈希更新检测。"""

    def __init__(self, seen: RawSeen) -> None:
        self._seen: RawSeen = seen
        self.skipped: int = 0
        self.updated: int = 0
        self.new: int = 0

    def keep(self, item: Dict[str, Any]) -> bool:
        """判定条目是否进入管线：全新 / 内容有变化 → True；无变化旧条目 → False。"""
        site = str(item.get("site") or "")
        raw_id = str(item.get("raw_id") or "")
        if not raw_id:
            # 极少数源站无 vod_id：以内容哈希为身份，保证去重仍生效
            raw_id = f"hash-{item_content_hash(item)[:16]}"
            item["raw_id"] = raw_id
        content_hash = item_content_hash(item)
        if self._seen.seen(site, raw_id):
            if self._seen.hash_of(site, raw_id) == content_hash:
                self.skipped += 1
                return False
            self.updated += 1
            self._seen.mark(site, raw_id, content_hash)
            return True
        self.new += 1
        self._seen.mark(site, raw_id, content_hash)
        return True


# ---------------------------------------------------------------- 采集任务

async def _dispatch_site(site: Dict[str, Any], *, client: AsyncHttpClient,
                         mode: str, hours: int, start_page: int,
                         deadline: Optional[float],
                         on_page: Optional[Callable[[str, int], None]],
                         settings: Settings,
                         concurrency: int, max_pages_hint: int) -> MaccmsResult:
    """按站点 type 分发：maccms_v10 → 新异步实现；未知 → 动态插件（async/sync）。"""
    site_type = str(site.get("type") or "maccms_v10")
    func = CRAWLER_DISPATCHER.get(site_type)
    if func is not None:
        return await func(site, client=client, mode=mode, hours=hours,
                          start_page=start_page, deadline=deadline,
                          on_page=on_page, settings=settings, concurrency=concurrency)

    # 动态插件：crawlers/{crawler_plugin or type}.crawl(site, ...)
    plugin_name = str(site.get("crawler_plugin") or site_type)
    try:
        module = importlib.import_module(f"crawlers.{plugin_name}")
    except Exception as exc:
        return MaccmsResult(site=str(site.get("name") or plugin_name),
                            error=f"plugin import failed: {exc}")
    fn = getattr(module, "crawl", None)
    if fn is None:
        return MaccmsResult(site=str(site.get("name") or plugin_name),
                            error=f"plugin {plugin_name} 缺少 crawl()")
    try:
        if inspect.iscoroutinefunction(fn):
            result = await fn(site, client=client, hours=hours, max_pages=max_pages_hint)
        else:  # sync 插件：线程池兜底（旧签名 crawl(site, session, hours, max_pages)）
            result = await asyncio.get_running_loop().run_in_executor(
                None, lambda: fn(site, None, hours=hours, max_pages=max_pages_hint))
        if isinstance(result, MaccmsResult):
            return result
        items = list(result or [])  # 旧约定：直接返回 items 列表
        return MaccmsResult(site=str(site.get("name") or plugin_name), items=items)
    except Exception as exc:
        return MaccmsResult(site=str(site.get("name") or plugin_name), error=str(exc))


# ---------------------------------------------------------------- 主入口

async def harvest(sites: Optional[List[Dict[str, Any]]] = None, *,
                  mode: str = "incremental",
                  hours: Optional[int] = None,
                  deadline: Optional[float] = None,
                  db: Optional[Any] = None,
                  client: Optional[AsyncHttpClient] = None,
                  progress_path: Optional[str] = None,
                  settings: Optional[Settings] = None,
                  config: Optional[Config] = None,
                  on_page: Optional[Callable[[str, int], None]] = None,
                  concurrency: Optional[int] = None) -> HarvestResult:
    """跨站并发采集（P1）。

    Args:
        sites: 站点配置列表；None 时取 config.enabled_sites。
        mode: "full"（阶段 0 全量建库）| "incremental"（日常增量，默认）。
        hours: 增量时间窗（小时）；None 用 settings.crawl_hours。
        deadline: 硬超时（time.monotonic 值）；incremental 默认 now + budget.crawl_max。
        db: SQLite 库（CacheDB 实例 / 路径 / None → 默认 product/suenmedia.db）。
        client: 共享 AsyncHttpClient；None 时内部创建并在结束时关闭。
        progress_path: progress.json 路径；None 用默认。
        settings / config: 配置实例；None 惰性加载。
        on_page: 覆盖默认进度写回调（测试注入用）。
        concurrency: 每站页级并发；None 用 settings.crawl_per_site_concurrency（默认 4）。

    Returns:
        HarvestResult（items = 去重后进入管线的 RawItem；reports / progress 报表）。
    """
    start = time.monotonic()
    settings = settings if settings is not None else load_settings()
    config = config if config is not None else load_config()
    sites = sites if sites is not None else config.enabled_sites
    sites = list(sites or [])
    mode = mode if mode in ("full", "incremental") else "incremental"

    crawl_hours = max(int(hours if hours is not None else settings.get("crawl_hours", 24)), 0)
    run_deadline = deadline
    if mode == "incremental":
        if run_deadline is None:
            crawl_seconds = int(settings.budget_value("crawl_max")
                                or settings.get("crawl_timeout", DEFAULT_CRAWL_TIMEOUT))
            run_deadline = time.monotonic() + max(crawl_seconds, 1)
    per_site_concurrency = concurrency if concurrency is not None \
        else int(settings.get("crawl_per_site_concurrency", 4) or 4)

    db_handle = db if isinstance(db, CacheDB) else CacheDB(db) if db else CacheDB()
    seen = RawSeen(db_handle)
    progress = ProgressStore(progress_path, settings=settings)
    progress.load()
    inner_client = client
    owns_client = inner_client is None
    if inner_client is None:
        inner_client = AsyncHttpClient()

    def _default_on_page(site_name: str, page: int) -> None:
        # 只写 full 模式的 page 游标：增量窗口前移，若写 page 会把后续
        # full 建库的断点起点污染（full 会从增量残留的 page+1 跳过前 N 页）。
        if mode == "full":
            progress.mark_page(site_name, page)
        elif on_page is None:
            pass  # 外部未注入回调时，增量不落 page 游标

    page_cb = on_page if on_page is not None else _default_on_page

    async def _task(site: Dict[str, Any]) -> Tuple[MaccmsResult, List[Dict[str, Any]]]:
        site_name = str(site.get("name") or "未命名源")
        if mode == "full" and progress.is_done(site_name):
            event("crawl.site.skip_done", site=site_name)
            return (MaccmsResult(site=site_name, done=True, reason="already_done"),
                    [], _SeenDeduper(seen))

        if not site.get("api_url"):
            event("crawl.site.skip_no_api", site=site_name)
            return (MaccmsResult(site=site_name, reason="empty"),
                    [], _SeenDeduper(seen))

        deduper = _SeenDeduper(seen)  # 每站独立统计

        # 断点游标只服务 full 模式（阶段 0 建库跨派发续跑）：
        # 增量窗口每天前移，正确起点永远在第 1 页（h=24 服务端裁窗 + RawSeen 去重兜底）。
        start_page = progress.start_page(site_name) if mode == "full" else 1
        result = await _dispatch_site(
            site, client=inner_client, mode=mode, hours=crawl_hours,
            start_page=start_page, deadline=run_deadline,
            on_page=page_cb, settings=settings,
            concurrency=per_site_concurrency,
            max_pages_hint=int(settings.get("max_pages_per_site", 20) or 20))
        if result.error:
            event("crawl.site.error", site=site_name, error=result.error)

        kept_items: List[Dict[str, Any]] = []
        for item in result.items:
            if item.get("site") != site_name:  # 插件可能不带 site 字段
                item["site"] = site_name
            if deduper.keep(item):
                kept_items.append(item)

        if mode == "full":
            progress.mark_done(site_name, result.done)
        return result, kept_items, deduper

    tasks = [_task(site) for site in sites]
    results: List[Tuple[MaccmsResult, List[Dict[str, Any]], "_SeenDeduper"]] = []
    if tasks:
        results = await asyncio.gather(*tasks)

    items: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    skipped_total = updated_total = new_total = 0
    for result, kept, deduper in results:
        items.extend(kept)
        report = result.to_report()
        report["kept"] = len(kept)
        report["skipped"] = deduper.skipped
        report["updated"] = deduper.updated
        report["new"] = deduper.new
        reports.append(report)
        skipped_total += deduper.skipped
        updated_total += deduper.updated
        new_total += deduper.new

    if owns_client:
        await inner_client.aclose()

    elapsed = time.monotonic() - start
    event("crawl.harvest.end", mode=mode, sites=len(sites), items=len(items),
          raw_total=sum(r.get("items", 0) for r in reports),
          skipped=skipped_total, updated=updated_total,
          elapsed=round(elapsed, 3))
    return HarvestResult(
        items=items, reports=reports, progress=progress.snapshot(),
        elapsed=elapsed, mode=mode)


def harvest_all(hours: Optional[int] = None, max_pages: Optional[int] = None,
                mode: str = "incremental") -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """同步便捷入口（兼容旧 harvest_all 签名）：返回 (items, reports)。

    Args:
        hours: 增量窗口；None 用 settings.crawl_hours。
        max_pages: 兼容参数（新实现按窗口/尽头收敛，不强制页数上限）。
        mode: "full" 或 "incremental"。

    Returns:
        (items, reports)——reports 结构与旧版兼容（site/count/error）。
    """
    settings = load_settings()
    crawl_h = hours if hours is not None else int(settings.get("crawl_hours", 24))
    result = asyncio.run(harvest(mode=mode, hours=crawl_h))
    legacy_reports = [
        {"site": r["site"], "count": r["items"], "error": r["error"] or None}
        for r in result.reports
    ]
    return result.items, legacy_reports


# ---------------------------------------------------------------- CLI

def _main(argv: Optional[List[str]] = None) -> int:
    """命令行入口：`python -m crawlers.harvest [full|incremental] [hours]`。"""
    import sys
    args = list(argv if argv is not None else sys.argv[1:])
    mode = "incremental"
    hours: Optional[int] = None
    for arg in args:
        if arg in ("full", "incremental"):
            mode = arg
        elif arg.isdigit():
            hours = int(arg)
    print(f"[harvest] mode={mode} hours={hours or 'default'}")
    result = asyncio.run(harvest(mode=mode, hours=hours))
    for report in result.reports:
        status = "error" if report.get("error") else "ok"
        print(f"  - {report['site']}: {status} pages={report['pages']} "
              f"raw={report['items']} kept={report.get('kept', 0)} "
              f"done={report['done']} reason={report['reason'] or '-'}")
    print(f"[harvest] total_kept={len(result.items)} elapsed={result.elapsed:.1f}s")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI 入口
    raise SystemExit(_main())