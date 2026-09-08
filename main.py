# -*- coding: utf-8 -*-
"""main.py —— SuenMedia 入口（设计文档 §2 P6 / T05 重写）

子命令：
- `harvest`：仅采集（P1）+ 前置过滤（L1-L5）+ 粗合并（P3）→ **素材库建库**。
  供阶段 0 全量建库（`--full`=翻站点尽头）与日常增量（`--hours N`）使用；
  不刮削、不导出 —— 素材库消费由 `run` 负责（§7 预算分配）。
- `run`：完整端到端流水线（采集 → 探活 → 粗合并 → 素材库刮削 → 精合并 →
  导出 → 报表 → 推送 → git），单轮硬上限 30 分钟（§7 预算）。

连续两次退出过滤：harvest 与 run 的采集入口都过 `pipeline.prefilter.keep()`。

分层：本文件是入口层，只做编排（胶水），业务逻辑都在 pipeline / crawlers / sources。
旧入口的 metadata_scraper / aggregator / retry_queue 依赖已全部移除（T05e 删除）。
"""

import sys
import time
import asyncio
import argparse
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    load_settings,
    load_config,
    send_pushplus,
    git_push_backup,
    get_session,
)
from core.cache import CacheDB, MetaCache, RawLibrary, RetryStore
from core.http import default_client
from core.logging import log_event
from crawlers.harvest import harvest as _harvest_async
from pipeline.prefilter import Prefilter
from pipeline.probe import DomainProber
from pipeline.coarse_merge import CoarseMerger
from pipeline.scrape import ScrapeOrchestrator
from pipeline.fine_merge import FineMerger
from pipeline.export import Exporter
from pipeline.budget import Budget
from report import RuntimeStats, build_pushplus_html, build_summary_text

# 确保控制台行缓冲
sys.stdout.reconfigure(line_buffering=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="SuenMedia 自动化影视采集与刮削托管引擎（T05）")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- harvest：素材库建库（阶段 0 / 日常增量） ---
    p_h = sub.add_parser("harvest", help="仅采集+粗合并进素材库（不刮削不导出）")
    p_h.add_argument("--full", action="store_true",
                     help="全量模式：翻到站点尽头建库（阶段 0）")
    p_h.add_argument("--hours", type=int, default=None,
                     help="增量时间窗（小时），默认读取 settings.crawl_hours")
    p_h.add_argument("--no-check", action="store_true", help="跳过线路探活")

    # --- run：端到端流水线 ---
    p_r = sub.add_parser("run", help="完整流水线（采集+刮削+合并+导出+推送）")
    p_r.add_argument("--hours", type=int, default=None,
                     help="增量时间窗（小时），默认读取 settings.crawl_hours")
    p_r.add_argument("--dry-run", action="store_true",
                     help="干跑：不执行 Git 推送与外部通知")
    p_r.add_argument("--no-check", action="store_true", help="跳过 M3U8 线路探活")
    p_r.add_argument("--scrape-only", action="store_true",
                     help="跳过采集，仅消费素材库存量刮削（部署排障用）")
    return parser.parse_args()


# ---------------------------------------------------------------- 采集阶段

def harvest_items(mode: str, hours: int | None, deadline: float | None,
                  settings: dict) -> tuple[list, list]:
    """P1 采集（crawlers.harvest）+ L1-L5 前置过滤。

    Returns:
        (raw_items, site_reports)：raw_items 已过 prefilter 放行。
    """
    config = load_config()
    active_sites = list(config.enabled_sites) if hasattr(config, "enabled_sites") \
        else [s for s in config.get("SITES", []) if s.get("enabled", True)]
    pref = Prefilter()
    kept: list = []
    drop: int = 0

    result = asyncio.run(_harvest_async(
        sites=active_sites, mode=mode,
        hours=hours if mode == "incremental" else None,
        deadline=deadline,
        settings=None, config=None))
    # 过滤在采集完成后统一执行（harvest 内部不做过滤）
    # （crawlers.harvest 返回的已是去重后 RawItem，逐条过 prefilter）
    for it in result.items:
        verdict = pref.keep(it)
        if verdict.ok:
            kept.append(it)
        else:
            drop += 1

    reports = [{
        "site": r.get("site", ""),
        "count": r.get("items", 0),
        "error": r.get("error", ""),
        "done": bool(r.get("done")),
    } for r in result.reports]

    if drop:
        print(f"[过滤] 前置规则拦截 {drop} 条（短剧/解说/微电影等非目标内容）")
    return kept, reports


# ---------------------------------------------------------------- run 流水线

def cmd_run(args) -> int:
    start_time = time.time()
    settings = load_settings()
    budget = Budget(
        budget_seconds=settings.get("run_budget_seconds", 1800) or 1800,
        limits=settings.get("budget") or None,
        rate_limits=settings.get("rate_limits") or None,
    )

    print("\n" + "=" * 60)
    print("SuenMedia 全量采集与刮削流水线启动（T05）")
    print(f"  执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  总预算: {budget._budget_total:.0f}s | "
          f"模式: {'干跑' if args.dry_run else '生产'} | "
          f"采集: {'跳过' if args.scrape_only else ('近 ' + str(args.hours or settings.get('crawl_hours', 24)) + ' 小时')}")
    print("=" * 60)

    db = CacheDB()
    cache = MetaCache(db=db)
    library = RawLibrary(db)
    retry = RetryStore(db)

    stats = RuntimeStats()
    phase_times: dict = {}

    # ---------------- P1 增量采集 ----------------
    raw_items: list = []
    site_reports: list = []
    if not args.scrape_only:
        budget.phase_start("crawl")
        # P1 采集硬超时：min(预算 deadline, T_CRAWL_MAX)
        crawl_limit = float((settings.get("budget") or {}).get("crawl_max", 420) or 420)
        crawl_deadline = min(budget.deadline, time.monotonic() + crawl_limit)
        raw_items, site_reports = harvest_items(
            "incremental", args.hours, crawl_deadline, settings)
        stats.raw_items = len(raw_items)
        phase_times["crawl"] = budget.phase_done("crawl")
        site_summary = "，".join(f"{r.get('site','')} {r.get('count',0)}" for r in site_reports)
        print(f"[P1] 增量采集完成: {site_summary or '无站点更新'}  |  放行 {len(raw_items)} 条")
    else:
        print("[P1] 采集已跳过（--scrape-only），直接消费素材库存量")

    # ---------------- P2 线路探活（仅新域名） ----------------
    prober = DomainProber()
    session = get_session()
    if raw_items and not args.no_check and settings.get("enable_m3u8_check", True):
        budget.phase_start("probe")
        for it in raw_items:
            lines = it.get("lines") or []
            if lines:
                it["lines"] = prober.filter_lines_sync(default_client(), lines)
        phase_times["probe"] = budget.phase_done("probe")
        print(f"[P2] 线路探活完成（域名级，网络请求极少）")

    # ---------------- P3 粗合并 → 素材库 ----------------
    budget.phase_start("coarse")
    merger = CoarseMerger(library=library)
    merge_stats = merger.merge(raw_items)
    stats.library_new = merge_stats["inserted"]
    stats.deduped = merge_stats["entities"]
    stats.library_pending = library.pending_count()
    phase_times["coarse"] = budget.phase_done("coarse")
    print(f"[P3] 粗合并入库: 新增 {merge_stats['inserted']} | "
          f"素材库待刮削: {stats.library_pending}")

    # ---------------- P4 素材库刮削（预算分配） ----------------
    budget.phase_start("scrape")
    scraper = ScrapeOrchestrator(cache=cache, library=library,
                                 settings=settings.to_dict())
    workers = int(settings.get("scrape_workers", 12) or 12)
    scrape_budget = budget.scrape_budget()
    print(f"[P4] 刮削预算: {scrape_budget:.0f}s | 并发: {workers} 线程")

    # 热通道（近 7 天新 IP，A+B 全字段）
    hot_items = library.hot_batch(hot_window_days=float(
        (settings.get("library") or {}).get("hot_window_days", 7) or 7),
        limit=500)
    if hot_items:
        print(f"[P4][热] 近 7 天新 IP {len(hot_items)} 条 → A+B 全字段")
        hot_res = scraper.scrape_batch(hot_items, tier="AB", workers=workers)
        stats.scraped_hit += hot_res["hit"]
        stats.scraped_miss += hot_res["miss"]
        stats.scraped_retryable += hot_res["retryable"]
        stats.scraped_discard += hot_res["discard"]

    # 冷通道（素材库游标，Tier A）
    rps = budget.rate_effective("tmdb")
    cold_quota = max(int(scrape_budget * rps), 0)
    processed_this_round = 0
    while cold_quota > 0 and not budget.is_timeout:
        batch = library.pending_batch(quota=min(cold_quota, 200))
        if not batch:
            break
        if not budget.rebalance(max(0, library.pending_count())):
            break
        cold_res = scraper.scrape_batch(batch, tier="A", workers=workers)
        stats.scraped_hit += cold_res["hit"]
        stats.scraped_miss += cold_res["miss"]
        stats.scraped_retryable += cold_res["retryable"]
        stats.scraped_discard += cold_res["discard"]
        budget.record_scraped(cold_res["hit"] + cold_res["miss"])
        processed_this_round += len(batch)
        cold_quota -= len(batch)
        if processed_this_round % 100 == 0:
            print(f"[P4][冷] 已处理 {processed_this_round} 条 | 剩余预算 {budget.remaining():.0f}s")
    print(f"[P4] 刮削完成: 命中 {stats.scraped_hit} | miss {stats.scraped_miss} | "
          f"retryable {stats.scraped_retryable} | discard {stats.scraped_discard} | "
          f"本轮处理 {processed_this_round + len(hot_items)} 条")
    phase_times["scrape"] = budget.phase_done("scrape")

    # ---------------- P5 精细合并 + P6 导出 ----------------
    budget.phase_start("fine")
    # 从素材库取已刮削命中条目（本库全部 scraped=hit 的 payload）
    hit_entities = _load_scraped_hits(library)
    if hit_entities:
        fine = FineMerger(settings=settings.to_dict())
        for h in hit_entities:
            fine.add_item(h)
        fused = fine.finish()
        final_items = fused["items"]
        unmatched = fused["unmatched"]
        stats.final_items = len(final_items)
        stats.unmatched = len(unmatched)
    else:
        final_items, unmatched = [], []
        stats.final_items = 0
        stats.unmatched = library.stats().get("pending", 0)
    phase_times["fine"] = budget.phase_done("fine")

    budget.phase_start("export")
    exporter = Exporter("product")
    export_stats = exporter.export(final_items, unmatched)
    if not export_stats["videos"] and not args.dry_run:
        pass  # 空产物也正常（本轮无命中），不报错
    phase_times["export"] = budget.phase_done("export")
    stats.export_ok = True

    # ---------------- 报表 / 推送 / git ----------------
    rep = budget.report(**phase_times)
    stats.elapsed = rep.t_total if rep.t_total else (time.time() - start_time)
    stats.budget_status = rep.status
    stats.limits = rep.limits
    stats.error_count = 0
    try:
        stats.category_stats = library.categories()
    except Exception:  # pylint: disable=broad-except
        pass

    print(build_summary_text(stats, start_time))

    if not args.dry_run:
        send_pushplus(
            title=f"SuenMedia 采集汇报: 库内共 {stats.final_items} 部",
            content=build_pushplus_html(stats))
        git_push_backup(
            f"auto: 影视库更新 +{stats.final_items} "
            f"[{datetime.now().strftime('%m-%d %H:%M')}]")

    print("\n全部采集与托管任务执行完毕\n")
    return 0


def _load_scraped_hits(library: RawLibrary) -> list:
    """从素材库取本库 scraped=hit 的 payload（供 P5 精合并）。

    RawLibrary 未提供全量 hit 查询 → 用其 categories + get() 枚举代价高；
    T05 简化：直接 SQL 取 raw_library 中 scrape_status='hit' 的 payload。
    """
    db = library._db
    rows = db.query_all(
        "SELECT payload_json, confidence FROM raw_library WHERE scrape_status='hit'")
    out = []
    for r in rows or []:
        payload = r["payload_json"] if r else ""
        if payload:
            try:
                import json as _json
                ent = _json.loads(payload)
            except (TypeError, ValueError):
                continue
            # 回填刮削字段（_library 状态）
            ent["_library"] = {"scrape_status": "hit"}
            # 防御：P5 消费顶层 `lines`——历史 CoarseEntity 曾丢线路，
            # 若 payload 缺失则从 primary/siblings 重组（T04/T05 修复）
            if not ent.get("lines"):
                lines, seen = [], set()
                for node in [ent.get("primary")] + list(ent.get("siblings") or []):
                    for line in (node or {}).get("lines") or []:
                        if not isinstance(line, dict) or not line.get("url"):
                            continue
                        dk = str(line["url"]).strip().rstrip("/")
                        if dk in seen:
                            continue
                        seen.add(dk)
                        lines.append(line)
                if lines:
                    ent["lines"] = lines
            # 防御：confidence 回填（旧版本 `_merge_entity` 曾覆盖为 None，
            # P5 `meets_gate` 需要 int；库列是最终可信来源）
            if ent.get("confidence") is None:
                ent["confidence"] = r["confidence"] if r else None
            out.append(ent)
    return out


# ---------------------------------------------------------------- harvest 子命令

def cmd_harvest(args) -> int:
    """阶段 0 建素材库 / 日常增量采集入库。"""
    start_time = time.time()
    settings = load_settings()
    mode = "full" if args.full else "incremental"

    print("\n" + "=" * 60)
    print(f"SuenMedia 采集入库（harvest）| 模式: {'全量建库' if mode == 'full' else '增量'}")
    print(f"  执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    db = CacheDB()
    library = RawLibrary(db)
    prober = DomainProber()

    raw_items, site_reports = harvest_items(
        mode, args.hours, None, settings)
    if not raw_items:
        print("[提示] 本轮未拉取到新作品数据，流程结束")
        return 0

    if not args.no_check and settings.get("enable_m3u8_check", True):
        for it in raw_items:
            lines = it.get("lines") or []
            if lines:
                it["lines"] = prober.filter_lines_sync(default_client(), lines)

    merger = CoarseMerger(library=library)
    merge_stats = merger.merge(raw_items)
    print(f"[P3] 粗合并入库: 新增 {merge_stats['inserted']} 条 | "
          f"素材库总量: {library.stats()['total']} | 待刮削: {library.pending_count()}")

    elapsed = time.time() - start_time
    print(f"\n[完成] harvest 耗时 {elapsed:.1f}s | 站点: "
          f"{'，'.join(r['site'] for r in site_reports) or '无'}")
    return 0


# ---------------------------------------------------------------- 入口

def main() -> None:
    args = parse_args()
    if args.command == "harvest":
        sys.exit(cmd_harvest(args))
    elif args.command == "run":
        sys.exit(cmd_run(args))
    else:  # pragma: no cover
        print("未知子命令")
        sys.exit(2)


if __name__ == "__main__":
    main()