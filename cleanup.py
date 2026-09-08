"""


"""
import sys
import os
import re
import json
import gzip
import time
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import load_settings, get_session
from core.http import default_client
from pipeline.probe import DomainProber, parse_domain
from pipeline.export import Exporter

#: 模块级共享探活器：域名注册表状态跨条目复用（同域名仅首条需探测）
_PROBER = None
_PRODUCT_DIR = "product"


def _load_catalog() -> dict:
    """从 v3 产物重建 {category: {bangou: item_with_episodes}}。

    T05 变更：不再依赖 aggregator.MediaAggregator 内存 catalog，
    直接读 videos.json（元数据）+ episodes.jsonl.gz（分集）组装。
    """
    catalog: dict = {}
    videos_path = os.path.join(_PRODUCT_DIR, "videos.json")
    episodes_path = os.path.join(_PRODUCT_DIR, "episodes.jsonl.gz")
    if not os.path.exists(videos_path):
        return catalog
    with open(videos_path, encoding="utf-8") as f:
        videos = json.load(f)
    # 分集索引 {bangou: [season...]}
    season_map: dict = {}
    if os.path.exists(episodes_path):
        with gzip.open(episodes_path, "rt", encoding="utf-8") as f:
            for line in f:
                row = json.loads(line)
                season_map.setdefault(row.get("bangou", ""), []).append(row)
    for it in videos:
        bangou = str(it.get("bangou") or it.get("id") or "")
        if not bangou:
            continue
        item = dict(it)
        # 回填分集（seasons + episodes）
        seasons = []
        for srow in season_map.get(bangou, []):
            seasons.append({
                "season_number": int(srow.get("season_number") or 1),
                "season_title": f"第 {int(srow.get('season_number') or 1)} 季",
                "episodes": srow.get("episodes") or [],
            })
        if seasons:
            item["seasons"] = seasons
            item["type"] = "series"
        elif item.get("url") or item.get("episode_count", 0) > 0:
            item["type"] = "video"
        category = str(it.get("category") or "movies")
        catalog.setdefault(category, {})[bangou] = item
    return catalog


def _get_prober() -> DomainProber:
    global _PROBER
    if _PROBER is None:
        _PROBER = DomainProber()
    return _PROBER


def check_single_m3u8(url: str, session=None, timeout: int = 2) -> bool:
    """域名级单 URL 探活（同签名兼容旧 check_single_m3u8）。

    底层 pipeline.probe：alive 域名直接放行（0 请求）、dead 直接拒绝、
    新域名 / 冷却到期探 1 次；session / timeout 为历史参数，
    探活统一走默认同步客户端 + DomainRegistry 持久状态。
    """
    if not url or not url.startswith(("http://", "https://")):
        return False
    domain = parse_domain(url)
    if not domain:
        return False
    return _get_prober().probe_domain_sync(default_client(), domain, url)

def is_item_old(item: dict, threshold_date: str) -> bool:
    """判断条目是否属于 90-180 天前的老数据"""
    date_str = item.get("last_updated") or item.get("year") or item.get("date") or item.get("first_air_date") or ""
    m = re.search(r'\d{4}-\d{2}-\d{2}', str(date_str))
    if m:
        return m.group(0) < threshold_date
    m_year = re.search(r'\b(19\d{2}|20\d{2})\b', str(date_str))
    if m_year:
        return f"{m_year.group(1)}-12-31" < threshold_date
    return False

def verify_movie_or_series_alive(item: dict, session) -> tuple[bool, dict]:
    """快速抽样探活电影或电视剧线路"""
    m_type = item.get("type", "video")

    if m_type == "video":
        primary_url = item.get("url")
        is_primary_ok = check_single_m3u8(primary_url, session, timeout=2) if primary_url else False
        alive_alts = []
        for alt in item.get("alt_urls", []):
            alt_u = alt.get("url")
            if alt_u and check_single_m3u8(alt_u, session, timeout=2):
                alive_alts.append(alt)

        if is_primary_ok:
            item["alt_urls"] = alive_alts
            return True, item
        if alive_alts:
            promoted = alive_alts.pop(0)
            item["url"] = promoted["url"]
            item["alt_urls"] = alive_alts
            return True, item
        return False, item

    else:
        # series: 逐季抽查首集有效性 (只查 S1E1 会漏掉后续几季线路已换但 S1 恰好全灭/或反之的情况)
        seasons = item.get("seasons", [])
        if not seasons:
            return False, item

        def _check_season_eps(s: dict) -> bool:
            eps = s.get("episodes") or []
            if not eps:
                # 该季无分集: 视为无数据, 不据此判定整条死链
                return None
            ep0 = eps[0]
            primary_ep_url = ep0.get("url")
            if primary_ep_url and check_single_m3u8(primary_ep_url, session, timeout=2):
                return True
            for alt in ep0.get("alt_urls", []):
                alt_u = alt.get("url")
                if alt_u and check_single_m3u8(alt_u, session, timeout=2):
                    return True
            return False

        # 任一季首集探活成功即保留; 所有有分集的季首集都死才判定失效
        saw_episodes = False
        for s in seasons:
            res = _check_season_eps(s)
            if res is None:
                continue
            saw_episodes = True
            if res:
                return True, item
        if not saw_episodes:
            return False, item
        return False, item

def run_cleanup(days: int = 90, dry_run: bool = False, max_workers: int = 20):
    # GitHub Actions 等非 TTY 环境下也实时刷新进度日志（行缓冲）
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    start_time = time.time()
    threshold_dt = datetime.now() - timedelta(days=days)
    threshold_str = threshold_dt.strftime("%Y-%m-%d")

    print("\n" + "=" * 60)
    print("🧹 SuenMedia 历史影视数据探活与安全清洗引擎启动")
    print(f"⏰ 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 审查门槛: 距今 {days} 天前 ({threshold_str} 之前的数据)")
    print(f"⚙️ 模式: {'预览测试 (dry-run, 不写回磁盘)' if dry_run else '生产正式清洗'}")
    print("=" * 60)

    session = get_session(use_proxy=False)
    agg_catalog = _load_catalog()
    total_inspected = 0
    total_old = 0
    total_retained = 0
    total_purged = 0

    for cat in agg_catalog:
        cat_entities = list(agg_catalog[cat].values())
        if not cat_entities:
            continue

        print(f"\n[审查] 分类 [{cat}]: 库内共 {len(cat_entities)} 部作品...")
        total_inspected += len(cat_entities)

        recent_items = []
        old_items = []

        for it in cat_entities:
            if is_item_old(it, threshold_str):
                old_items.append(it)
            else:
                recent_items.append(it)

        total_old += len(old_items)
        print(f"  - 近期活跃影视: {len(recent_items)} 部 (直接豁免保护)")
        print(f"  - 老旧待查影视: {len(old_items)} 部 (启动并发抽样探活)...")

        cleaned_old_items = []
        purged_in_cat = 0

        if old_items:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(verify_movie_or_series_alive, it, session): it for it in old_items}
                done = 0
                for fut in as_completed(futures):
                    done += 1
                    alive, clean_it = fut.result()
                    if alive:
                        cleaned_old_items.append(clean_it)
                    else:
                        purged_in_cat += 1
                    if done % 50 == 0 or done == len(old_items):
                        print(f"  [探活进度] {done}/{len(old_items)} ({done*100//len(old_items)}%)", flush=True)

        final_cat_items = recent_items + cleaned_old_items
        total_retained += len(final_cat_items)
        total_purged += purged_in_cat
        print(f"  -> 分类 [{cat}] 清洗结果: 存活保留 {len(final_cat_items)} 部, 安全下架全挂死链 {purged_in_cat} 部")

        if purged_in_cat > 0:
            agg_catalog[cat] = {
                (it.get("bangou") or it.get("id")): it
                for it in final_cat_items
                if (it.get("bangou") or it.get("id"))
            }

    if not dry_run and total_purged > 0:
        # T05：经 Exporter 重写 v3 产物（videos.json + episodes.jsonl.gz + manifest）
        all_items = [it for cat in agg_catalog.values() for it in cat.values()]
        unmatched = []
        um_path = os.path.join(_PRODUCT_DIR, "unmatched.json")
        if os.path.exists(um_path):
            try:
                with open(um_path, encoding="utf-8") as f:
                    unmatched = json.load(f)
            except (OSError, ValueError):
                unmatched = []
        Exporter(_PRODUCT_DIR).export(all_items, unmatched)

    duration = time.time() - start_time
    print("\n" + "=" * 60)
    print("📊 清洗报表汇总:")
    print(f"  ⏱️ 执行耗时: {duration:.1f} 秒")
    print(f"  📦 检查总作品数: {total_inspected} 部")
    print(f"  ⏳ 命中老旧数据 (> {days} 天): {total_old} 部")
    print(f"  ✅ 存活保留: {total_retained} 部")
    print(f"  🗑️ 安全剔除全失效死链: {total_purged} 部")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SuenMedia 历史影视数据生命周期清洗脚本")
    parser.add_argument("--days", type=int, default=90, help="审查天数门槛，默认 90 天前数据")
    parser.add_argument("--dry-run", action="store_true", help="演练模式，仅输出统计不修改数据")
    args = parser.parse_args()

    run_cleanup(days=args.days, dry_run=args.dry_run)
