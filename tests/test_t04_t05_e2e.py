# -*- coding: utf-8 -*-
"""T04/T05 端到端冒烟：素材库 → 刮削（打桩源，零网络）→ 精合并 → 导出。

对应生产 `main.py run` 的 P3→P4→P5→P6 主链路，验证本轮修复的闭环：

1. `CoarseMerger.group` 顶层汇合线路 + 提升搜索字段
   （scorer/tmdb 都读 item["search_title"|"title"]，缺了 S1 恒为 0 → 全 miss）；
2. 刮削命中后富化 payload（cover/overview/confidence/lines）写回素材库
   （此前 P5 从库里读到的仍是 CoarseEntity → 无封面无简介 → 全线 unmatched）；
3. 精合并 | unmatched==0、videos.json 非空、episodes.jsonl.gz 有分集、
   系列有 alt_urls、电影走 video 分支且顶层 url 落位；
4. `enqueue_many` 再入库不覆盖已刮削富化数据（防退变）。
"""

from __future__ import annotations

import gzip
import json
import os

from core.cache import CacheDB, MetaCache, RawLibrary
from pipeline.coarse_merge import CoarseMerger
from pipeline.export import Exporter
from pipeline.fine_merge import FineMerger
from pipeline.scrape import ScrapeOrchestrator
from sources import ST_HIT
from test_t04_t05 import FakeTmdb

from main import _load_scraped_hits  # 复用生产同一读取函数（精确同构）


class _IdFakeTmdb(FakeTmdb):
    """FakeTmdb 加 ID 映射：按标题分配不同 tmdb_id（真实源同片同 id、异片异 id）。

    基础 FakeTmdb 所有候选都硬编码 tmdb_id=999，会导致 P5 `_bucket_key` 按
    bangou 把异片误并成一条——用本桩还原真实语义。
    """

    def __init__(self):
        super().__init__()
        self._ids = {"测试剧集": "901", "测试动画": "902", "测试影片": "903"}

    def search(self, item):
        t = str(item.get("search_title") or item.get("title") or "")
        cands = [{
            "title": t, "original_title": "",
            "media_type": "movie" if item.get("category") == "movies" else "tv",
            "year": str(item.get("year") or ""), "popularity": 60,
            "tmdb_id": self._ids.get(t, "9"),
            "overview": "简介", "poster": "http://img/p.jpg",
        }]
        return {"_candidates": cands, "provider": self.name}, ST_HIT


def _mk_raw_items():
    """四类原料：剧集(两站同片)、动画、电影。update_time 近 7 天 → 热通道。"""
    import time
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    return [
        {"raw_id": "r1", "site": "源A", "priority": 1, "category": "tv",
         "norm_title": "测试剧集", "search_title": "测试剧集", "season": 1,
         "year": "2025",
         "lines": [{"name": "第1集", "url": "http://a1/e1.m3u8", "priority": 1},
                   {"name": "第2集", "url": "http://a1/e2.m3u8", "priority": 1}],
         "update_time": stamp},
        {"raw_id": "r2", "site": "源B", "priority": 2, "category": "tv",
         "norm_title": "测试剧集", "search_title": "测试剧集", "season": 1,
         "year": "2025",
         "lines": [{"name": "第1集", "url": "http://b1/e1.m3u8", "priority": 1}],
         "update_time": stamp},
        {"raw_id": "r3", "site": "源A", "priority": 1, "category": "anime",
         "norm_title": "测试动画", "search_title": "测试动画", "season": 1,
         "year": "2026",
         "lines": [{"name": "第1集", "url": "http://anime/e1.m3u8", "priority": 1}],
         "update_time": stamp},
        {"raw_id": "r4", "site": "源A", "priority": 1, "category": "movies",
         "norm_title": "测试影片", "search_title": "测试影片", "season": 1,
         "year": "2024",
         "lines": [{"name": "正片", "url": "http://movie/full.m3u8", "priority": 1},
                   {"name": "备用", "url": "http://movie2/full.m3u8", "priority": 2}],
         "update_time": stamp},
    ]


def _run_pipeline(tmp_path):
    """P3 → P4 → P5 → P6 全链路，返回 (product_dir, 剧集实体)。"""
    db = CacheDB(":memory:")
    library = RawLibrary(db)
    cache = MetaCache(db=db)

    # ---- P3 粗合并 → 素材库 ----
    merger = CoarseMerger(library=library)
    mstats = merger.merge(_mk_raw_items())
    assert mstats["inserted"] == 3  # 剧集同名两站合并为 1 实体

    # 实体顶层线路汇合 + 搜索字段提升
    ent = library.get("tv|测试剧集|1")
    assert ent["lines"], "CoarseEntity 顶层线路不能丢（P5 展开分集依赖）"
    assert ent["search_title"] == "测试剧集", "搜索字段应提升到顶层（scorer 依赖）"

    # ---- P4 刮削：fake 源打桩（热通道 AB 语义），富化结果写回素材库 ----
    tmdb = _IdFakeTmdb()
    orch = ScrapeOrchestrator(cache=cache, library=library,
                              sources={"tmdb": tmdb}, settings={})
    batch = library.pending_batch(10)
    assert len(batch) == 3
    out = orch.scrape_batch(batch, tier="AB")
    assert out["hit"] == 3 and out["miss"] == 0 and out["retryable"] == 0

    ent2 = library.get("tv|测试剧集|1")
    assert ent2["_library"]["scrape_status"] == "hit"
    assert int(ent2["confidence"] or 0) >= 55, "富化 payload 须带非 None confidence"
    assert ent2["cover"] and ent2["overview"], "富化 payload 须带封面/简介（P5 门槛）"
    assert ent2["matched"] is True
    assert ent2["lines"], "富化 payload 保留线路"

    # ---- P5 精合并（走生产同款读取） ----
    hits = _load_scraped_hits(library)
    assert len(hits) == 3
    fin = FineMerger(settings={})
    for h in hits:
        fin.add_item(h)
    fused = fin.finish()
    assert len(fused["items"]) == 3
    assert fused["unmatched"] == [], [u["reason"] for u in fused["unmatched"]]

    # ---- P6 导出 ----
    product = str(tmp_path / "product")
    res = Exporter(product).export(fused["items"], fused["unmatched"])
    assert res["videos"] == 3 and res["unmatched"] == 0

    videos = json.load(open(os.path.join(product, "videos.json"), encoding="utf-8"))
    assert len(videos) == 3
    assert "poster" not in json.dumps(videos)
    by_title = {v["title"]: v for v in videos}

    # 剧集：series + 元数据 + bangou
    tv = by_title.get("测试剧集"), by_title.get("测试动画")
    assert tv[0] and tv[0]["type"] == "series"
    assert tv[0]["bangou"] and tv[0]["cover"] and tv[0]["overview"]

    # 电影：video 分支 + 顶层 url 提升 + alt_urls
    mv = by_title["测试影片"]
    assert mv["type"] == "video"
    assert mv["url"] == "http://movie/full.m3u8", "电影播放地址须落位顶层 url"
    assert mv["alt_urls"], "电影多线路应提升进 alt_urls"

    # episodes.jsonl.gz：每 bangou 一行（剧集 2 集 + 动画 1 集；电影无分集）
    with gzip.open(os.path.join(product, "episodes.jsonl.gz"), "rt",
                   encoding="utf-8") as f:
        ep_rows = [json.loads(line) for line in f]
    assert len(ep_rows) == 2, f"预期 2 行分集，实际 {len(ep_rows)}"
    tv_row = [r for r in ep_rows if r["bangou"] == tv[0]["bangou"]][0]
    eps = {int(e["ep_number"]): e for e in tv_row["episodes"]}
    assert set(eps) == {1, 2}, f"剧集应有两集，实际 {sorted(eps)}"
    assert all(str(e["url"]).startswith("http") for e in eps.values())
    assert len(eps[1]["alt_urls"]) >= 1, "第 1 集跨站线路应进 alt_urls"

    return product, ent2


def test_e2e_full_pipeline(tmp_path):
    """端到端冒烟主用例：videos 非空 + 分集有货 + unmatched==0。"""
    product, tv_ent = _run_pipeline(tmp_path)
    # m3u8 播放清单（series 才有）
    m3u8_dir = os.path.join(product, "m3u8")
    playlist = [
        os.path.join(root, name)
        for root, _, names in os.walk(m3u8_dir) for name in names
        if name.endswith(".m3u8")
    ]
    assert len(playlist) == 2, f"剧集+动画应各成 1 份 m3u8，实际 {len(playlist)}"
    manifest = json.load(open(os.path.join(product, "manifest.json"), encoding="utf-8"))
    assert manifest["version"] == "v3"
    assert manifest["counts"]["videos"] == 3
    # 无 .tmp 残留（原子写）
    leaks = [f for f in os.listdir(product) if ".tmp" in f]
    assert not leaks


def test_reenqueue_keeps_enriched_payload():
    """再入库不覆盖已刮削富化数据（无热更新 → 保留 payload）。"""
    db = CacheDB(":memory:")
    library = RawLibrary(db)
    coarse = {"merge_key": "tv|测试剧|1", "category": "tv", "norm_title": "测试剧",
              "seq": 1, "year": "2025", "weight": 1.0, "last_updated": 0,
              "primary": {"site": "A", "lines": [{"url": "http://a/1.m3u8"}]}}
    library.enqueue_many([coarse])
    library.mark_scraped(
        ["tv|测试剧|1"], status="hit",
        payloads={"tv|测试剧|1": {"matched": True, "confidence": 88,
                                  "cover": "http://c.jpg", "overview": "x",
                                  "lines": [{"url": "http://a/1.m3u8"}]}})
    # 无新更新时间信号 → 保留富化 payload，只刷时间戳
    library.enqueue_many([dict(coarse)])
    ent = library.get("tv|测试剧|1")
    assert ent["confidence"] == 88
    assert ent["cover"] == "http://c.jpg"
    assert ent["matched"] is True
    assert ent["_library"]["scrape_status"] == "hit"

    # 热窗口新更新 → 重置刮削状态，下轮再刮（payload 换新粗实体）
    import time as _t
    fresh = dict(coarse)
    fresh["last_updated"] = int(_t.time())
    library.enqueue_many([fresh])
    ent2 = library.get("tv|测试剧|1")
    assert ent2["_library"]["scraped"] == 0
    assert ent2["_library"]["scrape_status"] is None or ent2["_library"]["scrape_status"] == ""


def test_video_line_promotion_without_gate_bypass():
    """video 分支：缺 cover 仍应被门槛拦下（不因 url 提升而漏网）。"""
    fin = FineMerger(settings={})
    fin.add_item({
        "merge_key": "movies|孤片|1", "bangou": "tmdb_77",
        "title": "孤片", "category": "movies", "season": 1,
        "search_title": "孤片", "confidence": 90, "matched": True,
        "cover": "", "overview": "简介", "provider": "TMDB",
        "lines": [{"name": "正片", "url": "http://x/f.m3u8", "priority": 1}],
    })
    out = fin.finish()
    assert out["items"] == []
    assert out["unmatched"][0]["reason"] == "no_cover"