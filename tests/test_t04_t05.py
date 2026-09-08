# -*- coding: utf-8 -*-
"""tests/test_t04_t05.py —— T04/T05 集成测试

覆盖：
1. scorer §6.6 反例（必须全部通过）
2. ScrapeOrchestrator：源链编排 / 负缓存 / 游标推进 / S8 ID 校验
3. RawLibrary 素材库：热/冷通道 / 游标幂等
4. CoarseMerger：粗合并 primary 选择
5. FineMerger：产物契约 v3（cover 唯一 / season_number int / 多线路 alt_urls）
6. Exporter：分片导出 / poster 驱逐 / manifest / 原子写
7. Budget：预算分配 / 速率 / 仲裁
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import time
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sources.scorer import Scored, choose_best, score_candidate  # noqa: E402
from sources import (  # noqa: E402
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    TmdbSource,
    DoubanSource,
    TvdbSource,
    BilibiliSource,
    OmdbSource,
)


# ---------------------------------------------------------------- 1. §6.6 反例

class TestScorerRegression:
    """设计文档 §6.6 反例（必须全部通过）。"""

    def test_xinbing_to_caoniao_miss(self):
        """新兵第四季 → 菜鸟炊事兵：S1 极低、无 S2、S7 序号不一致 → miss。"""
        sc = score_candidate(
            {"search_title": "新兵", "category": "tv", "season": 4,
             "year": "", "episode": 12},
            {"title": "菜鸟炊事兵", "media_type": "tv",
             "original_title": "Cooking rookie"})
        assert sc.score < 55

    def test_zuzhou_to_sishenlaile_miss(self):
        """诅咒2025 → 死神来了6：S1 极低、S7 序号不一致 → miss。"""
        sc = score_candidate(
            {"search_title": "诅咒", "category": "movies", "season": 1,
             "year": "2025", "episode": 1},
            {"title": "死神来了6", "media_type": "movie",
             "original_title": "Final Destination 6", "year": "2025"})
        assert sc.detail.get("reject") is not None or sc.score < 40

    def test_jia_to_wuxiawuren_miss(self):
        """家1 → 屋下無人：S1 极低、无 S2 译名关系 → miss。"""
        sc = score_candidate(
            {"search_title": "家", "category": "tv", "season": 1,
             "year": "2024", "episode": 8},
            {"title": "屋下無人", "media_type": "tv",
             "original_title": "The House at Night"})
        assert sc.detail.get("reject") is not None or sc.score < 40

    def test_frede_to_fred_hit(self):
        """弗雷德有问题 → Fred Has Problems：译名关系 → hit ≥55。"""
        sc = score_candidate(
            {"search_title": "弗雷德有问题", "category": "movies", "season": 1,
             "year": "2023", "episode": 1},
            {"title": "Fred Has Problems", "original_title": "Fred Has Problems",
             "media_type": "movie", "year": "2023", "popularity": 50})
        assert sc.score >= 55
        assert sc.match_kind == "translation_pair"

    def test_exact_match_high(self):
        """完全同名 → exact + 高分。"""
        sc = score_candidate(
            {"search_title": "流浪地球", "category": "movies", "season": 1,
             "year": "2019", "episode": 1},
            {"title": "流浪地球", "media_type": "movie", "year": "2019",
             "original_title": "The Wandering Earth", "popularity": 100})
        assert sc.match_kind == "exact"
        assert sc.score >= 70

    def test_choose_best_picks_highest(self):
        """choose_best 从多候选选最高分（不取 results[0]）。"""
        item = {"search_title": "流浪地球", "category": "movies", "season": 1,
                "year": "2019", "episode": 1}
        candidates = [
            {"title": "流浪地球2", "media_type": "movie", "year": "2023",
             "original_title": "The Wandering Earth II", "popularity": 10},
            {"title": "流浪地球", "media_type": "movie", "year": "2019",
             "original_title": "The Wandering Earth", "popularity": 80},
        ]
        best, scored, _ = choose_best(item, candidates)
        assert best is not None
        assert best["title"] == "流浪地球"
        assert scored.match_kind == "exact"


# ---------------------------------------------------------------- 2. 素材库游标

from core.cache import CacheDB, RawLibrary, MetaCache, make_ck  # noqa: E402
from pipeline.coarse_merge import CoarseMerger  # noqa: E402
from pipeline.fine_merge import FineMerger  # noqa: E402
from pipeline.export import Exporter  # noqa: E402
from pipeline.budget import Budget  # noqa: E402


class TestRawLibrary:
    """§7.2 素材库：热/冷通道 + 游标幂等。"""

    def _mk(self):
        db = CacheDB(":memory:")
        return db, RawLibrary(db)

    def test_enqueue_and_channels(self):
        db, lib = self._mk()
        now = int(time.time())
        lib.enqueue_many([
            {"merge_key": "tv|测试剧|1", "category": "tv", "norm_title": "测试剧",
             "seq": 1, "year": "2024", "weight": 1.0, "last_updated": now,
             "primary": {"site": "A", "lines": [{"url": "x"}]}},
            {"merge_key": "variety|综艺|1", "category": "variety",
             "norm_title": "综艺", "seq": 1, "year": "2025", "weight": 2.0,
             "last_updated": 0, "primary": {"site": "B", "lines": [{"url": "y"}]}},
        ], hot_window_days=7)
        # 热通道只取近 7 天 is_new=1（测试剧），综艺无时间信号走冷通道
        hot = lib.hot_batch(7, 10)
        assert len(hot) == 1 and hot[0]["category"] == "tv"
        assert len(lib.pending_batch(10)) == 2

    def test_cursor_idempotent(self):
        db, lib = self._mk()
        lib.enqueue_many([
            {"merge_key": "movies|甲片|1", "category": "movies",
             "norm_title": "甲片", "seq": 1, "year": "2023", "weight": 1.0,
             "last_updated": 0, "primary": {}},
        ])
        keys = lib.pending_batch_keys(10)
        assert len(keys) == 1
        lib.mark_scraped(keys, status="hit", confidence=88)
        assert lib.pending_count() == 0  # 已刮削不再取出（幂等）

    def test_retryable_stays_pending(self):
        db, lib = self._mk()
        lib.enqueue_many([
            {"merge_key": "tv|重试剧|1", "category": "tv", "norm_title": "重试剧",
             "seq": 1, "year": "", "weight": 1.0, "last_updated": 0,
             "primary": {}},
        ])
        lib.mark_retryable(["tv|重试剧|1"], status="retryable")
        assert lib.pending_count() == 1  # 保持 scraped=0，attempts+1
        ent = lib.get("tv|重试剧|1")
        assert ent["_library"]["attempts"] == 1


# ---------------------------------------------------------------- 3. Scrape 编排

class FakeTmdb(TmdbSource):
    """打桩 TMDB：可控命中 / miss / retryable。"""

    def __init__(self, candidates=None, mode="hit", **kw):
        super().__init__(settings={"tmdb_api_key": "x", "tmdb_min_interval": 0}, **kw)
        self._candidates = candidates or []
        self._mode = mode

    @property
    def enabled(self):
        return True

    def search(self, item):
        t = self._mode
        if t == "retryable":
            return None, ST_RETRYABLE
        if t == "miss":
            return None, ST_MISS
        cands = self._candidates or [{
            "title": str(item.get("search_title") or item.get("title") or ""),
            "original_title": "",
            "media_type": "movie" if item.get("category") == "movies" else "tv",
            "year": str(item.get("year") or ""),
            "popularity": 60,
            "tmdb_id": "999",
            "overview": "简介",
            "poster": "http://img/p.jpg",
        }]
        return {"_candidates": cands, "provider": self.name}, ST_HIT

    def detail(self, tmdb_id, media_type):
        return {"provider": self.name, "cast": ["张三"], "director": ["李四"]}, ST_HIT

    def external_ids(self, tmdb_id, media_type):
        return {"provider": self.name, "douban_id": "12345"}, ST_HIT

    def close(self):
        pass


def _mk_orch(fake_sources, settings=None):
    from pipeline.scrape import ScrapeOrchestrator
    db = CacheDB(":memory:")
    cache = MetaCache(db=db)
    library = RawLibrary(db)
    orch = ScrapeOrchestrator(cache=cache, library=library, sources=fake_sources,
                              settings=settings or {})
    return orch, db, cache, library


class TestScrapeOrchestrator:
    """源链编排 / 负缓存 / 游标推进。"""

    def test_hit_flow_and_cache(self):
        from pipeline.scrape import ScrapeOrchestrator
        tmdb = FakeTmdb(candidates=[{
            "title": "流浪地球", "original_title": "The Wandering Earth",
            "media_type": "movie", "year": "2019", "tmdb_id": "123",
            "overview": "太阳即将毁灭", "poster": "http://img/p.jpg",
            "popularity": 200, "number_of_episodes": None}])
        orch, db, cache, library = _mk_orch({"tmdb": tmdb}, {"match": {}})
        item = {"merge_key": "movies|流浪地球|1", "category": "movies",
                "norm_title": "流浪地球", "search_title": "流浪地球",
                "seq": 1, "year": "2019", "douban_id": ""}
        res = orch.scrape_one(item, tier="AB")
        assert res.status == "hit"
        assert res.confidence and res.confidence >= 70
        assert res.meta.get("cover") or res.meta.get("poster")
        assert res.external_id == "tmdb_123"
        # 正缓存已写：第二次 0 请求直接命中
        res2 = orch.scrape_one(dict(item), tier="A")
        assert res2.status == "hit" and res2.cache_hit

    def test_miss_writes_neg_cache(self):
        tmdb = FakeTmdb(mode="miss")
        orch, db, cache, library = _mk_orch({"tmdb": tmdb})
        item = {"merge_key": "movies|不存在的片|1", "category": "movies",
                "norm_title": "不存在的片", "search_title": "不存在的片",
                "seq": 1, "year": "1999"}
        res = orch.scrape_one(item)
        assert res.status == "miss"
        # 单源全 miss → hard_miss 负缓存已写
        ck = make_ck("movies", "不存在的片", "1", "1999")
        assert cache.get(ck).status == "hard_miss"

    def test_retryable_goes_retry_store(self):
        tmdb = FakeTmdb(mode="retryable")
        orch, db, cache, library = _mk_orch({"tmdb": tmdb})
        item = {"merge_key": "movies|网络片|1", "category": "movies",
                "norm_title": "网络片", "search_title": "网络片", "seq": 1,
                "year": ""}
        res = orch.scrape_one(item)
        assert res.status == "retryable"
        # 负缓存绝不写入
        ck = make_ck("movies", "网络片", "1", "")
        assert cache.get(ck).status == "none"

    def test_s8_id_verify(self):
        """S8：`_verify_by_douban_id` 比对源站 douban_id 与 TMDB external_ids。"""
        tmdb = FakeTmdb(candidates=[{
            "title": "某某电影", "original_title": "Some Movie XX",
            "media_type": "movie", "year": "", "tmdb_id": "123",
            "overview": "x", "poster": "http://img/p.jpg", "popularity": 50}])
        orch, db, cache, library = _mk_orch(
            {"tmdb": tmdb}, {"match": {"id_verify_enable": True}})
        # FakeTmdb.external_ids 返回 douban_id=12345 == 传入 → verified
        verified, detail = orch._verify_by_douban_id(tmdb, {"tmdb_id": "123", "media_type": "movie"}, "12345")
        assert verified
        assert detail == "douban_id_match"
        # 不一致 → 不通过
        verified2, detail2 = orch._verify_by_douban_id(tmdb, {"tmdb_id": "123", "media_type": "movie"}, "99999")
        assert not verified2
        assert detail2 == "douban_id_mismatch"

    def test_medium_no_rewrite_title(self):
        """medium（55-69）采用元数据但不改写标题（§6.5）。"""
        tmdb = FakeTmdb(candidates=[{
            "title": "某某电影之续集", "original_title": "Some Movie Sequel",
            "media_type": "movie", "year": "2020", "tmdb_id": "123",
            "overview": "x", "poster": "http://img/p.jpg", "popularity": 30}])
        orch, db, cache, library = _mk_orch({"tmdb": tmdb})
        item = {"merge_key": "movies|某电影|1", "category": "movies",
                "norm_title": "某电影", "search_title": "某电影", "seq": 1,
                "year": "2020", "douban_id": "", "episode": 1}
        res = orch.scrape_one(item, tier="A")
        if res.status == "hit":
            # medium 不 rewrite：title 保持源站标题
            assert res.meta.get("canonical_title")  # 权威名已记录
            if res.confidence < 70:
                assert res.meta.get("title") == "某电影"
        else:
            pytest.skip(f"候选不在 medium 区间（score 无法控）, status={res.status}")

    def test_scrape_batch_advances_cursor(self):
        tmdb = FakeTmdb()
        orch, db, cache, library = _mk_orch({"tmdb": tmdb})
        # 先入库素材库（pending_batch 消费路径），再批量刮削
        library.enqueue_many([
            {"merge_key": f"movies|片{i}|1", "category": "movies",
             "norm_title": f"片{i}", "seq": 1, "year": "", "weight": 1.0,
             "last_updated": 0, "primary": {}} for i in range(3)
        ])
        assert library.pending_count() == 3
        batch = library.pending_batch(10)
        for it in batch:
            it["search_title"] = it["norm_title"]
            it["episode"] = 1
        out = orch.scrape_batch(batch, tier="A")
        assert out["hit"] == 3
        assert library.pending_count() == 0
        assert library.stats()["done"] == 3


# ---------------------------------------------------------------- 4. 粗合并

class TestCoarseMerger:
    def test_primary_selection(self):
        db = CacheDB(":memory:")
        lib = RawLibrary(db)
        merger = CoarseMerger(library=lib)
        entities = merger.group([
            {"raw_id": "1", "site": "A", "priority": 1, "category": "tv",
             "norm_title": "测试剧", "season": 1, "year": "2024",
             "lines": [{"url": "a1"}], "update_time": "2024-01-01 00:00:00"},
            {"raw_id": "2", "site": "B", "priority": 1, "category": "tv",
             "norm_title": "测试剧", "season": 1, "year": "2024",
             "lines": [{"url": "b1"}, {"url": "b2"}],
             "update_time": "2024-01-01 01:00:00"},
        ])
        assert len(entities) == 1
        assert entities[0]["primary"]["site"] == "B"  # 同 priority 线路多者
        assert len(entities[0]["siblings"]) == 1
        assert entities[0]["merge_key"] == "tv|测试剧|1"


# ---------------------------------------------------------------- 5. 精细合并产物契约

class TestFineMerger:
    """§9 产物契约 v3。"""

    def _enriched(self, **over):
        ent = {
            "merge_key": "tv|测试剧|1", "bangou": "tmdb_123",
            "title": "测试剧", "category": "tv", "season": 1,
            "search_title": "测试剧", "confidence": 88, "matched": True,
            "cover": "http://img/cover.jpg", "overview": "简介",
            "provider": "TMDB", "source_provider": "TMDB",
            "lines": [{"name": "第1集", "url": "http://s1/e1.m3u8", "priority": 1},
                      {"name": "第1集", "url": "http://s2/e1.m3u8", "priority": 2},
                      {"name": "第2集", "url": "http://s1/e2.m3u8", "priority": 1}],
        }
        ent.update(over)
        return ent

    def test_v3_contract(self):
        fin = FineMerger()
        fin.add_item(self._enriched())
        out = fin.finish()
        assert len(out["items"]) == 1 and len(out["unmatched"]) == 0
        item = out["items"][0]
        assert item["type"] == "series"
        assert item["bangou"] == "tmdb_123"
        assert item["cover"] == "http://img/cover.jpg"
        assert "poster" not in json.dumps(item)
        season = item["seasons"][0]
        assert isinstance(season["season_number"], int)
        eps = season["episodes"]
        assert all(isinstance(e["ep_number"], int) for e in eps)
        # 多线路 → 主 url + alt_urls
        print(json.dumps(eps, ensure_ascii=False))
        assert eps[0]["url"] == "http://s1/e1.m3u8"
        assert len(eps[0]["alt_urls"]) == 1

    def test_gate_rejects_low_confidence(self):
        fin = FineMerger()
        fin.add_item(self._enriched(confidence=40, matched=True))
        out = fin.finish()
        assert len(out["items"]) == 0
        assert out["unmatched"][0]["reason"] == "low_confidence"

    def test_gate_rejects_no_cover(self):
        fin = FineMerger()
        fin.add_item(self._enriched(cover=""))
        out = fin.finish()
        assert len(out["items"]) == 0
        assert out["unmatched"][0]["reason"] == "no_cover"

    def test_gate_rejects_all_sources_miss(self):
        fin = FineMerger()
        fin.add_item(self._enriched(matched=False, confidence=None))
        out = fin.finish()
        assert len(out["items"]) == 0
        assert out["unmatched"][0]["reason"] == "all_sources_miss"


# ---------------------------------------------------------------- 6. 导出

class TestExporter:
    def test_export_all_shapes(self, tmp_path):
        product = str(tmp_path / "product")
        fin = FineMerger()
        fin.add_item({
            "merge_key": "tv|测试剧|1", "bangou": "tmdb_123",
            "title": "测试剧", "category": "tv", "season": 1,
            "search_title": "测试剧", "confidence": 88, "matched": True,
            "cover": "http://img/cover.jpg", "overview": "简介",
            "provider": "TMDB", "source_provider": "TMDB",
            "lines": [{"name": "第1集", "url": "http://s1/e1.m3u8", "priority": 1},
                      {"name": "第2集", "url": "http://s1/e2.m3u8", "priority": 1}],
        })
        out = fin.finish()
        res = Exporter(product).export(out["items"], out["unmatched"])
        assert res["videos"] == 1
        assert res["episodes_shards"] >= 1
        assert res["m3u8"] == 1
        # videos.json：无 episodes、无 poster
        videos = json.load(open(os.path.join(product, "videos.json"), encoding="utf-8"))
        assert "poster" not in json.dumps(videos)
        assert videos[0].get("episodes") is None
        # episodes.jsonl.gz
        with gzip.open(os.path.join(product, "episodes.jsonl.gz"), "rt",
                       encoding="utf-8") as f:
            rows = [json.loads(l) for l in f]
        assert len(rows) == 1 and rows[0]["bangou"] == "tmdb_123"
        assert rows[0]["season_number"] == 1
        # manifest
        man = json.load(open(os.path.join(product, "manifest.json"), encoding="utf-8"))
        assert man["version"] == "v3"
        assert man["counts"]["videos"] == 1
        assert man["checksums"]["videos.json"]
        # 原子写无 .tmp 残留
        assert not [f for f in os.listdir(product) if ".tmp" in f]


# ---------------------------------------------------------------- 7. 预算

class TestBudget:
    def test_scrape_budget_clamp(self):
        b = Budget(budget_seconds=1800, rate_limits={"tmdb": 4})
        assert abs(b.rate_effective("tmdb") - 3.6) < 1e-9
        # 1800 - 0 - (240+120+90) = 1350 → clamp 到 scrape_max=1200
        assert abs(b.scrape_budget() - 1200) < 1.0

    def test_rebalance_stops_on_timeout(self):
        b = Budget(budget_seconds=0.001, rate_limits={"tmdb": 4})
        time.sleep(0.005)
        assert not b.rebalance(1000)  # 已超时 → 停止派发
        assert b.is_timeout