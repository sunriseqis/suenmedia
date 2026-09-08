# -*- coding: utf-8 -*-
"""test_maccms.py —— crawlers.maccms 采集器测试（T03 验收①⑤⑥⑦ 的可测部分）。

覆盖：
- parse_play_urls：多线路 / 无 $ 分隔 / 非 http 过滤 / 空；
- build_item：字段映射 / 年份回落 / 季序号 / 集数提取 / 无线路返回 None；
- MaccmsSite 翻页状态机：
  * full 模式：空页停止（done）、条数 < 页大小停止、pagecount 边界停止；
  * incremental 模式：越窗停止（out_of_window）+ 页内逐条裁窗；
  * 硬超时到点停止派发新页、已完成页数据不丢（验收⑤）；
  * 每站并发严格受 semaphore(4) 约束（验收②）；
  * on_page 断点回调逐页上报（验收⑦）。
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta

import pytest

from core.config import Settings
from core.http import FetchResult, FetchStatus
from crawlers.maccms import (  # noqa: E402
    MaccmsSite,
    parse_play_urls,
    parse_vod_time,
    build_item,
    _set_monotonic,
)

SITE_A = {
    "name": "测试站A",
    "api_url": "https://a.example.com/api.php/provide/vod/",
    "fallbacks": [],
    "line_name": "A线路",
    "priority": 1,
}


# ---------------------------------------------------------------- 工具

def vod(vid, title="庆余年", type_name="国产剧", vod_time=None, year="2024"):
    return {
        "vod_id": vid,
        "vod_name": title,
        "type_name": type_name,
        "vod_time": vod_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "vod_year": year,
        "vod_pic": f"http://img.example.com/{vid}.jpg",
        "vod_content": f"<p>《{title}》简介</p>",
        "vod_play_from": "li1",
        "vod_play_url": f"第01集$https://cdn.example.com/{vid}/1.m3u8#第02集$https://cdn.example.com/{vid}/2.m3u8",
    }


def page_data(pg, vods, pagecount=None, limit=20):
    return {
        "code": 1, "msg": "ok", "page": pg,
        "pagecount": pagecount if pagecount is not None else max(pg, 1),
        "limit": limit, "total": len(vods),
        "list": vods,
    }


def _now_ok(vod_time):
    """最近 hours 内的时间串。"""
    return vod_time


class FakePageClient:
    """按 (api_url, page) 路由的假异步客户端；记录并发峰值。"""

    def __init__(self, routes: dict, delay: float = 0.005):
        # routes: {api_url: {page: data_dict | None}}；键统一去尾斜杠
        self.routes = {k.rstrip("/"): v for k, v in (routes or {}).items()}
        self.delay = delay
        self.calls = []          # (url, page)
        self._inflight = 0
        self.max_concurrent = 0

    async def get(self, url, params=None, timeout=None, retries=None, as_json=True, **kwargs):
        page = int((params or {}).get("pg", 0))
        self.calls.append((url, page))
        self._inflight += 1
        self.max_concurrent = max(self.max_concurrent, self._inflight)
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        self._inflight -= 1
        # MaccmsSite 会 rstrip('/') 后再请求 → 查找键同样去尾斜杠
        data = (self.routes.get(url.rstrip("/")) or {}).get(page)
        if data is None:
            return FetchResult(status=FetchStatus.ERROR, status_code=404,
                               text="", url=url)
        return FetchResult(status=FetchStatus.OK, status_code=200,
                           data=data, text="", url=url)

    def pages_fetched(self):
        return sorted({p for _u, p in self.calls})


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------- parse_play_urls

def test_parse_play_urls_multi_line():
    lines = parse_play_urls(
        "lzm3u8$$$ffm3u8",
        "第01集$https://a/1.m3u8#第02集$https://a/2.m3u8"
        "$$$第01集$https://b/1.m3u8#第02集$https://b/2.m3u8",
        default_line_name="默认线路")
    assert len(lines) == 2
    assert lines[0]["from"] == "lzm3u8" and lines[0]["line_name"] == "默认线路"
    assert lines[1]["from"] == "ffm3u8" and lines[1]["line_name"] == "默认线路-2"
    assert [e["name"] for e in lines[0]["episodes"]] == ["第01集", "第02集"]
    assert lines[0]["episodes"][0]["url"] == "https://a/1.m3u8"


def test_parse_play_urls_plain_url():
    """无 $ 分隔的分集按序号命名。"""
    lines = parse_play_urls("k1", "https://a/1.m3u8#https://a/2.m3u8")
    assert lines[0]["episodes"][0]["name"] == "第1集"
    assert lines[0]["episodes"][1]["name"] == "第2集"


def test_parse_play_urls_filters_non_http():
    lines = parse_play_urls("k1", "第1集$rtsp://a/1#第2集$https://b/2.m3u8")
    assert len(lines[0]["episodes"]) == 1
    assert lines[0]["episodes"][0]["url"] == "https://b/2.m3u8"


def test_parse_play_urls_empty():
    assert parse_play_urls("", "") == []
    assert parse_play_urls("k1", "$$$") == []


# ---------------------------------------------------------------- build_item

def test_build_item_fields():
    item = build_item(vod("100", title="庆余年 第二季"), SITE_A)
    assert item["raw_id"] == "100"
    assert item["site"] == "测试站A"
    assert item["title"] == "庆余年"
    assert item["season"] == 2
    assert item["category"] == "tv"
    assert item["year"] == "2024"
    assert item["update_time"]
    assert len(item["lines"]) == 1 and len(item["lines"][0]["episodes"]) == 2
    assert item["overview"] == "《庆余年 第二季》简介"


def test_build_item_movie_seq_and_year_from_title():
    item = build_item(vod("101", title="無間道Ⅱ", type_name="电影", year=""), SITE_A)
    assert item["season"] == 2
    assert item["category"] == "movies"
    assert item["title"] == "无间道"


def test_build_item_year_falls_back_to_title():
    item = build_item(vod("102", title="无间道（2002）", type_name="电影", year=""), SITE_A)
    assert item["year"] == "2002"


def test_build_item_episode_count():
    item = build_item(vod("103", title="狂飙 更新至39集"), SITE_A)
    assert item["episode"] == 39


def test_build_item_empty_title_or_no_lines():
    assert build_item(vod("104", title="", type_name="电影"), SITE_A) is None
    empty_play = vod("105")
    empty_play["vod_play_url"] = ""
    assert build_item(empty_play, SITE_A) is None


def test_parse_vod_time():
    assert parse_vod_time("2026-09-08 12:33:45") == datetime(2026, 9, 8, 12, 33, 45)
    assert parse_vod_time("2026-09-08") == datetime(2026, 9, 8)
    assert parse_vod_time("") is None
    assert parse_vod_time("垃圾") is None


# ---------------------------------------------------------------- full 模式停止

def test_full_mode_stops_at_empty_page():
    vods_2 = [vod("1"), vod("2")]
    routes = {
        SITE_A["api_url"]: {
            1: page_data(1, vods_2, pagecount=10, limit=2),
            2: page_data(2, [], pagecount=10, limit=2),
        }
    }
    client = FakePageClient(routes, delay=0)
    site = MaccmsSite(SITE_A, client, mode="full", settings=Settings())
    result = run(site.run())
    assert result.done is True and result.reason == "exhausted"
    assert result.pages == 2
    assert len(result.items) == 2


def test_full_mode_stops_when_items_less_than_page_size():
    """条目数 < 页大小（limit=2）→ 站点尽头。"""
    routes = {SITE_A["api_url"]: {1: page_data(1, [vod("1")], pagecount=999, limit=2)}}
    client = FakePageClient(routes, delay=0)
    site = MaccmsSite(SITE_A, client, mode="full", settings=Settings())
    result = run(site.run())
    assert result.done is True
    assert result.pages == 1 and len(result.items) == 1


def test_full_mode_stops_at_pagecount_boundary():
    """page 超过 pagecount → 尽头（批内多发的页会被 pagecount 边界吸收）。"""
    vods_2 = [vod("1"), vod("2")]
    routes = {
        SITE_A["api_url"]: {
            1: page_data(1, vods_2, pagecount=2, limit=2),
            2: page_data(2, vods_2, pagecount=2, limit=2),
            3: page_data(3, vods_2, pagecount=2, limit=2),
        }
    }
    client = FakePageClient(routes, delay=0)
    site = MaccmsSite(SITE_A, client, mode="full", settings=Settings())
    result = run(site.run())
    assert result.done is True
    assert result.pages == 2  # 只处理到第 2 页（第 3 页虽已入批但被边界判定吸收）
    assert len(result.items) == 4


# ---------------------------------------------------------------- incremental 越窗

def test_incremental_stops_out_of_window():
    old = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
    routes = {SITE_A["api_url"]: {1: page_data(1, [vod("1", vod_time=old)])}}
    client = FakePageClient(routes, delay=0)
    site = MaccmsSite(SITE_A, client, mode="incremental", hours=24,
                      settings=Settings.from_dict({"crawl_hours": 24}))
    result = run(site.run())
    assert result.done is True and result.reason == "out_of_window"
    assert result.items == []


def test_incremental_filters_mixed_page_by_window():
    fresh = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    old = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
    routes = {SITE_A["api_url"]: {1: page_data(
        1, [vod("1", vod_time=fresh), vod("2", vod_time=old)])}}
    client = FakePageClient(routes, delay=0)
    site = MaccmsSite(SITE_A, client, mode="incremental", hours=24,
                      settings=Settings.from_dict({"crawl_hours": 24}))
    result = run(site.run())
    assert [i["raw_id"] for i in result.items] == ["1"]


# ---------------------------------------------------------------- 并发 / 超时 / 断点

def test_concurrency_strictly_bounded_by_semaphore():
    """验收②：并发页数峰值 == 4（CONCURRENT_PAGES）。"""
    vods_2 = [vod("1"), vod("2")]
    routes = {SITE_A["api_url"]: {
        pg: page_data(pg, vods_2, pagecount=5, limit=2) for pg in range(1, 9)}}
    client = FakePageClient(routes, delay=0.01)
    site = MaccmsSite(SITE_A, client, mode="full", settings=Settings())
    result = run(site.run())
    assert result.pages >= 4
    assert client.max_concurrent <= 4
    assert client.max_concurrent == 4  # 批内恰满 4 页并发


def test_deadline_stops_dispatch_but_keeps_fetched():
    """验收⑤：硬超时到点停止派发新页；已采页数据不丢。"""
    clock = {"t": 1000.0}
    _set_monotonic(lambda: clock["t"])
    try:
        vods_2 = [vod("1"), vod("2")]
        routes = {SITE_A["api_url"]: {
            pg: page_data(pg, vods_2, pagecount=5, limit=2) for pg in range(1, 9)}}
        client = FakePageClient(routes, delay=0)

        pages_done = []

        def _on_page(site_name, page):
            pages_done.append(page)
            clock["t"] += 5.0  # 每完成一页把"单调时钟"拨快 5s

        site = MaccmsSite(SITE_A, client, mode="full",
                          deadline=1002.0, on_page=_on_page,
                          settings=Settings())
        result = run(site.run())
        assert result.reason == "deadline"
        assert result.done is False
        assert result.pages >= 4  # 第一批 4 页已采完
        assert len(result.items) == result.pages * 2  # 已采数据不丢
    finally:
        _set_monotonic(time.monotonic)


def test_on_page_callback_receives_each_page():
    vods_2 = [vod("1"), vod("2")]
    routes = {SITE_A["api_url"]: {1: page_data(1, vods_2, pagecount=1, limit=2)}}
    client = FakePageClient(routes, delay=0)
    pages_done = []
    site = MaccmsSite(SITE_A, client, mode="full",
                      on_page=lambda _s, p: pages_done.append(p),
                      settings=Settings())
    result = run(site.run())
    assert pages_done == [1]
    assert result.done is True


def test_start_page_resume():
    """验收⑦：从 start_page 断点续跑。"""
    vods_2 = [vod("1"), vod("2")]
    routes = {SITE_A["api_url"]: {
        1: page_data(1, vods_2, pagecount=3, limit=2),
        2: page_data(2, vods_2, pagecount=3, limit=2),
        3: page_data(3, vods_2, pagecount=3, limit=2),
    }}
    client = FakePageClient(routes, delay=0)
    site = MaccmsSite(SITE_A, client, mode="full", start_page=2,
                      settings=Settings())
    result = run(site.run())
    assert result.pages == 2  # 页 2、页 3
    fetched = client.pages_fetched()
    assert fetched == [2, 3] and 1 not in fetched