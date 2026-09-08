# -*- coding: utf-8 -*-
"""test_harvest.py —— crawlers.harvest 编排测试（T03 验收⑥⑦ 的可测部分）。

覆盖：
- full 模式：跨站并发、各站翻到尽头置 done=true（progress.json）、
  断点续跑从 start_page 继续、已完成站跳过；
- RawSeen 去重：同 (site, raw_id) 无变化条目跳过 / 内容变化重入管线；
- incremental 模式：按 24h 窗口停止；
- ProgressStore 文件读写（断点游标语义：最后完成页 + 1）。
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta

from core.cache import CacheDB
from core.config import Settings
from core.http import FetchResult, FetchStatus
from crawlers.harvest import ProgressStore, harvest, item_content_hash


def _vod(vid, title="庆余年", type_name="国产剧", vod_time=None, url=None):
    return {
        "vod_id": vid,
        "vod_name": title,
        "type_name": type_name,
        "vod_time": vod_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "vod_year": "2024",
        "vod_pic": f"http://img.example.com/{vid}.jpg",
        "vod_content": "简介",
        "vod_play_from": "k1",
        "vod_play_url": f"第01集${url or f'https://cdn.example.com/{vid}/1.m3u8'}",
    }


def _page(pg, vods, pagecount=None, limit=20):
    return {"code": 1, "page": pg,
            "pagecount": pagecount if pagecount is not None else max(pg, 1),
            "limit": limit, "list": vods}


SITE_PAGES_A = {
    # A 站共 3 页（limit=2，第 3 页不满页 → 尽头）
    1: _page(1, [_vod("a1"), _vod("a2")], pagecount=3, limit=2),
    2: _page(2, [_vod("a3"), _vod("a4")], pagecount=3, limit=2),
    3: _page(3, [_vod("a5")], pagecount=3, limit=2),   # 不满页 → 尽头
    4: _page(4, []),
}
SITE_PAGES_B = {
    1: _page(1, [_vod("b1")], pagecount=1, limit=2),
    2: _page(2, []),                       # 空 → 尽头（实际上不会派发第 2 页）
}


class RouteClient:
    """按 api_url 路由的假异步客户端。"""

    def __init__(self, routes: dict):
        # routes: {api_url: {page: data|None}}；键统一去尾斜杠
        self.routes = {k.rstrip("/"): v for k, v in (routes or {}).items()}
        self.calls = []

    async def get(self, url, params=None, timeout=None, retries=None, as_json=True, **kw):
        page = int((params or {}).get("pg", 0))
        self.calls.append((url, page))
        # MaccmsSite 会 rstrip('/') 后再请求 → 查找键同样去尾斜杠
        data = (self.routes.get(url.rstrip("/")) or {}).get(page)
        if data is None:
            return FetchResult(status=FetchStatus.ERROR, status_code=404,
                               text="", url=url)
        return FetchResult(status=FetchStatus.OK, status_code=200,
                           data=data, text="", url=url)


def _settings():
    return Settings.from_dict({"crawl_hours": 24})


def _sites():
    return [
        {"name": "A站", "api_url": "https://a/api", "enabled": True,
         "type": "maccms_v10", "priority": 1, "line_name": "A线路"},
        {"name": "B站", "api_url": "https://b/api", "enabled": True,
         "type": "maccms_v10", "priority": 2, "line_name": "B线路"},
    ]


def run(coro):
    return asyncio.run(coro)


def _fresh(hours_ago=1):
    return (datetime.now() - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _old(hours_ago=30):
    return _fresh(hours_ago)


# ---------------------------------------------------------------- full 模式

def test_full_mode_collects_and_marks_done(tmp_path):
    client = RouteClient({"https://a/api": SITE_PAGES_A, "https://b/api": SITE_PAGES_B})
    db = CacheDB(":memory:")
    progress_path = str(tmp_path / "progress.json")

    result = run(harvest(_sites(), mode="full", client=client, db=db,
                         progress_path=progress_path, settings=_settings()))

    assert {r["site"] for r in result.reports} == {"A站", "B站"}
    by_site = {r["site"]: r for r in result.reports}
    assert by_site["A站"]["done"] is True
    assert by_site["B站"]["done"] is True
    # A 3 页 5 条、B 1 页 1 条，全部全新入管线
    assert len(result.items) == 6

    # progress.json 已落盘且各站 done=true（验收⑥）
    saved = json.loads(open(progress_path, encoding="utf-8").read())
    assert saved["A站"]["done"] is True and saved["A站"]["page"] == 3
    assert saved["B站"]["done"] is True and saved["B站"]["page"] == 1

    # 各站独立游标
    assert result.progress["A站"]["page"] == 3


def test_full_mode_skips_already_done_site(tmp_path):
    """第二次 full：已完成站跳过，不做重复抓取。"""
    client = RouteClient({"https://a/api": SITE_PAGES_A, "https://b/api": SITE_PAGES_B})
    db = CacheDB(":memory:")
    progress_path = str(tmp_path / "progress.json")

    first = run(harvest(_sites(), mode="full", client=client, db=db,
                        progress_path=progress_path, settings=_settings()))
    assert len(first.items) == 6
    calls_after_first = len(client.calls)

    second = run(harvest(_sites(), mode="full", client=client, db=db,
                         progress_path=progress_path, settings=_settings()))
    assert len(second.items) == 0          # 全部跳过
    assert len(client.calls) == calls_after_first  # 0 新增网络请求
    assert all(r["reason"] == "already_done" for r in second.reports)


def test_full_mode_resumes_from_progress(tmp_path):
    """断点续跑：progress 里 page=1 → 从第 2 页继续（验收⑦）。"""
    client = RouteClient({"https://a/api": SITE_PAGES_A, "https://b/api": SITE_PAGES_B})
    db = CacheDB(":memory:")
    progress_path = str(tmp_path / "progress.json")
    with open(progress_path, "w", encoding="utf-8") as f:
        json.dump({"A站": {"page": 1, "done": False}}, f)

    result = run(harvest(_sites(), mode="full", client=client, db=db,
                         progress_path=progress_path, settings=_settings()))

    a_item_ids = sorted(i["raw_id"] for i in result.items if i["site"] == "A站")
    assert a_item_ids == ["a3", "a4", "a5"]  # 无重复无丢失，从断点继续
    assert not any(p == 1 for _u, p in client.calls if _u == "https://a/api")


# ---------------------------------------------------------------- 去重 / 内容变化

def test_dedup_skips_unchanged_and_keeps_changed(tmp_path):
    """同 raw_id 无变化跳过；内容变化（如老剧加集）重入管线。"""
    pages1 = {1: _page(1, [_vod("x1", url="https://cdn/x/1.m3u8")], pagecount=1)}
    client = RouteClient({"https://a/api": pages1})
    db = CacheDB(":memory:")
    progress_path = str(tmp_path / "progress.json")

    first = run(harvest(_sites()[:1], mode="full", client=client, db=db,
                        progress_path=progress_path, settings=_settings()))
    assert len(first.items) == 1 and first.reports[0]["new"] == 1

    # 第二轮：内容无变化（同一 URL）→ 跳过
    second = run(harvest(_sites()[:1], mode="full", client=client, db=db,
                         progress_path=progress_path, settings=_settings()))
    # 站点已 done → 跳过站点，items 0；用增量模式验证去重
    assert len(second.items) == 0

    # 用增量模式验证：无变化 → skipped=1
    inc1 = run(harvest(_sites()[:1], mode="incremental", client=client, db=db,
                       progress_path=progress_path, settings=_settings()))
    assert len(inc1.items) == 0 and inc1.reports[0]["skipped"] == 1

    # 内容变化：线路 URL 变更 → 重新入管线
    pages2 = {1: _page(1, [_vod("x1", url="https://cdn/x/NEW.m3u8")], pagecount=1)}
    client2 = RouteClient({"https://a/api": pages2})
    inc2 = run(harvest(_sites()[:1], mode="incremental", client=client2, db=db,
                       progress_path=progress_path, settings=_settings()))
    assert len(inc2.items) == 1 and inc2.reports[0]["updated"] == 1
    assert inc2.items[0]["lines"][0]["episodes"][0]["url"].endswith("NEW.m3u8")


# ---------------------------------------------------------------- incremental

def test_incremental_stops_by_window(tmp_path):
    """增量：全部越窗记录 → 0 条入库，站点按窗口停止。"""
    pages = {1: _page(1, [_vod("w1", vod_time=_old())], pagecount=1)}
    client = RouteClient({"https://b/api": pages})
    db = CacheDB(":memory:")
    result = run(harvest(_sites()[1:], mode="incremental", client=client, db=db,
                         progress_path=str(tmp_path / "p.json"), settings=_settings()))
    assert result.reports[0]["reason"] == "out_of_window"
    assert result.reports[0]["done"] is True
    assert len(result.items) == 0


def test_incremental_keeps_in_window(tmp_path):
    pages = {1: _page(1, [
        _vod("w1", vod_time=_fresh(1)),
        _vod("w2", vod_time=_old(30)),
    ], pagecount=1)}
    client = RouteClient({"https://b/api": pages})
    db = CacheDB(":memory:")
    result = run(harvest(_sites()[1:], mode="incremental", client=client, db=db,
                         progress_path=str(tmp_path / "p.json"), settings=_settings()))
    ids = sorted(i["raw_id"] for i in result.items)
    assert ids == ["w1"]


# ---------------------------------------------------------------- ProgressStore

def test_progress_store_semantics(tmp_path):
    store = ProgressStore(str(tmp_path / "progress.json"), settings=_settings())
    assert store.load() == {}
    assert store.start_page("A站") == 1
    store.mark_page("A站", 5)
    store.mark_page("A站", 3)            # 不回调：幂等取最大
    assert store.start_page("A站") == 6
    store.mark_done("A站")
    assert store.is_done("A站") is True
    # 重开实例：从磁盘恢复
    store2 = ProgressStore(str(tmp_path / "progress.json"), settings=_settings())
    assert store2.is_done("A站") is True
    assert store2.start_page("其他站") == 1


def test_item_content_hash_stable_and_sensitive():
    base = {
        "raw_id": "1", "title": "庆余年", "season": 2, "episode": 39,
        "year": "2024", "lines": [{"a": 1}], "poster": "p",
        "overview": "o", "douban_id": "", "douban_score": "",
    }
    h1 = item_content_hash(base)
    h2 = item_content_hash(dict(base, update_time="2026-09-08 12:00:00"))
    assert h1 == h2                 # update_time 不参与哈希
    h3 = item_content_hash(dict(base, lines=[{"a": 2}]))
    assert h1 != h3                 # 线路变化 → 哈希变化