# -*- coding: utf-8 -*-
"""test_probe.py —— pipeline.probe 域名级探活测试（T03 验收②③④）。

覆盖：
- 新域名探 1 次并转 alive；alive 域名后续 0 请求（验收③：第二轮起请求数 < 100）；
- dead 域名直接丢弃 0 请求；连续失败 4 次转 dead（验收④阈值）；
- dead + 冷却 15min → 半开重试成功转 alive（验收④）；
- 单轮内同一域名只探测一次（多条线路共享一次判定）；
- HEAD 403/405 → GET 降级；
- enable_m3u8_check=False 原样放行。
"""
from __future__ import annotations

import asyncio
import time

from core.cache import CacheDB, DomainRegistry
from core.config import Settings
from core.http import FetchResult, FetchStatus
from pipeline.probe import DomainProber, parse_domain

ADDR = "https://cdn1.example.com/vod/1.m3u8"
ADDR2 = "https://cdn2.example.com/vod/2.m3u8"
ADDR3 = "https://cdn1.example.com/vod/3.m3u8"
DEAD = "https://cdn-dead.example.com/vod/9.m3u8"
FALLBACK = "https://cdn-fallback.example.com/vod/8.m3u8"


def _lines(*urls):
    return [
        {"line_name": f"线路{i}", "from": "x",
         "episodes": [{"name": "第1集", "url": url}]}
        for i, url in enumerate(urls) if url
    ]


class FakeClient:
    """记录请求并模拟 HEAD/GET 结果（鸭子类型 AsyncHttpClient）。"""

    def __init__(self, ok_urls=(), get_fallback_urls=()):
        self.requests = []  # [(method, url)]
        self._ok = set(ok_urls)
        self._get_ok = set(get_fallback_urls)

    def count(self, method=None):
        if method is None:
            return len(self.requests)
        return sum(1 for m, _u in self.requests if m == method)

    async def request(self, method, url, **kwargs):
        self.requests.append((method, url))
        if url in self._ok:
            return FetchResult(status=FetchStatus.OK, status_code=200,
                               text="ok", url=url)
        if method == "HEAD" and url in self._get_ok:
            return FetchResult(status=FetchStatus.ERROR, status_code=403,
                               text="", url=url)
        if method == "GET" and url in self._get_ok:
            return FetchResult(status=FetchStatus.OK, status_code=200,
                               text="ok", url=url)
        return FetchResult(status=FetchStatus.ERROR, status_code=404,
                           text="", url=url)


def _new_prober(settings=None, **kwargs):
    db = CacheDB(":memory:")
    return DomainProber(DomainRegistry(db), settings=settings or Settings()), db


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------- 新域名 / alive

def test_new_domain_probe_once_then_zero_requests():
    """验收③：第一轮探 1 次，第二轮起 0 请求（域名已 alive）。"""
    prober, _db = _new_prober()
    client = FakeClient(ok_urls=(ADDR,))

    kept1 = run(prober.filter_lines(client, _lines(ADDR)))
    assert len(kept1) == 1
    assert client.count() == 1 and client.requests[0][0] == "HEAD"

    # 第二轮：同一 prober + registry，alive 直接放行
    kept2 = run(prober.filter_lines(client, _lines(ADDR)))
    assert len(kept2) == 1
    assert client.count() == 1  # 无新增请求
    assert prober.registry.state_of(parse_domain(ADDR)) == "alive"


def test_same_domain_multi_line_single_probe():
    """单轮内同一域名只探测一次。"""
    prober, _db = _new_prober()
    client = FakeClient(ok_urls=(ADDR, ADDR3))
    kept = run(prober.filter_lines(client, _lines(ADDR, ADDR3)))
    assert len(kept) == 2
    assert client.count() == 1


def test_multiple_domains_probed_separately():
    prober, _db = _new_prober()
    client = FakeClient(ok_urls=(ADDR, ADDR2))
    kept = run(prober.filter_lines(client, _lines(ADDR, ADDR2)))
    assert len(kept) == 2
    assert client.count() == 2


# ---------------------------------------------------------------- dead / 阈值

def test_dead_domain_dropped_with_zero_requests():
    """dead 域名后续线路直接丢弃（0 请求）。"""
    prober, _db = _new_prober()
    client = FakeClient()  # DEAD 不可达
    # 先连续失败 4 次跨轮累计 → dead
    for _ in range(4):
        kept = run(prober.filter_lines(client, _lines(DEAD)))
        assert kept == []
    assert prober.registry.state_of(parse_domain(DEAD)) == "dead"
    probes_before = client.count()
    kept = run(prober.filter_lines(client, _lines(DEAD)))
    assert kept == []
    assert client.count() == probes_before  # 0 新增请求


def test_half_open_cooldown_recovers_alive():
    """验收④：dead 冷却 15min 后半开探测 1 次，成功转 alive。"""
    prober, db = _new_prober()
    domain = parse_domain(DEAD)
    client_dead = FakeClient()
    for _ in range(4):
        run(prober.filter_lines(client_dead, _lines(DEAD)))
    assert prober.registry.state_of(domain) == "dead"

    # 模拟冷却到期：把 last_check 拨回 1000s 前
    db.execute("UPDATE domain_registry SET last_check=? WHERE domain=?",
               (int(time.time()) - 1000, domain))
    client_ok = FakeClient(ok_urls=(DEAD,))
    kept = run(prober.filter_lines(client_ok, _lines(DEAD)))
    assert len(kept) == 1
    assert client_ok.count() == 1
    assert prober.registry.state_of(domain) == "alive"


# ---------------------------------------------------------------- HEAD 降级

def test_head_403_falls_back_to_get():
    prober, _db = _new_prober()
    client = FakeClient(get_fallback_urls=(FALLBACK,))
    kept = run(prober.filter_lines(client, _lines(FALLBACK)))
    assert len(kept) == 1
    assert [m for m, _u in client.requests] == ["HEAD", "GET"]


# ---------------------------------------------------------------- 开关

def test_probe_disabled_passes_through():
    settings = Settings.from_dict({"enable_m3u8_check": False})
    prober, _db = _new_prober(settings=settings)
    client = FakeClient()  # 全部 404
    lines = _lines(DEAD)
    kept = run(prober.filter_lines(client, lines))
    assert kept == lines
    assert client.count() == 0


# ---------------------------------------------------------------- 工具

def test_parse_domain():
    assert parse_domain("https://A.CDN.Example.com/x/y.m3u8") == "a.cdn.example.com"
    assert parse_domain("") == ""
    assert parse_domain("not a url") == ""


def test_sync_filter_lines():
    """sync 接口（cleanup 15 天复检用）基本可用。"""
    prober, _db = _new_prober()
    http = None  # sync 用 HttpClient，这里用假对象验证判定路径
    calls = []

    class FakeHttp:
        def request(self, method, url, **kwargs):
            calls.append((method, url))
            return (FetchResult(status=FetchStatus.OK, status_code=200,
                                text="ok", url=url)
                    if url == ADDR else
                    FetchResult(status=FetchStatus.ERROR, status_code=404,
                                text="", url=url))

    kept = prober.filter_lines_sync(FakeHttp(), _lines(ADDR, DEAD))
    assert len(kept) == 1
    assert calls[0] == ("HEAD", ADDR)