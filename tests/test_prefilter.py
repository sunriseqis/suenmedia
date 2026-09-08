# -*- coding: utf-8 -*-
"""test_prefilter.py —— pipeline.prefilter L1-L5 规则测试（T03 验收相关）。

覆盖：
- L1 分类白名单（四类放行 / 白名单外交弃）；
- L2 分类黑名单（short_tv / discard）；
- L3 子分类黑词（短剧 / 解说 / 微电影 / 爽文 ...）；
- L4 标题正则黑名单 / 标题黑词；
- L5 空行 / 无有效线路 / 空标题；
- keep() 全放行冒烟 + keep_many 统计。
"""
from __future__ import annotations

import copy

from core.config import Config, Settings, load_config
from pipeline.prefilter import Prefilter


def _settings(**overrides):
    """最小化 Settings（不读磁盘）：prefilter 段按默认 + 覆盖。"""
    base = {
        "prefilter": {
            "enable": True,
            "skip_categories": ["short_tv", "discard"],
            "sub_category_blacklist": ["短剧", "解说", "微电影", "爽文", "伦理片"],
            "title_regex_blacklist": [r"^第\d+[集期]", r"^\d+$", "微电影", "解说"],
            "title_keywords": [],
        }
    }
    base.update(overrides)
    return Settings.from_dict(base)


def _config(blacklist=None):
    return Config.from_dict({
        "SUB_CATEGORY_BLACKLIST": blacklist or [],
        "SITES": [],
    })


def _item(**overrides):
    item = {
        "raw_id": "1",
        "site": "测试站",
        "priority": 1,
        "raw_title": "庆余年 第二季",
        "title": "庆余年",
        "search_title": "庆余年",
        "category": "tv",
        "sub_category": "国产剧",
        "season": 2,
        "episode": 0,
        "year": "2024",
        "poster": "http://img.example.com/p.jpg",
        "overview": "描述",
        "douban_id": "",
        "douban_score": "",
        "actor": "",
        "director": "",
        "remarks": "",
        "update_time": "2026-09-08 10:00:00",
        "lines": [{"line_name": "线路1", "from": "x", "episodes": [
            {"name": "第1集", "url": "https://cdn.example.com/1.m3u8"}]}],
    }
    item.update(overrides)
    return item


def _pf(**kwargs):
    return Prefilter(settings=_settings(), config=_config(), **kwargs)


# ---------------------------------------------------------------- L1

def test_l1_pass_four_categories():
    pf = _pf()
    for cat in ("movies", "tv", "anime", "variety"):
        assert pf.keep(_item(category=cat)).ok, cat


def test_l1_drop_non_whitelist():
    pf = _pf()
    result = pf.keep(_item(category="discard"))
    assert not result.ok and result.reason.startswith("L1|"), result.reason


def test_l1_category_missing_falls_back_to_sub_category():
    """分类缺失时按子分类归一（电视剧'国产剧' → tv）。"""
    pf = _pf()
    result = pf.keep(_item(category="", sub_category="国产剧"))
    assert result.ok, result.reason


# ---------------------------------------------------------------- L2

def test_l2_skip_category():
    """分类黑名单：short_tv/discard 被拦截（L1 白名单或 L2 黑名单先命中的一级为准）。"""
    pf = _pf()
    for cat in ("short_tv", "discard"):
        result = pf.keep(_item(category=cat))
        assert not result.ok and result.reason.startswith(("L1|", "L2|")), result.reason


# ---------------------------------------------------------------- L3

def test_l3_sub_category_blackword():
    pf = _pf()
    for word in ("短剧", "解说", "微电影", "爽文"):
        result = pf.keep(_item(sub_category=f"国产{word}"))
        assert not result.ok and result.reason.startswith("L3|"), (word, result.reason)


def test_l3_sub_category_clean_passes():
    pf = _pf()
    assert pf.keep(_item(sub_category="国产剧")).ok
    assert pf.keep(_item(sub_category="综艺")).ok


# ---------------------------------------------------------------- L4

def test_l4_title_regex_blacklist():
    pf = _pf()
    cases = ("第3期", "第12集", "12345")
    for title in cases:
        result = pf.keep(_item(raw_title=title, title=title))
        assert not result.ok and result.reason.startswith("L4|"), (title, result.reason)


def test_l4_title_keyword():
    pf = Prefilter(settings=_settings(), config=_config())
    # title_keywords 合并了 crawl_skip_title_keywords；这里用默认词表里的黑词
    result = pf.keep(_item(raw_title="西游记微电影合集", title="西游记"))
    assert not result.ok and result.reason.startswith("L4|"), result.reason


def test_l4_normal_title_passes():
    pf = _pf()
    assert pf.keep(_item(raw_title="庆余年 第二季", title="庆余年")).ok


# ---------------------------------------------------------------- L5

def test_l5_empty_lines():
    pf = _pf()
    result = pf.keep(_item(lines=[]))
    assert not result.ok and result.reason.startswith("L5|"), result.reason


def test_l5_line_without_http_url():
    pf = _pf()
    lines = [{"line_name": "x", "from": "y", "episodes": [
        {"name": "a", "url": "rtsp://cdn.example.com/1"}]}]
    result = pf.keep(_item(lines=lines))
    assert not result.ok and result.reason.startswith("L5|"), result.reason


def test_l5_no_episodes():
    pf = _pf()
    lines = [{"line_name": "x", "from": "y", "episodes": []}]
    assert not pf.keep(_item(lines=lines)).ok


def test_l5_empty_title():
    pf = _pf()
    assert not pf.keep(_item(raw_title="", title="")).ok


def test_l5_require_cjk():
    """require_cjk=True 时纯外文标题丢弃。"""
    pf = _pf(require_cjk=True)
    result = pf.keep(_item(raw_title="Avengers Endgame", title="avengers endgame"))
    assert not result.ok and result.reason.startswith("L5|"), result.reason


# ---------------------------------------------------------------- 组合 / 批量

def test_keep_all_rules_satisfied():
    pf = _pf()
    assert pf.keep(_item()).ok


def test_prefilter_disabled_passes_everything():
    pf = Prefilter(settings=_settings(prefilter={"enable": False}),
                   config=_config())
    assert pf.keep(_item(category="discard", lines=[])).ok


def test_keep_many_counts_reasons():
    pf = _pf()
    items = [
        _item(raw_id="1", category="tv"),
        _item(raw_id="2", category="discard"),
        _item(raw_id="3", sub_category="短剧精品"),
    ]
    kept, reasons = pf.keep_many(items)
    assert len(kept) == 1 and kept[0]["raw_id"] == "1"
    assert any(r.startswith("L1|") for r in reasons)
    assert any(r.startswith("L3|") for r in reasons)


def test_keep_is_idempotent():
    pf = _pf()
    item = _item()
    assert pf.keep(copy.deepcopy(item)).ok == pf.keep(copy.deepcopy(item)).ok


# ---------------------------------------------------------------- 兼容

def test_legacy_skip_keywords_merged():
    """crawl_skip_type_keywords 等历史键并入规则（声明字段直赋，非 raw）。"""
    settings = _settings()
    settings.crawl_skip_type_keywords = ["NBA", "体育"]
    settings.crawl_skip_title_keywords = ["预告片"]
    pf = Prefilter(settings=settings, config=_config(["伦理片"]))
    assert not pf.keep(_item(sub_category="NBA赛事集锦")).ok
    assert not pf.keep(_item(raw_title="变形金刚之预告片", title="变形金刚")).ok
    assert not pf.keep(_item(sub_category="伦理片")).ok