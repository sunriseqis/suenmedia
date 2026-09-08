# -*- coding: utf-8 -*-
"""schema.py 单元测试：产物契约 v3 校验（§9）。"""
import copy

import pytest

import schema
from schema import (
    MIN_CONFIDENCE,
    blank_item,
    coerce_item,
    gate_reason,
    make_bangou,
    meets_gate,
    new_episode,
    new_season,
    validate_item,
    validate_items,
)


@pytest.fixture()
def movie_item():
    """一个合规的电影条目（type=video）。"""
    item = blank_item(bangou="tmdb_123", title="无间道", item_type="video")
    item.update({
        "cover": "https://image.tmdb.org/t/p/w500/abc.jpg",
        "overview": "两个卧底的故事。",
        "region": "电影",
        "group_name": "动作片",
        "year": "2002",
        "site": "索尼资源",
        "tags": "犯罪,悬疑",
        "genres": ["犯罪", "惊悚"],
        "status": "completed",
        "rating": 9.1,
        "rating_source": "TMDB",
        "vote_count": 5000,
        "first_air_date": "2002-12-12",
        "runtime": 101,
        "url": "https://cdn.example.com/a.m3u8",
        "url_type": "m3u8",
        "confidence": 88,
    })
    return item


@pytest.fixture()
def series_item():
    """一个合规的剧集条目（type=series，含 seasons/episodes）。"""
    item = blank_item(bangou="tmdb_456", title="庆余年", item_type="series")
    item.update({
        "cover": "https://image.tmdb.org/t/p/w500/def.jpg",
        "overview": "少年范闲的江湖。",
        "region": "国产剧",
        "group_name": "国产剧",
        "year": "2019",
        "site": "红牛资源",
        "tags": "古装,权谋",
        "genres": ["剧情"],
        "status": "completed",
        "rating": 8.0,
        "rating_source": "TMDB",
        "vote_count": 12000,
        "first_air_date": "2019-11-26",
        "runtime": 45,
        "number_of_seasons": 2,
        "number_of_episodes": 46,
        "confidence": 90,
    })
    season1 = new_season(1, date="2019")
    season1["episodes"] = [
        new_episode(1, "https://cdn.example.com/s1e1.m3u8"),
        new_episode(2, "https://cdn.example.com/s1e2.m3u8"),
    ]
    season2 = new_season(2, date="2024")
    season2["episodes"] = [new_episode(1, "https://cdn.example.com/s2e1.m3u8")]
    item["seasons"] = [season1, season2]
    return item


# ---------------------------------------------------------------- 合法用例

def test_valid_movie_item(movie_item):
    """合规电影条目：零错误。"""
    result = validate_item(movie_item)
    assert result.ok is True, result.errors
    assert result.errors == []


def test_valid_series_item(series_item):
    """合规剧集条目：零错误（含 seasons/episodes）。"""
    result = validate_item(series_item)
    assert result.ok is True, result.errors


def test_validate_items_batch(movie_item, series_item):
    """批量校验统计正确。"""
    report = validate_items([movie_item, series_item])
    assert report["total"] == 2
    assert report["ok"] == 2
    assert report["failed"] == 0
    assert report["errors"] == []


def test_result_is_tuple():
    """ValidationResult 是 NamedTuple，可按三元组解包。"""
    ok, errors, warnings = validate_item({"bangou": "x"})
    assert ok is False
    assert isinstance(errors, list)
    assert isinstance(warnings, list)


# ---------------------------------------------------------------- 必填与类型

@pytest.mark.parametrize("missing", [
    "bangou", "type", "title", "cover", "overview", "region",
    "group_name", "year", "site", "status",
])
def test_required_fields(movie_item, missing):
    """§9.1 必填字段缺失 → 报错。"""
    item = copy.deepcopy(movie_item)
    item.pop(missing, None)
    result = validate_item(item)
    assert result.ok is False
    assert any(missing in e for e in result.errors)


def test_type_enum(movie_item):
    """type 必须是 video / series。"""
    item = copy.deepcopy(movie_item)
    item["type"] = "movie"
    result = validate_item(item)
    assert any("type 必须是" in e for e in result.errors)


def test_status_enum(movie_item):
    """status 必须是 completed / ongoing。"""
    item = copy.deepcopy(movie_item)
    item["status"] = "完结"
    assert any("status 必须是" in e for e in validate_item(item).errors)


def test_year_format(movie_item):
    """year 必须 4 位年份。"""
    item = copy.deepcopy(movie_item)
    item["year"] = "2002年"
    assert any("year 必须是 4 位年份" in e for e in validate_item(item).errors)

    item = copy.deepcopy(movie_item)
    item.pop("year")
    item["date"] = "2002-12-12"
    result = validate_item(item)
    assert result.ok is True          # date 可兜底
    assert any("year" in w for w in result.warnings)


def test_genres_must_be_str_list(movie_item):
    """genres 必须是字符串数组。"""
    item = copy.deepcopy(movie_item)
    item["genres"] = "犯罪,惊悚"
    assert any("genres 必须是字符串数组" in e for e in validate_item(item).errors)


def test_tags_must_be_str(movie_item):
    """tags 是逗号分隔字符串。"""
    item = copy.deepcopy(movie_item)
    item["tags"] = ["犯罪", "惊悚"]
    assert any("tags 必须是字符串" in e for e in validate_item(item).errors)


# ---------------------------------------------------------------- B5：poster 禁用

def test_poster_forbidden(movie_item):
    """B5：产物中禁止出现 poster（统一 cover）。"""
    item = copy.deepcopy(movie_item)
    item["poster"] = "https://image.tmdb.org/t/p/w500/abc.jpg"
    result = validate_item(item)
    assert any("poster" in e for e in result.errors)


# ---------------------------------------------------------------- 数值字段

def test_rating_null_allowed(movie_item):
    """无评分时 rating 必须是 null（不得用 0.0 污染）。"""
    item = copy.deepcopy(movie_item)
    item["rating"] = None
    result = validate_item(item)
    assert result.ok is True
    assert result.warnings == []


def test_rating_zero_warns(movie_item):
    """rating=0.0 触发告警（不属于硬性错误）。"""
    item = copy.deepcopy(movie_item)
    item["rating"] = 0.0
    result = validate_item(item)
    assert result.ok is True
    assert any("0.0" in w for w in result.warnings)


def test_rating_must_be_number_or_null(movie_item):
    """rating 类型错误（字符串/布尔）→ 错误。"""
    item = copy.deepcopy(movie_item)
    item["rating"] = "9.1"
    assert any("rating 必须是数字或 null" in e for e in validate_item(item).errors)


def test_int_fields_type(movie_item):
    """vote_count / runtime 必须是非负 int。"""
    item = copy.deepcopy(movie_item)
    item["vote_count"] = "5000"
    assert any("vote_count 必须是 int" in e for e in validate_item(item).errors)

    item = copy.deepcopy(movie_item)
    item["runtime"] = -1
    assert any("runtime 不能为负数" in e for e in validate_item(item).errors)


# ---------------------------------------------------------------- series 契约

def test_series_requires_seasons(movie_item):
    """type=series 必须有非空 seasons。"""
    item = copy.deepcopy(movie_item)
    item["type"] = "series"
    result = validate_item(item)
    assert any("seasons 必须为非空数组" in e for e in result.errors)


def test_season_number_must_be_int(series_item):
    """§9.2：season_number 必须 int，否则整季被消费端跳过。"""
    item = copy.deepcopy(series_item)
    item["seasons"][0]["season_number"] = "1"
    result = validate_item(item)
    assert any("season_number 必须是 int" in e for e in result.errors)


def test_episode_number_must_be_int(series_item):
    """§9.2：ep_number 必须 int 才写入排序。"""
    item = copy.deepcopy(series_item)
    item["seasons"][0]["episodes"][0]["ep_number"] = 1.0
    result = validate_item(item)
    assert any("ep_number 必须是 int" in e for e in result.errors)


def test_duplicate_season_number(series_item):
    """同季号重复 → 报错。"""
    item = copy.deepcopy(series_item)
    item["seasons"][1]["season_number"] = 1
    result = validate_item(item)
    assert any("season_number 重复" in e for e in result.errors)


def test_episode_requires_url(series_item):
    """分集必须带 url。"""
    item = copy.deepcopy(series_item)
    item["seasons"][0]["episodes"][0]["url"] = ""
    assert any(".url 必须为非空字符串" in e for e in validate_item(item).errors)


def test_variety_air_date_episode(series_item):
    """综艺日期期号：air_date 写入 YYYY-MM-DD 且 ep_number 为 int（§5 / §9）。"""
    item = copy.deepcopy(series_item)
    season = new_season(1, date="2024")
    season["episodes"] = [
        {"ep_id": "s1_e1", "ep_number": 1, "ep_title": "2026-09-05 纯享版",
         "air_date": "2026-09-05", "url": "https://cdn.example.com/1.m3u8",
         "url_type": "m3u8", "alt_urls": []},
        {"ep_id": "s1_e2", "ep_number": 2, "ep_title": "2026-09-12",
         "air_date": "2026-09-12", "url": "https://cdn.example.com/2.m3u8",
         "url_type": "m3u8", "alt_urls": [
             {"source": "红牛线路", "url": "https://cdn2.example.com/2.m3u8",
              "url_type": "m3u8"}]},
    ]
    item["seasons"] = [season]
    result = validate_item(item)
    assert result.ok is True, result.errors


def test_bad_air_date_warns(series_item):
    """air_date 格式不合法只告警。"""
    item = copy.deepcopy(series_item)
    item["seasons"][0]["episodes"][0]["air_date"] = "2026/09/05"
    result = validate_item(item)
    assert result.ok is True
    assert any("air_date 建议为 YYYY-MM-DD" in w for w in result.warnings)


def test_alt_urls_structure(series_item):
    """alt_urls 元素必须带 url。"""
    item = copy.deepcopy(series_item)
    item["alt_urls"] = [{"source": "红牛线路"}]      # 缺 url
    assert any("alt_urls[0].url" in e for e in validate_item(item).errors)


def test_video_requires_url(movie_item):
    """type=video 必须带主源 url。"""
    item = copy.deepcopy(movie_item)
    item["url"] = ""
    assert any("url 必须为非空字符串" in e for e in validate_item(item).errors)


# ---------------------------------------------------------------- 入库门槛 §9.3

def test_gate_pass(movie_item):
    """cover + overview + confidence >= 55 → 通过门槛。"""
    assert meets_gate(movie_item) is True
    assert gate_reason(movie_item) == ""


@pytest.mark.parametrize("confidence,reason", [
    (54, "low_confidence"),
    (40, "low_confidence"),
    (0, "low_confidence"),
])
def test_gate_low_confidence(movie_item, confidence, reason):
    """confidence < 55 → low_confidence。"""
    item = copy.deepcopy(movie_item)
    item["confidence"] = confidence
    assert gate_reason(item) == reason
    assert validate_item(item, require_gate=True).ok is False


def test_gate_no_cover(movie_item):
    """cover 为空 → no_cover。"""
    item = copy.deepcopy(movie_item)
    item["cover"] = ""
    assert gate_reason(item) == "no_cover"


def test_gate_no_overview(movie_item):
    """overview 为空 → no_overview。"""
    item = copy.deepcopy(movie_item)
    item["overview"] = "   "
    assert gate_reason(item) == "no_overview"


def test_gate_all_sources_miss(movie_item):
    """matched=False → all_sources_miss。"""
    item = copy.deepcopy(movie_item)
    item["matched"] = False
    assert gate_reason(item) == "all_sources_miss"


def test_min_confidence_constant():
    """门槛常量为 55。"""
    assert MIN_CONFIDENCE == 55


# ---------------------------------------------------------------- bangou

@pytest.mark.parametrize("provider,ext,season,expected", [
    ("tmdb", 123, None, "tmdb_123"),
    ("TMDB", "123", 1, "tmdb_123"),
    ("tmdb", 123, 2, "tmdb_123_s2"),
    ("douban", "456", None, "douban_456"),
    ("豆瓣", "456", 3, "douban_456_s3"),
    ("tvdb", 789, None, "tvdb_789"),
    ("TheTVDB", 789, 2, "tvdb_789_s2"),
    ("bilibili", "BV1xx", None, "bili_BV1xx"),
    ("omdb", "tt123", None, "omdb_tt123"),
])
def test_make_bangou(provider, ext, season, expected):
    """§9.1 bangou 生成规则（季 >1 时追加 _s{n}）。"""
    assert make_bangou(provider, ext, season) == expected


def test_make_bangou_fallback():
    """provider / external_id 缺失 → v_{md5(title|url)} 兜底。"""
    bangou = make_bangou("", None, None, title="无间道", url="https://a.m3u8")
    assert bangou.startswith("v_")
    assert len(bangou) == 34
    # 同输入稳定
    assert bangou == make_bangou("", None, None, title="无间道", url="https://a.m3u8")
    # 不同输入不同
    assert bangou != make_bangou("", None, None, title="无间道2", url="https://a.m3u8")


# ---------------------------------------------------------------- coerce_item

def test_coerce_season_number_str_to_int():
    """导出前修复：字符串季号转 int。"""
    item = blank_item(bangou="tmdb_1", title="x", item_type="series")
    item["seasons"] = [{"season_number": "2", "episodes": [{"ep_number": "3"}]}]
    item, changes = coerce_item(item)
    assert item["seasons"][0]["season_number"] == 2
    assert item["seasons"][0]["episodes"][0]["ep_number"] == 3
    assert changes


def test_coerce_poster_to_cover():
    """coerce 把残留 poster 迁移为 cover。"""
    item = {"bangou": "x", "poster": "https://img/a.jpg", "cover": ""}
    item, changes = coerce_item(item)
    assert "poster" not in item
    assert item["cover"] == "https://img/a.jpg"
    assert any("poster → cover" in c for c in changes)


def test_coerce_rating_invalid_to_none():
    """无法转数字的 rating → None。"""
    item = {"rating": "N/A"}
    item, _ = coerce_item(item)
    assert item["rating"] is None


def test_coerce_int_fields():
    """vote_count / runtime 字符串数字 → int。"""
    item = {"vote_count": "1200", "runtime": "45.0", "number_of_seasons": None}
    item, _ = coerce_item(item)
    assert item["vote_count"] == 1200
    assert item["runtime"] == 45
    assert item["number_of_seasons"] == 0


# ---------------------------------------------------------------- 模板

def test_blank_item_shape():
    """blank_item 字段齐全。"""
    item = blank_item("tmdb_1", "标题", "series")
    assert item["type"] == "series"
    assert item["rating"] is None
    assert item["genres"] == []
    assert item["seasons"] == []


def test_new_season_and_episode():
    """new_season / new_episode 产出合规对象。"""
    season = new_season(3)
    assert season["season_number"] == 3
    assert season["season_title"] == "第 3 季"
    episode = new_episode(12, "https://a/b.m3u8")
    assert episode["ep_number"] == 12
    assert episode["url_type"] == "m3u8"
    assert episode["ep_title"] == "第12集"


def test_module_exports():
    """模块导出契约常量。"""
    assert schema.CONTRACT_VERSION == "v3"
    assert schema.TYPES == ("video", "series")
    assert "low_confidence" in schema.UNMATCHED_REASONS
