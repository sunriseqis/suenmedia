# -*- coding: utf-8 -*-
"""core.cache 单元测试：正/负缓存、TTL、负缓存四道闸、崩溃安全（§8）。"""
import os
import sqlite3
import subprocess
import sys
import time

import pytest

from core.cache import (
    DOMAIN_ALIVE,
    DOMAIN_DEAD,
    DOMAIN_UNKNOWN,
    STATUS_HARD_MISS,
    STATUS_NONE,
    STATUS_SOFT_MISS,
    STATUS_STALE,
    CacheDB,
    DomainRegistry,
    MetaCache,
    NegCacheGuard,
    RawSeen,
    RetryStore,
    TitleIndex,
    make_ck,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHILD = os.path.join(ROOT, "tests", "crash_child.py")


@pytest.fixture()
def db_path(tmp_path):
    """临时 SQLite 库路径。"""
    return str(tmp_path / "test_cache.db")


@pytest.fixture()
def cache(db_path):
    """临时 MetaCache（禁用时间触发的 flush，便于断言 pending）。"""
    mc = MetaCache(path=db_path, buffer_size=10000, flush_interval=3600)
    yield mc
    mc.close()


# ---------------------------------------------------------------- 基础读写

def test_put_and_get_hit(cache):
    """正缓存写入后可命中，元数据完整还原。"""
    ck = make_ck("movies", "无间道", 1, "2002")
    assert cache.put_hit(ck, {"title": "无间道", "rating": 9.1}, provider="TMDB",
                         confidence=92) is True
    entry = cache.get(ck)
    assert entry.status == "hit"
    assert entry.is_hit is True
    assert entry.meta["title"] == "无间道"
    assert entry.meta["rating"] == 9.1
    assert entry.provider == "TMDB"
    assert entry.confidence == 92


def test_put_and_get_soft_miss(cache):
    """负缓存（soft_miss）写入后可命中，且不带元数据。"""
    ck = make_ck("tv", "某不存在的剧", 1, "2024")
    assert cache.put_miss(ck, STATUS_SOFT_MISS, confirmed_by=["TMDB"]) is True
    entry = cache.get(ck)
    assert entry.status == STATUS_SOFT_MISS
    assert entry.is_miss is True
    assert entry.is_negative is True
    assert entry.meta is None
    assert entry.confirmed_by == ["TMDB"]


def test_hard_miss_confirmed_by_multi(cache):
    """hard_miss 记录多个确认源。"""
    ck = make_ck("anime", "某不存在的番", 1, "2024")
    cache.put_miss(ck, STATUS_HARD_MISS, confirmed_by=["TMDB", "豆瓣", "TheTVDB"])
    entry = cache.get(ck)
    assert entry.status == STATUS_HARD_MISS
    assert set(entry.confirmed_by) == {"TMDB", "豆瓣", "TheTVDB"}


def test_miss_never_overwritten_by_retryable(cache):
    """闸①：retryable 绝不写负缓存（现有 bug 的反例）。"""
    ck = make_ck("movies", "限流片", 1, "2024")
    assert cache.put_miss(ck, "retryable") is False
    assert cache.put_retryable(ck, reason="429") is False
    cache.flush()
    assert cache.get(ck).status == STATUS_NONE
    # 但错误率必须被记账
    assert cache.guard.total_samples >= 2


def test_cache_key_format():
    """缓存键格式：v6|{category}|{norm_title}|{seq}|{year}。"""
    assert make_ck("tv", "庆余年", 2, "2019") == "v6|tv|庆余年|2|2019"
    assert make_ck("movies", "无间道", 1, None) == "v6|movies|无间道|1|"


def test_unknown_key_returns_none(cache):
    """未缓存的键返回 status=none 的空条目。"""
    entry = cache.get("v6|movies|没查过|1|")
    assert entry.status == STATUS_NONE
    assert entry.meta is None
    # NamedTuple 可直接二元组解包（设计文档 §3 的 get(ck) tuple 契约）
    status, meta = entry[0], entry[1]
    assert status == STATUS_NONE and meta is None


# ---------------------------------------------------------------- TTL

def test_ttl_expiry_and_purge(cache):
    """TTL 到期后 get 返回 none，purge_expired 清理掉行。"""
    ck_hit = make_ck("movies", "短命片", 1, "2024")
    ck_miss = make_ck("movies", "短命miss", 1, "2024")
    cache.put_hit(ck_hit, {"title": "短命片"}, ttl=1)
    cache.put_miss(ck_miss, STATUS_SOFT_MISS, ttl=1)
    cache.flush()
    assert cache.get(ck_hit).is_hit is True

    time.sleep(1.2)
    assert cache.get(ck_hit).status == STATUS_NONE
    assert cache.get(ck_miss).status == STATUS_NONE

    removed = cache.purge_expired()
    assert removed == 2
    stats = cache.stats()
    assert stats["total"] == 0


def test_ttl_default_days(cache):
    """默认 TTL：hit 90 天 / soft_miss 14 天 / hard_miss 45 天 / 老片 hard_miss 90 天。"""
    this_year = int(time.strftime("%Y"))
    ck_old = make_ck("movies", "老片", 1, str(this_year - 10))
    cache.put_miss(ck_old, STATUS_HARD_MISS)
    cache.flush()
    row = cache.db.query_one("SELECT expires-ts AS span FROM meta_cache WHERE ck=?", (ck_old,))
    assert 89 * 86400 <= int(row["span"]) <= 91 * 86400

    ck_new = make_ck("movies", "新片", 1, str(this_year))
    cache.put_miss(ck_new, STATUS_HARD_MISS)
    cache.flush()
    row = cache.db.query_one("SELECT expires-ts AS span FROM meta_cache WHERE ck=?", (ck_new,))
    assert 44 * 86400 <= int(row["span"]) <= 46 * 86400


def test_stale_while_error(cache):
    """§8.3：过期后重查失败 → 保留旧值（stale-while-error）。"""
    ck = make_ck("movies", "过期但可留", 1, "2024")
    cache.put_hit(ck, {"title": "过期但可留"}, ttl=1)
    cache.flush()
    time.sleep(1.2)
    assert cache.get(ck).status == STATUS_NONE

    stale_entry = cache.stale(ck)
    assert stale_entry.status == STATUS_STALE
    assert stale_entry.meta["title"] == "过期但可留"
    assert cache.keep_stale(ck, ttl=60) is True
    assert cache.get(ck).is_hit is True


# ---------------------------------------------------------------- 负缓存闸门

def test_guard_closes_neg_cache_at_30pct_error_rate():
    """验收③：错误率 > 30%（样本足够）→ 负缓存写入被自动关闭。"""
    guard = NegCacheGuard(error_rate_threshold=0.30, min_samples=50)
    for _ in range(60):
        guard.record(ok=True)
    for _ in range(40):      # 100 样本 / 40 错误 = 40% > 30%
        guard.record(ok=False)
    assert guard.samples == 100
    assert guard.error_rate == pytest.approx(0.40)
    assert guard.neg_cache_enabled is False
    assert "error_rate" in guard.reason

    cache = MetaCache(path=":memory:", guard=guard)
    ck = make_ck("tv", "高错误率期的新miss", 1, "2024")
    assert cache.put_miss(ck, STATUS_SOFT_MISS) is False
    cache.flush()
    assert cache.get(ck).status == STATUS_NONE
    # 正缓存不受影响（闸门只管负缓存）
    assert cache.put_hit(ck, {"title": "x"}) is True


def test_guard_keeps_enabled_when_below_threshold():
    """错误率 20% < 30% → 负缓存照常写入。"""
    guard = NegCacheGuard(error_rate_threshold=0.30, min_samples=50)
    for _ in range(80):
        guard.record(ok=True)
    for _ in range(20):
        guard.record(ok=False)
    assert guard.error_rate == pytest.approx(0.20)
    assert guard.neg_cache_enabled is True


def test_guard_no_verdict_when_samples_insufficient():
    """样本数 < min_samples 时不判定（避免开局偶发错误误伤）。"""
    guard = NegCacheGuard(error_rate_threshold=0.30, min_samples=50)
    for _ in range(40):      # 40 < 50，即便 100% 错误也不跳闸
        guard.record(ok=False)
    assert guard.error_rate == pytest.approx(1.0)
    assert guard.neg_cache_enabled is True


def test_guard_429_pause_blocks_negative_write():
    """闸③：429 暂停期内不写负缓存；暂停结束后恢复。"""
    guard = NegCacheGuard()
    guard.mark_429(retry_after=0.05)
    assert guard.paused is True
    assert guard.neg_cache_enabled is False
    time.sleep(0.1)
    assert guard.paused is False
    assert guard.neg_cache_enabled is True


def test_guard_manual_disable(cache):
    """闸④：人工关闭（--force-refresh 语义）。"""
    ck = make_ck("movies", "人工清除", 1, "2024")
    cache.guard.disable("force_refresh")
    assert cache.put_miss(ck, STATUS_SOFT_MISS) is False
    cache.guard.enable()
    assert cache.put_miss(ck, STATUS_SOFT_MISS) is True


def test_drop_misses_and_forget(cache):
    """人工清负缓存 / 单条 forget。"""
    ck_hit = make_ck("movies", "保留", 1, "2024")
    ck_miss = make_ck("movies", "清除", 1, "2024")
    cache.put_hit(ck_hit, {"title": "保留"})
    cache.put_miss(ck_miss, STATUS_SOFT_MISS)
    cache.flush()
    assert cache.drop_misses() == 1
    assert cache.get(ck_miss).status == STATUS_NONE
    assert cache.get(ck_hit).is_hit is True

    assert cache.forget(ck_hit) is True
    assert cache.get(ck_hit).status == STATUS_NONE


# ---------------------------------------------------------------- 缓冲与落盘

def test_buffer_flush_threshold(db_path):
    """缓冲条数达到 buffer_size 自动落盘。"""
    mc = MetaCache(path=db_path, buffer_size=5, flush_interval=3600)
    for i in range(4):
        mc.put_hit(f"ck{i}", {"i": i})
    assert mc.pending_count == 4
    mc.put_hit("ck4", {"i": 4})
    assert mc.pending_count == 0
    assert mc.db.query_one("SELECT COUNT(*) AS n FROM meta_cache")["n"] == 5
    mc.close()


def test_pending_readable_before_flush(db_path):
    """未落盘的缓冲也要能被读到（避免重复请求）。"""
    mc = MetaCache(path=db_path, buffer_size=10000, flush_interval=3600)
    ck = make_ck("tv", "未落盘", 1, "2024")
    mc.put_hit(ck, {"title": "未落盘"})
    assert mc.pending_count == 1
    assert mc.get(ck).is_hit is True
    mc.close()


# ---------------------------------------------------------------- 崩溃安全

def test_kill_minus_9_does_not_corrupt_data(db_path, tmp_path):
    """验收⑤：进程被硬杀后重开，数据不损坏、已提交数据不丢。"""
    count = 500
    proc = subprocess.Popen(
        [sys.executable, CHILD, db_path, str(count)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        cwd=ROOT,
    )
    try:
        line = proc.stdout.readline().strip()
        assert line == "READY", f"子进程未就绪：{line}"
    finally:
        proc.kill()          # Windows: TerminateProcess；POSIX: SIGKILL
        proc.wait(timeout=30)

    assert proc.returncode is not None and proc.returncode != 0

    # Windows 上 TerminateProcess 后文件句柄可能尚未完全释放，立即重开会
    # 偶发 sqlite disk I/O error → 短等待 + 有限重试（真实场景进程退出即释放）。
    db = None
    for _attempt in range(5):
        try:
            db = CacheDB(db_path)
            break
        except sqlite3.OperationalError:
            time.sleep(0.5)
            db = None
    assert db is not None, "硬杀后无法重新打开数据库"
    try:
        assert db.integrity_check() == "ok"
        row = db.query_one("SELECT COUNT(*) AS n FROM meta_cache")
        assert int(row["n"]) == count
        # 抽查内容完整性
        row = db.query_one("SELECT meta_json FROM meta_cache WHERE ck LIKE '%测试片499%'")
        assert "测试片499" in str(row["meta_json"])
        # WAL 模式下数据可正常读出
        assert db.query_one("PRAGMA journal_mode")["journal_mode"].lower() == "wal"
    finally:
        db.close()


def test_wal_mode_enabled(db_path):
    """默认开启 WAL + synchronous=NORMAL（§8.5）。"""
    db = CacheDB(db_path)
    try:
        assert db.query_one("PRAGMA journal_mode")["journal_mode"].lower() == "wal"
        assert db.query_one("PRAGMA synchronous")["synchronous"] == 1  # NORMAL
    finally:
        db.close()


# ---------------------------------------------------------------- TitleIndex / 其余表

def test_title_index_roundtrip(db_path):
    """TitleIndex：0 请求命中本地索引。"""
    db = CacheDB(db_path)
    try:
        index = TitleIndex(db)
        assert index.get("tv|庆余年|2") is None
        assert index.put("tv|庆余年|2", "TMDB", "12345", "tv") is True
        got = index.get("tv|庆余年|2")
        assert got is not None
        provider, external_id, media_type = got
        assert provider == "TMDB" and external_id == "12345" and media_type == "tv"
        # UPSERT 覆盖
        index.put("tv|庆余年|2", "豆瓣", "999", "tv")
        assert index.get("tv|庆余年|2")[1] == "999"
        assert index.count() == 1
        assert index.forget("tv|庆余年|2") is True
        assert index.count() == 0
    finally:
        db.close()


def test_domain_registry_lifecycle(db_path):
    """DomainRegistry：新域名 → alive → 连续失败 4 次转 dead → 冷却后半开。"""
    db = CacheDB(db_path)
    try:
        reg = DomainRegistry(db, fail_threshold=4, half_open_cooldown=0)
        assert reg.state_of("cdn.example.com") == DOMAIN_UNKNOWN
        assert reg.probe_new("cdn.example.com") is True       # 首次登记
        assert reg.probe_new("cdn.example.com") is False      # 已存在
        assert reg.should_probe("cdn.example.com") is True

        reg.mark_ok("cdn.example.com")
        assert reg.state_of("cdn.example.com") == DOMAIN_ALIVE
        assert reg.should_probe("cdn.example.com") is False   # alive 不再探测（0 请求）

        assert reg.mark_fail("cdn.example.com") == DOMAIN_UNKNOWN
        assert reg.mark_fail("cdn.example.com") == DOMAIN_UNKNOWN
        assert reg.mark_fail("cdn.example.com") == DOMAIN_UNKNOWN
        assert reg.mark_fail("cdn.example.com") == DOMAIN_DEAD  # 第 4 次
        assert reg.should_probe("cdn.example.com") is True      # 冷却 0s → 半开

        reg.mark_ok("cdn.example.com")                        # 半开成功转 alive
        assert reg.state_of("cdn.example.com") == DOMAIN_ALIVE
        stats = reg.stats()
        assert stats[DOMAIN_ALIVE] == 1
    finally:
        db.close()


def test_domain_registry_recheck_list(db_path):
    """list_for_recheck 返回超期未复检的域名。"""
    db = CacheDB(db_path)
    try:
        reg = DomainRegistry(db, full_recheck_days=15)
        reg.probe_new("a.com")
        reg.mark_ok("a.com")
        assert "a.com" in reg.list_for_recheck(days=0)     # days=0 → 全部超期
        assert "a.com" not in reg.list_for_recheck(days=30)
    finally:
        db.close()


def test_raw_seen(db_path):
    """RawSeen：断点续采去重。"""
    db = CacheDB(db_path)
    try:
        seen = RawSeen(db)
        assert seen.seen("索尼资源", "1001") is False
        assert seen.mark("索尼资源", "1001", "hash1") is True     # 首次
        assert seen.mark("索尼资源", "1001", "hash2") is False    # 再次
        assert seen.seen("索尼资源", "1001") is True
        assert seen.hash_of("索尼资源", "1001") == "hash2"
        assert seen.count() == 1
        assert seen.forget("索尼资源", "1001") is True
        assert seen.count() == 0
    finally:
        db.close()


def test_retry_store(db_path):
    """RetryStore：attempts 累加、出队、清空。"""
    db = CacheDB(db_path)
    try:
        store = RetryStore(db)
        assert store.push("k1", {"title": "x"}, "429") == 1
        assert store.push("k1", {"title": "x"}, "429") == 2
        assert store.push("k2", {"title": "y"}, "timeout") == 1
        assert store.count() == 2
        assert store.get("k1")["attempts"] == 2
        assert store.get("k1")["item"]["title"] == "x"
        pending = store.pending(limit=10, max_attempts=1)
        assert [p["qk"] for p in pending] == ["k2"]
        assert store.remove("k1") is True
        assert store.clear() == 1
        assert store.count() == 0
    finally:
        db.close()


def test_stats_snapshot(cache):
    """stats() 快照字段完整。"""
    cache.put_hit("ck1", {"a": 1})
    cache.put_miss("ck2", STATUS_SOFT_MISS)
    stats = cache.stats()
    assert stats["hit"] == 1 and stats["soft_miss"] == 1 and stats["total"] == 2
    assert stats["neg_cache_enabled"] is True
    assert "error_rate" in stats["guard"]
