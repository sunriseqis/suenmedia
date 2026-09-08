# -*- coding: utf-8 -*-
"""持久化缓存层：SQLite(WAL) 实现 MetaCache / TitleIndex / DomainRegistry / RawSeen。

设计文档 §8：用 stdlib sqlite3 + WAL 取代现有 5MB 单文件 JSON 缓存。

为什么换：
- 启动 O(1)（不 json.load 全量），30 万条规模下 JSON 会膨胀到 60-90MB；
- 写入增量 UPSERT（现状每 500 条全量重写 0.6-1s）；
- TTL 清理走索引 `DELETE WHERE expires < ?`；
- `PRAGMA journal_mode=WAL; synchronous=NORMAL` → kill -9 不损坏，已提交数据不丢。

负缓存四道防污染闸（§8.4）：
1. **retryable 绝不写负缓存** —— `put_miss(kind="retryable")` 直接拒绝并计入错误率；
2. **全局健康闸门** —— 滚动窗口 `error_rate > 0.30 且 samples >= 50` 时本轮关闭
   负缓存写入（`NegCacheGuard`，默认 sticky：一旦跳闸保持关闭直到显式 enable/reset）；
3. **429 专用** —— `on_429()` 暂停期内不计 miss（由 RateLimiter 暂停派发，
   本模块的 guard 同步进入暂停态）；
4. **人工清除** —— `forget(ck)` / `drop_misses()` / `MetaCache(force_refresh=True)`。

表结构见 §8.2（meta_cache / title_index / domain_registry / raw_seen / retry_queue）。
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import sqlite3
import threading
import time
import weakref
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, NamedTuple, Optional, Tuple

from .config import Settings, cache_db_path, load_settings
from .logging import get_logger, log_event

__all__ = [
    "CacheDB",
    "CacheEntry",
    "NegCacheGuard",
    "MetaCache",
    "TitleIndex",
    "DomainRegistry",
    "RawSeen",
    "RetryStore",
    "RawLibrary",
    "STATUS_HIT",
    "STATUS_SOFT_MISS",
    "STATUS_HARD_MISS",
    "STATUS_NONE",
    "STATUS_STALE",
    "make_ck",
    "install_exit_hooks",
]

_LOGGER = get_logger("cache")

# ---------------------------------------------------------------- 状态常量

STATUS_HIT: str = "hit"
STATUS_SOFT_MISS: str = "soft_miss"       # TMDB 200 且 results 为空
STATUS_HARD_MISS: str = "hard_miss"       # TMDB 空 且 ≥1 备源也确认空
STATUS_RETRYABLE: str = "retryable"       # 只进 retry_queue，永不写缓存
STATUS_NONE: str = "none"                 # 未命中
STATUS_STALE: str = "stale"               # 已过期但仍在库（stale-while-error 用）

#: 域名状态
DOMAIN_ALIVE: str = "alive"
DOMAIN_DEAD: str = "dead"
DOMAIN_UNKNOWN: str = "unknown"

#: 写缓冲触发阈值（§8.5）
DEFAULT_BUFFER_SIZE: int = 200
DEFAULT_FLUSH_INTERVAL: float = 30.0
#: 错误率滚动窗口
DEFAULT_GUARD_WINDOW: float = 300.0
DEFAULT_MAX_WINDOW_SAMPLES: int = 10000

_DAY: int = 86400

# ---------------------------------------------------------------- Schema

SCHEMA_SQL: str = """
CREATE TABLE IF NOT EXISTS meta_cache(
  ck TEXT PRIMARY KEY,          -- "v6|{category}|{norm_title}|{seq}|{year}"
  status TEXT NOT NULL,         -- hit | soft_miss | hard_miss
  provider TEXT,                -- TMDB/豆瓣/TheTVDB/Bilibili/OMDb
  meta_json TEXT,               -- hit 时的元数据 JSON；miss 时 NULL
  confidence INTEGER,
  confirmed_by TEXT,            -- JSON array（miss 的确认源）
  ts INTEGER NOT NULL,
  expires INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_meta_expires ON meta_cache(expires);

CREATE TABLE IF NOT EXISTS title_index(
  nk TEXT PRIMARY KEY,          -- "{category}|{norm_title}|{seq}"
  provider TEXT, external_id TEXT, media_type TEXT, ts INTEGER
);

CREATE TABLE IF NOT EXISTS domain_registry(
  domain TEXT PRIMARY KEY, state TEXT,   -- alive|dead|unknown
  ok_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0,
  first_seen INTEGER, last_check INTEGER, last_ok INTEGER
);

CREATE TABLE IF NOT EXISTS raw_seen(
  site TEXT, raw_id TEXT, content_hash TEXT,
  first_seen INTEGER, last_seen INTEGER,
  PRIMARY KEY(site, raw_id)
);

CREATE TABLE IF NOT EXISTS retry_queue(
  qk TEXT PRIMARY KEY, item_json TEXT, attempts INTEGER, last_err TEXT, ts INTEGER
);

CREATE TABLE IF NOT EXISTS raw_library(
  merge_key TEXT PRIMARY KEY,     -- "{category}|{norm_title}|{seq}"
  category TEXT, norm_title TEXT, seq INTEGER, year TEXT,
  payload_json TEXT,              -- CoarseEntity 序列化
  first_seen INTEGER,             -- 进入素材库时间（FIFO 排序键）
  last_updated INTEGER,           -- 源站 update_time（热通道判定）
  scraped INTEGER DEFAULT 0,      -- 0=未刮削 1=已刮削(Tier A) 2=已补全(Tier B)
  scrape_status TEXT,             -- hit|soft_miss|hard_miss|retryable|category_discard
  confidence INTEGER,
  attempts INTEGER DEFAULT 0,
  weight REAL DEFAULT 1.0,        -- 类目加权（variety=2.0）
  is_new INTEGER DEFAULT 0        -- 近 7 天 update_time（热通道，§7.1 P0）
);
CREATE INDEX IF NOT EXISTS idx_lib_pending ON raw_library(scraped, weight, first_seen);

CREATE TABLE IF NOT EXISTS meta_meta(
  k TEXT PRIMARY KEY, v TEXT
);
"""

_UPSERT_META_SQL: str = """
INSERT INTO meta_cache(ck, status, provider, meta_json, confidence, confirmed_by, ts, expires)
VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(ck) DO UPDATE SET
  status=excluded.status,
  provider=excluded.provider,
  meta_json=excluded.meta_json,
  confidence=excluded.confidence,
  confirmed_by=excluded.confirmed_by,
  ts=excluded.ts,
  expires=excluded.expires
"""


# ---------------------------------------------------------------- 工具

def make_ck(category: str, norm_title: str, seq: Any = 1,
            year: Any = None, version: str = "v6") -> str:
    """构造 meta_cache 主键（§8.2）。

    Args:
        category: movies / tv / anime / variety。
        norm_title: 归一化标题。
        seq: 季/部序号。
        year: 年份（可空）。
        version: 缓存版本号，规则变更时递增可整体失效旧缓存。

    Returns:
        缓存键字符串。
    """
    seq_text = "1" if seq in (None, "") else str(seq)
    year_text = "" if year in (None, "") else str(year)
    return f"{version}|{category}|{norm_title}|{seq_text}|{year_text}"


def _now() -> int:
    """当前 Unix 时间戳（秒）。"""
    return int(time.time())


def _dumps(value: Any) -> Optional[str]:
    """JSON 序列化；None 原样返回。"""
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


# meta_cache 行字段固定序（_pending 缓冲 tuple / SELECT 列 / sqlite3.Row 共用）
_PENDING_COLS = ("ck", "status", "provider", "meta_json",
                 "confidence", "confirmed_by", "ts", "expires")


def _loads(text: Optional[str], default: Any = None) -> Any:
    """JSON 反序列化；失败返回 default。"""
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """尽力转 int，失败返回 default。"""
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------- DB 句柄

class CacheDB:
    """SQLite 连接封装：WAL + 自动提交 + 单一写锁。

    Args:
        path: 库文件路径；目录不存在时自动创建。
        timeout: 等锁超时（秒）。
        wal: 是否开启 WAL（默认开；`:memory:` 自动降级为 memory journal）。
        busy_timeout_ms: busy 超时毫秒数。
    """

    def __init__(self, path: Optional[str] = None, timeout: float = 30.0,
                 wal: bool = True, busy_timeout_ms: int = 30000) -> None:
        self.path: str = path or cache_db_path()
        if self.path != ":memory:":
            parent = os.path.dirname(os.path.abspath(self.path))
            if parent:
                os.makedirs(parent, exist_ok=True)
        self._conn: sqlite3.Connection = sqlite3.connect(
            self.path, timeout=float(timeout), check_same_thread=False,
            isolation_level=None,  # 自动提交：每次写即落盘，kill -9 不丢已提交数据
        )
        self._conn.row_factory = sqlite3.Row
        self._write_lock = threading.Lock()
        self._lock = threading.RLock()
        self._closed: bool = False

        self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        if wal and self.path != ":memory:":
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
        # NORMAL：WAL 下崩溃不损坏，性能与 FULL 差距小而安全性足够（§8.5）
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA cache_size=-16000")  # ~16MB page cache
        self._init_schema()
        _register_db(self)

    # ---------------------------------------------------------- schema

    def _init_schema(self) -> None:
        """建表（幂等）。"""
        with self._write_lock:
            self._conn.executescript(SCHEMA_SQL)

    # ---------------------------------------------------------- 查询

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        """查一行；无结果返回 None。"""
        with self._lock:
            cursor = self._conn.execute(sql, tuple(params))
            return cursor.fetchone()

    def query_all(self, sql: str, params: Iterable[Any] = ()) -> List[sqlite3.Row]:
        """查多行。"""
        with self._lock:
            cursor = self._conn.execute(sql, tuple(params))
            return list(cursor.fetchall())

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        """执行单条写语句。"""
        with self._write_lock:
            return self._conn.execute(sql, tuple(params))

    def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> int:
        """批量写；返回受影响行数估计（None 时返回 0）。"""
        rows = list(seq)
        if not rows:
            return 0
        with self._write_lock:
            cursor = self._conn.executemany(sql, rows)
            return int(cursor.rowcount or 0)

    def executescript(self, sql: str) -> None:
        """执行脚本（建表 / 迁移）。"""
        with self._write_lock:
            self._conn.executescript(sql)

    # ---------------------------------------------------------- 维护

    def integrity_check(self) -> str:
        """`PRAGMA integrity_check` 结果字符串（'ok' 表示完好）。"""
        row = self.query_one("PRAGMA integrity_check")
        return str(row[0]) if row else "unknown"

    def checkpoint(self) -> None:
        """WAL 截断检查点（关闭前调用，把 WAL 内容合并回主库）。"""
        if self.path == ":memory:" or self._closed:
            return
        try:
            with self._write_lock:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.DatabaseError:
            pass

    def close(self) -> None:
        """关闭连接（幂等）。"""
        if self._closed:
            return
        self._closed = True
        self.checkpoint()
        try:
            self._conn.close()
        except sqlite3.Error:
            pass
        _unregister_db(self)

    @property
    def closed(self) -> bool:
        """连接是否已关闭。"""
        return self._closed

    def __enter__(self) -> "CacheDB":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


# ---------------------------------------------------------------- 退出钩子

_OPEN_DBS: List["weakref.ref[CacheDB]"] = []
_EXIT_HOOK_INSTALLED: bool = False
_EXIT_LOCK = threading.RLock()


def _register_db(db: CacheDB) -> None:
    """登记打开的连接，供 atexit 统一关闭。"""
    with _EXIT_LOCK:
        _OPEN_DBS.append(weakref.ref(db))


def _unregister_db(db: CacheDB) -> None:
    """注销连接。"""
    with _EXIT_LOCK:
        _OPEN_DBS[:] = [ref for ref in _OPEN_DBS if ref() is not None and ref() is not db]


def flush_all() -> int:
    """flush + 关闭全部登记的数据库连接；返回处理的连接数。"""
    count = 0
    with _EXIT_LOCK:
        refs = list(_OPEN_DBS)
    for ref in refs:
        db = ref()
        if db is None:
            continue
        count += 1
        try:
            db.close()
        except Exception:  # pylint: disable=broad-except
            pass
    return count


def _on_exit() -> None:  # pragma: no cover - 进程退出路径
    """进程正常退出：尽力关闭所有连接（WAL 检查点）。"""
    try:
        flush_all()
    except Exception:  # pylint: disable=broad-except
        pass


def _on_signal(signum: Any, frame: Any) -> None:  # pragma: no cover - 信号路径
    """SIGTERM/SIGINT：先 flush 缓存，再交回默认行为（§7.2）。"""
    _on_exit()
    raise SystemExit(128 + int(signum) if isinstance(signum, int) else 1)


def install_exit_hooks(signals: Optional[Iterable[int]] = None) -> None:
    """注册 atexit + 信号处理（SIGTERM/SIGINT）强制 flush 缓存。

    Args:
        signals: 需要接管的信号；默认 (SIGTERM, SIGINT)。传空序列只注册 atexit。
    """
    global _EXIT_HOOK_INSTALLED
    with _EXIT_LOCK:
        if not _EXIT_HOOK_INSTALLED:
            atexit.register(_on_exit)
            _EXIT_HOOK_INSTALLED = True
    if signals is None:
        signals = (signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError, AttributeError):
            # 非主线程注册会抛 ValueError，忽略即可（atexit 仍生效）
            continue


# atexit 默认注册（无副作用，仅保证 WAL 检查点）
install_exit_hooks(signals=())


# ---------------------------------------------------------------- 负缓存闸门

class NegCacheGuard:
    """负缓存污染防护闸门（§8.4 闸②③）。

    判定：`error_rate = 错误数 / 样本数 > threshold` **且** `样本数 >= min_samples`
    → 本轮关闭负缓存写入。样本不足时不判定（避免开局偶发错误误伤）。

    Args:
        error_rate_threshold: 错误率阈值（默认 0.30）。
        min_samples: 判定所需最小样本数（默认 50）。
        window_seconds: 滚动窗口（默认 300s）。
        sticky: True 时一旦跳闸保持关闭，直到 `enable()` / `reset()`（"本轮关闭"语义）。
        pause_seconds: 429 暂停时长。
    """

    def __init__(self, error_rate_threshold: float = 0.30, min_samples: int = 50,
                 window_seconds: float = DEFAULT_GUARD_WINDOW,
                 sticky: bool = True, pause_seconds: float = 30.0) -> None:
        self._threshold: float = float(error_rate_threshold)
        self._min_samples: int = int(min_samples)
        self._window_seconds: float = float(window_seconds)
        self._sticky: bool = bool(sticky)
        self._pause_seconds: float = float(pause_seconds)

        self._lock = threading.RLock()
        self._window: Deque[Tuple[float, bool]] = deque()
        self._errors: int = 0
        self._total: int = 0          # 累计（本轮）样本
        self._enabled: bool = True    # 人工开关
        self._tripped: bool = False   # 健康闸门跳闸
        self._pause_until: float = 0.0
        self._reason: str = ""

    # ---------------------------------------------------------- 上报

    def record(self, ok: bool = True) -> None:
        """记录一次请求结果（False = retryable 类错误）。"""
        with self._lock:
            now = time.monotonic()
            self._window.append((now, not ok))
            self._total += 1
            if not ok:
                self._errors += 1
            self._trim(now)
            self._evaluate()

    def record_success(self) -> None:
        """记录一次成功。"""
        self.record(True)

    def record_error(self) -> None:
        """记录一次可重试错误（429 / 5xx / 超时 / 连接失败 / 反爬）。"""
        self.record(False)

    def mark_429(self, retry_after: Optional[float] = None) -> float:
        """429：计入错误 + 进入暂停态（暂停期不计 miss）。

        Returns:
            实际暂停秒数。
        """
        seconds = float(retry_after) if retry_after and retry_after > 0 else self._pause_seconds
        with self._lock:
            self._pause_until = max(self._pause_until, time.monotonic() + seconds)
        self.record(False)
        return seconds

    # ---------------------------------------------------------- 查询

    def _trim(self, now: Optional[float] = None) -> None:
        """裁剪滚动窗口（调用方持锁）。"""
        cutoff = (now if now is not None else time.monotonic()) - self._window_seconds
        window = self._window
        while window and window[0][0] < cutoff:
            window.popleft()
        while len(window) > DEFAULT_MAX_WINDOW_SAMPLES:
            window.popleft()

    def _window_counts(self) -> Tuple[int, int]:
        """窗口内 (样本, 错误)（调用方持锁）。"""
        now = time.monotonic()
        self._trim(now)
        cutoff = now - self._window_seconds
        samples = 0
        errors = 0
        for ts, is_error in self._window:
            if ts < cutoff:
                continue
            samples += 1
            if is_error:
                errors += 1
        return samples, errors

    @property
    def samples(self) -> int:
        """滚动窗口样本数。"""
        with self._lock:
            return self._window_counts()[0]

    @property
    def errors(self) -> int:
        """滚动窗口错误数。"""
        with self._lock:
            return self._window_counts()[1]

    @property
    def error_rate(self) -> float:
        """滚动窗口错误率 [0,1]；无样本返回 0.0。"""
        with self._lock:
            samples, errors = self._window_counts()
            return (errors / samples) if samples else 0.0

    @property
    def total_samples(self) -> int:
        """本轮累计样本数（不受窗口裁剪影响）。"""
        with self._lock:
            return self._total

    @property
    def paused(self) -> bool:
        """是否处于 429 暂停期。"""
        with self._lock:
            return time.monotonic() < self._pause_until

    @property
    def reason(self) -> str:
        """负缓存被关闭的原因（未关闭返回空串）。"""
        with self._lock:
            if not self._enabled:
                return self._reason or "manually_disabled"
            if time.monotonic() < self._pause_until:
                return "429_pause"
            if self._tripped:
                return self._reason or "error_rate_exceeded"
            return ""

    @property
    def neg_cache_enabled(self) -> bool:
        """是否允许写负缓存。"""
        with self._lock:
            if not self._enabled:
                return False
            if time.monotonic() < self._pause_until:
                return False
            return not self._tripped

    # ---------------------------------------------------------- 控制

    def _evaluate(self) -> None:
        """按窗口统计决定是否跳闸（调用方持锁）。"""
        if self._tripped and self._sticky:
            return
        samples, errors = self._window_counts()
        if samples < self._min_samples:
            if not self._sticky:
                self._tripped = False
                self._reason = ""
            return
        rate = errors / samples
        self._tripped = rate > self._threshold
        self._reason = (f"error_rate={rate:.2%}>{self._threshold:.0%} "
                        f"samples={samples}" if self._tripped else "")

    def disable(self, reason: str = "manually_disabled") -> None:
        """人工关闭负缓存写入（如 `--force-refresh`）。"""
        with self._lock:
            self._enabled = False
            self._reason = reason

    def enable(self) -> None:
        """重新开启（同时清除跳闸与暂停态）。"""
        with self._lock:
            self._enabled = True
            self._tripped = False
            self._pause_until = 0.0
            self._reason = ""

    def reset(self) -> None:
        """清空统计并恢复开启。"""
        with self._lock:
            self._window.clear()
            self._errors = 0
            self._total = 0
            self.enable()

    def snapshot(self) -> Dict[str, Any]:
        """快照（报表 / 测试断言用）。"""
        with self._lock:
            samples, errors = self._window_counts()
            return {
                "enabled": self.neg_cache_enabled,
                "tripped": self._tripped,
                "reason": self.reason,
                "window_samples": samples,
                "window_errors": errors,
                "error_rate": (errors / samples) if samples else 0.0,
                "total_samples": self._total,
                "total_errors": self._errors,
                "threshold": self._threshold,
                "min_samples": self._min_samples,
                "paused": self.paused,
            }


# ---------------------------------------------------------------- 缓存条目

class CacheEntry(NamedTuple):
    """`MetaCache.get()` 返回条目（本身即 tuple，可 `status, meta = entry[:2]`）。

    Attributes:
        status: hit / soft_miss / hard_miss / none / stale。
        meta: hit 时的元数据 dict；其余为 None（stale 时返回旧值）。
        provider: 命中来源。
        confidence: 匹配置信度。
        confirmed_by: miss 的确认源列表。
        ts: 写入时间戳。
        expires: 过期时间戳。
    """

    status: str
    meta: Optional[Dict[str, Any]]
    provider: Optional[str] = None
    confidence: Optional[int] = None
    confirmed_by: Optional[List[str]] = None
    ts: int = 0
    expires: int = 0

    @property
    def is_hit(self) -> bool:
        """是否为正缓存命中。"""
        return self.status == STATUS_HIT

    @property
    def is_miss(self) -> bool:
        """是否为负缓存命中。"""
        return self.status in (STATUS_SOFT_MISS, STATUS_HARD_MISS)

    @property
    def is_negative(self) -> bool:
        """是否为负缓存（等价于 is_miss）。"""
        return self.is_miss

    @property
    def kind(self) -> str:
        """负缓存类型（soft_miss / hard_miss），正缓存返回 ''。"""
        return self.status if self.is_miss else ""

    def remaining(self, now: Optional[int] = None) -> int:
        """剩余有效秒数（<=0 表示已过期）。"""
        return int(self.expires - (now if now is not None else _now()))


_EMPTY_ENTRY = CacheEntry(status=STATUS_NONE, meta=None)


# ---------------------------------------------------------------- MetaCache

class MetaCache:
    """元数据正/负缓存（§8.2 meta_cache 表）。

    Args:
        db: CacheDB 实例或库路径字符串；为空时用配置里的默认路径。
        path: 库路径（db 为空时生效）。
        ttl: TTL 覆盖（秒）：{hit, soft_miss, hard_miss, old_hard_miss}；
            为空时用 settings.cache_ttl（天）。
        guard: 负缓存闸门；为空时按 settings.neg_cache_guard 构造。
        settings: 配置；为空时 load_settings()。
        buffer_size: 写缓冲条数阈值。
        flush_interval: 写缓冲时间阈值（秒）。
        force_refresh: True 时关闭负缓存写入并忽略已有负缓存（--force-refresh）。
    """

    def __init__(self, db: Optional[Any] = None, path: Optional[str] = None,
                 ttl: Optional[Dict[str, float]] = None,
                 guard: Optional[NegCacheGuard] = None,
                 settings: Optional[Settings] = None,
                 buffer_size: int = DEFAULT_BUFFER_SIZE,
                 flush_interval: float = DEFAULT_FLUSH_INTERVAL,
                 force_refresh: bool = False) -> None:
        self._settings: Settings = settings if settings is not None else load_settings()
        if isinstance(db, CacheDB):
            self._db: CacheDB = db
            self._owns_db: bool = False
        else:
            self._db = CacheDB(path or (db if isinstance(db, str) else None) or cache_db_path())
            self._owns_db = True

        # TTL（秒）：显式 ttl 优先，否则由配置的"天"换算
        ttl_map: Dict[str, float] = {}
        for key in ("hit", "soft_miss", "hard_miss", "old_hard_miss"):
            if ttl and key in ttl:
                ttl_map[key] = float(ttl[key])
            else:
                ttl_map[key] = self._settings.ttl_days(key) * _DAY
        self._ttl: Dict[str, float] = ttl_map

        if guard is not None:
            self._guard: NegCacheGuard = guard
        else:
            self._guard = NegCacheGuard(
                error_rate_threshold=self._settings.guard_value("error_rate_threshold") or 0.30,
                min_samples=int(self._settings.guard_value("min_samples") or 50),
            )
        self._buffer_size: int = max(int(buffer_size), 1)
        self._flush_interval: float = max(float(flush_interval), 0.0)
        self._pending: Dict[str, Tuple[Any, ...]] = {}
        self._last_flush: float = time.monotonic()
        self._lock = threading.RLock()
        self._hits: int = 0
        self._misses: int = 0
        self._lookups: int = 0
        self._dropped_misses: int = 0
        if force_refresh:
            self._guard.disable("force_refresh")

    # ---------------------------------------------------------- 属性

    @property
    def db(self) -> CacheDB:
        """底层 CacheDB。"""
        return self._db

    @property
    def guard(self) -> NegCacheGuard:
        """负缓存闸门。"""
        return self._guard

    @property
    def neg_cache_enabled(self) -> bool:
        """当前是否允许写负缓存。"""
        return self._guard.neg_cache_enabled

    @property
    def pending_count(self) -> int:
        """未落盘的缓冲条数。"""
        with self._lock:
            return len(self._pending)

    # ---------------------------------------------------------- 读

    def get(self, ck: str, include_stale: bool = False) -> CacheEntry:
        """读缓存。

        Args:
            ck: 缓存键（见 make_ck）。
            include_stale: True 时已过期条目以 status="stale" 返回（供
                stale-while-error 使用，§8.3）。

        Returns:
            CacheEntry；未命中为 status="none" 的空条目。
        """
        now = _now()
        with self._lock:
            self._lookups += 1
            row = self._pending.get(ck)
            if row is None:
                row = self._db.query_one(
                    "SELECT ck,status,provider,meta_json,confidence,confirmed_by,ts,expires "
                    "FROM meta_cache WHERE ck=?", (ck,))
            if row is None:
                return _EMPTY_ENTRY
            # _pending 存固定序 tuple、DB 返回 sqlite3.Row：统一为 dict 支持字符串键访问
            if not isinstance(row, dict):
                row = dict(zip(_PENDING_COLS, row))
            status = str(row["status"])
            meta = _loads(row["meta_json"], None) if row["meta_json"] else None
            entry = CacheEntry(
                status=status,
                meta=meta,
                provider=row["provider"],
                confidence=row["confidence"],
                confirmed_by=_loads(row["confirmed_by"], None),
                ts=int(row["ts"] or 0),
                expires=int(row["expires"] or 0),
            )
            expired = entry.expires <= now
            if not expired:
                if entry.is_hit:
                    self._hits += 1
                elif entry.is_miss:
                    self._misses += 1
                return entry
            if include_stale:
                return entry._replace(status=STATUS_STALE)
            return _EMPTY_ENTRY

    def stale(self, ck: str) -> CacheEntry:
        """取已过期的旧条目（无则返回 status=none）。"""
        return self.get(ck, include_stale=True)

    def keep_stale(self, ck: str, ttl: Optional[float] = None) -> bool:
        """stale-while-error：把已过期条目续期（重查失败时保留旧值，§8.3）。

        Returns:
            是否续期成功（条目存在且已过期）。
        """
        entry = self.get(ck, include_stale=True)
        if entry.status != STATUS_STALE:
            return False
        seconds = float(ttl if ttl is not None else self._ttl.get("hit", 90 * _DAY))
        with self._lock:
            row = self._pending.get(ck)
            if row is not None:
                self._pending[ck] = row[:7] + (_now() + int(seconds),)
                return True
            self._db.execute("UPDATE meta_cache SET expires=? WHERE ck=?",
                             (_now() + int(seconds), ck))
            return True

    # ---------------------------------------------------------- 写

    def _ttl_seconds(self, kind: str, year: Any = None) -> float:
        """按类型与年份解析 TTL（秒）。

        hard_miss 且 year <= 当年-3 → 老片 90 天（§8.3）。
        """
        if kind == STATUS_HARD_MISS and year:
            year_int = _safe_int(year, 0)
            if year_int > 0 and year_int <= int(time.strftime("%Y")) - 3:
                return self._ttl.get("old_hard_miss", 90 * _DAY)
        return self._ttl.get(kind, 14 * _DAY)

    def put_hit(self, ck: str, meta: Dict[str, Any], provider: Optional[str] = None,
                confidence: Optional[int] = None, ttl: Optional[float] = None) -> bool:
        """写正缓存（TTL 默认 90 天）。

        Args:
            ck: 缓存键。
            meta: 元数据 dict（会被 JSON 序列化）。
            provider: 命中来源。
            confidence: 匹配置信度。
            ttl: 覆盖 TTL（秒）。

        Returns:
            是否入缓冲成功。
        """
        if not ck:
            return False
        seconds = float(ttl if ttl is not None else self._ttl.get("hit", 90 * _DAY))
        now = _now()
        row = (ck, STATUS_HIT, provider, _dumps(meta),
               None if confidence is None else int(confidence), None, now, now + int(seconds))
        with self._lock:
            self._pending[ck] = row
            self._maybe_flush_locked()
        self._guard.record(True)
        return True

    def put_miss(self, ck: str, kind: str = STATUS_SOFT_MISS,
                 confirmed_by: Optional[List[str]] = None,
                 ttl: Optional[float] = None, year: Any = None,
                 provider: Optional[str] = None) -> bool:
        """写负缓存（受四道闸约束）。

        闸①：kind="retryable" 一律拒绝（限流/不可达/超时绝不写负缓存）；
        闸②：健康闸门跳闸时拒绝写入（本轮关闭）；
        闸③：429 暂停期内拒绝写入；
        闸④：人工关闭（force_refresh / disable）时拒绝写入。

        Args:
            ck: 缓存键。
            kind: soft_miss / hard_miss（其他值按 soft_miss 处理）。
            confirmed_by: 确认"确实没有"的来源列表。
            ttl: 覆盖 TTL（秒）。
            year: 年份（老片 hard_miss 用 90 天 TTL）。
            provider: 最后确认的来源。

        Returns:
            是否写入（False 表示被闸门拦下）。
        """
        if not ck:
            return False
        if kind == STATUS_RETRYABLE:
            # 闸①：retryable 只记错误率，绝不落负缓存
            self._guard.record(False)
            with self._lock:
                self._dropped_misses += 1
            return False
        if kind not in (STATUS_SOFT_MISS, STATUS_HARD_MISS):
            kind = STATUS_SOFT_MISS
        if not self._guard.neg_cache_enabled:
            with self._lock:
                self._dropped_misses += 1
            log_event("cache.neg_blocked", "WARNING", None, ck=ck, miss_kind=kind,
                      reason=self._guard.reason)
            return False
        seconds = float(ttl if ttl is not None else self._ttl_seconds(kind, year))
        if ttl is None and not year:
            # 调用方未显式给 year 时从缓存键末段解析（v6|cat|title|seq|year），
            # 使"老片 hard_miss 用 90 天 TTL"不依赖调用方传参
            tail = str(ck).rsplit("|", 1)[-1]
            if tail.isdigit() and len(tail) == 4:
                seconds = float(self._ttl_seconds(kind, tail))
        now = _now()
        row = (ck, kind, provider, None, None, _dumps(confirmed_by or []), now,
               now + int(seconds))
        with self._lock:
            self._pending[ck] = row
            self._maybe_flush_locked()
        self._guard.record(True)  # 确认过的 miss 是"成功的一次查询"
        return True

    def put_retryable(self, ck: str, reason: str = "") -> bool:
        """登记一次可重试失败：只进错误率统计，**永不写缓存**（闸①）。

        Returns:
            恒为 False（语义：没有写入缓存）。
        """
        self._guard.record(False)
        log_event("cache.retryable", "DEBUG", None, ck=ck, reason=reason)
        return False

    # 兼容语义别名
    def record_retryable(self, ck: str = "", reason: str = "") -> bool:
        """`put_retryable` 的别名。"""
        return self.put_retryable(ck, reason)

    def record_success(self) -> None:
        """登记一次成功查询（影响错误率）。"""
        self._guard.record(True)

    def on_429(self, retry_after: Optional[float] = None) -> float:
        """429 上报：进入暂停态，暂停期内不写负缓存（闸③）。"""
        return self._guard.mark_429(retry_after)

    def forget(self, ck: str) -> bool:
        """删除单条缓存（正负皆可，闸④ 人工清除）。

        Returns:
            是否有数据被删除。
        """
        with self._lock:
            self._pending.pop(ck, None)
        cursor = self._db.execute("DELETE FROM meta_cache WHERE ck=?", (ck,))
        return bool(cursor and cursor.rowcount)

    def drop_misses(self) -> int:
        """清空全部负缓存（--force-refresh 用）。

        Returns:
            删除条数。
        """
        with self._lock:
            for ck in [k for k, row in self._pending.items() if row[1] != STATUS_HIT]:
                self._pending.pop(ck, None)
        cursor = self._db.execute(
            "DELETE FROM meta_cache WHERE status IN (?,?)", (STATUS_SOFT_MISS, STATUS_HARD_MISS))
        return int(cursor.rowcount or 0) if cursor else 0

    # ---------------------------------------------------------- 落盘

    def _maybe_flush_locked(self) -> None:
        """按阈值触发 flush（调用方持锁）。"""
        if len(self._pending) >= self._buffer_size:
            self.flush_locked()

    def flush_locked(self) -> int:
        """flush 缓冲（调用方已持 self._lock）。"""
        if not self._pending:
            self._last_flush = time.monotonic()
            return 0
        rows = list(self._pending.values())
        self._pending.clear()
        self._last_flush = time.monotonic()
        self._db.executemany(_UPSERT_META_SQL, rows)
        log_event("cache.flush", "DEBUG", None, rows=len(rows), table="meta_cache")
        return len(rows)

    def flush(self) -> int:
        """把写缓冲落盘；返回落盘条数。"""
        with self._lock:
            return self.flush_locked()

    def maybe_flush(self) -> int:
        """按时间阈值检查是否需要 flush（长任务中定期调用）。"""
        with self._lock:
            if self._pending and (time.monotonic() - self._last_flush) >= self._flush_interval:
                return self.flush_locked()
            return 0

    def purge_expired(self, now: Optional[int] = None) -> int:
        """清理过期条目（TTL 清理，走 idx_meta_expires 索引）。

        Returns:
            删除条数。
        """
        self.flush()
        cursor = self._db.execute("DELETE FROM meta_cache WHERE expires <= ?",
                                  (int(now if now is not None else _now()),))
        removed = int(cursor.rowcount or 0) if cursor else 0
        if removed:
            log_event("cache.purge", "INFO", None, rows=removed, table="meta_cache")
        return removed

    # ---------------------------------------------------------- 统计

    def stats(self) -> Dict[str, Any]:
        """缓存统计快照。"""
        self.flush()
        rows = self._db.query_all("SELECT status, COUNT(*) AS n FROM meta_cache GROUP BY status")
        by_status = {str(r["status"]): int(r["n"]) for r in rows}
        total = sum(by_status.values())
        return {
            "total": total,
            "hit": by_status.get(STATUS_HIT, 0),
            "soft_miss": by_status.get(STATUS_SOFT_MISS, 0),
            "hard_miss": by_status.get(STATUS_HARD_MISS, 0),
            "lookups": self._lookups,
            "hits": self._hits,
            "misses": self._misses,
            "dropped_misses": self._dropped_misses,
            "pending": self.pending_count,
            "neg_cache_enabled": self.neg_cache_enabled,
            "guard": self._guard.snapshot(),
        }

    def close(self) -> None:
        """flush 并关闭（仅关闭自己创建的连接）。"""
        try:
            self.flush()
        finally:
            if self._owns_db:
                self._db.close()

    def __enter__(self) -> "MetaCache":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


# ---------------------------------------------------------------- TitleIndex

class TitleIndex:
    """Stage 0 本地索引：归一化键 → (provider, external_id)，0 请求命中（§8.2）。"""

    def __init__(self, db: Any) -> None:
        self._db: CacheDB = db if isinstance(db, CacheDB) else CacheDB(db)
        self._lock = threading.RLock()

    def get(self, nk: str) -> Optional[Tuple[str, str, str]]:
        """查索引；命中返回 (provider, external_id, media_type)，否则 None。"""
        row = self._db.query_one(
            "SELECT provider, external_id, media_type FROM title_index WHERE nk=?", (nk,))
        if row is None:
            return None
        return (str(row["provider"] or ""), str(row["external_id"] or ""),
                str(row["media_type"] or ""))

    def put(self, nk: str, provider: str, external_id: str,
            media_type: str = "") -> bool:
        """写索引（UPSERT）。

        Returns:
            是否执行成功。
        """
        if not nk or not external_id:
            return False
        with self._lock:
            self._db.execute(
                "INSERT INTO title_index(nk, provider, external_id, media_type, ts) "
                "VALUES(?,?,?,?,?) ON CONFLICT(nk) DO UPDATE SET "
                "provider=excluded.provider, external_id=excluded.external_id, "
                "media_type=excluded.media_type, ts=excluded.ts",
                (nk, provider, str(external_id), media_type, _now()))
        return True

    def forget(self, nk: str) -> bool:
        """删除索引条目。"""
        cursor = self._db.execute("DELETE FROM title_index WHERE nk=?", (nk,))
        return bool(cursor and cursor.rowcount)

    def count(self) -> int:
        """索引总条数。"""
        row = self._db.query_one("SELECT COUNT(*) AS n FROM title_index")
        return int(row["n"]) if row else 0


# ---------------------------------------------------------------- DomainRegistry

class DomainRegistry:
    """探活域名状态表（§2 P2 / §8.2 domain_registry）。

    规则：alive → 后续线路全放行（0 请求）；dead → 直接丢弃（0 请求）；
    dead + 冷却 15min → 半开允许 1 次探测，成功转 alive。
    """

    def __init__(self, db: Any, fail_threshold: int = 4,
                 half_open_cooldown: float = 900.0,
                 full_recheck_days: float = 15.0) -> None:
        self._db: CacheDB = db if isinstance(db, CacheDB) else CacheDB(db)
        self._fail_threshold: int = max(int(fail_threshold), 1)
        self._half_open_cooldown: float = float(half_open_cooldown)
        self._full_recheck_days: float = float(full_recheck_days)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- 查询

    def state_of(self, domain: str) -> str:
        """域名状态：alive / dead / unknown。"""
        row = self._db.query_one("SELECT state FROM domain_registry WHERE domain=?", (domain,))
        if row is None:
            return DOMAIN_UNKNOWN
        return str(row["state"] or DOMAIN_UNKNOWN)

    def should_probe(self, domain: str) -> bool:
        """是否需要探测：新域名必探；dead 冷却到期后半开探测 1 次。"""
        row = self._db.query_one(
            "SELECT state, last_check FROM domain_registry WHERE domain=?", (domain,))
        if row is None:
            return True
        state = str(row["state"] or DOMAIN_UNKNOWN)
        if state == DOMAIN_ALIVE:
            return False
        if state == DOMAIN_UNKNOWN:
            return True
        last_check = int(row["last_check"] or 0)
        return (time.time() - last_check) >= self._half_open_cooldown

    def probe_new(self, domain: str) -> bool:
        """登记域名（首次出现返回 True 并置 unknown 状态）。

        Returns:
            True 表示这是首次见到的新域名（调用方应发起 1 次探测）。
        """
        if not domain:
            return False
        with self._lock:
            row = self._db.query_one("SELECT domain FROM domain_registry WHERE domain=?", (domain,))
            if row is not None:
                return False
            now = _now()
            self._db.execute(
                "INSERT INTO domain_registry(domain, state, ok_count, fail_count, "
                "first_seen, last_check, last_ok) VALUES(?,?,?,?,?,?,?)",
                (domain, DOMAIN_UNKNOWN, 0, 0, now, now, 0))
            return True

    # ---------------------------------------------------------- 回写

    def mark_ok(self, domain: str) -> None:
        """探测成功：转 alive，连续失败计数清零。"""
        now = _now()
        with self._lock:
            self._db.execute(
                "INSERT INTO domain_registry(domain, state, ok_count, fail_count, "
                "first_seen, last_check, last_ok) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(domain) DO UPDATE SET state=?, ok_count=ok_count+1, "
                "fail_count=0, last_check=?, last_ok=?",
                (domain, DOMAIN_ALIVE, 1, 0, now, now, now,
                 DOMAIN_ALIVE, now, now))

    def mark_fail(self, domain: str) -> str:
        """探测失败：连续失败达阈值转 dead。

        Returns:
            更新后的状态（dead / alive / unknown）。
        """
        now = _now()
        with self._lock:
            row = self._db.query_one(
                "SELECT ok_count, fail_count FROM domain_registry WHERE domain=?", (domain,))
            fails = int(row["fail_count"] or 0) + 1 if row else 1
            oks = int(row["ok_count"] or 0) if row else 0
            # 失败回退状态机：未达阈值即回 unknown 触发重新探测（alive 保持
            # 会导致 should_probe=False、fail_count 永不累计、熔断失效）；
            # 达阈值转 dead。
            state = DOMAIN_DEAD if fails >= self._fail_threshold else DOMAIN_UNKNOWN
            self._db.execute(
                "INSERT INTO domain_registry(domain, state, ok_count, fail_count, "
                "first_seen, last_check, last_ok) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(domain) DO UPDATE SET state=?, fail_count=?, last_check=?",
                (domain, state, 0, fails, now, now, 0, state, fails, now))
            return state

    def list_for_recheck(self, days: Optional[float] = None) -> List[str]:
        """列出需要全量复检的域名（超过 N 天未检查，§2 每 15 天复检 147 个）。"""
        days = self._full_recheck_days if days is None else float(days)
        cutoff = _now() - int(days * _DAY)
        rows = self._db.query_all(
            "SELECT domain FROM domain_registry WHERE last_check IS NULL OR last_check <= ? "
            "ORDER BY last_check", (cutoff,))
        return [str(r["domain"]) for r in rows]

    def stats(self) -> Dict[str, int]:
        """各状态域名数。"""
        rows = self._db.query_all(
            "SELECT state, COUNT(*) AS n FROM domain_registry GROUP BY state")
        out = {DOMAIN_ALIVE: 0, DOMAIN_DEAD: 0, DOMAIN_UNKNOWN: 0}
        for row in rows:
            out[str(row["state"])] = int(row["n"])
        return out

    def forget(self, domain: str) -> bool:
        """删除域名记录。"""
        cursor = self._db.execute("DELETE FROM domain_registry WHERE domain=?", (domain,))
        return bool(cursor and cursor.rowcount)


# ---------------------------------------------------------------- RawSeen

class RawSeen:
    """采集去重 / 断点续采（§8.2 raw_seen）。"""

    def __init__(self, db: Any) -> None:
        self._db: CacheDB = db if isinstance(db, CacheDB) else CacheDB(db)
        self._lock = threading.RLock()

    def seen(self, site: str, raw_id: str) -> bool:
        """该 (site, raw_id) 是否已采集过。"""
        row = self._db.query_one(
            "SELECT 1 AS hit FROM raw_seen WHERE site=? AND raw_id=?", (site, str(raw_id)))
        return row is not None

    def mark(self, site: str, raw_id: str, content_hash: str = "") -> bool:
        """登记已采集；返回 True 表示首次登记（新条目）。

        Args:
            site: 站点名。
            raw_id: 源站条目 ID。
            content_hash: 内容哈希（用于判断"老项目加集"是否有变化）。
        """
        now = _now()
        with self._lock:
            row = self._db.query_one(
                "SELECT content_hash FROM raw_seen WHERE site=? AND raw_id=?", (site, str(raw_id)))
            self._db.execute(
                "INSERT INTO raw_seen(site, raw_id, content_hash, first_seen, last_seen) "
                "VALUES(?,?,?,?,?) ON CONFLICT(site, raw_id) DO UPDATE SET "
                "content_hash=excluded.content_hash, last_seen=excluded.last_seen",
                (site, str(raw_id), content_hash, now, now))
            return row is None

    def hash_of(self, site: str, raw_id: str) -> str:
        """取已记录的内容哈希（无记录返回空串）。"""
        row = self._db.query_one(
            "SELECT content_hash FROM raw_seen WHERE site=? AND raw_id=?", (site, str(raw_id)))
        return str(row["content_hash"] or "") if row else ""

    def forget(self, site: str, raw_id: str) -> bool:
        """删除记录。"""
        cursor = self._db.execute(
            "DELETE FROM raw_seen WHERE site=? AND raw_id=?", (site, str(raw_id)))
        return bool(cursor and cursor.rowcount)

    def count(self, site: Optional[str] = None) -> int:
        """记录数；site 为空统计全部。"""
        if site:
            row = self._db.query_one("SELECT COUNT(*) AS n FROM raw_seen WHERE site=?", (site,))
        else:
            row = self._db.query_one("SELECT COUNT(*) AS n FROM raw_seen")
        return int(row["n"]) if row else 0


# ---------------------------------------------------------------- RetryStore

class RetryStore:
    """SQLite 版重试队列（§8.2 retry_queue；替代现有 JSON 文件实现）。"""

    def __init__(self, db: Any) -> None:
        self._db: CacheDB = db if isinstance(db, CacheDB) else CacheDB(db)
        self._lock = threading.RLock()

    def push(self, qk: str, item: Any, last_err: str = "") -> int:
        """入队（已存在则 attempts+1）。

        Returns:
            更新后的 attempts 次数。
        """
        now = _now()
        with self._lock:
            row = self._db.query_one("SELECT attempts FROM retry_queue WHERE qk=?", (qk,))
            attempts = int(row["attempts"] or 0) + 1 if row else 1
            self._db.execute(
                "INSERT INTO retry_queue(qk, item_json, attempts, last_err, ts) "
                "VALUES(?,?,?,?,?) ON CONFLICT(qk) DO UPDATE SET "
                "item_json=excluded.item_json, attempts=?, last_err=excluded.last_err, ts=excluded.ts",
                (qk, _dumps(item) or "", attempts, last_err, now, attempts))
            return attempts

    def get(self, qk: str) -> Optional[Dict[str, Any]]:
        """取单条记录（含解析后的 item）。"""
        row = self._db.query_one(
            "SELECT qk, item_json, attempts, last_err, ts FROM retry_queue WHERE qk=?", (qk,))
        if row is None:
            return None
        return {
            "qk": str(row["qk"]),
            "item": _loads(row["item_json"], None),
            "attempts": int(row["attempts"] or 0),
            "last_err": str(row["last_err"] or ""),
            "ts": int(row["ts"] or 0),
        }

    def pending(self, limit: int = 100, max_attempts: int = 0) -> List[Dict[str, Any]]:
        """取待重试列表（按时间升序）。

        Args:
            limit: 最多返回条数。
            max_attempts: >0 时只返回 attempts <= 该值的记录。
        """
        if max_attempts > 0:
            rows = self._db.query_all(
                "SELECT qk, item_json, attempts, last_err, ts FROM retry_queue "
                "WHERE attempts<=? ORDER BY ts LIMIT ?", (int(max_attempts), int(limit)))
        else:
            rows = self._db.query_all(
                "SELECT qk, item_json, attempts, last_err, ts FROM retry_queue "
                "ORDER BY ts LIMIT ?", (int(limit),))
        return [{
            "qk": str(r["qk"]),
            "item": _loads(r["item_json"], None),
            "attempts": int(r["attempts"] or 0),
            "last_err": str(r["last_err"] or ""),
            "ts": int(r["ts"] or 0),
        } for r in rows]

    def remove(self, qk: str) -> bool:
        """出队。"""
        cursor = self._db.execute("DELETE FROM retry_queue WHERE qk=?", (qk,))
        return bool(cursor and cursor.rowcount)

    def clear(self) -> int:
        """清空队列，返回删除条数。"""
        cursor = self._db.execute("DELETE FROM retry_queue")
        return int(cursor.rowcount or 0) if cursor else 0

    def count(self) -> int:
        """队列长度。"""
        row = self._db.query_one("SELECT COUNT(*) AS n FROM retry_queue")
        return int(row["n"]) if row else 0


# ---------------------------------------------------------------- RawLibrary（素材库 + 刮削游标）

#: 素材库刮削状态常量
SCRAPE_PENDING: int = 0      # 未刮削
SCRAPE_DONE: int = 1         # 已刮削（Tier A，达到入库门槛或确认 miss）
SCRAPE_FULL: int = 2         # 已补全（Tier B 深度字段）
STATUS_SCRAPE_HIT: str = "hit"
STATUS_SCRAPE_MISS: str = "miss"              # 全源真未命中（soft/hard miss 已缓存）
STATUS_SCRAPE_RETRYABLE: str = "retryable"
STATUS_SCRAPE_DISCARD: str = "category_discard"


class RawLibrary:
    """阶段 0 素材库 + P4 刮削游标（§7.2 raw_library 表）。

    职责：CoarseEntity 入库存量 → 按（游标 FIFO / 热通道优先）取批次 →
    事务内批量推进 `scraped` 状态，保证崩溃不丢不重。

    - 冷通道：`pending_batch(quota)` 取 `scraped=0 AND attempts<max`，
      `ORDER BY (first_seen / weight) ASC`（weight 作除数实现 variety ×2 优先）。
    - 热通道：`hot_batch(days)` 取 `scraped=0 AND is_new=1`（近 N 天 update_time）。
    - 幂等：`mark_scraped` 只按 merge_key 集合 UPDATE，同一 key 不会两轮重复取出。
    - 重试：`mark_retryable` 保持 scraped=0 且 attempts+1，下轮自然再取；
      attempts 达上限后 `pending_batch` 不再返回（降级进 unmatched 由编排层处理）。
    """

    def __init__(self, db: Any) -> None:
        self._db: CacheDB = db if isinstance(db, CacheDB) else CacheDB(db)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- 入库

    @staticmethod
    def merge_key_of(category: str, norm_title: str, seq: Any = 1) -> str:
        """构造素材库主键（与 §P3 merge_key 一致：category|norm_title|seq）。"""
        seq_text = "1" if seq in (None, "", 0) else str(seq)
        return f"{category}|{norm_title}|{seq_text}"

    def enqueue_many(self, entities: Iterable[Dict[str, Any]],
                     hot_window_days: float = 7.0) -> int:
        """批量入库（UPSERT；同 merge_key 已存在则刷新 last_updated / payload）。

        **已刮削条目的产物保护**（T04/T05 修复）：
        - 已刮削（scraped>0）且非热更新 → 只刷 last_updated / is_new，
          保留 P4 写回的富化 payload（否则每轮 harvest 会把 EnrichedEntity
          覆盖回 CoarseEntity，P5 读不到封面/简介 → 全线 unmatched）；
        - 已刮削但属热窗口新更新（is_new=1）→ payload 用新粗实体并重置
          scraped=0 / scrape_status / confidence / attempts，下一轮触发再刮削；
        - 未刮削 / 新条目 → 正常 UPSERT payload。

        Args:
            entities: CoarseEntity dict 列表（含 merge_key / category / norm_title /
                seq / year / primary / siblings / line_count / lines）。
            hot_window_days: 热通道判定窗口（update_time 距今 ≤ N 天 → is_new=1）。

        Returns:
            新增条数（不含覆盖）。
        """
        insert_rows: List[Tuple[Any, ...]] = []
        plain_updates: List[Tuple[Any, ...]] = []     # 未刮削 → 刷 payload
        protect_updates: List[Tuple[Any, ...]] = []   # 已刮削非热更新 → 保留 payload
        rescrape_updates: List[Tuple[Any, ...]] = []  # 已刮削热更新 → 重置再刮
        now = _now()
        cutoff = now - int(float(hot_window_days) * _DAY)
        inserted = 0
        with self._lock:
            for ent in entities:
                mk = str(ent.get("merge_key") or "")
                if not mk:
                    continue
                last_updated = _safe_int(str(ent.get("last_updated") or ent.get(
                    "update_time") or ""), 0)
                if last_updated <= 0:
                    last_updated = 0  # 无时间信号 → 走冷通道（不判热）
                weight = float(ent.get("weight", 1.0) or 1.0)
                is_new = 1 if last_updated >= cutoff else 0
                payload = _dumps(ent)
                existing = self._db.query_one(
                    "SELECT scraped FROM raw_library WHERE merge_key=?", (mk,))
                if existing is None:
                    insert_rows.append((
                        mk, str(ent.get("category") or ""),
                        str(ent.get("norm_title") or ""),
                        _safe_int(ent.get("seq"), 1) or 1, str(ent.get("year") or ""),
                        payload, now, last_updated, 0, None, None, 0, weight, is_new,
                    ))
                    inserted += 1
                else:
                    old_scraped = int(existing["scraped"] or 0)
                    if old_scraped > 0 and is_new:
                        # 热窗口新更新：payload 换新 + 重置刮削状态 → 下轮再刮削
                        rescrape_updates.append((payload, last_updated, is_new, mk))
                    elif old_scraped > 0:
                        # 已刮削且非热更新：保留富化 payload，只刷时间信号
                        protect_updates.append((last_updated, is_new, mk))
                    else:
                        plain_updates.append((payload, last_updated, is_new, mk))
            if insert_rows:
                self._db.executemany(
                    "INSERT INTO raw_library(merge_key, category, norm_title, seq, year, "
                    "payload_json, first_seen, last_updated, scraped, scrape_status, "
                    "confidence, attempts, weight, is_new) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    insert_rows)
            if protect_updates:
                self._db.executemany(
                    "UPDATE raw_library SET last_updated=?, is_new=? WHERE merge_key=?",
                    protect_updates)
            if rescrape_updates:
                self._db.executemany(
                    "UPDATE raw_library SET payload_json=?, last_updated=?, is_new=?, "
                    "scraped=0, scrape_status=NULL, confidence=NULL, attempts=0 "
                    "WHERE merge_key=?",
                    rescrape_updates)
            if plain_updates:
                self._db.executemany(
                    "UPDATE raw_library SET payload_json=?, last_updated=?, is_new=? "
                    "WHERE merge_key=?",
                    plain_updates)
        return inserted

    # ---------------------------------------------------------- 取批次（游标）

    def pending_batch(self, quota: int, max_attempts: int = 3,
                      include_weight: bool = True) -> List[Dict[str, Any]]:
        """冷通道取未刮削批次（§7.2：weight 作除数让 variety 优先）。"""
        quota = max(int(quota), 0)
        if quota <= 0:
            return []
        order = "(first_seen / weight) ASC" if include_weight else "first_seen ASC"
        with self._lock:
            rows = self._db.query_all(
                f"SELECT payload_json FROM raw_library "
                f"WHERE scraped={SCRAPE_PENDING} AND attempts<{int(max_attempts)} "
                f"ORDER BY {order} LIMIT ?", (quota,))
        return [_loads(r["payload_json"], None) for r in rows if r["payload_json"]]

    def pending_batch_keys(self, quota: int, max_attempts: int = 3,
                           include_weight: bool = True) -> List[str]:
        """冷通道取批次的主键列表（供 mark_* 事务推进）。"""
        quota = max(int(quota), 0)
        if quota <= 0:
            return []
        order = "(first_seen / weight) ASC" if include_weight else "first_seen ASC"
        with self._lock:
            rows = self._db.query_all(
                f"SELECT merge_key FROM raw_library "
                f"WHERE scraped={SCRAPE_PENDING} AND attempts<{int(max_attempts)} "
                f"ORDER BY {order} LIMIT ?", (quota,))
        return [str(r["merge_key"]) for r in rows]

    def hot_batch(self, hot_window_days: float = 7.0,
                  limit: int = 500) -> List[Dict[str, Any]]:
        """热通道：近 N 天 update_time 的未刮削条目（§7.1 P0，A+B 全字段）。"""
        cutoff = _now() - int(float(hot_window_days) * _DAY)
        rows = self._db.query_all(
            "SELECT payload_json FROM raw_library "
            "WHERE scraped=? AND is_new=1 ORDER BY last_updated DESC LIMIT ?",
            (SCRAPE_PENDING, max(int(limit), 1)))
        return [_loads(r["payload_json"], None) for r in rows if r["payload_json"]]

    def get(self, merge_key: str) -> Optional[Dict[str, Any]]:
        """按主键取条目（解析后的 CoarseEntity）。"""
        row = self._db.query_one(
            "SELECT payload_json, scraped, scrape_status, confidence, attempts "
            "FROM raw_library WHERE merge_key=?", (merge_key,))
        if row is None:
            return None
        ent = _loads(row["payload_json"], None) or {}
        ent["_library"] = {
            "merge_key": merge_key,
            "scraped": int(row["scraped"] or 0),
            "scrape_status": str(row["scrape_status"] or ""),
            "confidence": row["confidence"],
            "attempts": int(row["attempts"] or 0),
        }
        return ent

    # ---------------------------------------------------------- 游标推进（事务）

    def _advance(self, keys: Iterable[str], scraped: int, status: str,
                 confidence: Any = None, bump_attempts: bool = False,
                 payloads: Optional[Dict[str, Any]] = None) -> int:
        """批量更新 scraped / scrape_status / confidence / attempts（可选 payload）。

        retryable（bump_attempts=True）时 scraped 保持 0，attempts+1（§7.2 重试回写）。
        payloads: {merge_key: EnrichedEntity}——刮削命中时把富化结果写回
        raw_library.payload_json，供 P5 `_load_scraped_hits` 直读（T04/T05 修复）。
        """
        key_list = [k for k in (keys or []) if k]
        if not key_list:
            return 0
        attempt_sql = "attempts = attempts + 1" if bump_attempts else "attempts = attempts"
        if payloads is not None:
            sql = ("UPDATE raw_library SET scraped=?, scrape_status=?, confidence=?, "
                   f"payload_json=COALESCE(?, payload_json), {attempt_sql} WHERE merge_key=?")
            rows = [(int(scraped), str(status),
                     None if confidence is None else int(confidence),
                     _dumps(payloads.get(k)) if isinstance(payloads.get(k), dict) else None,
                     k) for k in key_list]
        else:
            sql = (f"UPDATE raw_library SET scraped=?, scrape_status=?, confidence=?, "
                   f"{attempt_sql} WHERE merge_key=?")
            rows = [(int(scraped), str(status),
                     None if confidence is None else int(confidence), k) for k in key_list]
        with self._lock:
            cur = self._db.executemany(sql, rows)
            return int(cur or 0)

    def mark_scraped(self, keys: Iterable[str], status: str = STATUS_SCRAPE_HIT,
                     confidence: Optional[int] = None,
                     backfilled: bool = False,
                     payloads: Optional[Dict[str, Any]] = None) -> int:
        """Tier A 完成：置 scraped=1（或 2，backfilled=Tier B 补全）。

        payloads: 可选——{merge_key: EnrichedEntity}，命中的富化结果
        一并写回素材库 payload（P5 直读源）。
        """
        return self._advance(keys, SCRAPE_FULL if backfilled else SCRAPE_DONE,
                             status, confidence, payloads=payloads)

    def mark_retryable(self, keys: Iterable[str], status: str = STATUS_SCRAPE_RETRYABLE,
                       confidence: Optional[int] = None) -> int:
        """限流/不可达：scraped 保持 0、attempts+1，下一轮自然重试（§7.2）。"""
        return self._advance(keys, SCRAPE_PENDING, status, confidence, bump_attempts=True)

    # ---------------------------------------------------------- 统计

    def stats(self) -> Dict[str, Any]:
        """素材库总量 / 各状态分布。"""
        rows = self._db.query_all(
            "SELECT scraped, COUNT(*) AS n FROM raw_library GROUP BY scraped")
        by_scraped = {int(r["scraped"]): int(r["n"]) for r in rows}
        by_status: Dict[str, int] = {}
        srows = self._db.query_all(
            "SELECT scrape_status, COUNT(*) AS n FROM raw_library "
            "WHERE scrape_status IS NOT NULL GROUP BY scrape_status")
        for r in srows:
            by_status[str(r["scrape_status"])] = int(r["n"])
        hot = self._db.query_one(
            "SELECT COUNT(*) AS n FROM raw_library WHERE scraped=? AND is_new=1",
            (SCRAPE_PENDING,))
        return {
            "total": sum(by_scraped.values()),
            "pending": by_scraped.get(SCRAPE_PENDING, 0),
            "done": by_scraped.get(SCRAPE_DONE, 0),
            "full": by_scraped.get(SCRAPE_FULL, 0),
            "hot_pending": int(hot["n"]) if hot else 0,
            "by_status": by_status,
        }

    def pending_count(self, max_attempts: int = 3) -> int:
        """可被冷通道取出的未刮削条数。"""
        row = self._db.query_one(
            "SELECT COUNT(*) AS n FROM raw_library "
            f"WHERE scraped={SCRAPE_PENDING} AND attempts<{int(max_attempts)}")
        return int(row["n"]) if row else 0

    def categories(self) -> Dict[str, int]:
        """按类目统计（报表用）。"""
        rows = self._db.query_all(
            "SELECT category, COUNT(*) AS n FROM raw_library GROUP BY category")
        return {str(r["category"]): int(r["n"]) for r in rows}
