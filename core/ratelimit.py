# -*- coding: utf-8 -*-
"""令牌桶限速：per-source 独立限速 + 429 熔断（设计文档 §1.1 / §8.4 闸③）。

设计要点：
- **稳态严格等于 rate**：`TokenBucket.capacity` 默认 1.0（不预支突发令牌），
  因此 4 rps 的桶在任意 10 秒窗口内稳定放行 ~40 次，不会像"容量=速率"那样
  在首秒多放出一批把实测值顶到 44。需要突发时显式传 `capacity`。
- **精确等待**：用 `threading.Condition.wait(剩余秒数)` 精确睡眠，并在
  penalize / pause 时 `notify_all` 唤醒全部等待者重算，避免"睡过头"导致
  吞吐低于目标（实测 4 rps × 10s ∈ [38, 42]）。
- **线程安全**：令牌增减与状态变更全部在锁内；等待期间释放锁。
- **penalize/pause**：429 时把速率减半 + 暂停派发，`error_rate()` 供
  MetaCache 的负缓存闸门（§8.4 闸②）判定健康度。

用法：
    limiter = RateLimiter.from_settings(load_settings())
    limiter.acquire("tmdb")            # 阻塞直到拿到令牌
    limiter.on_429("tmdb", retry_after=2.0)
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

from .config import DEFAULT_RATE_LIMITS, Settings, load_settings

__all__ = ["TokenBucket", "RateLimiter", "RateLimiterStats", "DEFAULT_RATES"]

#: per-source 默认限速（req/s），与设计文档 §13 `rate_limits` 一致
DEFAULT_RATES: Dict[str, float] = dict(DEFAULT_RATE_LIMITS)

#: 429 默认惩罚系数与暂停时长（§8.4 闸③）
DEFAULT_PENALTY_FACTOR: float = 0.5
DEFAULT_PAUSE_SECONDS: float = 30.0
#: 错误率滚动窗口（秒）
DEFAULT_WINDOW_SECONDS: float = 300.0
#: 滚动窗口最大样本数（防止无限增长）
MAX_WINDOW_SAMPLES: int = 10000


class TokenBucket:
    """单源令牌桶。

    Args:
        rate: 令牌生成速率（个/秒），即目标 rps。
        capacity: 桶容量（突发上限）。默认 1.0 —— 不预支突发，保证稳态吞吐
            严格等于 rate。设为 >1 可允许短时突发，但会抬高短窗口实测值。
        name: 来源名，仅用于日志/调试。

    Attributes:
        requests: 累计放行的令牌次数。
    """

    def __init__(self, rate: float, capacity: Optional[float] = None,
                 name: str = "default") -> None:
        if rate <= 0:
            raise ValueError(f"rate 必须为正数，收到 {rate}")
        self.name: str = name
        self._base_rate: float = float(rate)
        self._capacity: float = 1.0 if capacity is None else float(capacity)
        if self._capacity <= 0:
            raise ValueError(f"capacity 必须为正数，收到 {self._capacity}")
        self._tokens: float = self._capacity      # 初始满桶（=1 个令牌）
        self._last: float = time.monotonic()
        self._cond = threading.Condition(threading.RLock())
        self._penalty_factor: float = 1.0
        self._penalty_until: float = 0.0
        self._paused_until: float = 0.0
        self.requests: int = 0
        self.penalties: int = 0

    # ---------------------------------------------------------- 属性

    @property
    def base_rate(self) -> float:
        """基准速率（不含惩罚）。"""
        return self._base_rate

    @property
    def capacity(self) -> float:
        """桶容量。"""
        return self._capacity

    @property
    def tokens(self) -> float:
        """当前可用令牌数（按当前时间补齐后）。"""
        with self._cond:
            self._refill(time.monotonic())
            return self._tokens

    @property
    def rate(self) -> float:
        """当前有效速率（含惩罚系数）。"""
        with self._cond:
            return self._rate_at(time.monotonic())

    @property
    def penalty_active(self) -> bool:
        """是否处于惩罚期（速率被压缩）。"""
        with self._cond:
            return time.monotonic() < self._penalty_until

    @property
    def paused(self) -> bool:
        """是否处于暂停期（429 后的派发暂停）。"""
        with self._cond:
            return time.monotonic() < self._paused_until

    def remaining_pause(self) -> float:
        """剩余暂停秒数（未暂停返回 0）。"""
        with self._cond:
            return max(0.0, self._paused_until - time.monotonic())

    # ---------------------------------------------------------- 内部

    def _rate_at(self, now: float) -> float:
        """按时间取有效速率（调用方持锁）。"""
        if now < self._penalty_until:
            return max(self._base_rate * self._penalty_factor, 0.01)
        return self._base_rate

    def _refill(self, now: float) -> None:
        """按当前有效速率补齐令牌（调用方持锁）。"""
        elapsed = now - self._last
        if elapsed <= 0:
            return
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate_at(now))
        self._last = now

    # ---------------------------------------------------------- 取令牌

    def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> float:
        """阻塞取令牌，返回实际等待秒数。

        Args:
            tokens: 需要的令牌数（默认 1）。
            timeout: 最长等待秒数；超时抛 TimeoutError。None 表示无限等待。

        Returns:
            实际阻塞等待的秒数（用于统计被限速损耗的时间）。

        Raises:
            TimeoutError: 超过 timeout 仍未取到令牌。
            ValueError: tokens 超过桶容量（永远取不到）。
        """
        if tokens > self._capacity:
            raise ValueError(
                f"请求 {tokens} 个令牌超过桶容量 {self._capacity}（源 {self.name}），永远无法满足")
        start = time.monotonic()
        with self._cond:
            while True:
                now = time.monotonic()
                self._refill(now)
                if now < self._paused_until:
                    sleep_for = self._paused_until - now
                elif self._tokens >= tokens:
                    self._tokens -= tokens
                    self.requests += 1
                    return now - start
                else:
                    sleep_for = (tokens - self._tokens) / self._rate_at(now)
                if timeout is not None and (now - start) + sleep_for > timeout:
                    raise TimeoutError(
                        f"取令牌超时：源 {self.name} 需等待 {sleep_for:.3f}s，"
                        f"超过 timeout={timeout}s")
                self._cond.wait(sleep_for)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """非阻塞取令牌：成功 True，令牌不足或暂停中 False。"""
        if tokens > self._capacity:
            return False
        with self._cond:
            now = time.monotonic()
            self._refill(now)
            if now < self._paused_until:
                return False
            if self._tokens >= tokens:
                self._tokens -= tokens
                self.requests += 1
                return True
            return False

    # ---------------------------------------------------------- 惩罚 / 暂停

    def penalize(self, factor: float = DEFAULT_PENALTY_FACTOR,
                 seconds: float = DEFAULT_PAUSE_SECONDS) -> None:
        """压缩速率一段时长（429 时 rps 减半）。

        Args:
            factor: 速率乘数（0.5 = 减半）。
            seconds: 惩罚持续秒数。
        """
        with self._cond:
            now = time.monotonic()
            self._penalty_factor = max(float(factor), 0.01)
            self._penalty_until = now + max(float(seconds), 0.0)
            self.penalties += 1
            self._cond.notify_all()

    def pause(self, seconds: float = DEFAULT_PAUSE_SECONDS) -> None:
        """暂停派发一段时长（暂停期内的 acquire 全部阻塞）。"""
        with self._cond:
            now = time.monotonic()
            self._paused_until = max(self._paused_until, now + max(float(seconds), 0.0))
            self._cond.notify_all()

    def clear_penalty(self) -> None:
        """立即清除惩罚与暂停，并唤醒等待者。"""
        with self._cond:
            self._penalty_until = 0.0
            self._paused_until = 0.0
            self._penalty_factor = 1.0
            self._cond.notify_all()

    def reset(self) -> None:
        """重置为满桶并清空惩罚/统计。"""
        with self._cond:
            self._tokens = self._capacity
            self._last = time.monotonic()
            self._penalty_until = 0.0
            self._paused_until = 0.0
            self._penalty_factor = 1.0
            self.requests = 0
            self.penalties = 0
            self._cond.notify_all()

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return (f"TokenBucket(name={self.name!r}, rate={self._base_rate}, "
                f"capacity={self._capacity}, tokens={self.tokens:.2f}, "
                f"requests={self.requests})")


class RateLimiterStats:
    """限速统计快照（便于报表与测试断言）。"""

    __slots__ = ("source", "rate", "requests", "penalties", "error_rate", "samples")

    def __init__(self, source: str, rate: float, requests: int, penalties: int,
                 error_rate: float, samples: int) -> None:
        self.source: str = source
        self.rate: float = rate
        self.requests: int = requests
        self.penalties: int = penalties
        self.error_rate: float = error_rate
        self.samples: int = samples

    def to_dict(self) -> Dict[str, Any]:
        """转 dict。"""
        return {
            "source": self.source,
            "rate": self.rate,
            "requests": self.requests,
            "penalties": self.penalties,
            "error_rate": self.error_rate,
            "samples": self.samples,
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return (f"RateLimiterStats({self.source!r}, rate={self.rate}, "
                f"requests={self.requests}, error_rate={self.error_rate:.2%})")


class RateLimiter:
    """多源限速器：每个 source 一个 TokenBucket + 滚动错误率窗口。

    Args:
        rates: {source: rps}；缺失来源用 `default`（再缺失用 2.0）。
        default_rate: 未登记来源的兜底速率。
        window_seconds: 错误率滚动窗口（秒）。
        error_threshold: 错误率阈值（> 该值且样本足够即判定不健康）。
        min_samples: 判定所需最小样本数。
        penalty_factor: 429 时的速率乘数。
        pause_seconds: 429 时的默认暂停秒数。
    """

    def __init__(self, rates: Optional[Dict[str, float]] = None,
                 default_rate: float = 2.0,
                 window_seconds: float = DEFAULT_WINDOW_SECONDS,
                 error_threshold: float = 0.30,
                 min_samples: int = 50,
                 penalty_factor: float = DEFAULT_PENALTY_FACTOR,
                 pause_seconds: float = DEFAULT_PAUSE_SECONDS) -> None:
        self._rates: Dict[str, float] = dict(rates or DEFAULT_RATES)
        self._default_rate: float = float(default_rate)
        self._window_seconds: float = float(window_seconds)
        self._error_threshold: float = float(error_threshold)
        self._min_samples: int = int(min_samples)
        self._penalty_factor: float = float(penalty_factor)
        self._pause_seconds: float = float(pause_seconds)

        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = threading.RLock()
        # 滚动窗口：(时间戳, source, 是否错误)
        self._window: Deque[Tuple[float, str, bool]] = deque()
        self._errors_total: int = 0
        self._requests_total: int = 0

    # ---------------------------------------------------------- 构造

    @classmethod
    def from_settings(cls, settings: Optional[Settings] = None,
                      **kwargs: Any) -> "RateLimiter":
        """按 settings.rate_limits / neg_cache_guard 构造。"""
        cfg = settings if settings is not None else load_settings()
        rates = dict(cfg.rate_limits or DEFAULT_RATES)
        return cls(
            rates=rates,
            default_rate=float(rates.get("default", 2.0)),
            error_threshold=float(cfg.guard_value("error_rate_threshold") or 0.30),
            min_samples=int(cfg.guard_value("min_samples") or 50),
            **kwargs,
        )

    # ---------------------------------------------------------- 桶管理

    def bucket(self, source: str) -> TokenBucket:
        """取（或惰性创建）指定来源的令牌桶。"""
        with self._lock:
            bucket = self._buckets.get(source)
            if bucket is None:
                rate = float(self._rates.get(source, self._default_rate))
                bucket = TokenBucket(rate=rate, name=source)
                self._buckets[source] = bucket
            return bucket

    def rate_of(self, source: str) -> float:
        """指定来源的基准速率。"""
        with self._lock:
            return float(self._rates.get(source, self._default_rate))

    def set_rate(self, source: str, rate: float) -> None:
        """运行时调整某来源速率（已存在的桶同步更新）。"""
        with self._lock:
            self._rates[source] = float(rate)
            bucket = self._buckets.get(source)
            if bucket is not None:
                bucket._base_rate = float(rate)  # pylint: disable=protected-access

    # ---------------------------------------------------------- 取令牌

    def acquire(self, source: str, tokens: float = 1.0,
                timeout: Optional[float] = None) -> float:
        """按来源取令牌，返回等待秒数。"""
        return self.bucket(source).acquire(tokens=tokens, timeout=timeout)

    def try_acquire(self, source: str, tokens: float = 1.0) -> bool:
        """按来源非阻塞取令牌。"""
        return self.bucket(source).try_acquire(tokens=tokens)

    # ---------------------------------------------------------- 结果上报

    def record(self, source: str, ok: bool = True) -> None:
        """记录一次请求结果（用于错误率统计）。

        Args:
            source: 来源名。
            ok: True 成功（含正常 200 与业务空结果），False 可重试错误
                （429 / 5xx / 超时 / 连接失败 / 反爬）。
        """
        with self._lock:
            now = time.monotonic()
            self._window.append((now, source, not ok))
            self._requests_total += 1
            if not ok:
                self._errors_total += 1
            self._trim(now)

    def on_429(self, source: str, retry_after: Optional[float] = None) -> float:
        """429 处理：rps 减半 + 暂停派发，并计入错误率。

        Args:
            source: 来源名。
            retry_after: 服务端 Retry-After 秒数；为空用默认 pause_seconds。

        Returns:
            实际暂停的秒数。
        """
        seconds = float(retry_after) if retry_after and retry_after > 0 else self._pause_seconds
        self.bucket(source).penalize(self._penalty_factor, self._pause_seconds)
        self.bucket(source).pause(seconds)
        self.record(source, ok=False)
        return seconds

    def pause(self, source: str, seconds: Optional[float] = None) -> float:
        """手动暂停某来源派发。"""
        seconds = float(seconds) if seconds and seconds > 0 else self._pause_seconds
        self.bucket(source).pause(seconds)
        return seconds

    def clear_penalty(self, source: Optional[str] = None) -> None:
        """清除惩罚/暂停；source 为空时清除全部。"""
        with self._lock:
            if source:
                self.bucket(source).clear_penalty()
                return
            for bucket in self._buckets.values():
                bucket.clear_penalty()

    # ---------------------------------------------------------- 统计

    def _trim(self, now: Optional[float] = None) -> None:
        """裁剪滚动窗口（调用方持锁）。"""
        cutoff = (now or time.monotonic()) - self._window_seconds
        window = self._window
        while window and window[0][0] < cutoff:
            window.popleft()
        while len(window) > MAX_WINDOW_SAMPLES:
            window.popleft()

    def _counts(self, source: Optional[str] = None) -> Tuple[int, int]:
        """窗口内 (样本数, 错误数)（调用方持锁）。"""
        now = time.monotonic()
        self._trim(now)
        cutoff = now - self._window_seconds
        samples = 0
        errors = 0
        for ts, src, is_error in self._window:
            if ts < cutoff:
                continue
            if source is not None and src != source:
                continue
            samples += 1
            if is_error:
                errors += 1
        return samples, errors

    def samples(self, source: Optional[str] = None) -> int:
        """滚动窗口内样本数。"""
        with self._lock:
            return self._counts(source)[0]

    def error_rate(self, source: Optional[str] = None) -> float:
        """滚动窗口内错误率 [0,1]；无样本返回 0.0。"""
        with self._lock:
            samples, errors = self._counts(source)
            return (errors / samples) if samples else 0.0

    def healthy(self, source: Optional[str] = None) -> bool:
        """是否健康：错误率未超阈值 或 样本数不足（不判定）。"""
        with self._lock:
            samples, errors = self._counts(source)
            if samples < self._min_samples:
                return True
            return (errors / samples) <= self._error_threshold

    def stats(self, source: Optional[str] = None) -> Dict[str, RateLimiterStats]:
        """各来源统计快照；source 指定时只返回该来源。"""
        with self._lock:
            sources = [source] if source else sorted(self._buckets.keys())
            out: Dict[str, RateLimiterStats] = {}
            for name in sources:
                bucket = self.bucket(name)
                samples, errors = self._counts(name)
                out[name] = RateLimiterStats(
                    source=name,
                    rate=self.rate_of(name),
                    requests=bucket.requests,
                    penalties=bucket.penalties,
                    error_rate=(errors / samples) if samples else 0.0,
                    samples=samples,
                )
            return out

    def reset_stats(self) -> None:
        """清空滚动窗口与累计计数（不重置桶内令牌）。"""
        with self._lock:
            self._window.clear()
            self._errors_total = 0
            self._requests_total = 0

    def reset_all(self) -> None:
        """重置全部桶（令牌、惩罚、统计）。"""
        with self._lock:
            for bucket in self._buckets.values():
                bucket.reset()
            self._window.clear()
            self._errors_total = 0
            self._requests_total = 0

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        with self._lock:
            return f"RateLimiter(sources={sorted(self._buckets.keys())}, rates={self._rates})"
