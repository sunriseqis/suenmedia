# -*- coding: utf-8 -*-
"""core.ratelimit 单元测试：令牌桶速率实测、429 惩罚、错误率统计。"""
import threading
import time

import pytest

from core.ratelimit import RateLimiter, TokenBucket


# ---------------------------------------------------------------- 速率实测

def test_token_bucket_4rps_10s_in_range():
    """验收②：4 rps 令牌桶在 10 秒内的放行次数 ∈ [38, 42]。"""
    bucket = TokenBucket(rate=4.0)
    window = 10.0
    start = time.monotonic()
    end = start + window
    count = 0
    while time.monotonic() < end:
        bucket.acquire()
        count += 1
    elapsed = time.monotonic() - start
    measured = count / elapsed
    print(f"\n令牌桶实测：{count} 次 / {elapsed:.3f}s = {measured:.2f} req/s")
    assert 38 <= count <= 42, f"10 秒窗口放行 {count} 次，超出 [38,42]"
    assert 3.8 <= measured <= 4.3, f"实测速率 {measured:.2f} req/s"


def test_token_bucket_8rps_short_window():
    """8 rps 桶在 2 秒内放行约 16 次（±2）。"""
    bucket = TokenBucket(rate=8.0)
    end = time.monotonic() + 2.0
    count = 0
    while time.monotonic() < end:
        bucket.acquire()
        count += 1
    print(f"\n8rps 实测：{count} 次 / 2s")
    assert 14 <= count <= 18, f"8 rps × 2s 期望 ~16，实际 {count}"


def test_token_bucket_burst_capacity():
    """capacity>1 时允许突发，但总量仍受速率约束。"""
    bucket = TokenBucket(rate=4.0, capacity=4.0)
    # 满桶 4 个令牌可立即取走
    for _ in range(4):
        assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
    time.sleep(0.3)
    assert bucket.try_acquire() is True


def test_token_bucket_default_capacity_is_one():
    """默认 capacity=1：不预支突发，稳态严格等于 rate。"""
    bucket = TokenBucket(rate=4.0)
    assert bucket.capacity == 1.0
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_try_acquire_non_blocking():
    """try_acquire 不阻塞，令牌不足立即返回 False。"""
    bucket = TokenBucket(rate=1.0)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False


def test_acquire_timeout():
    """acquire 超时抛 TimeoutError。"""
    bucket = TokenBucket(rate=1.0)
    bucket.acquire()
    with pytest.raises(TimeoutError):
        bucket.acquire(timeout=0.1)


def test_acquire_more_than_capacity_raises():
    """请求令牌数超过桶容量 → 直接 ValueError（永远不会满足）。"""
    bucket = TokenBucket(rate=4.0, capacity=2.0)
    with pytest.raises(ValueError):
        bucket.acquire(tokens=3.0)


def test_invalid_rate():
    """rate 必须为正。"""
    with pytest.raises(ValueError):
        TokenBucket(rate=0)


# ---------------------------------------------------------------- 并发

def test_thread_safety_under_concurrency():
    """8 线程并发取令牌，总量仍受速率约束（不超发）。"""
    bucket = TokenBucket(rate=20.0)
    window = 1.0
    counter = {"n": 0}
    lock = threading.Lock()

    def worker(end_at: float) -> None:
        while time.monotonic() < end_at:
            bucket.acquire()
            with lock:
                counter["n"] += 1

    end = time.monotonic() + window
    threads = [threading.Thread(target=worker, args=(end,)) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"\n并发实测：{counter['n']} 次 / {window}s（目标 20）")
    assert 16 <= counter["n"] <= 26, f"并发下超发或欠发：{counter['n']}"


# ---------------------------------------------------------------- 惩罚 / 暂停

def test_penalize_halves_rate():
    """penalize(0.5) 后有效速率减半。"""
    bucket = TokenBucket(rate=4.0)
    assert bucket.rate == pytest.approx(4.0)
    bucket.penalize(factor=0.5, seconds=5)
    assert bucket.rate == pytest.approx(2.0)
    assert bucket.penalty_active is True
    bucket.clear_penalty()
    assert bucket.rate == pytest.approx(4.0)
    assert bucket.penalty_active is False


def test_pause_blocks_acquire():
    """pause 期间 acquire 被阻塞，到点自动放行。"""
    bucket = TokenBucket(rate=10.0)
    bucket.pause(0.3)
    assert bucket.paused is True
    assert bucket.try_acquire() is False
    start = time.monotonic()
    bucket.acquire()
    waited = time.monotonic() - start
    assert waited >= 0.25, f"暂停未生效，仅等待 {waited:.3f}s"


def test_penalty_wakes_waiters():
    """penalize 会唤醒等待中的线程重算（不睡过头）。"""
    bucket = TokenBucket(rate=1.0)
    bucket.acquire()  # 取空
    result = {"waited": 0.0}

    def waiter() -> None:
        bucket.acquire()
        result["waited"] = 1.0

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.05)
    bucket.clear_penalty()
    t.join(timeout=5)
    assert result["waited"] == 1.0


# ---------------------------------------------------------------- RateLimiter

def test_rate_limiter_per_source_rates():
    """per-source 独立限速：tmdb 4 / douban 3 / tvdb 8 / bilibili 3 / omdb 2。"""
    limiter = RateLimiter()
    assert limiter.rate_of("tmdb") == 4.0
    assert limiter.rate_of("douban") == 3.0
    assert limiter.rate_of("tvdb") == 8.0
    assert limiter.rate_of("bilibili") == 3.0
    assert limiter.rate_of("omdb") == 2.0
    assert limiter.rate_of("unknown_source") == 2.0   # default 兜底


def test_rate_limiter_acquire_per_source():
    """各来源的桶互相独立。"""
    limiter = RateLimiter()
    assert limiter.try_acquire("tmdb") is True
    assert limiter.try_acquire("tmdb") is False
    assert limiter.try_acquire("douban") is True      # 不同来源不受影响


def test_rate_limiter_on_429():
    """on_429：rps 减半 + 暂停派发，并计入错误率。"""
    limiter = RateLimiter()
    seconds = limiter.on_429("tmdb", retry_after=0.2)
    assert seconds == pytest.approx(0.2)
    bucket = limiter.bucket("tmdb")
    assert bucket.penalty_active is True
    assert bucket.paused is True
    assert bucket.rate == pytest.approx(2.0)          # 4 → 2
    assert bucket.penalties == 1
    assert limiter.error_rate("tmdb") == 1.0


def test_rate_limiter_error_rate_window():
    """滚动窗口错误率：60% 错误率被正确统计。"""
    limiter = RateLimiter(min_samples=10)
    for _ in range(4):
        limiter.record("tmdb", ok=True)
    for _ in range(6):
        limiter.record("tmdb", ok=False)
    assert limiter.samples("tmdb") == 10
    assert limiter.error_rate("tmdb") == pytest.approx(0.6)
    assert limiter.healthy("tmdb") is False


def test_rate_limiter_healthy_when_samples_insufficient():
    """样本不足 min_samples 时不判定不健康。"""
    limiter = RateLimiter(min_samples=50)
    for _ in range(10):
        limiter.record("tmdb", ok=False)
    assert limiter.error_rate("tmdb") == 1.0
    assert limiter.healthy("tmdb") is True


def test_rate_limiter_from_settings():
    """from_settings 使用 settings.rate_limits 与 neg_cache_guard。"""
    from core.config import Settings
    settings = Settings(
        rate_limits={"tmdb": 4.0, "douban": 3.0, "default": 2.0},
        neg_cache_guard={"error_rate_threshold": 0.30, "min_samples": 50},
    )
    limiter = RateLimiter.from_settings(settings)
    assert limiter.rate_of("tmdb") == 4.0
    assert limiter.rate_of("douban") == 3.0
    stats = limiter.stats("tmdb")["tmdb"]
    assert stats.rate == 4.0
    assert stats.to_dict()["source"] == "tmdb"


def test_rate_limiter_set_rate_runtime():
    """运行时调整速率。"""
    limiter = RateLimiter()
    limiter.set_rate("tmdb", 1.0)
    assert limiter.rate_of("tmdb") == 1.0
    assert limiter.bucket("tmdb").base_rate == 1.0


def test_rate_limiter_reset_all():
    """reset_all 清空桶与统计。"""
    limiter = RateLimiter()
    limiter.acquire("tmdb")
    limiter.record("tmdb", ok=False)
    limiter.reset_all()
    assert limiter.bucket("tmdb").requests == 0
    assert limiter.samples("tmdb") == 0
    assert limiter.error_rate("tmdb") == 0.0
