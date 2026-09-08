# -*- coding: utf-8 -*-
"""pipeline/budget.py —— 30 分钟预算分配（设计文档 §2 P6 / §7 / T05）

30 分钟（1800s）硬上限：

```
T_BUDGET    = settings.run_budget_seconds          # 1800
T_CRAWL_MAX = 420      # P1 采集硬超时
T_PROBE_MAX = 90       # P2 探活硬超时
T_COARSE    = 60       # P3 粗合并硬超时
T_EXPORT    = 240      # P5+P6 预留
T_FINE      = 120      # P5 精合并预留
T_SAFETY    = 90       # 安全余量
T_SCRAPE_MAX= 1200     # P4 上限

t_spent = now() - t0
T_SCRAPE = clamp(T_BUDGET - t_spent - (T_EXPORT + T_FINE + T_SAFETY), 0, T_SCRAPE_MAX)
```

- 热通道优先：近 7 天 update_time 新 IP 走 A+B（2 req/条）；
- 剩余预算全部给冷通道：素材库游标取未刮削条目（Tier A，1 req/条）；
- rps_effective = 源 rate_limit × 0.9（安全系数）；
- **预算重仲裁**：每消费 200 条复核一次 projected_end = t_spent + (剩余量/rps)，
  超过 deadline → 停止派发（硬超时保护）；
- 每轮结束产出 BudgetReport（t_spent / T_SCRAPE / 消耗 / 状态）。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.logging import event, log_event

__all__ = ["Budget", "BudgetReport", "DEFAULT_BUDGET", "SAFETY_FACTOR"]

#: 缺省预算分片（§7）
DEFAULT_BUDGET: Dict[str, float] = {
    "crawl_max": 420.0,
    "probe_max": 90.0,
    "coarse_max": 60.0,
    "fine_max": 120.0,
    "export_max": 240.0,
    "safety": 90.0,
    "scrape_max": 1200.0,
}

#: 有效速率安全系数（§7：rps_effective = rate × 0.9）
SAFETY_FACTOR: float = 0.9

#: 预算重仲裁间隔（条）
REBALANCE_EVERY: int = 200


@dataclass
class BudgetReport:
    """一轮预算执行报告。"""

    t0: float = 0.0
    elapsed: float = 0.0
    t_crawl: float = 0.0
    t_probe: float = 0.0
    t_coarse: float = 0.0
    t_scrape: float = 0.0
    t_export: float = 0.0
    t_total: float = 0.0
    scrape_budget: float = 0.0
    items_scraped: int = 0
    items_consumed: int = 0
    status: str = "ok"            # ok / timeout
    limits: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "t0": self.t0, "elapsed": round(self.elapsed, 2),
            "t_crawl": round(self.t_crawl, 2),
            "t_probe": round(self.t_probe, 2),
            "t_coarse": round(self.t_coarse, 2),
            "t_scrape": round(self.t_scrape, 2),
            "t_export": round(self.t_export, 2),
            "t_total": round(self.t_total, 2),
            "scrape_budget": round(self.scrape_budget, 2),
            "items_scraped": self.items_scraped,
            "items_consumed": self.items_consumed,
            "status": self.status,
            "limits": self.limits,
        }


class Budget:
    """预算分配器（线程安全）。

    Args:
        budget_seconds: 总预算（缺省读取 settings.run_budget_seconds，默认 1800）。
        limits: 阶段硬超时覆盖（settings.budget）。
        rate_limits: 各源速率（req/s，settings.rate_limits）。
    """

    def __init__(self, budget_seconds: Optional[float] = None,
                 limits: Optional[Dict[str, float]] = None,
                 rate_limits: Optional[Dict[str, Any]] = None) -> None:
        self._budget_total: float = float(
            budget_seconds if budget_seconds not in (None, 0) else 1800.0)
        self._limits: Dict[str, float] = dict(DEFAULT_BUDGET)
        if limits:
            self._limits.update({k: float(v) for k, v in limits.items()
                                 if v not in (None, "", 0)})
        self._rates: Dict[str, float] = {}
        for k, v in (rate_limits or {}).items():
            try:
                r = float(v)
                if r > 0:
                    self._rates[k] = r
            except (TypeError, ValueError):
                continue
        self._t0: float = time.time()
        self._phase_start: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._consumed: int = 0
        self._items_scraped: int = 0
        self._timeout: bool = False

    # ---------------------------------------------------------- 阶段计时

    def phase_start(self, name: str) -> None:
        with self._lock:
            self._phase_start[name] = time.time()

    def phase_done(self, name: str) -> float:
        """阶段结束，返回该阶段耗时秒。"""
        with self._lock:
            start = self._phase_start.pop(name, None)
            if start is None:
                start = self._t0
            return time.time() - start

    def phase_elapsed(self, name: str) -> float:
        with self._lock:
            start = self._phase_start.get(name)
        return (time.time() - start) if start else 0.0

    @property
    def elapsed(self) -> float:
        return time.time() - self._t0

    @property
    def deadline(self) -> float:
        return self._t0 + self._budget_total

    def remaining(self) -> float:
        return max(0.0, self.deadline - time.time())

    # ---------------------------------------------------------- 刮削预算

    def scrape_budget(self) -> float:
        """T_SCRAPE = clamp(total - spent - (export+fine+safety), 0, scrape_max)。"""
        spent = self.elapsed
        reserved = (self._limits["export_max"] + self._limits["fine_max"]
                    + self._limits["safety"])
        raw = self._budget_total - spent - reserved
        return max(0.0, min(raw, self._limits["scrape_max"]))

    def rate_effective(self, source: str = "tmdb") -> float:
        """rps_effective = rate × 0.9（§7）。"""
        rate = self._rates.get(source)
        if not rate:
            return 4.0 * SAFETY_FACTOR if source == "tmdb" else 1.0
        return rate * SAFETY_FACTOR

    # ---------------------------------------------------------- 预算仲裁

    def consume(self, n: int = 1) -> bool:
        """消费 N 个预算单位；返回值指示是否仍有余量。"""
        with self._lock:
            self._consumed += max(int(n), 1)
            over = self.elapsed >= self.deadline
            if over:
                self._timeout = True
            return not over

    def rebalance(self, backlog: int) -> bool:
        """预算重仲裁：判断是否还能安全消费 backlog 条目。

        projected_end = now + (backlog / rps_effective)；
        projected_end > deadline → 停止派发（返回 False）。
        """
        with self._lock:
            self._consumed += 1
            # 硬超时优先：已过 deadline 无论计数直接停
            if self.remaining() <= 0:
                self._timeout = True
                return False
            if self._consumed % REBALANCE_EVERY != 0:
                return not self._timeout
            rps = self.rate_effective()
            if rps <= 0:
                return not self._timeout
            projected = self.elapsed + (int(backlog) / rps)
            if projected > self._budget_total:
                self._timeout = True
                log_event("budget.timeout", "WARNING", None, projected=round(projected, 1),
                          budget=self._budget_total, backlog=int(backlog))
                return False
            return True

    def record_scraped(self, n: int = 1) -> None:
        with self._lock:
            self._items_scraped += max(int(n), 1)

    @property
    def items_scraped(self) -> int:
        return self._items_scraped

    @property
    def is_timeout(self) -> bool:
        return self._timeout

    # ---------------------------------------------------------- 报告

    def report(self, **phase_times: float) -> BudgetReport:
        rep = BudgetReport(
            t0=self._t0,
            elapsed=round(self.elapsed, 2),
            t_crawl=phase_times.get("crawl", 0.0),
            t_probe=phase_times.get("probe", 0.0),
            t_coarse=phase_times.get("coarse", 0.0),
            t_scrape=phase_times.get("scrape", 0.0),
            t_export=phase_times.get("export", 0.0),
            t_total=round(self.elapsed, 2),
        )
        rep.scrape_budget = round(self.scrape_budget(), 2)
        rep.items_scraped = self._items_scraped
        rep.items_consumed = self._consumed
        rep.status = "timeout" if self._timeout else "ok"
        rep.limits = {k: round(v, 1) for k, v in self._limits.items()}
        # t_total = crawl + probe + coarse + scrape + export（去重 elapsed）
        rep.t_total = round(rep.t_crawl + rep.t_probe + rep.t_coarse
                            + rep.t_scrape + rep.t_export, 2)
        return rep

    def to_dict(self) -> Dict[str, Any]:
        return self.report().to_dict()