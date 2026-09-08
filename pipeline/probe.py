# -*- coding: utf-8 -*-
"""pipeline/probe.py —— 域名级探活（设计文档 §2 P2 / T03，重构自根目录 m3u8_checker.py）

规则（§2 P2 已拍板）：

| 域名状态 | 行为 | 请求数 |
|---|---|---|
| 新域名（首次出现） | 探 1 次：HEAD timeout=3s；403/405 降级 GET | 1 |
| alive | 后续线路直接放行，不再测 | 0 |
| dead | 后续线路直接丢弃 | 0 |
| dead + 冷却 15min | 半开：允许 1 次探测，成功转 alive | ≤1 |

核心提速：449,935 条线路只分布在 147 个 CDN 域名（Top3 各占 13.3%），
域名级判定把旧实现「每条线路首集串行 HEAD（~70 分钟）」压缩到「每轮仅新域名探测（<1 分钟）」。

实现要点：
- **单轮内同一域名只探测一次**（in-run memo）：域名一旦判定，本轮其余线路 0 请求。
  探测失败也只记 1 次 fail_count（跨轮累计满 4 次才转 dead，§2 B4 / Registry 状态机）。
- **状态持久化**：core.cache.DomainRegistry（SQLite），重启不重探已 alive 的域名。
- **dead 判定**：连续失败 fail_threshold(4) 次（Registry 负责）；dead 后冷却
  half_open_cooldown(900s) 到期才允许半开探测 1 次。
- **双接口**：`filter_lines`（async，采集管线用）与 `filter_lines_sync`（sync，
  cleanup.py 每 15 天全量复检用），核心判定逻辑唯一（`_decide` 一族）。

分层注意：本模块属于管道层，只依赖 core / 标准库，不 import crawlers / sources。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlparse

from core.cache import DOMAIN_ALIVE, CacheDB, DomainRegistry, cache_db_path as _cache_db_path
from core.config import DEFAULT_PROBE, Settings, load_settings
from core.http import AsyncHttpClient, FetchResult, HttpClient
from core.logging import log_event

__all__ = [
    "DomainProber",
    "parse_domain",
    "extract_representative_url",
    "is_probeable_url",
]

#: HEAD 视为可达的状态码（含重定向中间态；httpx 默认跟随重定向，最终 200/206）
_PROBE_OK_HEAD: frozenset = frozenset({200, 206, 301, 302, 307, 308})
#: GET 降级探测的可达状态码（403/405 后）
_PROBE_OK_GET: frozenset = frozenset({200, 206})


def parse_domain(url: str) -> str:
    """从 URL 中提取小写域名（netloc）；非法 URL 返回空串。"""
    if not url:
        return ""
    try:
        return (urlparse(url).netloc or "").lower()
    except (ValueError, TypeError):  # pragma: no cover - 防御
        return ""


def extract_representative_url(line: Dict[str, Any]) -> str:
    """取一条线路的代表 URL（第一集）。"""
    episodes = line.get("episodes")
    if not isinstance(episodes, list) or not episodes:
        return ""
    first = episodes[0]
    url = first.get("url") if isinstance(first, dict) else None
    return str(url or "")


def is_probeable_url(url: str) -> bool:
    """校验 URL 是否可安全探测。

    源站 play_url 可能含脏数据（缺协议头、非法端口、分号拼接串等），
    httpx 解析时会抛 `InvalidURL` 直接打崩整轮任务；这里统一前置校验，
    非法 URL 一律视为不可探测（不请求、不崩）。
    """
    if not url:
        return False
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return False
    if parts.scheme.lower() not in ("http", "https"):
        return False
    # 注意：urlsplit().port 是 property，端口非数字时访问它会直接抛
    # ValueError（例: `http://x.com:8080;dn1.com`），必须单独保护。
    try:
        if parts.port is None:
            return True
        return int(parts.port) > 0
    except ValueError:
        return False


class DomainProber:
    """域名级探活器。

    Args:
        registry: DomainRegistry 实例；也可传 CacheDB / 路径 / None（惰性建默认库）。
        settings: Settings 实例；None 时惰性加载（probe 段配置）。
        timeout_head: HEAD 探测超时（秒），默认 3。
        timeout_get: GET 降级探测超时（秒），默认 5。
        probes_per_run: 只读统计，本轮实际发起探测的次数（验收标准③用）。
    """

    def __init__(self, registry: Optional[Any] = None,
                 settings: Optional[Settings] = None,
                 timeout_head: Optional[float] = None,
                 timeout_get: Optional[float] = None) -> None:
        self._settings: Settings = settings if settings is not None else load_settings()
        probe_cfg = self._settings.section("probe") or dict(DEFAULT_PROBE)
        self.timeout_head: float = float(
            timeout_head if timeout_head is not None
            else probe_cfg.get("timeout_head", DEFAULT_PROBE["timeout_head"]))
        self.timeout_get: float = float(
            timeout_get if timeout_get is not None
            else probe_cfg.get("timeout_get", DEFAULT_PROBE["timeout_get"]))
        self._registry: DomainRegistry = self._resolve_registry(registry)
        self._memo: Dict[str, bool] = {}
        self._memo_lock = threading.RLock()
        self.probes_per_run: int = 0

    # ---------------------------------------------------------- 构造

    def _resolve_registry(self, registry: Optional[Any]) -> DomainRegistry:
        """把任意 registry 参数归一成 DomainRegistry 实例。"""
        if isinstance(registry, DomainRegistry):
            return registry
        if registry is None:
            path = _cache_db_path()
            return DomainRegistry(CacheDB(path))
        if isinstance(registry, CacheDB):
            return DomainRegistry(registry)
        if isinstance(registry, str):  # 路径
            return DomainRegistry(CacheDB(registry))
        raise TypeError(f"无法识别的 registry 参数: {type(registry)!r}")

    @property
    def registry(self) -> DomainRegistry:
        """底层域名注册表（状态查询 / 15 天复检列出）。"""
        return self._registry

    # ---------------------------------------------------------- 状态查询

    def state_of(self, domain: str) -> str:
        """域名状态：alive / dead / unknown。"""
        return self._registry.state_of(domain)

    def stats(self) -> Dict[str, int]:
        """各状态域名数。"""
        return self._registry.stats()

    # ---------------------------------------------------------- 探测

    async def probe_domain(self, client: AsyncHttpClient, domain: str, url: str) -> bool:
        """对单个域名发起 1 次探测（HEAD → 403/405 降级 GET）。

        Args:
            client: httpx 异步客户端（鸭子类型：需实现 `request()` 返回 FetchResult）。
            domain: 目标域名（仅写日志用）。
            url: 探测代表 URL。

        Returns:
            True=可达。探测结果已回写 Registry（mark_ok / mark_fail）。
        """
        ok = await self._probe_async(client, url)
        self.probes_per_run += 1
        if ok:
            self._registry.mark_ok(domain)
        else:
            self._registry.mark_fail(domain)
        log_event("probe.done", "INFO", None, domain=domain, ok=ok,
                  probes=self.probes_per_run)
        return ok

    async def _probe_async(self, client: AsyncHttpClient, url: str) -> bool:
        """async 单 URL 探测：HEAD（3s）→ 403/405 时 GET（5s）。"""
        result: FetchResult = await client.request(
            "HEAD", url, timeout=self.timeout_head, retries=1, as_json=False, sleep=True)
        if result.status_code in _PROBE_OK_HEAD:
            return True
        if result.status_code in (403, 405):
            result = await client.request(
                "GET", url, timeout=self.timeout_get, retries=1, as_json=False, sleep=True)
            return result.status_code in _PROBE_OK_GET
        return False

    def probe_domain_sync(self, http: HttpClient, domain: str, url: str) -> bool:
        """sync 版本（cleanup.py 15 天复检用）。"""
        # 脏 URL 前置拦截：不请求、不计入探测次数（统计口径与批量路径一致）
        if not is_probeable_url(url):
            return False
        ok = self._probe_sync(http, url)
        self.probes_per_run += 1
        if ok:
            self._registry.mark_ok(domain)
        else:
            self._registry.mark_fail(domain)
        return ok

    def _probe_sync(self, http: HttpClient, url: str) -> bool:
        """sync 单 URL 探测。"""
        if not is_probeable_url(url):  # 脏 URL 前置拦截，避免 httpx.InvalidURL 打崩整轮
            return False
        result: FetchResult = http.request(
            "HEAD", url, timeout=self.timeout_head, retries=1, as_json=False, sleep=True)
        if result.status_code in _PROBE_OK_HEAD:
            return True
        if result.status_code in (403, 405):
            result = http.request(
                "GET", url, timeout=self.timeout_get, retries=1, as_json=False, sleep=True)
            return result.status_code in _PROBE_OK_GET
        return False

    # ---------------------------------------------------------- 线路过滤

    async def _decide_async(self, client: AsyncHttpClient, domain: str, url: str) -> bool:
        """async 域名判定：需要探测则探一次，否则按注册表状态直接判定。"""
        try:
            if self._registry.should_probe(domain):
                return await self.probe_domain(client, domain, url)
            return self._registry.state_of(domain) == DOMAIN_ALIVE
        except Exception:  # pylint: disable=broad-except - 脏 URL / 瞬时 IO 异常不拖垮整轮
            return False

    def _decide_sync(self, http: HttpClient, domain: str, url: str) -> bool:
        """sync 域名判定。

        脏 URL（非法端口 / 协议头缺失 / 分号拼接串）会让 httpx 抛
        `InvalidURL` 并直接打崩整轮任务；这里统一兜底为"不可用"，
        只丢该条线路，不记录失败（非连通性证据，不应污染域名状态）。
        """
        try:
            if self._registry.should_probe(domain):
                return self.probe_domain_sync(http, domain, url)
            return self._registry.state_of(domain) == DOMAIN_ALIVE
        except Exception:  # pylint: disable=broad-except - 脏 URL / 瞬时 IO 异常不拖垮整轮
            return False

    async def filter_lines(self, client: AsyncHttpClient, lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """async 域名级线路过滤：保留有效线路，返回过滤后的 lines。

        单轮内同一域名只判定一次（in-run memo），其余线路 0 请求。
        settings.enable_m3u8_check=False 时原样返回（兼容旧开关）。
        """
        if not self._settings.get("enable_m3u8_check", True):
            return lines
        with self._memo_lock:
            self._memo.clear()
            self.probes_per_run = 0
        return await self._filter_lines_async(client, lines)

    async def _filter_lines_async(self, client: AsyncHttpClient,
                                  lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """async 过滤实现（共享 memo）。"""
        kept: List[Dict[str, Any]] = []
        for line in lines or []:
            if not isinstance(line, dict):
                continue
            url = extract_representative_url(line)
            if not is_probeable_url(url):
                continue  # 脏 URL：不请求、不崩（httpx 解析非法端口会抛 InvalidURL）
            domain = parse_domain(url)
            if not domain:
                continue  # 无有效代表 URL 的线路直接弃
            with self._memo_lock:
                decision = self._memo.get(domain)
            if decision is None:
                decision = await self._decide_async(client, domain, url)
                with self._memo_lock:
                    self._memo[domain] = decision
            if decision:
                kept.append(line)
        return kept

    def filter_lines_sync(self, http: HttpClient,
                          lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """sync 域名级线路过滤（cleanup.py 等同步场景，单条目）。

        单条目调用会重置 memo；多条目请改用 `filter_items_sync`
        （memo 跨条目复用做域名去重，避免数十万条目逐条清空重探）。
        """
        if not self._settings.get("enable_m3u8_check", True):
            return lines
        with self._memo_lock:
            self._memo.clear()
            self.probes_per_run = 0
        return self._filter_lines_sync(http, lines)

    def filter_items_sync(self, http: HttpClient,
                          items: List[Dict[str, Any]],
                          progress_every: int = 50000) -> List[Dict[str, Any]]:
        """sync 批量线路过滤（单轮多条目，P2.5 探活主线入口）。

        与 `filter_lines_sync` 的区别：**memo 只清一次、跨条目复用**——
        全量建库放行约 65 万条目，域名级去重后实际只探测上百个域名；
        若逐条目调用 `filter_lines_sync` 会每次清空 memo 导致域名重复
        探测，几十万次网络调用会卡住数十分钟。
        """
        if not self._settings.get("enable_m3u8_check", True):
            return items
        with self._memo_lock:
            self._memo.clear()
            self.probes_per_run = 0
        t0 = time.time()
        total = len(items or [])
        every = max(int(progress_every or 0), 0)
        kept: List[Dict[str, Any]] = []
        for idx, item in enumerate(items or [], 1):
            if not isinstance(item, dict):
                continue
            lines = item.get("lines") or []
            if lines:
                item["lines"] = self._filter_lines_sync(http, lines)
            kept.append(item)
            if every and idx % every == 0:
                log_event("probe.items_progress", "INFO", None,
                          done=idx, total=total,
                          probes=self.probes_per_run,
                          domains=len(self._memo),
                          elapsed=round(time.time() - t0, 1),
                          message=(
                              f"[P2] 线路探活 已处理 {idx}/{total} 条 | "
                              f"本轮探测 {self.probes_per_run} 个域名 | "
                              f"用时{(time.time() - t0) / 60.0:.1f}分钟"))
        log_event("probe.items_done", "INFO", None,
                  items=total, probes=self.probes_per_run,
                  domains=len(self._memo), elapsed=round(time.time() - t0, 1),
                  message=(f"[P2] 线路探活完成：处理 {total} 条 | "
                           f"本轮探测 {self.probes_per_run} 个域名 | "
                           f"用时{(time.time() - t0) / 60.0:.1f}分钟"))
        return kept

    def _filter_lines_sync(self, http: HttpClient,
                           lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """sync 单条目线路过滤（memo 跨条目复用，重置由调用方负责）。"""
        kept: List[Dict[str, Any]] = []
        for line in lines or []:
            if not isinstance(line, dict):
                continue
            url = extract_representative_url(line)
            if not is_probeable_url(url):
                continue  # 脏 URL：不请求、不崩（httpx 解析非法端口会抛 InvalidURL）
            domain = parse_domain(url)
            if not domain:
                continue
            with self._memo_lock:
                decision = self._memo.get(domain)
            if decision is None:
                decision = self._decide_sync(http, domain, url)
                with self._memo_lock:
                    self._memo[domain] = decision
            if decision:
                kept.append(line)
        return kept