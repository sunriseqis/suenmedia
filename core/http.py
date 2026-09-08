# -*- coding: utf-8 -*-
"""统一 HTTP 客户端（httpx）：超时 / 重试 / 退避 / 429 与 Retry-After。

设计文档 §11 T01 / §12：全链路统一 httpx（async 采集 + sync 刮削），
避免 requests 与 httpx 两套栈并存。

三态结果（与 sources/base.py 的 hit/miss/retryable 对齐）：
- `ok`：2xx，data 为解析后的 JSON（或文本）；
- `retryable`：429 / 5xx / 超时 / 连接失败 / 反爬 —— 调用方应进 retry_queue，
  **绝不写负缓存**（§8.4 闸①）；
- `error`：其他 4xx 等业务性失败，重试无意义。

要点：
- 重试在**调用级**唯一实现（session/transport 层不再挂 Retry），避免双层重试放大；
- 退避：`backoff * 2**attempt` 并加抖动，上限 `backoff_max`；429 优先用 Retry-After
  （上限 `max_retry_after` 秒，防止服务端给个 3600 把整轮预算吃掉）；
- 可选接 `RateLimiter`：每次尝试前 `acquire(source)`，429 时自动 `on_429()`。
"""

from __future__ import annotations

import asyncio
import json as _json
import random
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import httpx

from .logging import get_logger, log_event

__all__ = [
    "HttpClient",
    "AsyncHttpClient",
    "FetchResult",
    "FetchStatus",
    "RETRYABLE_STATUS",
    "default_client",
    "get_json",
    "close_all",
]

_LOGGER = get_logger("http")

#: 默认 UA（与源站采集/刮削通用）
DEFAULT_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

#: 视为可重试的 HTTP 状态码
RETRYABLE_STATUS: frozenset = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509})

#: 默认重试 / 退避参数
DEFAULT_TIMEOUT: float = 12.0
DEFAULT_CONNECT_TIMEOUT: float = 5.0
DEFAULT_RETRIES: int = 3
DEFAULT_BACKOFF: float = 0.5
DEFAULT_BACKOFF_MAX: float = 8.0
DEFAULT_MAX_RETRY_AFTER: float = 30.0


class FetchStatus:
    """取回结果状态常量。"""

    OK: str = "ok"
    RETRYABLE: str = "retryable"
    ERROR: str = "error"


class FetchResult:
    """HTTP 取回结果（统一返回，不抛异常）。

    Attributes:
        status: ok / retryable / error。
        status_code: HTTP 状态码（0 表示未收到响应）。
        data: 解析后的 JSON（dict/list）或 None。
        text: 原始响应文本。
        error: 错误描述（成功时为空串）。
        retryable: 是否可重试。
        retry_after: 服务端建议的重试等待秒数（可空）。
        url: 最终请求 URL。
        attempts: 实际尝试次数。
    """

    __slots__ = ("status", "status_code", "data", "text", "error", "retryable",
                 "retry_after", "url", "attempts")

    def __init__(self, status: str, status_code: int = 0, data: Any = None,
                 text: str = "", error: str = "", retryable: bool = False,
                 retry_after: Optional[float] = None, url: str = "",
                 attempts: int = 0) -> None:
        self.status: str = status
        self.status_code: int = int(status_code or 0)
        self.data: Any = data
        self.text: str = text or ""
        self.error: str = error or ""
        self.retryable: bool = bool(retryable)
        self.retry_after: Optional[float] = retry_after
        self.url: str = url or ""
        self.attempts: int = int(attempts or 0)

    @property
    def ok(self) -> bool:
        """是否成功取回（2xx）。"""
        return self.status == FetchStatus.OK

    @property
    def is_retryable(self) -> bool:
        """是否可重试错误。"""
        return self.status == FetchStatus.RETRYABLE

    def to_dict(self) -> Dict[str, Any]:
        """转 dict（报表 / 日志用）。"""
        return {
            "status": self.status,
            "status_code": self.status_code,
            "error": self.error,
            "retryable": self.retryable,
            "retry_after": self.retry_after,
            "url": self.url,
            "attempts": self.attempts,
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return (f"FetchResult(status={self.status!r}, code={self.status_code}, "
                f"attempts={self.attempts}, error={self.error!r})")


# ---------------------------------------------------------------- 工具

def _parse_retry_after(response: Optional[httpx.Response]) -> Optional[float]:
    """解析 Retry-After 头（秒数或 HTTP 日期，仅处理秒数形式）。"""
    if response is None:
        return None
    raw = response.headers.get("Retry-After") or response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(float(str(raw).strip()), 0.0)
    except (TypeError, ValueError):
        return None


def _backoff_delay(attempt: int, backoff: float, backoff_max: float,
                   retry_after: Optional[float] = None,
                   max_retry_after: float = DEFAULT_MAX_RETRY_AFTER) -> float:
    """计算退避秒数。

    Args:
        attempt: 第几次重试（从 0 开始）。
        backoff: 退避基数。
        backoff_max: 退避上限（不含 Retry-After 场景）。
        retry_after: 服务端建议等待秒数；有效且未超上限时优先采用。
        max_retry_after: Retry-After 采纳上限。

    Returns:
        应睡眠的秒数。
    """
    if retry_after is not None and 0 <= retry_after <= max_retry_after:
        return float(retry_after)
    base = min(backoff * (2 ** max(attempt, 0)), backoff_max)
    return float(base) * (0.5 + random.random() / 2.0)  # 抖动 ±50%，避免重试风暴


def _build_timeout(timeout: Optional[float], connect_timeout: Optional[float]) -> httpx.Timeout:
    """构造 httpx.Timeout。"""
    total = float(timeout if timeout is not None else DEFAULT_TIMEOUT)
    connect = float(connect_timeout if connect_timeout is not None else DEFAULT_CONNECT_TIMEOUT)
    return httpx.Timeout(total, connect=min(connect, total))


def _client_kwargs(proxy: Optional[str], verify: bool, timeout: Optional[float],
                   connect_timeout: Optional[float], headers: Optional[Dict[str, str]],
                   follow_redirects: bool, http2: bool) -> Dict[str, Any]:
    """构造 httpx.Client / AsyncClient 公共 kwargs（兼容 0.27 / 0.28 代理参数差异）。"""
    kwargs: Dict[str, Any] = {
        "timeout": _build_timeout(timeout, connect_timeout),
        "verify": verify,
        "follow_redirects": follow_redirects,
        "http2": bool(http2),
        "headers": dict(DEFAULT_HEADERS),
    }
    if headers:
        kwargs["headers"].update(headers)
    if proxy:
        # httpx >= 0.28 用 proxy=，旧版用 proxies=，按可用性选择
        kwargs["proxy"] = proxy
    return kwargs


def _extract_data(response: httpx.Response, as_json: bool = True) -> Tuple[Any, str]:
    """从响应里取数据与文本。"""
    text = response.text or ""
    if not as_json:
        return text, text
    content_type = (response.headers.get("content-type") or "").lower()
    if "json" not in content_type and not text.lstrip().startswith(("{", "[")):
        return None, text
    try:
        return response.json(), text
    except ValueError:
        return None, text


def _classify(response: Optional[httpx.Response], exc: Optional[BaseException],
              url: str = "") -> FetchResult:
    """把响应/异常归类为 FetchResult。"""
    if exc is not None:
        retryable = isinstance(exc, (httpx.TimeoutException, httpx.NetworkError,
                                     httpx.RemoteProtocolError, httpx.ProxyError))
        return FetchResult(
            status=FetchStatus.RETRYABLE if retryable else FetchStatus.ERROR,
            status_code=0,
            error=f"{type(exc).__name__}: {exc}",
            retryable=retryable,
            url=url,
        )
    if response is None:  # pragma: no cover - 防御分支
        return FetchResult(status=FetchStatus.ERROR, error="no response", url=url)

    code = int(response.status_code)
    retry_after = _parse_retry_after(response) if code == 429 else None
    if 200 <= code < 300:
        data, text = _extract_data(response)
        return FetchResult(status=FetchStatus.OK, status_code=code, data=data,
                           text=text, url=str(response.url))
    if code in RETRYABLE_STATUS:
        return FetchResult(status=FetchStatus.RETRYABLE, status_code=code,
                           text=response.text or "",
                           error=f"HTTP {code}", retryable=True,
                           retry_after=retry_after, url=str(response.url))
    return FetchResult(status=FetchStatus.ERROR, status_code=code,
                       text=response.text or "", error=f"HTTP {code}",
                       retryable=False, url=str(response.url))


# ---------------------------------------------------------------- 同步客户端

class HttpClient:
    """同步 HTTP 客户端（刮削层用，线程池安全）。

    Args:
        timeout: 总超时（秒）。
        connect_timeout: 连接超时（秒）。
        retries: 最大尝试次数（含首次）。
        backoff: 退避基数。
        backoff_max: 退避上限。
        max_retry_after: 采纳 Retry-After 的上限（秒）。
        headers: 附加请求头。
        user_agent: UA；为空用默认。
        proxy: 代理地址。
        verify: 是否校验证书（源站多为自签，默认 False）。
        limiter: 可选 RateLimiter；提供时按 source 取令牌。
        default_source: limiter 的默认来源名。
        http2: 是否启用 HTTP/2。
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT,
                 connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
                 retries: int = DEFAULT_RETRIES,
                 backoff: float = DEFAULT_BACKOFF,
                 backoff_max: float = DEFAULT_BACKOFF_MAX,
                 max_retry_after: float = DEFAULT_MAX_RETRY_AFTER,
                 headers: Optional[Dict[str, str]] = None,
                 user_agent: Optional[str] = None,
                 proxy: Optional[str] = None,
                 verify: bool = False,
                 limiter: Optional[Any] = None,
                 default_source: Optional[str] = None,
                 http2: bool = False) -> None:
        self._timeout: float = float(timeout)
        self._connect_timeout: float = float(connect_timeout)
        self._retries: int = max(int(retries), 1)
        self._backoff: float = float(backoff)
        self._backoff_max: float = float(backoff_max)
        self._max_retry_after: float = float(max_retry_after)
        self._limiter: Optional[Any] = limiter
        self._default_source: Optional[str] = default_source
        self._lock = threading.RLock()

        extra = dict(headers or {})
        if user_agent is not None:
            extra["User-Agent"] = user_agent
        else:
            extra.setdefault("User-Agent", DEFAULT_USER_AGENT)
        kwargs = _client_kwargs(proxy, verify, self._timeout, self._connect_timeout,
                                extra, True, http2)
        try:
            self._client: Optional[httpx.Client] = httpx.Client(**kwargs)
        except TypeError:  # httpx < 0.28：proxies= 参数
            if "proxy" in kwargs:
                kwargs["proxies"] = kwargs.pop("proxy")
            self._client = httpx.Client(**kwargs)

    # ---------------------------------------------------------- 请求

    def request(self, method: str, url: str, *,
                params: Optional[Dict[str, Any]] = None,
                json_body: Optional[Any] = None,
                data: Optional[Any] = None,
                headers: Optional[Dict[str, str]] = None,
                source: Optional[str] = None,
                timeout: Optional[float] = None,
                retries: Optional[int] = None,
                as_json: bool = True,
                sleep: bool = True) -> FetchResult:
        """发起请求（含限速、重试、退避）。

        Args:
            method: HTTP 方法。
            url: 目标 URL。
            params: query 参数。
            json_body: JSON 请求体。
            data: 表单/原始请求体。
            headers: 单次请求附加头。
            source: 限速来源（默认 default_source）。
            timeout: 覆盖总超时。
            retries: 覆盖最大尝试次数。
            as_json: 是否按 JSON 解析响应体。
            sleep: False 时不做退避睡眠（测试用）。

        Returns:
            FetchResult。
        """
        max_attempts = max(int(retries if retries is not None else self._retries), 1)
        src = source or self._default_source
        result = FetchResult(status=FetchStatus.ERROR, error="not attempted", url=url)
        for attempt in range(max_attempts):
            self._wait_for_token(src)
            try:
                response = self._send(method, url, params=params, json_body=json_body,
                                      data=data, headers=headers, timeout=timeout)
            except httpx.HTTPError as exc:
                result = _classify(None, exc, url)
                result.attempts = attempt + 1
            else:
                result = _classify(response, None, url)
                result.attempts = attempt + 1
                if as_json and result.ok:
                    data_obj, text = _extract_data(response)
                    result.data = data_obj
                    result.text = text
                if result.status_code == 429 and self._limiter is not None and src:
                    self._limiter.on_429(src, result.retry_after)
            if result.ok or not result.retryable:
                break
            if attempt < max_attempts - 1:
                delay = _backoff_delay(attempt, self._backoff, self._backoff_max,
                                       result.retry_after, self._max_retry_after)
                log_event("http.retry", "DEBUG", None, url=url, attempt=attempt + 1,
                          code=result.status_code, delay=round(delay, 3))
                if sleep:
                    time.sleep(delay)
        return result

    def get(self, url: str, **kwargs: Any) -> FetchResult:
        """GET 快捷方法（参数同 request）。"""
        return self.request("GET", url, **kwargs)

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None,
                 **kwargs: Any) -> FetchResult:
        """GET + JSON 解析（等效 request(as_json=True)）。"""
        kwargs["as_json"] = True
        return self.request("GET", url, params=params, **kwargs)

    def head(self, url: str, **kwargs: Any) -> FetchResult:
        """HEAD 快捷方法（探活用）。"""
        return self.request("HEAD", url, **kwargs)

    def _wait_for_token(self, source: Optional[str]) -> None:
        """限速取令牌（无 limiter 时直接返回）。"""
        if self._limiter is None or not source:
            return
        try:
            self._limiter.acquire(source)
        except Exception as exc:  # pylint: disable=broad-except
            log_event("http.limiter_error", "WARNING", None, source=source, error=str(exc))

    def _send(self, method: str, url: str, *, params: Optional[Dict[str, Any]],
              json_body: Optional[Any], data: Optional[Any],
              headers: Optional[Dict[str, str]],
              timeout: Optional[float]) -> httpx.Response:
        """真正发一次请求。"""
        if self._client is None:
            raise httpx.HTTPError("HttpClient 已关闭")
        request = self._client.build_request(
            method, url, params=params,
            json=json_body if json_body is not None else None,
            data=data if data is not None else None,
            headers=headers,
            timeout=None if timeout is None else _build_timeout(timeout, self._connect_timeout),
        )
        return self._client.send(request)

    # ---------------------------------------------------------- 生命周期

    def close(self) -> None:
        """关闭底层连接池（幂等）。"""
        with self._lock:
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:  # pylint: disable=broad-except
                    pass
                self._client = None

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


# ---------------------------------------------------------------- 异步客户端

class AsyncHttpClient:
    """异步 HTTP 客户端（采集层用，§1.1 并发模型：10 站 × semaphore(4)）。

    参数与 HttpClient 一致；并发控制（semaphore）由调用方持有，本类不内置，
    便于跨站共享同一个 client 而各自限流。
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT,
                 connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
                 retries: int = DEFAULT_RETRIES,
                 backoff: float = DEFAULT_BACKOFF,
                 backoff_max: float = DEFAULT_BACKOFF_MAX,
                 max_retry_after: float = DEFAULT_MAX_RETRY_AFTER,
                 headers: Optional[Dict[str, str]] = None,
                 user_agent: Optional[str] = None,
                 proxy: Optional[str] = None,
                 verify: bool = False,
                 limiter: Optional[Any] = None,
                 default_source: Optional[str] = None,
                 http2: bool = False,
                 limits: Optional[Any] = None) -> None:
        self._timeout = float(timeout)
        self._connect_timeout = float(connect_timeout)
        self._retries = max(int(retries), 1)
        self._backoff = float(backoff)
        self._backoff_max = float(backoff_max)
        self._max_retry_after = float(max_retry_after)
        self._limiter = limiter
        self._default_source = default_source
        self._kwargs = _client_kwargs(proxy, verify, self._timeout, self._connect_timeout,
                                      self._merge_headers(headers, user_agent), True, http2)
        self._limits = limits
        self._client: Optional[httpx.AsyncClient] = None

    @staticmethod
    def _merge_headers(headers: Optional[Dict[str, str]],
                       user_agent: Optional[str]) -> Dict[str, str]:
        """合并默认头与自定义头。"""
        extra = dict(headers or {})
        extra.setdefault("User-Agent", user_agent or DEFAULT_USER_AGENT)
        return extra

    async def _ensure_client(self) -> httpx.AsyncClient:
        """惰性创建 AsyncClient（需在事件循环内）。"""
        if self._client is None:
            kwargs = dict(self._kwargs)
            if self._limits is not None:
                kwargs["limits"] = self._limits
            try:
                self._client = httpx.AsyncClient(**kwargs)
            except TypeError:
                if "proxy" in kwargs:
                    kwargs["proxies"] = kwargs.pop("proxy")
                self._client = httpx.AsyncClient(**kwargs)
        return self._client

    async def request(self, method: str, url: str, *,
                      params: Optional[Dict[str, Any]] = None,
                      json_body: Optional[Any] = None,
                      data: Optional[Any] = None,
                      headers: Optional[Dict[str, str]] = None,
                      source: Optional[str] = None,
                      timeout: Optional[float] = None,
                      retries: Optional[int] = None,
                      as_json: bool = True,
                      sleep: bool = True) -> FetchResult:
        """异步发起请求（含限速、重试、退避）。"""
        client = await self._ensure_client()
        max_attempts = max(int(retries if retries is not None else self._retries), 1)
        src = source or self._default_source
        result = FetchResult(status=FetchStatus.ERROR, error="not attempted", url=url)
        for attempt in range(max_attempts):
            if self._limiter is not None and src:
                try:
                    self._limiter.acquire(src)
                except Exception as exc:  # pylint: disable=broad-except
                    log_event("http.limiter_error", "WARNING", None, source=src, error=str(exc))
            try:
                request = client.build_request(
                    method, url, params=params,
                    json=json_body if json_body is not None else None,
                    data=data if data is not None else None,
                    headers=headers,
                    timeout=(None if timeout is None
                             else _build_timeout(timeout, self._connect_timeout)),
                )
                response = await client.send(request)
            except httpx.HTTPError as exc:
                result = _classify(None, exc, url)
                result.attempts = attempt + 1
            else:
                result = _classify(response, None, url)
                result.attempts = attempt + 1
                if as_json and result.ok:
                    data_obj, text = _extract_data(response)
                    result.data = data_obj
                    result.text = text
            if result.ok or not result.retryable:
                break
            if attempt < max_attempts - 1:
                delay = _backoff_delay(attempt, self._backoff, self._backoff_max,
                                       result.retry_after, self._max_retry_after)
                if sleep:
                    await asyncio.sleep(delay)
        return result

    async def get(self, url: str, **kwargs: Any) -> FetchResult:
        """异步 GET。"""
        return await self.request("GET", url, **kwargs)

    async def get_json(self, url: str, params: Optional[Dict[str, Any]] = None,
                       **kwargs: Any) -> FetchResult:
        """异步 GET + JSON 解析。"""
        kwargs["as_json"] = True
        return await self.request("GET", url, params=params, **kwargs)

    async def aclose(self) -> None:
        """关闭连接池。"""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # pylint: disable=broad-except
                pass
            self._client = None

    async def __aenter__(self) -> "AsyncHttpClient":
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        await self.aclose()
        return False


# ---------------------------------------------------------------- 模块级便捷入口

_DEFAULT_CLIENT: Optional[HttpClient] = None
_DEFAULT_LOCK = threading.RLock()


def default_client(refresh: bool = False, **kwargs: Any) -> HttpClient:
    """进程内共享的默认同步客户端。

    Args:
        refresh: True 时重建。
        **kwargs: 传给 HttpClient 的构造参数。

    Returns:
        HttpClient 实例。
    """
    global _DEFAULT_CLIENT
    with _DEFAULT_LOCK:
        if _DEFAULT_CLIENT is None or refresh:
            if _DEFAULT_CLIENT is not None:
                _DEFAULT_CLIENT.close()
            _DEFAULT_CLIENT = HttpClient(**kwargs)
        return _DEFAULT_CLIENT


def get_json(url: str, params: Optional[Dict[str, Any]] = None,
             source: Optional[str] = None, **kwargs: Any) -> FetchResult:
    """用默认客户端发一次 GET（取 JSON）。"""
    return default_client(**kwargs).get_json(url, params=params, source=source)


def close_all() -> None:
    """关闭默认客户端（退出前调用）。"""
    global _DEFAULT_CLIENT
    with _DEFAULT_LOCK:
        if _DEFAULT_CLIENT is not None:
            _DEFAULT_CLIENT.close()
            _DEFAULT_CLIENT = None
