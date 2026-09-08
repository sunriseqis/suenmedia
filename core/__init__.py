# -*- coding: utf-8 -*-
"""SuenMedia 基础设施层 (core)。

分层位置（设计文档 §1.1 最底层）：

```
入口层 main.py
管道层 pipeline/
适配层 sources/ crawlers/
领域层 normalize/ taxonomy.py schema.py
基础设施 core/  ← 本包：config · cache · ratelimit · http · logging
```

职责边界：
- 只提供**与业务无关**的通用能力，不 import 任何上层模块（pipeline/sources/...），
  保证无循环依赖，可独立 import。
- 各子模块依赖方向：`config` ← `cache` / `ratelimit` / `http` / `logging`，
  彼此之间横向无依赖（`http` 只可选引用 `ratelimit` 的接口做取令牌）。
"""

from __future__ import annotations

__version__ = "1.0.0"

# 子模块按依赖顺序导入：config 无依赖，logging 只依赖 config，
# cache / ratelimit 依赖 config + logging，http 依赖 config + logging + ratelimit。
from . import config as config  # noqa: E402,F401
from . import logging as logging  # noqa: E402,F401
from . import cache as cache  # noqa: E402,F401
from . import ratelimit as ratelimit  # noqa: E402,F401
from . import http as http  # noqa: E402,F401

__all__ = [
    "config",
    "logging",
    "cache",
    "ratelimit",
    "http",
    "Settings",
    "Config",
    "load_settings",
    "load_config",
    "MetaCache",
    "TitleIndex",
    "DomainRegistry",
    "RawSeen",
    "RetryStore",
    "CacheDB",
    "TokenBucket",
    "RateLimiter",
    "HttpClient",
    "AsyncHttpClient",
    "FetchResult",
]

# 便捷再导出（避免调用方写 from core.cache import MetaCache 的长路径）
from .config import Config, Settings, load_config, load_settings  # noqa: E402,F401
from .cache import (  # noqa: E402,F401
    CacheDB,
    DomainRegistry,
    MetaCache,
    RawSeen,
    RetryStore,
    TitleIndex,
)
from .ratelimit import RateLimiter, TokenBucket  # noqa: E402,F401
from .http import AsyncHttpClient, FetchResult, HttpClient  # noqa: E402,F401
