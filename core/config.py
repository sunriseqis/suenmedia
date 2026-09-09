# -*- coding: utf-8 -*-
"""配置加载：settings.json / config.json → 类型化 Settings / Config。

设计要点：
- **类型化**：把设计文档 §13 新增的配置段落成 dataclass 字段，调用方
  `settings.rate_limits["tmdb"]` 直接可用，不必到处 `int(cfg.get(..., 4))`。
- **向后兼容**：settings.json 里未在本模块声明的历史键（如 `crawl_skip_*
  系列）全部保留在 `Settings.raw` 中，通过 `settings.get(key, default)` 仍可取到，
  现有 `common.py` / `crawl_maccms.py` 的读法不会失效。
- **环境变量覆盖**：密钥类走专用环境变量（TMDB_API_KEY 等），其余支持通用
  `SUENMEDIA_<KEY>` 前缀；类型按 dataclass 字段默认值自动推断（bool/int/float/
  list/dict 走 JSON 解析）。
- **缓存**：进程内缓存，避免重复读盘；`refresh=True` 或 `cache_clear()` 强制重读
  （运行期改写配置文件的场景，对齐旧 `common.load_config.cache_clear()` 用法）。
- **线程安全**：所有加载与缓存读写持同一把 RLock。

用法：
    from core.config import load_settings, load_config
    settings = load_settings()
    rps = settings.rate_limit("tmdb")            # 4.0
    sites = load_config().sites
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------- 路径常量

#: 项目根目录（core/ 的上一级）
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SETTINGS_FILENAME: str = "settings.json"
CONFIG_FILENAME: str = "config.json"
DOTENV_FILENAME: str = ".env"

#: 缓存文件默认目录（设计文档 §8：SQLite 取代 5MB 单文件 JSON）
DEFAULT_CACHE_DIR: str = os.path.join("product")
DEFAULT_CACHE_DB: str = os.path.join(DEFAULT_CACHE_DIR, "suenmedia.db")


# ---------------------------------------------------------------- 默认值

DEFAULT_RATE_LIMITS: Dict[str, float] = {
    "tmdb": 4.0,      # 官方 40 req / 10s
    "douban": 3.0,
    "tvdb": 8.0,
    "bilibili": 3.0,
    "omdb": 2.0,
    "default": 2.0,   # 未登记来源的保守兜底
}

DEFAULT_CACHE_TTL: Dict[str, float] = {
    "hit_days": 90.0,
    "soft_miss_days": 14.0,
    "hard_miss_days": 45.0,
    "old_hard_miss_days": 90.0,
}

DEFAULT_NEG_CACHE_GUARD: Dict[str, float] = {
    "error_rate_threshold": 0.30,
    "min_samples": 50.0,
}

DEFAULT_BUDGET: Dict[str, int] = {
    "crawl_max": 420,
    "probe_max": 90,
    "coarse_max": 60,
    "fine_max": 120,
    "export_max": 240,
    "safety": 90,
    "scrape_max": 1200,
}

DEFAULT_PROBE: Dict[str, Any] = {
    "strategy": "domain",
    "timeout_head": 3,
    "timeout_get": 5,
    "fail_threshold": 4,
    "half_open_cooldown": 900,
    "full_recheck_days": 15,
}

DEFAULT_MATCH: Dict[str, Any] = {
    "high": 70,
    "medium": 55,
    "low": 40,
    "id_verify_enable": True,
    "candidate_limit": 5,
}

DEFAULT_EXPORT: Dict[str, Any] = {
    "version": "v3",
    "shard_episodes": True,
    "gzip": True,
}

#: 设计文档 §13 `prefilter.sub_category_blacklist`（与现有 crawl_skip_type_keywords 对齐）
DEFAULT_SUB_CATEGORY_BLACKLIST: List[str] = [
    "现代都市", "古装仙侠", "AI漫剧", "漫剧", "爽文", "爽文短剧", "反转爽剧",
    "女频恋爱", "言情总裁", "年代穿越", "穿越年代", "脑洞悬疑", "反转爽文",
    "重生民国", "现代言情", "都市脑洞", "女恋总裁", "家庭篇", "成长逆袭",
    "解说", "电影解说", "微电影", "短剧", "伦理片",
]

DEFAULT_PREFILTER: Dict[str, Any] = {
    "enable": True,
    "skip_categories": ["short_tv", "discard"],
    "sub_category_blacklist": list(DEFAULT_SUB_CATEGORY_BLACKLIST),
    "title_regex_blacklist": [r"^第\d+[集期]", r"^\d+$", "微电影", "解说"],
}

#: settings.json 缺失时使用的兜底默认值（仅核心键，其余走 dataclass 默认值）
_FALLBACK_SETTINGS: Dict[str, Any] = {
    "tmdb_api_key": "",
    "tmdb_api_base": "https://api.themoviedb.org/3",
    "tmdb_image_base": "https://image.tmdb.org/t/p/w500",
    "enable_tmdb": True,
    "enable_douban_fallback": True,
    "enable_m3u8_check": True,
    "m3u8_timeout": 2,
    "max_workers": 10,
    "pipeline_workers": 20,
}

#: 专用环境变量映射：settings 键 → 环境变量名（优先级最高）
ENV_VAR_MAP: Dict[str, str] = {
    "tmdb_api_key": "TMDB_API_KEY",
    "tvdb_api_key": "TVDB_API_KEY",
    "omdb_api_key": "OMDB_API_KEY",
    "tmdb_api_base": "TMDB_API_BASE",
    "tmdb_image_base": "TMDB_IMAGE_BASE",
    "proxy": "CRAWL_PROXY",
    "run_budget_seconds": "RUN_BUDGET_SECONDS",
    "scrape_workers": "SCRAPE_WORKERS",
}

#: 通用环境变量前缀：SUENMEDIA_TMDB_MIN_INTERVAL → tmdb_min_interval
ENV_PREFIX: str = "SUENMEDIA_"

_LOCK = threading.RLock()


# ---------------------------------------------------------------- .env 支持

def load_dotenv(path: Optional[str] = None) -> int:
    """加载本地 .env（不入库）：KEY=VALUE 格式，不覆盖已有环境变量。

    Args:
        path: .env 路径；默认项目根目录下的 `.env`。

    Returns:
        成功 setdefault 进去的变量条数（文件不存在返回 0）。
    """
    env_path = path or os.path.join(BASE_DIR, DOTENV_FILENAME)
    if not os.path.exists(env_path):
        return 0
    count = 0
    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if not key:
                    continue
                if key not in os.environ:
                    os.environ[key] = value
                    count += 1
    except OSError:
        return 0
    return count


def _coerce(raw: str, template: Any) -> Any:
    """按模板值的类型把字符串环境变量转成目标类型。

    Args:
        raw: 环境变量原始字符串。
        template: 该配置项的默认值，作为类型模板。

    Returns:
        转换后的值；转换失败时原样返回字符串。
    """
    if isinstance(template, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on", "y")
    if isinstance(template, int) and not isinstance(template, bool):
        try:
            return int(raw)
        except (TypeError, ValueError):
            return template
    if isinstance(template, float):
        try:
            return float(raw)
        except (TypeError, ValueError):
            return template
    if isinstance(template, (list, dict)):
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return template
        return parsed if isinstance(parsed, type(template)) else template
    return raw


# ---------------------------------------------------------------- Settings

@dataclass
class Settings:
    """类型化全局配置（settings.json）。

    已声明字段按类型直接访问；未声明的历史/新增键保留在 `raw` 字典里，
    通过 `get()` / `section()` 读取，保证与旧代码 `settings.get(...)` 兼容。
    """

    #: 原始配置全量快照（含未声明键）
    raw: Dict[str, Any] = field(default_factory=dict)

    # --- 密钥与站点 ---
    tmdb_api_key: str = ""
    tmdb_api_base: str = "https://api.themoviedb.org/3"
    tmdb_image_base: str = "https://image.tmdb.org/t/p/w500"
    tvdb_api_key: str = ""
    tvdb_translation_langs: List[str] = field(default_factory=lambda: ["zho", "chi"])
    omdb_api_key: str = ""
    proxy: str = ""

    # --- 功能开关 ---
    enable_tmdb: bool = True
    enable_douban_fallback: bool = True
    enable_m3u8_check: bool = True
    bilibili_enable: bool = True

    # --- 并发与限速（§13） ---
    tmdb_min_interval: float = 0.25          # 4 req/s（原 0.15 → 6.7 req/s 是 429 元凶）
    douban_min_interval: float = 0.5
    bilibili_min_interval: float = 1.0
    scrape_workers: int = 12                 # 8-16
    crawl_per_site_concurrency: int = 4
    max_workers: int = 10
    pipeline_workers: int = 20
    run_budget_seconds: int = 1800           # 每轮 30 分钟

    # --- 预算（§7） ---
    budget: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_BUDGET))

    # --- per-source 限速（§13） ---
    rate_limits: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_RATE_LIMITS))
    douban_budget_per_run: int = 500

    # --- 缓存 TTL（§8.3） ---
    cache_ttl: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_CACHE_TTL))
    neg_cache_guard: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_NEG_CACHE_GUARD))

    # --- 分段配置（§13） ---
    prefilter: Dict[str, Any] = field(default_factory=lambda: json.loads(
        json.dumps(DEFAULT_PREFILTER, ensure_ascii=False)))
    probe: Dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_PROBE))
    match: Dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_MATCH))
    export: Dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_EXPORT))

    # --- 采集相关（历史键，保留兼容） ---
    crawl_hours: int = 24
    max_pages_per_site: int = 20
    sites_per_run: int = 5
    retry_max_attempts: int = 3
    m3u8_timeout: int = 2
    crawl_skip_categories: List[str] = field(default_factory=lambda: ["short_tv"])
    crawl_skip_title_keywords: List[str] = field(default_factory=lambda: ["微电影"])
    crawl_skip_type_keywords: List[str] = field(default_factory=list)

    # ---------------------------------------------------------- 构造

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Settings":
        """从字典构造：已声明键按字段类型取值，dict 字段做浅合并保留默认值。

        Args:
            data: settings.json 解析后的字典（允许为 None / 部分键）。

        Returns:
            Settings 实例。
        """
        data = dict(data or {})
        kwargs: Dict[str, Any] = {}
        for f in fields(cls):
            if f.name == "raw":
                continue
            if f.name not in data or data[f.name] is None:
                continue
            value = data[f.name]
            default_value = getattr(cls(), f.name, None)
            if isinstance(default_value, dict) and isinstance(value, dict):
                merged = dict(default_value)
                merged.update(value)
                kwargs[f.name] = merged
            else:
                kwargs[f.name] = value
        obj = cls(**kwargs)
        obj.raw = data
        return obj

    # ---------------------------------------------------------- 读取

    def get(self, key: str, default: Any = None) -> Any:
        """取值：优先已声明字段，其次 raw 字典，最后 default。"""
        if hasattr(self, key) and key != "raw":
            value = getattr(self, key)
            if value is not None:
                return value
        if key in self.raw:
            return self.raw[key]
        return default

    def section(self, name: str) -> Dict[str, Any]:
        """取子配置段（不存在返回空 dict）。"""
        value = self.get(name, {})
        return dict(value) if isinstance(value, dict) else {}

    def rate_limit(self, source: str) -> float:
        """取指定来源的限速（req/s），未登记则用 `default` 或 2.0。"""
        try:
            return float(self.rate_limits.get(source, self.rate_limits.get("default", 2.0)))
        except (AttributeError, TypeError, ValueError):
            return 2.0

    def ttl_days(self, kind: str) -> float:
        """取缓存 TTL（天）。kind ∈ hit / soft_miss / hard_miss / old_hard_miss。"""
        key = kind if kind.endswith("_days") else f"{kind}_days"
        try:
            return float(self.cache_ttl.get(key, DEFAULT_CACHE_TTL.get(key, 14.0)))
        except (AttributeError, TypeError, ValueError):
            return 14.0

    def guard_value(self, key: str) -> float:
        """取负缓存闸门参数（error_rate_threshold / min_samples）。"""
        try:
            return float(self.neg_cache_guard.get(key, DEFAULT_NEG_CACHE_GUARD.get(key, 0.0)))
        except (AttributeError, TypeError, ValueError):
            return 0.0

    def budget_value(self, key: str) -> int:
        """取预算参数（秒），缺失回落到 DEFAULT_BUDGET。"""
        try:
            return int(self.budget.get(key, DEFAULT_BUDGET.get(key, 0)))
        except (AttributeError, TypeError, ValueError):
            return 0

    def to_dict(self) -> Dict[str, Any]:
        """导出为普通 dict（字段值优先，raw 兜底，便于写回/打印）。"""
        out = dict(self.raw)
        for f in fields(self):
            if f.name == "raw":
                continue
            out[f.name] = getattr(self, f.name)
        return out


@dataclass
class Config:
    """类型化采集配置（config.json）：站点与分类规则。"""

    raw: Dict[str, Any] = field(default_factory=dict)
    sites: List[Dict[str, Any]] = field(default_factory=list)
    category_rules: Dict[str, List[str]] = field(default_factory=dict)
    sub_category_blacklist: List[str] = field(default_factory=lambda: list(DEFAULT_SUB_CATEGORY_BLACKLIST))
    region_rules: Dict[str, Any] = field(default_factory=dict)
    genre_rules: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        """从字典构造；SITES / CATEGORY_RULES 大小写不敏感。"""
        data = dict(data or {})
        sites = data.get("SITES") or data.get("sites") or []
        rules = data.get("CATEGORY_RULES") or data.get("category_rules") or {}
        blacklist = (data.get("SUB_CATEGORY_BLACKLIST")
                     or data.get("sub_category_blacklist")
                     or list(DEFAULT_SUB_CATEGORY_BLACKLIST))
        return cls(
            raw=data,
            sites=[dict(s) for s in sites] if isinstance(sites, list) else [],
            category_rules={k: list(v) for k, v in rules.items()} if isinstance(rules, dict) else {},
            sub_category_blacklist=list(blacklist),
            region_rules=dict(data.get("REGION_RULES") or {}),
            genre_rules=dict(data.get("GENRE_RULES") or {}),
        )

    @property
    def enabled_sites(self) -> List[Dict[str, Any]]:
        """启用且按 priority 升序排序的站点列表。"""
        sites = [s for s in self.sites if s.get("enabled", True)]
        return sorted(sites, key=lambda s: int(s.get("priority", 99) or 99))

    def get(self, key: str, default: Any = None) -> Any:
        """兼容旧 dict 读法：config.get("SITES")。"""
        if key in self.raw:
            return self.raw[key]
        return getattr(self, key.lower(), default)


# ---------------------------------------------------------------- 加载入口

def _read_json(path: str) -> Dict[str, Any]:
    """读 JSON 文件；失败返回空 dict（不抛异常，避免单点配置错误打断整轮）。"""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _apply_env(settings: Settings) -> Settings:
    """环境变量覆盖：专用映射 + SUENMEDIA_ 通用前缀。"""
    for key, env_name in ENV_VAR_MAP.items():
        raw = os.getenv(env_name)
        if raw is None or raw == "":
            continue
        template = getattr(settings, key, raw)
        setattr(settings, key, _coerce(raw, template))
        settings.raw[key] = getattr(settings, key)

    prefix_len = len(ENV_PREFIX)
    for env_name, raw in os.environ.items():
        if not env_name.startswith(ENV_PREFIX) or raw == "":
            continue
        key = env_name[prefix_len:].lower()
        if not hasattr(settings, key) or key == "raw":
            continue
        template = getattr(settings, key)
        setattr(settings, key, _coerce(raw, template))
        settings.raw[key] = getattr(settings, key)
    return settings


def _resolve_root(root: Optional[str]) -> str:
    """解析项目根目录；默认本模块推导的 BASE_DIR。"""
    return os.path.abspath(root) if root else BASE_DIR


def load_settings(root: Optional[str] = None, refresh: bool = False) -> Settings:
    """加载 settings.json 并缓存；环境变量优先级最高。

    Args:
        root: 项目根目录；默认 BASE_DIR。
        refresh: True 时强制重读磁盘（清空该 root 的缓存）。

    Returns:
        Settings 实例。
    """
    root_path = _resolve_root(root)
    with _LOCK:
        if not refresh and root_path in _SETTINGS_CACHE:
            return _SETTINGS_CACHE[root_path]
        load_dotenv(os.path.join(root_path, DOTENV_FILENAME))
        data = _read_json(os.path.join(root_path, SETTINGS_FILENAME))
        if not data:
            data = dict(_FALLBACK_SETTINGS)
        else:
            merged = dict(_FALLBACK_SETTINGS)
            merged.update(data)
            data = merged
        settings = _apply_env(Settings.from_dict(data))
        _SETTINGS_CACHE[root_path] = settings
        return settings


def load_config(root: Optional[str] = None, refresh: bool = False) -> Config:
    """加载 config.json 并缓存（运行期改写文件需 refresh=True）。

    Args:
        root: 项目根目录；默认 BASE_DIR。
        refresh: True 时强制重读磁盘。

    Returns:
        Config 实例。
    """
    root_path = _resolve_root(root)
    with _LOCK:
        if not refresh and root_path in _CONFIG_CACHE:
            return _CONFIG_CACHE[root_path]
        data = _read_json(os.path.join(root_path, CONFIG_FILENAME))
        cfg = Config.from_dict(data)
        _CONFIG_CACHE[root_path] = cfg
        return cfg


def cache_clear() -> None:
    """清空全部配置缓存（测试 / 运行期热重载用）。"""
    with _LOCK:
        _SETTINGS_CACHE.clear()
        _CONFIG_CACHE.clear()


def settings_path(root: Optional[str] = None) -> str:
    """settings.json 绝对路径。"""
    return os.path.join(_resolve_root(root), SETTINGS_FILENAME)


def config_path(root: Optional[str] = None) -> str:
    """config.json 绝对路径。"""
    return os.path.join(_resolve_root(root), CONFIG_FILENAME)


def cache_db_path(root: Optional[str] = None) -> str:
    """缓存 SQLite 库绝对路径（目录不存在时由 core.cache 负责创建）。"""
    root_path = _resolve_root(root)
    cfg = load_settings(root_path)
    db_rel = str(cfg.get("cache_db_path", DEFAULT_CACHE_DB) or DEFAULT_CACHE_DB)
    if not os.path.isabs(db_rel):
        db_rel = os.path.join(root_path, db_rel)
    return os.path.normpath(db_rel)


# 进程内缓存（key = 项目根目录绝对路径）
_SETTINGS_CACHE: Dict[str, Settings] = {}
_CONFIG_CACHE: Dict[str, Config] = {}
