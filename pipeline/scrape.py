# -*- coding: utf-8 -*-
"""pipeline/scrape.py —— P4 刮削编排（设计文档 §2 P4 / §6 / §7 / T04）

职责：消费素材库（RawLibrary）的未刮削条目 → 沿源优先级链刮削 →
scorer 置信度决策 → 缓存读写（正/负）→ 游标推进（scraped/mark_retryable）。

关键流程（每条目）：

1. 构造缓存键 `make_ck(category, norm_title, seq, year)`，先读 MetaCache：
   - hit → 直接用缓存 meta（0 请求），mark_scraped(DONE/FULL, hit, confidence)；
   - soft/hard_miss → 负缓存命中（0 请求），按状态 mark_scraped(DONE, miss)；
   - none/stale → 继续真实刮削。
2. 源链按序（TMDB → TheTVDB → 豆瓣 → Bilibili → OMDb）尝试：
   - `search()` 返回候选 → `choose_best()` 打分 → §6.5 阈值决策：
     - high（≥high）：改写权威标题，写入全部元数据；
     - medium（≥medium）：采用元数据但不改写标题（保留源站标题）；
     - low（≥low）：触发 S8 ID 校验（item 有 douban_id 且预算允许）；
     - miss（<low）：`put_miss(soft_miss)`，继续下一候选 / 下一源。
   - 命中 → 可选 Tier B `detail()` 补深字段（热通道 AB / backfill_b）；
     写正缓存 `put_hit`。
   - retryable → **绝不写负缓存**，`record_retryable` + 进 RetryStore，
     源本地降级不计入（豆瓣失败率滚动窗口在源内处理）。
3. 全源 miss → `put_miss(hard_miss, confirmed_by=全部源)`。
4. 分类闸门：`taxonomy.authoritative_category(provider, meta)` 判非四类
   → category_discard（丢审计，不写正缓存）。
5. 汇总：按结果批量 `mark_scraped` / `mark_retryable` 推进游标（§7.2 幂等）。

Tier 策略（§7）：hot="AB"（search+detail 全字段）/ cold="A"（仅 search）/
backfill_b=True 时对 scraped=1 的存量条目机会主义补 detail。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import taxonomy
from core.cache import (
    STATUS_HARD_MISS,
    STATUS_HIT,
    STATUS_SOFT_MISS,
    STATUS_STALE,
    CacheDB,
    MetaCache,
    RawLibrary,
    RetryStore,
    make_ck,
)
from core.logging import event, log_event
from sources import (
    ST_CATEGORY_DISCARD,
    ST_HIT,
    ST_MISS,
    ST_RETRYABLE,
    ProviderError,
    SourceProvider,
    choose_best,
)
from sources.base import ST_CATEGORY_DISCARD as _SD  # noqa: F401  兼容别名

__all__ = ["ScrapeOrchestrator", "EnrichResult", "enrich_entity", "SCRAPE_DECISIONS"]


# ---------------------------------------------------------------- 决策常量

#: §6.5 阈值缺省
_DEFAULT_THRESHOLDS: Dict[str, int] = {"high": 70, "medium": 55, "low": 40}

#: 源链（与 sources.REGISTRY 顺序一致，供编排层显式遍历）
SOURCE_CHAIN: Tuple[str, ...] = ("tmdb", "tvdb", "douban", "bilibili", "omdb")


@dataclass
class EnrichResult:
    """单条目刮削结果（§P4 EnrichResult 契约）。"""

    status: str = "miss"                 # hit / miss / retryable / category_discard
    provider: str = ""                   # 最终采用源
    confidence: Optional[int] = None     # 0-100
    match_kind: str = ""                 # exact / fuzzy / translation_pair / id_confirmed
    meta: Dict[str, Any] = field(default_factory=dict)   # 合并后 EnrichedEntity
    reason: str = ""                     # 未命中原因（unmatched 审计）
    external_id: str = ""                # tmdb_123 / douban_456 ...
    cache_hit: bool = False              # 是否读缓存直接命中
    tier_b_backfilled: bool = False      # 本轮是否已 Tier B 补全

    def ok(self) -> bool:
        return self.status == "hit"


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _external_id_of(provider: str, meta: Dict[str, Any]) -> str:
    """按采用源构造 external_id（tmdb_123 / douban_456 / tvdb_789 / bili_101 / imdb_tt123）。"""
    p = str(provider or "").lower()
    if p == "tmdb":
        return f"tmdb_{meta.get('tmdb_id')}" if meta.get("tmdb_id") else ""
    if p in ("douban", "豆瓣"):
        return f"douban_{meta.get('douban_id')}" if meta.get("douban_id") else ""
    if p == "tvdb" or p == "thetvdb":
        return f"tvdb_{meta.get('tvdb_id')}" if meta.get("tvdb_id") else ""
    if p == "bilibili":
        return f"bili_{meta.get('bilibili_season_id')}" if meta.get("bilibili_season_id") else ""
    if p == "omdb":
        return f"imdb_{meta.get('imdb_id')}" if meta.get("imdb_id") else ""
    return ""


def _merge_entity(item: Dict[str, Any], meta: Dict[str, Any],
                  rewrite_title: bool) -> Dict[str, Any]:
    """Scored meta → EnrichedEntity（保留 RawItem 线路/源站字段 + 覆盖刮削字段）。

    注意：`confidence` / `provider` 由调用方按命中结果回填（不做 None 覆盖，
    T04/T05 修复——P5 `meets_gate` 依赖非 None 的 int confidence）。
    """
    ent: Dict[str, Any] = dict(item)
    ent["matched"] = True
    for k in ("title", "original_title", "year", "first_air_date", "overview",
              "rating", "rating_source", "vote_count", "runtime", "genres",
              "cast", "director", "country", "original_language", "studio",
              "logo", "certification", "popularity", "number_of_seasons",
              "number_of_episodes", "cover", "backdrop", "external_id",
              # ID 硬证据（external_id / bangou 构造依赖）
              "tmdb_id", "douban_id", "tvdb_id", "bilibili_season_id",
              "imdb_id", "media_type"):
        if meta.get(k) not in (None, "", [], 0.0):
            ent[k] = meta[k]
    if rewrite_title and meta.get("title"):
        ent["title"] = meta["title"]
        ent["canonical_title"] = meta["title"]
    elif meta.get("title"):
        # medium：保留源站标题，但记录权威名
        ent["canonical_title"] = meta["title"]
    if meta.get("poster") and not ent.get("cover"):
        ent["cover"] = meta["poster"]
    return ent


class ScrapeOrchestrator:
    """刮削编排器。

    Args:
        cache: MetaCache 实例（正/负缓存）。
        library: RawLibrary 实例（素材库游标）。
        sources: {注册名: SourceProvider}（缺省 build_sources(settings)）。
        settings: settings dict。
        db: 可复用 CacheDB（RetryStore 用；缺省取 cache.db）。
    """

    def __init__(self, cache: Optional[MetaCache] = None,
                 library: Optional[RawLibrary] = None,
                 sources: Optional[Dict[str, SourceProvider]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 db: Optional[CacheDB] = None) -> None:
        self._settings = dict(settings or {})
        self._cache: MetaCache = cache if cache is not None else MetaCache()
        self._library: RawLibrary = library if library is not None else RawLibrary(self._cache.db)
        retry_db = db if db is not None else self._cache.db
        self._retry: RetryStore = RetryStore(retry_db)
        if sources is None:
            from sources import build_sources
            sources = build_sources(settings=self._settings, enabled_only=True)
        self._sources: Dict[str, SourceProvider] = dict(sources or {})
        self._thresholds: Dict[str, int] = dict(_DEFAULT_THRESHOLDS)
        match_cfg = self._settings.get("match") or {}
        for k in ("high", "medium", "low"):
            if match_cfg.get(k) not in (None, ""):
                self._thresholds[k] = int(match_cfg[k])
        self._id_verify: bool = bool(match_cfg.get("id_verify_enable", True))
        self._candidate_limit: int = int(match_cfg.get("candidate_limit", 5) or 5)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- 读缓存

    def _read_cache(self, item: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
        """读缓存，返回 (状态, meta)。状态 ∈ hit / soft_miss / hard_miss / none。"""
        ck = make_ck(str(item.get("category") or "movies"),
                     str(item.get("norm_title") or item.get("search_title") or ""),
                     item.get("seq") or item.get("season") or 1,
                     str(item.get("year") or ""))
        entry = self._cache.get(ck, include_stale=True)
        if entry.status == STATUS_HIT and entry.meta:
            return "hit", entry.meta
        if entry.status in (STATUS_SOFT_MISS, STATUS_HARD_MISS):
            return entry.status, None
        return "none", None

    # ---------------------------------------------------------- S8 ID 校验

    def _verify_by_douban_id(self, tmdb_source: SourceProvider,
                             cand: Dict[str, Any],
                             douban_id: str) -> Tuple[bool, str]:
        """S8：40≤score<70 时用 TMDB external_ids 比对源站 douban_id。

        Returns:
            (verified, detail)；verified=True → score+=60 升档 id_confirmed。
        """
        media_type = str(cand.get("media_type") or "movie")
        tmdb_id = str(cand.get("tmdb_id") or "")
        if not tmdb_id:
            return False, "no_tmdb_id"
        try:
            ext, st = tmdb_source.external_ids(tmdb_id, media_type)  # type: ignore[attr-defined]
        except (ProviderError, AttributeError):
            return False, "ext_err"
        if st != ST_HIT or not ext:
            return False, "ext_retryable"
        ext_douban = str((ext or {}).get("douban_id") or "").strip().lstrip("0")
        src_douban = str(douban_id or "").strip().lstrip("0")
        if src_douban and ext_douban == src_douban:
            return True, "douban_id_match"
        return False, "douban_id_mismatch"

    # ---------------------------------------------------------- 单源尝试

    def _try_source(self, item: Dict[str, Any], name: str,
                    tier: str) -> Optional[EnrichResult]:
        """对单一源执行 search + 打分 + 决策。

        Returns:
            EnrichResult（status=hit 或 miss）；None 表示该源不可用/可重试
            由编排层统一处理（进 RetryStore 不判终态）。
        """
        src = self._sources.get(name)
        if src is None or not src.enabled:
            return None
        category = str(item.get("category") or "movies")
        # 源类型闸门：绝不跨类型用错源（如电影不查 TheTVDB）
        if name == "tvdb" and category not in ("tv", "anime", "variety"):
            return None
        if name == "bilibili" and category != "anime":
            return None
        if name == "omdb":
            return EnrichResult(status="skip", provider=name)  # 评分兜底，跳过主链

        try:
            meta, st = src.search(item)
        except ProviderError as e:
            log_event("scrape.source_err", "WARNING", None, source=name,
                      title=item.get("title"), err=str(e))
            return None
        except Exception as e:  # pylint: disable=broad-except
            log_event("scrape.source_exc", "WARNING", None, source=name,
                      title=item.get("title"), err=f"{type(e).__name__}: {e}")
            return None

        if st == ST_RETRYABLE:
            return None  # 可重试：交给编排层统一进 RetryStore
        if st == ST_MISS:
            return EnrichResult(status="miss", provider=name, reason="all_sources_miss")
        if st == ST_CATEGORY_DISCARD:
            return EnrichResult(status="category_discard", provider=name,
                                reason="not_in_whitelist")
        if st != ST_HIT or not isinstance(meta, dict):
            return EnrichResult(status="miss", provider=name, reason="no_valid_line")

        # 候选打分（§6）
        candidates = meta.get("_candidates") or [meta]
        best, scored, _all = choose_best(item, candidates,
                                         candidate_limit=self._candidate_limit)
        if best is None:
            return EnrichResult(status="miss", provider=name,
                                reason=scored.detail.get("reject") or "low_confidence")
        decision = scored.decision(self._thresholds)
        if decision == "miss":
            return EnrichResult(status="miss", provider=name,
                                reason=scored.detail.get("reject") or "low_confidence")

        # --- S8：low 区间（40≤score<medium）且预算允许 → ID 校验 ---
        if decision == "low" and self._id_verify:
            douban_id = str(item.get("douban_id") or "").strip()
            tmdb_src = self._sources.get("tmdb")
            if douban_id and tmdb_src is not None:
                verified, detail = self._verify_by_douban_id(tmdb_src, best, douban_id)
                if verified:
                    scored.score = max(scored.score + 60, int(self._thresholds["high"]))
                    scored.match_kind = "id_confirmed"
                    decision = scored.decision(self._thresholds)
                else:
                    return EnrichResult(status="miss", provider=name,
                                        reason=f"id_unverified({detail})")
            else:
                # 无 douban_id 可校验 / TMDB 源不可用 → low 区间不入库（§6.5）
                return EnrichResult(status="miss", provider=name,
                                    reason="low_confidence")
        if scored.score < int(self._thresholds["low"]):
            return EnrichResult(status="miss", provider=name,
                                reason="low_confidence")

        # --- 合并 meta + Tier B 深度补全 ---
        merged = self._finalize_meta(item, best, scored, src, tier)
        # 回填命中结果（P5 门槛依赖非 None 的 confidence；bangou 构造依赖 provider/external_id）
        merged["confidence"] = int(scored.score)
        merged["provider"] = name
        merged["external_id"] = _external_id_of(name, merged)
        # --- OMDb 评分兜底（§P4）：主源命中但 rating 缺失且候选有 imdb_id 时补一枪 ---
        omdb_src = self._sources.get("omdb")
        if (omdb_src is not None and omdb_src.enabled
                and merged.get("rating") in (None, "", 0.0, 0)
                and merged.get("imdb_id")):
            try:
                score_meta, st = omdb_src.by_id(str(merged["imdb_id"]))
                if st == ST_HIT and isinstance(score_meta, dict):
                    merged["rating"] = score_meta.get("rating") or merged.get("rating")
                    merged["vote_count"] = score_meta.get("vote_count") or merged.get("vote_count")
                    if score_meta.get("rating_source"):
                        merged["rating_source"] = score_meta["rating_source"]
            except (ProviderError, AttributeError):
                pass
        result = EnrichResult(
            status="hit",
            provider=name,
            confidence=int(scored.score),
            match_kind=scored.match_kind,
            meta=merged,
            external_id=_external_id_of(name, merged),
        )
        return result

    def _finalize_meta(self, item: Dict[str, Any], cand: Dict[str, Any],
                       scored: Any, src: SourceProvider,
                       tier: str) -> Dict[str, Any]:
        """候选 meta + [可选 Tier B detail] → 合并 EnrichedEntity。"""
        meta: Dict[str, Any] = dict(cand)
        # 分类闸门（taxonomy 权威校正）：非四类 → category_discard（由调用方判）
        try:
            gate_cat, gate_reason = taxonomy.authoritative_category(src.name, meta)
        except Exception:  # pylint: disable=broad-except
            gate_cat, gate_reason = "", ""
        if gate_cat is None:
            meta["_gate_discard"] = f"{gate_reason or 'not_in_whitelist'}"

        rewrite_title = scored.score >= int(self._thresholds["high"])
        # Tier B 深度补全：热通道 AB / 存量 backfill 都补 cast/director/runtime
        need_detail = (tier in ("AB", "full") or
                       bool(self._settings.get("scrape_tier", {}).get("backfill_b")))
        if need_detail:
            try:
                if src.name == "TMDB" and meta.get("tmdb_id"):
                    media_type = str(meta.get("media_type") or "movie")
                    d_meta, d_st = src.detail(str(meta["tmdb_id"]), media_type)  # type: ignore[attr-defined]
                    if d_st == ST_HIT and isinstance(d_meta, dict):
                        for k in ("cast", "director", "runtime", "certification",
                                  "homepage", "number_of_seasons",
                                  "number_of_episodes", "genres", "imdb_id", "logo"):
                            if d_meta.get(k) not in (None, "", []):
                                meta[k] = d_meta[k]
            except (ProviderError, AttributeError) as e:
                log_event("scrape.detail_err", "WARNING", None, source=src.name,
                          title=item.get("title"), err=str(e))
        return _merge_entity(item, meta, rewrite_title)

    # ---------------------------------------------------------- 主入口

    def scrape_one(self, item: Dict[str, Any], tier: str = "A",
                   backfill_b: bool = False) -> EnrichResult:
        """处理单个素材库条目。

        Args:
            item: CoarseEntity（含 norm_title / seq / year / douban_id）。
            tier: "A"（仅 search）/ "AB"/"full"（search+detail）。
            backfill_b: 存量 Tier B 回填（scraped=1 → detail 补齐后升 scraped=2）。

        Returns:
            EnrichResult；调用方据此 mark_scraped / mark_retryable。
        """
        # 1) 读缓存（0 请求路径）
        cache_status, cached_meta = self._read_cache(item)
        if cache_status == "hit" and cached_meta:
            cached_meta = dict(cached_meta)
            cached_meta["matched"] = True
            ent = _merge_entity(item, cached_meta,
                                rewrite_title=True)
            ent["confidence"] = int(cached_meta.get("confidence") or 55)
            provider = str(cached_meta.get("provider") or "TMDB")
            return EnrichResult(status="hit", provider=provider,
                                confidence=ent["confidence"],
                                match_kind=str(cached_meta.get("match_kind") or "exact"),
                                meta=ent, external_id=_external_id_of(provider, ent),
                                cache_hit=True)
        if cache_status in ("soft_miss", "hard_miss"):
            return EnrichResult(status="miss",
                                reason="soft_miss" if cache_status == STATUS_SOFT_MISS
                                else "hard_miss")
        if backfill_b and item.get("_library", {}).get("scraped") == 1:
            # 存量条目 Tier B 回填：Tier A 已有 search meta，这里补 detail 即可
            return self._backfill_detail(item)

        # 2) 源链依次尝试
        tried: List[str] = []
        retry_sources: List[str] = []
        for name in SOURCE_CHAIN:
            if name not in self._sources:
                continue
            result = self._try_source(item, name, tier)
            if result is None:
                retry_sources.append(name)
                continue
            if result.status == "skip":
                continue  # 评分兜底源不参与主链（不计入 hit/miss/retryable）
            tried.append(name)
            if result.status == "hit":
                self._write_hit_cache(item, result)
                return result
            if result.status == "category_discard":
                return result
            # miss：继续下一源

        # 3) 全源 miss / 部分 retryable
        if retry_sources:
            # 至少一个源可重试 → 不判终态，进 RetryStore 下轮重试
            for name in retry_sources:
                self._retry.push(self._key_of(item), item, last_err=f"{name}: retryable")
            return EnrichResult(status="retryable", reason="+".join(retry_sources))
        # 全源真未命中 → hard_miss 负缓存（confirmed_by=全部尝试源）
        ck = make_ck(str(item.get("category") or "movies"),
                     str(item.get("norm_title") or item.get("search_title") or ""),
                     item.get("seq") or item.get("season") or 1,
                     str(item.get("year") or ""))
        self._cache.put_miss(ck, kind=STATUS_HARD_MISS,
                             confirmed_by=tried or None,
                             year=str(item.get("year") or ""),
                             provider=",".join(tried))
        return EnrichResult(status="miss", reason="all_sources_miss")

    def _write_hit_cache(self, item: Dict[str, Any], result: EnrichResult) -> None:
        """命中后写正缓存（含 provider / confidence / match_kind 供下次零请求复用）。"""
        ck = make_ck(str(item.get("category") or "movies"),
                     str(item.get("norm_title") or item.get("search_title") or ""),
                     item.get("seq") or item.get("season") or 1,
                     str(item.get("year") or ""))
        meta = dict(result.meta)
        meta["provider"] = result.provider
        meta["confidence"] = result.confidence
        meta["match_kind"] = result.match_kind
        self._cache.put_hit(ck, meta, provider=result.provider,
                            confidence=result.confidence)

    def _backfill_detail(self, item: Dict[str, Any]) -> EnrichResult:
        """存量 Tier B 回填：用缓存 meta 补 detail（不重跑 search）。"""
        ck = make_ck(str(item.get("category") or "movies"),
                     str(item.get("norm_title") or item.get("search_title") or ""),
                     item.get("seq") or item.get("season") or 1,
                     str(item.get("year") or ""))
        entry = self._cache.get(ck)
        if not entry.is_hit or not entry.meta:
            return EnrichResult(status="miss", reason="no_cache_for_backfill")
        meta = dict(entry.meta)
        src = self._sources.get("tmdb")
        if src is not None and meta.get("tmdb_id"):
            try:
                d_meta, d_st = src.detail(str(meta["tmdb_id"]),
                                          str(meta.get("media_type") or "movie"))
                if d_st == ST_HIT:
                    for k in ("cast", "director", "runtime", "certification",
                              "number_of_seasons", "number_of_episodes", "logo"):
                        if d_meta.get(k) not in (None, "", []):
                            meta[k] = d_meta[k]
                    meta["provider"] = "TMDB"
            except (ProviderError, AttributeError):
                pass
        ent = _merge_entity(item, meta, rewrite_title=True)
        ent["confidence"] = int(meta.get("confidence") or 55)
        prov = str(meta.get("provider") or entry.provider or "TMDB")
        return EnrichResult(status="hit", provider=prov,
                            confidence=ent["confidence"],
                            meta=ent, external_id=_external_id_of(prov, ent),
                            tier_b_backfilled=True)

    def _key_of(self, item: Dict[str, Any]) -> str:
        return make_ck(str(item.get("category") or "movies"),
                       str(item.get("norm_title") or item.get("search_title") or ""),
                       item.get("seq") or item.get("season") or 1,
                       str(item.get("year") or ""))

    # ---------------------------------------------------------- 批量 + 游标推进

    def scrape_batch(self, items: List[Dict[str, Any]], tier: str = "A",
                     backfill_b: bool = False,
                     workers: int = 1) -> Dict[str, Any]:
        """批量刮削并**事务内推进素材库游标**（§7.2 幂等）。

        Items 的 merge_key 需在 item["merge_key"]（CoarseEntity 已含）。
        workers > 1 时用 ThreadPoolExecutor 并发（源内有互斥节流，线程安全）。

        Returns:
            {"results": [EnrichResult], "hit": n, "miss": n,
             "retryable": n, "discard": n}
        """
        from concurrent.futures import ThreadPoolExecutor

        workers = max(int(workers), 1)

        def _one(it: Dict[str, Any]) -> Tuple[Dict[str, Any], EnrichResult]:
            mk = str(it.get("merge_key") or "")
            try:
                res = self.scrape_one(it, tier=tier, backfill_b=backfill_b)
            except Exception as e:  # pylint: disable=broad-except
                log_event("scrape.item_exc", "WARNING", None, title=it.get("title"),
                          err=f"{type(e).__name__}: {e}")
                if mk:
                    self._retry.push(mk, it, last_err=f"{type(e).__name__}: {e}")
                    return it, EnrichResult(status="retryable", reason="item_exc")
                return it, EnrichResult(status="miss", reason="item_exc")
            return it, res

        if workers <= 1 or len(items) <= 1:
            pairs = [_one(it) for it in items]
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                pairs = list(pool.map(_one, items))

        results: List[EnrichResult] = []
        hit_keys, miss_keys, retry_keys, discard_keys, full_keys = [], [], [], [], []
        hit_payloads: Dict[str, Dict[str, Any]] = {}
        full_payloads: Dict[str, Dict[str, Any]] = {}
        for it, res in pairs:
            results.append(res)
            mk = str(it.get("merge_key") or "")
            if not mk:
                continue
            if res.status == "hit":
                if res.tier_b_backfilled:
                    full_keys.append(mk)      # 存量回填完成 → 升 scraped=2
                    if res.meta:
                        full_payloads[mk] = res.meta
                else:
                    hit_keys.append(mk)
                    if res.meta:
                        hit_payloads[mk] = res.meta
            elif res.status == "miss":
                miss_keys.append(mk)
            elif res.status == "retryable":
                retry_keys.append(mk)
            elif res.status == "category_discard":
                discard_keys.append(mk)

        # 游标推进（事务内批量，幂等）
        # 命中条目把富化结果（EnrichedEntity）写回 payload，P5 `_load_scraped_hits`
        # 才有封面/简介/confidence/lines 可消费（T04/T05 修复）
        if hit_keys:
            self._library.mark_scraped(hit_keys, status="hit",
                                       payloads=hit_payloads)
        if full_keys:
            self._library.mark_scraped(full_keys, status="hit", backfilled=True,
                                       payloads=full_payloads)
        if miss_keys:
            self._library.mark_scraped(miss_keys, status="miss")
        if retry_keys:
            self._library.mark_retryable(retry_keys, status="retryable")
        if discard_keys:
            self._library.mark_scraped(discard_keys,
                                       status="category_discard")

        return {
            "results": results,
            "hit": len(hit_keys) + len(full_keys),
            "miss": len(miss_keys),
            "retryable": len(retry_keys),
            "discard": len(discard_keys),
        }

    def close(self) -> None:
        """关闭各源（释放自建 HTTP 连接池）。"""
        for src in self._sources.values():
            try:
                src.close()
            except Exception:  # pylint: disable=broad-except
                pass


# ---------------------------------------------------------------- 便捷函数

def enrich_entity(item: Dict[str, Any],
                  sources: Optional[Dict[str, SourceProvider]] = None,
                  cache: Optional[MetaCache] = None,
                  library: Optional[RawLibrary] = None,
                  settings: Optional[Dict[str, Any]] = None,
                  tier: str = "A") -> EnrichResult:
    """单条目便捷刮削入口（测试/脚本用）。"""
    orch = ScrapeOrchestrator(cache=cache, library=library, sources=sources,
                              settings=settings)
    try:
        return orch.scrape_one(item, tier=tier)
    finally:
        orch.close()