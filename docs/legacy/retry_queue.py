# -*- coding: utf-8 -*-
"""
retry_queue.py — 刮削限流/不可达条目的待办队列 (子任务 6 · 决策 3)

三分类语义:
  HIT        -> 正常入库 videos.json
  MISS       -> 真未命中, 丢弃, 仅记 unmatched.json (reason=all_sources_miss)
  RETRYABLE  -> 限流/超时/连接失败/反爬, 进本队列, 等下一轮任务重新刮削

存储取舍说明: 独立 product/retry_queue.json, 不复用 unmatched.json。
理由: unmatched 是"终态审计"(本轮已判定丢弃, 由 aggregator 整体重写),
retry queue 是"活跃待办"(下轮要 pop 出来重新处理, 带 attempts 计数),
生命周期与写入方都不同; 混用会导致下轮注入时误把终态条目也重刮,
且 aggregator 全量重写 unmatched.json 会覆盖队列状态。

重试策略:
  - 注入即出队 (pop): 下轮启动时把 attempts < max 的条目重新注入刮削管道;
    重试成功 -> 命中入库 (队列已出队无需清理); 重试仍限流 -> record 重新入队且 attempts+1;
    重试真未命中 -> 正常丢弃 (源站可能已下架, 符合决策 2)。
  - 连续 N 次 (settings.retry_max_attempts, 默认 3) 仍不可达 -> 降级:
    从队列移除, 记入 unmatched.json (reason=retryable_exhausted) 供人工审计。
    取舍: 降级条目"保留进 unmatched 而非直接丢弃"——它们从未得到一次公平的
    刮削机会 (一直被网络问题挡住), 不符合决策 2 "全源真未命中才丢" 的前提,
    但也不进 videos.json (与决策 2 一致: 只有刮削命中才入库)。
"""
import json
import os
import threading
from datetime import datetime

RETRY_FILE = "product/retry_queue.json"
UNMATCHED_FILE = "product/unmatched.json"

_LOCK = threading.Lock()

# 进程内缓存 (首次访问时从磁盘加载), 所有变更立即原子落盘
_CACHE = None


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _load() -> dict:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if os.path.exists(RETRY_FILE):
        try:
            with open(RETRY_FILE, "r", encoding="utf-8") as f:
                _CACHE = json.load(f)
        except Exception as e:
            print(f"[警告] 重试队列加载失败, 视为空队列重建: {type(e).__name__}: {e}")
            _CACHE = None
    if not isinstance(_CACHE, dict) or "entries" not in (_CACHE or {}):
        _CACHE = {"schema_version": "2.1", "updated_at": _now(), "entries": []}
    if not isinstance(_CACHE.get("entries"), list):
        _CACHE["entries"] = []
    return _CACHE


def _save(data: dict):
    """原子写 (tmp + os.replace)"""
    os.makedirs(os.path.dirname(RETRY_FILE) or ".", exist_ok=True)
    data["updated_at"] = _now()
    tmp = f"{RETRY_FILE}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, RETRY_FILE)
    except Exception as e:
        print(f"[警告] 重试队列保存失败: {type(e).__name__}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def make_key(category: str, search_title: str, year) -> str:
    """与 main.py 跨站去重同源的三元组 key: (category, search_title, year)"""
    return f"{(category or 'movies').strip()}|{(search_title or '').strip()}|{str(year or '').strip()}"


def key_of(item: dict) -> str:
    return make_key(item.get("category", "movies"),
                    item.get("search_title") or item.get("title") or "",
                    item.get("year"))


def record(key: str, item: dict, failed_sources: list) -> int:
    """限流/不可达条目入队; 已存在则 attempts+1。返回该条目当前 attempts。

    failed_sources: [{"source": "TMDB", "reason": "429"} ...]
    """
    with _LOCK:
        data = _load()
        for e in data["entries"]:
            if e.get("key") == key:
                e["attempts"] = int(e.get("attempts", 0)) + 1
                e["last_attempt"] = _now()
                # 失败源明细取并集 (不同轮次可能卡在不同源)
                seen = {(s.get("source"), s.get("reason")) for s in e.get("failed_sources", [])}
                for s in failed_sources or []:
                    if (s.get("source"), s.get("reason")) not in seen:
                        e.setdefault("failed_sources", []).append(s)
                _save(data)
                return e["attempts"]
        entry = {
            "key": key,
            "title": item.get("title") or item.get("search_title") or "",
            "category": item.get("category", "movies"),
            "site": item.get("site", ""),
            "url": _first_source_url(item),
            "failed_sources": list(failed_sources or []),
            "attempts": 0,
            "queued_at": _now(),
            "last_attempt": _now(),
            # 完整原始条目: 重试命中后仍需 lines/lines 元数据才能入库
            "item": item,
        }
        data["entries"].append(entry)
        _save(data)
        return 0


def resolve(key: str) -> bool:
    """条目刮削成功/真未命中 -> 从待办移除。返回是否确有移除。"""
    with _LOCK:
        data = _load()
        before = len(data["entries"])
        data["entries"] = [e for e in data["entries"] if e.get("key") != key]
        if len(data["entries"]) != before:
            _save(data)
            return True
        return False


def _first_source_url(item: dict) -> str:
    for line in item.get("lines") or []:
        for ep in line.get("episodes") or []:
            if ep.get("url"):
                return ep["url"]
    return ""


def demote_exhausted(max_attempts: int = 3) -> list:
    """把连续重试 >= max_attempts 次仍不可达的条目移出队列并返回 (调用方负责降级落盘)"""
    with _LOCK:
        data = _load()
        keep, demoted = [], []
        for e in data["entries"]:
            (demoted if int(e.get("attempts", 0)) >= max_attempts else keep).append(e)
        if demoted:
            data["entries"] = keep
            _save(data)
        return demoted


def pop_pending(max_attempts: int = 3) -> list:
    """取本轮待重试条目 (attempts < max_attempts) 并出队 (注入即出队, 失败由 record 重新入队)"""
    with _LOCK:
        data = _load()
        keep, pending = [], []
        for e in data["entries"]:
            (pending if int(e.get("attempts", 0)) < max_attempts else keep).append(e)
        if pending:
            data["entries"] = keep
            _save(data)
        return pending


def pending_count() -> int:
    with _LOCK:
        return len(_load().get("entries", []))


def flush_demoted_to_unmatched(demoted: list):
    """降级条目写入 unmatched.json (reason=retryable_exhausted), 与 enrich/aggregator
    的丢弃审计共用同一份文件; 只追加, 不触碰 aggregator 的内存态。"""
    if not demoted:
        return
    payload_items = []
    for e in demoted:
        it = e.get("item") or {}
        payload_items.append({
            "bangou": it.get("bangou") or "",
            "title": it.get("title") or e.get("title") or "",
            "category": it.get("category") or e.get("category") or "movies",
            "site": it.get("site", ""),
            "url": it.get("url") or e.get("url") or "",
            "matched": False,
            "reason": "retryable_exhausted",
            "attempts": e.get("attempts", 0),
            "failed_sources": e.get("failed_sources", []),
            "sub_category": it.get("sub_category", ""),
            "year": str(it.get("year") or ""),
        })
    existing = {"schema_version": "2.1", "items": []}
    if os.path.exists(UNMATCHED_FILE):
        try:
            with open(UNMATCHED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, list):
                existing = {"schema_version": "2.1", "items": existing}
            existing.setdefault("items", [])
        except Exception as e:
            print(f"[警告] unmatched.json 读取失败, 降级条目将重建文件: {type(e).__name__}: {e}")
            existing = {"schema_version": "2.1", "items": []}
    have = {(it.get("bangou"), it.get("title")) for it in existing["items"]}
    added = 0
    for it in payload_items:
        if (it.get("bangou"), it.get("title")) in have:
            continue
        existing["items"].append(it)
        added += 1
    os.makedirs(os.path.dirname(UNMATCHED_FILE) or ".", exist_ok=True)
    tmp = f"{UNMATCHED_FILE}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False)
    os.replace(tmp, UNMATCHED_FILE)
    print(f"[待办降级] {added} 条重试耗尽条目转入 unmatched.json (reason=retryable_exhausted)")


def reset_for_test(path: str):
    """测试专用: 重定向队列文件路径并清空进程内缓存"""
    global RETRY_FILE, _CACHE
    RETRY_FILE = path
    _CACHE = None
