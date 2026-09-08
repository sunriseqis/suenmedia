# -*- coding: utf-8 -*-
"""pipeline/export.py —— P6 产物导出（设计文档 §2 P6 / §8 / T05）

契约 v3（§9）：分片导出 + gzip，避免单文件过大（65k 条目 × 平均 58 集）。

```
product/
├── videos.json               # 元数据主文件（无 episodes），供 suenplayer 导入
├── episodes.jsonl.gz         # {bangou, season_number, episodes[]} 逐行
├── unmatched.json            # 隔离区审计
├── m3u8/{category}/{bangou}.m3u8
└── manifest.json             # 版本/条目数/生成时间/校验和
```

要点：
- **videos.json 不含 episodes**（元数据主文件）；episodes 按 bangou 分片写 jsonl.gz。
- **cover 唯一封面字段**：导出前对 poster 字段做最后一道清洗（poster → 全删，
  绝不让 poster 出现在产物里）。
- **manifest.json** 记版本、各文件条目数、生成时间、sha256。
- 原子写：先写 `.tmp` 再 `os.replace`，杜绝半截文件。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import time
from typing import Any, Dict, Iterable, List, Optional

from core.logging import log_event

__all__ = ["Exporter", "export_items", "write_json_atomic", "sha256_file"]

#: 分片大小（episodes 每行一条 bangou → 每片最多 N 条）
SHARD_LINES: int = 2000

VERSION: str = "v3"


def write_json_atomic(path: str, payload: Any) -> None:
    """JSON 原子写（.tmp → os.replace）。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _clean_product(item: Dict[str, Any]) -> Dict[str, Any]:
    """导出前清洗：poster 字段驱逐 + 深拷贝防污染内存。"""
    out = json.loads(json.dumps(item, ensure_ascii=False, default=str))
    out.pop("poster", None)
    for season in out.get("seasons") or []:
        season.pop("poster", None)
        season.pop("season_poster", None)
        for ep in season.get("episodes") or []:
            ep.pop("poster", None)
    return out


def _iter_episode_rows(item: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """按 (bangou, season_number) 产 episodes 分片行。"""
    bangou = str(item.get("bangou") or "")
    if not bangou:
        return
    for season in item.get("seasons") or []:
        eps = season.get("episodes") or []
        if not eps:
            continue
        yield {
            "bangou": bangou,
            "season_number": int(season.get("season_number") or 1),
            "title": item.get("title"),
            "episodes": eps,
        }


class Exporter:
    """分片导出器。

    Args:
        product_dir: 产物目录（缺省 product/）。
    """

    def __init__(self, product_dir: Optional[str] = None) -> None:
        self._root: str = product_dir or os.path.join(
            os.getcwd(), "product")
        self._m3u8_dir: str = os.path.join(self._root, "m3u8")

    # ---------------------------------------------------------- 导出

    def export(self, items: List[Dict[str, Any]],
               unmatched: Optional[List[Dict[str, Any]]] = None,
               version: str = VERSION) -> Dict[str, Any]:
        """导出全部产物文件。

        Returns:
            {"videos": n, "episodes_shards": n, "unmatched": n,
             "m3u8": n, "manifest": str}
        """
        started = time.time()
        os.makedirs(self._root, exist_ok=True)
        os.makedirs(self._m3u8_dir, exist_ok=True)

        cleaned = [_clean_product(it) for it in (items or [])]
        # videos.json：剥离 episodes 只留元数据
        videos = []
        for it in cleaned:
            meta = {k: v for k, v in it.items() if k != "seasons"}
            if meta.get("episodes"):
                meta.pop("episodes")
            meta["episode_count"] = sum(
                len(s.get("episodes") or []) for s in (it.get("seasons") or []))
            videos.append(meta)
        videos_path = os.path.join(self._root, "videos.json")
        write_json_atomic(videos_path, videos)

        # episodes.jsonl.gz 分片
        shards = self._export_episode_shards(cleaned, version)

        # unmatched.json
        unmatched_path = os.path.join(self._root, "unmatched.json")
        write_json_atomic(unmatched_path, unmatched or [])

        # m3u8 分片
        m3u8_count = self._export_m3u8(cleaned)

        # manifest.json
        elapsed = time.time() - started
        manifest = {
            "version": version,
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_sec": round(elapsed, 2),
            "counts": {
                "videos": len(videos),
                "episode_shards": shards,
                "unmatched": len(unmatched or []),
                "m3u8": m3u8_count,
            },
            "checksums": {
                "videos.json": sha256_file(videos_path),
                "episodes.jsonl.gz": sha256_file(self._episodes_path),
                "unmatched.json": sha256_file(unmatched_path),
            },
        }
        manifest_path = os.path.join(self._root, "manifest.json")
        write_json_atomic(manifest_path, manifest)
        log_event("export.done", "INFO", None, videos=len(videos),
                  unmatched=len(unmatched or []), m3u8=m3u8_count,
                  elapsed_sec=round(elapsed, 2))
        return {
            "videos": len(videos),
            "episodes_shards": shards,
            "unmatched": len(unmatched or []),
            "m3u8": m3u8_count,
            "manifest": manifest_path,
        }

    # ---------------------------------------------------------- 分片

    @property
    def _episodes_path(self) -> str:
        return os.path.join(self._root, "episodes.jsonl.gz")

    def _export_episode_shards(self, cleaned: List[Dict[str, Any]],
                               version: str) -> int:
        """episodes.jsonl.gz 分片写（单文件多行 gzip，POST 兼容读）。"""
        rows = [r for it in cleaned for r in _iter_episode_rows(it)]
        path = self._episodes_path
        tmp = f"{path}.tmp.{os.getpid()}"
        shard = 0
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            for idx, row in enumerate(rows):
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                if (idx + 1) % SHARD_LINES == 0:
                    shard += 1
        os.replace(tmp, path)
        return max(1, (len(rows) + SHARD_LINES - 1) // SHARD_LINES) if rows else 0

    # ---------------------------------------------------------- m3u8

    def _export_m3u8(self, cleaned: List[Dict[str, Any]]) -> int:
        """m3u8 播放列表（§8 契约：按 category/bangou 组织）。"""
        count = 0
        for it in cleaned:
            bangou = str(it.get("bangou") or "")
            category = str(it.get("category") or "movies")
            if not bangou:
                continue
            lines = ["#EXTM3U"]
            for season in it.get("seasons") or []:
                for ep in season.get("episodes") or []:
                    ep_title = str(ep.get("ep_title") or f"第{ep.get('ep_number')}集")
                    lines.append(f"#EXTINF:-1,{bangou} S{season.get('season_number')}E{ep.get('ep_number')} {ep_title}")
                    lines.append(str(ep.get("url") or ""))
            if len(lines) <= 1:
                continue
            out_dir = os.path.join(self._m3u8_dir, category)
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"{bangou}.m3u8")
            tmp = f"{path}.tmp.{os.getpid()}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            os.replace(tmp, path)
            count += 1
        return count

    # ---------------------------------------------------------- 清理

    def clear(self) -> None:
        """清空旧产物（保留目录）。"""
        if os.path.isdir(self._root):
            shutil.rmtree(self._root, ignore_errors=True)
        os.makedirs(self._m3u8_dir, exist_ok=True)


def export_items(items: List[Dict[str, Any]],
                 unmatched: Optional[List[Dict[str, Any]]] = None,
                 product_dir: Optional[str] = None,
                 clear_first: bool = False) -> Dict[str, Any]:
    """便捷函数：整链导出。"""
    exporter = Exporter(product_dir=product_dir)
    if clear_first:
        exporter.clear()
    return exporter.export(items, unmatched)