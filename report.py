# -*- coding: utf-8 -*-
"""report.py —— 运行报表（设计文档 §2 P6 / T05）

职责：汇总一轮 run 的统计（采集 / 刮削 / 合并 / 导出 / 超时状态），
生成控制台摘要 + PushPlus 微信通知 HTML。

纯函数、无 IO（调用方负责落盘与推送），便于测试与 GA 双环境复用。
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

__all__ = ["RuntimeStats", "build_summary_text", "build_pushplus_html"]


class RuntimeStats:
    """一轮 run 的运行时统计聚合。"""

    def __init__(self, **kw: Any) -> None:
        self.raw_items: int = int(kw.get("raw_items", 0))
        self.deduped: int = int(kw.get("deduped", 0))
        self.library_new: int = int(kw.get("library_new", 0))
        self.library_pending: int = int(kw.get("library_pending", 0))
        self.scraped_hit: int = int(kw.get("scraped_hit", 0))
        self.scraped_miss: int = int(kw.get("scraped_miss", 0))
        self.scraped_retryable: int = int(kw.get("scraped_retryable", 0))
        self.scraped_discard: int = int(kw.get("scraped_discard", 0))
        self.final_items: int = int(kw.get("final_items", 0))
        self.unmatched: int = int(kw.get("unmatched", 0))
        self.export_ok: bool = bool(kw.get("export_ok", True))
        self.error_count: int = int(kw.get("error_count", 0))
        self.elapsed: float = float(kw.get("elapsed", 0.0))
        self.budget_status: str = str(kw.get("budget_status", "ok"))
        self.limits: Dict[str, float] = dict(kw.get("limits") or {})
        self.category_stats: Dict[str, int] = dict(kw.get("category_stats") or {})

    # ---------------------------------------------------------- 格式化

    def to_text(self) -> str:
        lines = [
            "采集原始条目: %d 部" % self.raw_items,
            "跨站去重后: %d 部" % self.deduped,
            "素材库新增: %d 条 (待刮削 %d)" % (self.library_new, self.library_pending),
            "刮削命中: %d / 未命中: %d / 可重试: %d / 丢弃: %d"
            % (self.scraped_hit, self.scraped_miss,
               self.scraped_retryable, self.scraped_discard),
            "精细合并入库: %d 部" % self.final_items,
            "未匹配隔离: %d 部" % self.unmatched,
            "管道错误: %d 处" % self.error_count,
            "预算状态: %s" % ("超时降级" if self.budget_status == "timeout" else "正常"),
        ]
        if self.category_stats:
            detail = " | ".join(f"{k}: {v}" for k, v in
                                sorted(self.category_stats.items()))
            lines.append(f"分类明细: {detail}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "raw_items": self.raw_items,
            "deduped": self.deduped,
            "library_new": self.library_new,
            "library_pending": self.library_pending,
            "scraped_hit": self.scraped_hit,
            "scraped_miss": self.scraped_miss,
            "scraped_retryable": self.scraped_retryable,
            "scraped_discard": self.scraped_discard,
            "final_items": self.final_items,
            "unmatched": self.unmatched,
            "export_ok": self.export_ok,
            "error_count": self.error_count,
            "elapsed": round(self.elapsed, 2),
            "budget_status": self.budget_status,
            "category_stats": self.category_stats,
        }


def build_summary_text(stats: RuntimeStats, started: float) -> str:
    """控制台摘要文本。"""
    return (
        "\n" + "=" * 60 + "\n"
        "SuenMedia 运行报表\n"
        f"  执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  总耗时: {time.time() - started:.1f}s\n"
        "  ----------------\n"
        + "\n".join(f"  {line}" for line in stats.to_text().splitlines())
        + "\n" + "=" * 60
    )


def build_pushplus_html(stats: RuntimeStats) -> str:
    """PushPlus 微信通知 HTML（简化行内样式，无 emoji，与 UI 规范一致）。"""
    budget_badge = (
        "<span style='color:#c0392b'>超时降级</span>"
        if stats.budget_status == "timeout" else
        "<span style='color:#27ae60'>正常</span>"
    )
    rows = [
        ("执行时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("总计耗时", f"{stats.elapsed:.1f} 秒"),
        ("采集原始条目", f"{stats.raw_items} 部"),
        ("跨站去重后", f"{stats.deduped} 部"),
        ("素材库新增", f"{stats.library_new} 条（待刮削 {stats.library_pending}）"),
        ("刮削命中", f"{stats.scraped_hit} 部"),
        ("刮削未命中", f"{stats.scraped_miss} 部"),
        ("可重试", f"{stats.scraped_retryable} 部"),
        ("分类丢弃", f"{stats.scraped_discard} 部"),
        ("精细合并入库", f"{stats.final_items} 部"),
        ("未匹配隔离", f"{stats.unmatched} 部"),
        ("管道错误", f"{stats.error_count} 处"),
        ("预算状态", budget_badge),
    ]
    if stats.category_stats:
        for k, v in sorted(stats.category_stats.items()):
            rows.append((f"分类-{k}", f"{v} 部"))
    item_html = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:#666'>{k}</td>"
        f"<td style='padding:4px 0'>{v}</td></tr>"
        for k, v in rows)
    return (
        f"<table style='border-collapse:collapse;font-size:14px;line-height:1.6'>"
        f"{item_html}</table>"
    )