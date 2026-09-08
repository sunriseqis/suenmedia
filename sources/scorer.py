# -*- coding: utf-8 -*-
"""sources/scorer.py —— 置信度打分模型（设计文档 §6 / T04）

解决旧实现 `best = results[0]` 的 3-8% 错配，并区分「翻译差异」与「真错配」。

信号与权重（§6.1，基础分 100，归一后叠加 S7 修正）：
| 信号 | 权重 | 说明 |
|---|---|---|
| S1 中文名相似度 | 0-40 | SequenceMatcher(norm(源), norm(候选.name))×40 |
| S2 原名/译名关系 | 0-20 | 源含 ASCII 且候选有 original_title → ratio×20；缺失剔除分母 |
| S3 年份 | 0-15 | 差 0→15 / ±1→10 / ±2→5 / >2→0；缺年份 → 8 中性 |
| S4 类型一致 | 0-10 | movies→movie；其余→tv |
| S5 集数量级 | 0-10 | tv/anime：源集数/候选 ∈[0.5,2]→10 / [0.25,4]→5；缺数据剔除分母 |
| S6 热度先验 | 0-5 | min(5, log10(popularity+1)) |
| S7 序号一致 | ±10 | 源 seq 与候选名 seq 一致 → +10；不一致 → −10 |

翻译差异保护（§6.2）：S1<0.3 且源含 CJK 且候选有 original_title 且 S2≥0.6
→ S1 改判 30 分，match_kind="translation_pair"。

强负信号（§6.3）：S4 不一致且 S1<0.5 / 年份差>3 且 S1<0.8 / S1<0.15 且非译名关系
→ 直接判 miss。

阈值（§6.5）：≥70 高置信（改写标题）；55-69 中置信（不改写）；40-54 低置信
（S8 ID 校验，未通过不进库）；<40 判 miss 继续下一候选。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from normalize.title import extract_seq, to_simplified

__all__ = [
    "score_candidate",
    "choose_best",
    "Scored",
]

# ---------------------------------------------------------------- 归一

_SEP_RE = re.compile(r"[\s\-_·・:：/()（）\[\]【】《》\"'“”‘’,，.。]+")
_NUM_RE = re.compile(r"\d{2,}")


def _norm(text: Any) -> str:
    """打分用归一：繁体转简 → 小写 → 去分隔符（保留汉字与字母数字）。"""
    s = str(text or "").strip().lower()
    s = to_simplified(s)
    s = _SEP_RE.sub("", s)
    return s


def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _has_cjk(text: Any) -> bool:
    """是否含 CJK 字符（titles 用；与 pipeline.prefilter 同一判定）。"""
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


# ---------------------------------------------------------------- 结果

@dataclass
class Scored:
    """一次候选打分结果。"""

    score: int = 0
    match_kind: str = ""          # exact / fuzzy / translation_pair / id_confirmed
    detail: Dict[str, Any] = field(default_factory=dict)

    def decision(self, thresholds: Dict[str, int]) -> str:
        """按阈值区间判处置：high / medium / low / miss。"""
        s = self.score
        if s >= int(thresholds.get("high", 70)):
            return "high"
        if s >= int(thresholds.get("medium", 55)):
            return "medium"
        if s >= int(thresholds.get("low", 40)):
            return "low"
        return "miss"


# ---------------------------------------------------------------- 核心

def _s7_seq(src_seq: Any, cand_title: str) -> Tuple[int, bool]:
    """S7 序号一致：一致 +10、不一致 -10、候选无法解析则 0 且计有效。"""
    src_seq_i = 1 if src_seq in (None, "", 0) else int(src_seq)
    cand_i = extract_seq(cand_title) or 1
    return (10, True) if src_seq_i == cand_i else (-10, True)


def score_candidate(item: Dict[str, Any], cand: Dict[str, Any]) -> Scored:
    """对单个候选打分（§6.1-6.3）。

    Args:
        item: 待刮削条目（source_title / category / season / year / episode）。
        cand: 源候选 meta（title / original_title / year / media_type /
            popularity / number_of_episodes）。

    Returns:
        Scored（score ∈ [0,100] 已 clamp；miss 原因见 detail.reject）。
    """
    src = str(item.get("search_title") or item.get("title") or "")
    category = str(item.get("category") or "movies")
    src_norm = _norm(src)
    cand_title = str(cand.get("title") or "")
    cand_norm = _norm(cand_title)
    orig = str(cand.get("original_title") or "")
    orig_norm = _norm(orig)

    detail: Dict[str, Any] = {}

    # --- S1 中文名相似度 ---
    s1_ratio = _ratio(src_norm, cand_norm)
    s1 = s1_ratio * 40.0

    # --- S2 原名/译名关系（§6.1 语义：源含 ASCII 时比较源 ↔ 候选 original_title）---
    s2_valid = bool(orig_norm) and bool(re.search(r"[A-Za-z]", str(src)))
    s2 = _ratio(src_norm, orig_norm) * 20.0 if s2_valid else 0.0

    # --- 翻译差异保护（§6.2）---
    # 源含 CJK 且 S1 极低时，SequenceMatcher(源, 原名) 无可比性（中英字符集不同）。
    # 改为验证「候选自身是否为译名·原名对」：候选 title↔original_title 相似
    # （如 "Fred Has Problems" ↔ "Fred Has Problems"）→ 源中文译名与原名同指一部作品。
    # 判定：ratio(候选.title, 候选.original_title) ≥ 0.6，且两者至少一个含 ASCII。
    match_kind = ""
    cand_title_ascii = bool(re.search(r"[A-Za-z]", cand_title))
    orig_ascii = bool(re.search(r"[A-Za-z]", orig))
    trans_pair_ratio = (_ratio(cand_norm, orig_norm)
                        if cand_norm and orig_norm and (cand_title_ascii or orig_ascii)
                        else 0.0)
    if (s1_ratio < 0.3 and _has_cjk(src) and orig_norm and trans_pair_ratio >= 0.6):
        # 译名 ↔ 原名关系成立（S1 改判 30 分）
        s1 = 30.0
        match_kind = "translation_pair"
    elif s1_ratio >= 0.99 or (src_norm == cand_norm):
        match_kind = "exact"
    elif s1_ratio >= 0.55:
        match_kind = "fuzzy"

    # --- S3 年份 ---
    src_year = str(item.get("year") or "").strip()
    cand_year = str(cand.get("year") or "").strip()
    if src_year.isdigit() and cand_year.isdigit():
        diff = abs(int(src_year) - int(cand_year))
        s3 = {0: 15.0, 1: 10.0, 2: 5.0}.get(diff, 0.0)
    else:
        s3 = 8.0  # 缺年份 → 中性

    # --- S4 类型一致（movies→movie；其余→tv）---
    want_kind = "movie" if category == "movies" else "tv"
    got_kind = str(cand.get("media_type") or "")
    s4 = 10.0 if (got_kind == want_kind) else 0.0

    # --- S5 集数量级一致（仅 tv/anime；缺数据剔除分母）---
    s5_valid = category in ("tv", "anime") and int(item.get("episode") or 0) > 0
    cand_eps = int(cand.get("number_of_episodes") or 0)
    s5_valid = s5_valid and cand_eps > 0
    if s5_valid:
        ratio_eps = int(item.get("episode") or 0) / cand_eps
        s5 = 10.0 if 0.5 <= ratio_eps <= 2 else (5.0 if 0.25 <= ratio_eps <= 4 else 0.0)
    else:
        s5 = 0.0

    # --- S6 热度先验 ---
    try:
        pop = float(cand.get("popularity") or 0.0)
    except (TypeError, ValueError):
        pop = 0.0
    s6 = min(5.0, __import__("math").log10(pop + 1.0)) if pop > 0 else 0.0

    # --- 归一：100 × Σ得分 / Σ有效满分 ---
    full = 40.0
    gained = s1
    if s2_valid:
        full += 20.0
        gained += s2
    full += 15.0
    gained += s3
    full += 10.0
    gained += s4
    if s5_valid:
        full += 10.0
        gained += s5
    full += 5.0
    gained += s6

    score = 100.0 * gained / full if full > 0 else 0.0

    # --- S7 序号一致（叠加修正）---
    seq_delta, _ = _s7_seq(item.get("season", item.get("seq")), cand_title)
    score += seq_delta

    # --- 强负信号（§6.3）---
    if s4 == 0.0 and s1_ratio < 0.5:
        detail["reject"] = f"type_mismatch(s1={s1_ratio:.2f})"
        return Scored(score=0, match_kind=match_kind, detail=detail)
    if s3 == 0.0 and s1_ratio < 0.8 and src_year.isdigit() and cand_year.isdigit():
        detail["reject"] = f"year_far(s1={s1_ratio:.2f})"
        return Scored(score=0, match_kind=match_kind, detail=detail)
    if s1_ratio < 0.15 and match_kind != "translation_pair":
        detail["reject"] = f"name_unrelated(s1={s1_ratio:.2f})"
        return Scored(score=0, match_kind=match_kind, detail=detail)

    score = max(0, min(100, round(score)))
    detail.update({
        "s1": round(s1_ratio, 3),
        "s2": round(s2, 2) if s2_valid else None,
        "s3": s3, "s4": s4, "s5": s5 if s5_valid else None, "s6": s6,
        "s7": seq_delta, "full": full,
    })
    return Scored(score=score, match_kind=match_kind, detail=detail)


def choose_best(item: Dict[str, Any], candidates: List[Dict[str, Any]],
                candidate_limit: int = 5) -> Tuple[Optional[Dict[str, Any]], Scored, List[Scored]]:
    """从候选列表选最优（§P4：不许 `results[0]` 直接拍板）。

    Args:
        item: 待刮削条目。
        candidates: 源返回的候选 meta 列表。
        candidate_limit: 单源最多打分候选数（§13 match.candidate_limit=5）。

    Returns:
        (best, scored, all_scored)；best 为 None 表示全部 miss。
    """
    scored_all: List[Scored] = []
    best: Optional[Dict[str, Any]] = None
    best_scored = Scored()
    for cand in (candidates or [])[:max(int(candidate_limit), 1)]:
        sc = score_candidate(item, cand)
        scored_all.append(sc)
        if sc.score > best_scored.score or (sc.score == best_scored.score
                                            and best is None):
            best_scored = sc
            best = cand
    if best_scored.decision({}) == "miss":
        return None, best_scored, scored_all
    return best, best_scored, scored_all