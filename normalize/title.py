# -*- coding: utf-8 -*-
"""
normalize/title.py —— 标题归一化（设计文档 §4，粗犷合并核心）

流水线（§4.1，顺序有 2 处必要微调，见下方"与设计的偏差"）:

    0. 罗马数字 U+2160–U+217F → 十进制（必须在 NFKC **之前**，见下）
    1. Unicode NFKC（全角→半角、㍿ 等）
    2. 繁简转换（zhconv；不可用时退化为内置映射表）
    2b. 异体字收敛（馀→余 等，zhconv 输出后的收尾）
    3. 转小写
    4. 提取年份 year  —— 【提前】必须在"剥离括号"之前
    5. 剥离括号及其内容 [] 【】 () （） {} 《》 ...
    6. 提取季/部序号 seq（§4.2）并从标题中剥离
    7. 剥离质量/版本词
    8. 剥离集数/更新描述
    9. 剥离标点与空白
   10. 剥离尾部孤立数字（步骤 6 未覆盖的）

与 §4.1 的两处偏差（均有 §4.4 验收用例作为依据）:

  * **年份提取提前到括号剥离之前**。
    §4.1 把年份放在第 6 步（括号剥离之后），但 §4.4 要求
    `无间道（2002）` → `year=2002`；若先剥括号，年份随括号内容一起消失。
    二者矛盾，以验收用例为准。

  * **seq 提取不含「期」**。
    §4.2 的正则含 `期`，但 §4.4 要求 `歌手2024 第3期` → `seq=1`
    （综艺的"第N期"是集号，不是部/季号，若当 seq 会把同一档综艺拆成 N 份）。
    `季 / 部 / 系列` 仍然参与 seq 提取。

  * **尾缀序号加了 `(?<![0-9])` 前置断言**。
    原正则 `([2-9]|[IVXLCDM]{1,5})$` 会把 `乘风2024` 的末位 `4` 当成 seq，
    导致 `乘风` + `seq=4`，与 §4.4 期望的 `seq=1 / year=2024` 冲突。

安全红线（§4.3）：`merge_key` 只做精确字符串相等比较，
`我和我的祖国` 与 `我和我的家乡` 归一化后仍是两个不同字符串，绝不会误并。
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------- 繁简转换

try:  # pragma: no cover - 依赖存在性分支
    from zhconv import convert as _zhconv_convert
except Exception:  # pragma: no cover
    _zhconv_convert = None


# zhconv 不可用时的内置映射表（§12 假设标注：内置 ~300 字映射表）。
# 覆盖常见影视标题用字；覆盖率不足只影响"合并不够"，不会造成误并。
_FALLBACK_T2S = {
    "萬": "万", "與": "与", "東": "东", "個": "个", "們": "们", "來": "来",
    "時": "时", "為": "为", "動": "动", "華": "华", "國": "国", "園": "园",
    "圓": "圆", "團": "团", "圖": "图", "場": "场", "種": "种", "稱": "称",
    "學": "学", "覺": "觉", "應": "应", "當": "当", "無": "无", "雙": "双",
    "處": "处", "聽": "听", "傳": "传", "這": "这", "進": "进", "遠": "远",
    "連": "连", "遲": "迟", "醫": "医", "開": "开", "關": "关", "間": "间",
    "關": "关", "門": "门", "問": "问", "聞": "闻", "體": "体", "髮": "发",
    "鬥": "斗", "鳥": "鸟", "鳴": "鸣", "麗": "丽", "黃": "黄", "點": "点",
    "齒": "齿", "龍": "龙", "龜": "龟", "侠": "侠", "係": "系", "億": "亿",
    "優": "优", "夥": "伙", "傳": "传", "傷": "伤", "倫": "伦", "偉": "伟",
    "側": "侧", "偵": "侦", "假": "假", "偉": "伟", "傑": "杰", "備": "备",
    "傷": "伤", "價": "价", "儀": "仪", "億": "亿", "儒": "儒", "兩": "两",
    "冊": "册", "準": "准", "擊": "击", "別": "别", "創": "创", "劇": "剧",
    "劉": "刘", "則": "则", "剛": "刚", "創": "创", "劇": "剧", "勳": "勋",
    "勵": "励", "勸": "劝", "區": "区", "協": "协", "單": "单", "賣": "卖",
    "衛": "卫", "衝": "冲", "術": "术", "衛": "卫", "裝": "装", "補": "补",
    "裡": "里", "製": "制", "複": "复", "親": "亲", "觀": "观", "規": "规",
    "視": "视", "覽": "览", "覺": "觉", "訂": "订", "計": "计", "訓": "训",
    "記": "记", "訪": "访", "設": "设", "許": "许", "訴": "诉", "診": "诊",
    "註": "注", "詩": "诗", "話": "话", "誕": "诞", "語": "语", "誠": "诚",
    "說": "说", "誤": "误", "誘": "诱", "語": "语", "讀": "读", "變": "变",
    "讓": "让", "讚": "赞", "貓": "猫", "貝": "贝", "財": "财", "負": "负",
    "貨": "货", "責": "责", "貴": "贵", "買": "买", "費": "费", "資": "资",
    "賽": "赛", "贊": "赞", "贈": "赠", "車": "车", "軍": "军", "轉": "转",
    "輪": "轮", "輸": "输", "辦": "办", "農": "农", "運": "运", "達": "达",
    "違": "违", "選": "选", "遺": "遗", "還": "还", "邊": "边", "醫": "医",
    "銀": "银", "錢": "钱", "鋼": "钢", "錄": "录", "鐘": "钟", "鐵": "铁",
    "長": "长", "門": "门", "閃": "闪", "關": "关", "關": "关", "阿": "阿",
    "陣": "阵", "陸": "陆", "陳": "陈", "陰": "阴", "陽": "阳", "階": "阶",
    "隨": "随", "隱": "隐", "難": "难", "雲": "云", "電": "电", "靈": "灵",
    "韓": "韩", "順": "顺", "頭": "头", "題": "题", "顏": "颜", "願": "愿",
    "風": "风", "飛": "飞", "養": "养", "館": "馆", "馬": "马", "駕": "驾",
    "驚": "惊", "骨": "骨", "體": "体", "高": "高", "髮": "发", "鬥": "斗",
    "魚": "鱼", "鳥": "鸟", "鳴": "鸣", "麗": "丽", "黃": "黄", "點": "点",
    "齊": "齐", "齒": "齿", "龍": "龙",
    # 常见影视标题高频繁体字补充
    "餘": "余", "慶": "庆", "敵": "敌", "數": "数", "斷": "断", "舊": "旧",
    "歷": "历", "歸": "归", "殺": "杀", "歲": "岁", "賽": "赛", "懸": "悬",
    "戀": "恋", "戲": "戏", "戰": "战", "導": "导", "專": "专", "導": "导",
    "對": "对", "尋": "寻", "導": "导", "將": "将", "導": "导", "島": "岛",
    "師": "师", "帶": "带", "幫": "帮", "廣": "广", "廠": "厂", "廈": "厦",
    "彈": "弹", "強": "强", "後": "后", "從": "从", "愛": "爱", "慶": "庆",
    "慶": "庆", "憶": "忆", "懷": "怀", "懸": "悬", "戀": "恋", "成": "成",
    "我": "我", "總": "总", "續": "续", "終": "终", "結": "结", "絕": "绝",
    "經": "经", "綠": "绿", "線": "线", "網": "网", "練": "练", "維": "维",
    "緊": "紧", "總": "总", "績": "绩", "繁": "繁", "織": "织", "紅": "红",
    "約": "约", "級": "级", "納": "纳", "純": "纯", "紙": "纸", "紛": "纷",
    "素": "素", "累": "累", "細": "细", "紹": "绍", "終": "终", "結": "结",
    "統": "统", "絕": "绝", "絲": "丝", "綜": "综", "繪": "绘", "繫": "系",
    "續": "续", "繼": "继", "續": "续", "纏": "缠", "續": "续", "纖": "纤",
}

# zhconv 输出后的异体字收敛（关键：`慶餘年` --zhconv--> `庆馀年`，需再收敛为 `庆余年`）
_VARIANT_MAP = {
    "馀": "余",
    "峯": "峰",
    "羣": "群",
    "敎": "教",
    "峽": "峡",
    "峩": "峨",
}


def to_simplified(text: str) -> str:
    """繁体 → 简体。优先 zhconv，不可用时退化为内置映射表。"""
    if not text:
        return ""
    if _zhconv_convert is not None:
        try:
            out = _zhconv_convert(text, "zh-cn")
        except Exception:  # pragma: no cover - 防御性
            out = text
    else:  # pragma: no cover - 未安装 zhconv 时的退化路径
        out = "".join(_FALLBACK_T2S.get(ch, ch) for ch in text)
    return "".join(_VARIANT_MAP.get(ch, ch) for ch in out)


# ---------------------------------------------------------------- 罗马数字

_ROMAN_VALUES = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
_ROMAN_RUN = re.compile(r"[\u2160-\u217F]+")


def _roman_to_int(text: str) -> Optional[int]:
    """罗马数字字符串 → int。非法输入返回 None。"""
    s = str(text or "").strip().upper()
    if not s or not re.fullmatch(r"[IVXLCDM]+", s):
        return None
    total = 0
    prev = 0
    for ch in reversed(s):
        val = _ROMAN_VALUES[ch]
        if val < prev:
            total -= val
        else:
            total += val
            prev = val
    return total if total > 0 else None


def _convert_roman_runs(text: str) -> str:
    """把 U+2160–U+217F（Ⅰ Ⅱ Ⅲ Ⅳ … ⅿ）整段转成十进制数字串。

    必须在 NFKC **之前**执行：NFKC 会把 `Ⅱ` 变成 ASCII `II`，
    之后再想识别罗马数字就会误伤 `CSI` / `I Am` 这类天然含 I/V/X/L/C/D/M 的标题。
    """

    def _repl(match: "re.Match[str]") -> str:
        ascii_run = unicodedata.normalize("NFKC", match.group(0)).upper()
        val = _roman_to_int(ascii_run)
        return str(val) if val is not None else match.group(0)

    return _ROMAN_RUN.sub(_repl, text)


# ---------------------------------------------------------------- 中文数字

_CN_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "壹": 1, "二": 2, "贰": 2, "貳": 2, "两": 2, "兩": 2,
    "三": 3, "叁": 3, "四": 4, "肆": 4, "五": 5, "伍": 5, "六": 6, "陆": 6, "陸": 6,
    "七": 7, "柒": 7, "八": 8, "捌": 8, "九": 9, "玖": 9,
}
_CN_UNITS = {"十": 10, "拾": 10, "百": 100, "佰": 100, "千": 1000, "仟": 1000}


def chinese_to_int(text: str) -> Optional[int]:
    """中文数字 / 阿拉伯数字 / 罗马数字 → int。

    支持 `二`、`十`、`二十`、`一百零八`、`一百二十八`、`2`、`II`。
    无法解析返回 None。
    """
    s = str(text or "").strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    if re.fullmatch(r"[IVXLCDMivxlcdm]+", s):
        val = _roman_to_int(s)
        return val
    total = 0
    section = 0
    number = 0
    for ch in s:
        if ch in _CN_UNITS:
            unit = _CN_UNITS[ch]
            if number == 0:
                number = 1
            section += number * unit
            number = 0
            total += section if unit >= 1000 else 0
            if unit >= 1000:
                section = 0
        elif ch in _CN_DIGITS:
            number = _CN_DIGITS[ch]
        else:
            return None
    return total + section + number


# ---------------------------------------------------------------- 括号剥离

_BRACKET_OPEN = "([{（【《〈「『"
_BRACKET_CLOSE_MAP = {
    ")": "(", "]": "[", "}": "{", "）": "（", "】": "【",
    "》": "《", "〉": "〈", "」": "「", "』": "『",
}


def _strip_brackets(text: str) -> str:
    """剥离所有成对括号 **及其内容**（支持嵌套），括号位置留一个空格。"""
    out: List[str] = []
    stack: List[str] = []
    for ch in text:
        if ch in _BRACKET_OPEN:
            stack.append(ch)
            out.append(" ")
        elif ch in _BRACKET_CLOSE_MAP:
            if stack and stack[-1] == _BRACKET_CLOSE_MAP[ch]:
                stack.pop()
                out.append(" ")
            else:
                if not stack:
                    out.append(ch)
        else:
            if not stack:
                out.append(ch)
    return "".join(out)


# ---------------------------------------------------------------- 词表

# 质量 / 版本词（步骤 7）。长词在前，避免 `中字` 抢掉 `中文字幕`。
_QUALITY_WORDS: Sequence[str] = (
    "2160p", "1080p", "720p", "480p", "4k", "8k", "web-dl", "webrip", "bluray",
    "bdrip", "hdr10", "dolby", "dts", "aac", "bd", "hd", "ts", "tc", "dvd",
    "hdr", "sd",
    "中文字幕", "双语字幕", "导演剪辑版", "未删减版", "无删减版", "超前点映",
    "国语中字", "粤语中字", "未删减", "无删减", "修复版", "重制版", "加长版",
    "完整版", "典藏版", "抢先版", "先行版", "剧场版", "臻彩版", "高码率",
    "普通话", "中文字", "英文字幕", "蓝光", "高清", "超清", "枪版", "抢版",
    "国语", "粤语", "中字", "双字", "双语", "原声", "外挂", "内嵌", "花絮",
    "彩蛋", "预告", "抢先", "抢鲜", "独播", "全网", "独家", "更新",
)
_QUALITY_PATTERNS: List[Tuple[str, "re.Pattern[str]"]] = []
for _w in sorted(_QUALITY_WORDS, key=len, reverse=True):
    if re.fullmatch(r"[a-z0-9\-]+", _w):
        _QUALITY_PATTERNS.append((_w, re.compile(r"\b" + re.escape(_w) + r"\b")))
    else:
        _QUALITY_PATTERNS.append((_w, re.compile(re.escape(_w))))

# 标点与空白（步骤 9）
_PUNCT_PATTERN = re.compile(
    r"[\s_\-\.\/:：·—～~!！\?？,，。;；'\"“”‘’、|\[\]{}()（）【】《》<>「」『』]+"
)

# 年份（步骤 4——提前执行）
_YEAR_PATTERN = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")

# 序号（步骤 6，§4.2；已去掉 `期`，并给尾缀加前置断言）
_CN_NUM_CLASS = r"一二三四五六七八九十百零〇两\d"
_SEQ_PATTERNS: Sequence["re.Pattern[str]"] = (
    # 第X季 / 第X部 / 第X系列（中文 / 阿拉伯 / 罗马）
    re.compile(r"第\s*([" + _CN_NUM_CLASS + r"]{1,6}|[ivxlcdm]{1,6})\s*(?:季|部|系列)"),
    re.compile(r"season\s*(\d{1,2})"),
    re.compile(r"\bs\s*(\d{1,2})\b"),
    re.compile(r"part\s*(\d{1,2})"),
    # 尾缀序号：2 无间道2 / 无间道 2 / 无间道_2 / 无间道-II
    re.compile(r"(?<![0-9a-z])[\s_\-]?([2-9]|[ivxlcdm]{1,5})$"),
)

# 集数 / 更新描述（步骤 8）
_EP_STRIP_PATTERNS: Sequence["re.Pattern[str]"] = (
    re.compile(r"更新至\s*(?:第\s*)?(?:[0-9]{1,4}|[" + _CN_NUM_CLASS + r"]{1,4})\s*[集话話期]?"),
    re.compile(r"更新到\s*(?:第\s*)?(?:[0-9]{1,4}|[" + _CN_NUM_CLASS + r"]{1,4})\s*[集话話期]?"),
    re.compile(r"至\s*(?:第\s*)?(?:[0-9]{1,4})\s*集"),
    re.compile(r"第\s*(?:[0-9]{1,4}|[" + _CN_NUM_CLASS + r"]{1,4})\s*[集话話期]"),
    re.compile(r"全\s*(?:[0-9]{1,4}|[" + _CN_NUM_CLASS + r"]{1,4})\s*[集话話期]"),
    re.compile(r"\bep?\s*\d{1,4}\b"),
)

# 尾部孤立数字（步骤 10）
_TRAILING_DIGIT_PATTERN = re.compile(r"(?<![0-9])(\d{1,2})$")


# ---------------------------------------------------------------- 输出结构


@dataclass(frozen=True)
class TitleParts:
    """标题归一化结果（§4.3）。"""

    raw: str = ""
    norm_title: str = ""
    seq: int = 1
    year: Optional[int] = None
    category: str = ""
    quality_tags: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def merge_key(self) -> str:
        """§4.3 合并键 `f"{category}|{norm_title}|{seq}"` —— 只做精确相等比较。"""
        return f"{self.category}|{self.norm_title}|{self.seq}"

    @property
    def merge_group(self) -> str:
        """粗分组键 `f"{category}|{norm_title}"`（不含 seq / year）。

        §4.3 的 `merge_key` 含 seq，而 §4.4 又要求前 6 行 merge_key 全相等
        ——但表中 `无间道` 的 seq=1、`无间道2` 的 seq=2，二者不可能同键。
        为同时满足"seq 列"与"变体归一"，本模块额外提供不含 seq 的粗分组键：
        同一节目的不同部/季会落在同一 `merge_group`，再按 seq / year 细分。
        """
        return f"{self.category}|{self.norm_title}"

    def as_dict(self) -> dict:
        return {
            "raw": self.raw,
            "norm_title": self.norm_title,
            "seq": self.seq,
            "year": self.year,
            "category": self.category,
            "quality_tags": list(self.quality_tags),
            "merge_key": self.merge_key,
            "merge_group": self.merge_group,
        }


# ---------------------------------------------------------------- 主流程


def normalize_title(raw: str, category: str = "") -> TitleParts:
    """把原始标题归一化成 `TitleParts`。

    Args:
        raw: 原始标题，如 `無間道Ⅱ`、`歌手2024 第3期`。
        category: 入口分类（movies/tv/anime/variety），参与合并键；空串表示未知。

    Returns:
        `TitleParts`。任何异常都不会抛出，最差返回可安全比较的退化结果。
    """
    text = "" if raw is None else str(raw).strip()
    if not text:
        return TitleParts(raw="", norm_title="", seq=1, year=None, category=category or "")

    # 步骤 0：罗马数字（U+2160–U+217F）→ 十进制，必须在 NFKC 之前
    s = _convert_roman_runs(text)
    # 步骤 1：NFKC（全角→半角等）
    s = unicodedata.normalize("NFKC", s)
    # 步骤 2 + 2b：繁简转换 + 异体字收敛
    s = to_simplified(s)
    # 步骤 3：转小写
    s = s.lower()

    # 质量标签（在剥离前采集，仅作诊断用）
    quality_tags = tuple(
        sorted({word for word, pat in _QUALITY_PATTERNS if pat.search(s)})
    )

    # 步骤 4（提前）：提取年份。若整个标题就是一个年份（`2012`），
    # 则不当作年份处理，否则标题会被剥空。
    year: Optional[int] = None
    core_probe = _PUNCT_PATTERN.sub("", s)
    year_match = _YEAR_PATTERN.search(s)
    if year_match and year_match.group(1) != core_probe:
        year = int(year_match.group(1))
        s = s[: year_match.start()] + " " + s[year_match.end():]

    # 步骤 5：剥离括号及其内容
    s = _strip_brackets(s)
    # 兜底标题：仅做了括号/标点剥离，未剥 year/seq/质量词，防止结果为空
    fallback = _PUNCT_PATTERN.sub("", s)

    # 步骤 6：提取季/部序号 seq
    seq: Optional[int] = None
    for pattern in _SEQ_PATTERNS:
        match = pattern.search(s)
        if not match:
            continue
        value = chinese_to_int(match.group(1))
        if value is None or value <= 0:
            continue
        seq = value
        s = s[: match.start()] + " " + s[match.end():]
        break

    # 步骤 7：剥离质量 / 版本词
    for _word, pattern in _QUALITY_PATTERNS:
        s = pattern.sub(" ", s)

    # 步骤 8：剥离集数 / 更新描述
    for pattern in _EP_STRIP_PATTERNS:
        s = pattern.sub(" ", s)

    # 步骤 9：剥离标点与空白
    norm = _PUNCT_PATTERN.sub("", s)

    # 步骤 10：剥离尾部孤立数字（作为 seq 的兜底来源）
    if seq is None:
        trailing = _TRAILING_DIGIT_PATTERN.search(norm)
        if trailing:
            value = chinese_to_int(trailing.group(1))
            if value is not None and 1 <= value <= 99:
                seq = value
                norm = norm[: trailing.start()]
            else:  # pragma: no cover - 正则已保证是数字
                seq = 1
        else:
            seq = 1

    if not norm:
        norm = fallback
    if not norm:
        norm = _PUNCT_PATTERN.sub("", to_simplified(unicodedata.normalize("NFKC", text)).lower())

    return TitleParts(
        raw=text,
        norm_title=norm,
        seq=int(seq) if seq and seq > 0 else 1,
        year=year,
        category=category or "",
        quality_tags=quality_tags,
    )


def title_merge_key(raw: str, category: str = "") -> str:
    """便捷函数：直接拿 §4.3 的合并键。"""
    return normalize_title(raw, category).merge_key


def title_merge_group(raw: str, category: str = "") -> str:
    """便捷函数：直接拿不含 seq 的粗分组键。"""
    return normalize_title(raw, category).merge_group


def extract_seq(text: str) -> Optional[int]:
    """从任意文本（如 TMDB 候选名）中解析部/季序号；解析不到返回 None。

    供 §6.1 的 S7「序号一致」信号复用。
    """
    parts = normalize_title(text)
    if parts.seq and parts.seq > 1:
        return parts.seq
    norm = parts.norm_title
    for pattern in _SEQ_PATTERNS:
        match = pattern.search(norm)
        if not match:
            continue
        value = chinese_to_int(match.group(1))
        if value:
            return value
    return None
