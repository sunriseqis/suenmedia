# -*- coding: utf-8 -*-
"""
taxonomy.py —— 分类标签体系词表与权威校正 (集中管理, 与子任务 4 白名单共用 config.json 来源)

设计 (子任务 5, 「刮削权威数据优先」):
- category (movies/tv/anime/variety) 与 region/group_name 均以刮削命中后的权威数据
  (TMDB/豆瓣/OMDb/Bilibili 的 genres/country/original_language/端点类型) 为准;
  源站 sub_category 只做入口粗筛与未命中回退。
- 词表三来源 (优先级递减):
  1) config.json 的 REGION_RULES / GENRE_RULES (可运营期调整)
  2) 本模块 DEFAULT_* (与 config 同构的内置默认)
  3) 兜底常量 (其他剧 / 未分类)
- 地区词 (内地/国产/港/台/泰/韩/日/美/欧/海外...) 永不作 group_name, 只参与 region 判定。
"""
import re
from functools import lru_cache

from common import load_config

# ---------------- 内置默认词表 ----------------

DEFAULT_COUNTRY_REGION_MAP = {
    # 中国大陆 -> 国产剧
    "cn": "国产剧", "中国大陆": "国产剧", "大陆": "国产剧", "中国": "国产剧",
    "china": "国产剧", "mainland china": "国产剧", "prc": "国产剧", "中华人民共和国": "国产剧",
    # 港台 -> 并入国产剧 (用户第④条)
    "hk": "国产剧", "中国香港": "国产剧", "香港": "国产剧", "hong kong": "国产剧",
    "tw": "国产剧", "中国台湾": "国产剧", "台湾": "国产剧", "taiwan": "国产剧",
    # 日韩 -> 日韩剧
    "kr": "日韩剧", "韩国": "日韩剧", "south korea": "日韩剧", "korea": "日韩剧",
    "jp": "日韩剧", "日本": "日韩剧", "japan": "日韩剧",
    # 东南亚/南亚/蒙古等 -> 海外剧
    "th": "海外剧", "泰国": "海外剧", "thailand": "海外剧",
    "vn": "海外剧", "越南": "海外剧", "vietnam": "海外剧",
    "my": "海外剧", "马来西亚": "海外剧", "malaysia": "海外剧",
    "sg": "海外剧", "新加坡": "海外剧", "singapore": "海外剧",
    "id": "海外剧", "印度尼西亚": "海外剧", "印尼": "海外剧", "indonesia": "海外剧",
    "ph": "海外剧", "菲律宾": "海外剧", "philippines": "海外剧",
    "kh": "海外剧", "柬埔寨": "海外剧", "mm": "海外剧", "缅甸": "海外剧",
    "in": "海外剧", "印度": "海外剧", "india": "海外剧",
    "pk": "海外剧", "巴基斯坦": "海外剧", "lk": "海外剧", "斯里兰卡": "海外剧",
    "np": "海外剧", "尼泊尔": "海外剧", "mn": "海外剧", "蒙古": "海外剧",
    "tr": "海外剧", "土耳其": "海外剧", "turkey": "海外剧", "turkiye": "海外剧",
    # 欧美 -> 美剧
    "us": "美剧", "美国": "美剧", "usa": "美剧", "united states": "美剧",
    "gb": "美剧", "uk": "美剧", "英国": "美剧", "united kingdom": "美剧",
    "fr": "美剧", "法国": "美剧", "france": "美剧",
    "de": "美剧", "德国": "美剧", "germany": "美剧",
    "it": "美剧", "意大利": "美剧", "italy": "美剧",
    "es": "美剧", "西班牙": "美剧", "spain": "美剧",
    "ca": "美剧", "加拿大": "美剧", "canada": "美剧",
    "au": "美剧", "澳大利亚": "美剧", "australia": "美剧",
    "ru": "美剧", "俄罗斯": "美剧", "russia": "美剧",
    "nl": "美剧", "荷兰": "美剧", "be": "美剧", "比利时": "美剧",
    "se": "美剧", "瑞典": "美剧", "no": "美剧", "挪威": "美剧",
    "dk": "美剧", "丹麦": "美剧", "fi": "美剧", "芬兰": "美剧",
    "pl": "美剧", "波兰": "美剧", "cz": "美剧", "捷克": "美剧",
    "at": "美剧", "奥地利": "美剧", "ch": "美剧", "瑞士": "美剧",
    "ie": "美剧", "爱尔兰": "美剧", "nz": "美剧", "新西兰": "美剧",
    "br": "美剧", "巴西": "美剧", "mx": "美剧", "墨西哥": "美剧",
    "ar": "美剧", "阿根廷": "美剧", "za": "美剧", "南非": "美剧",
}

DEFAULT_LANGUAGE_REGION_MAP = {
    # TMDB ISO 639-1 / 豆瓣与 OMDb 语言名 -> 地区桶 (country 判不出时辅助)
    "zh": "国产剧", "cn": "国产剧", "普通话": "国产剧", "汉语": "国产剧", "国语": "国产剧",
    "粤语": "国产剧", "闽南语": "国产剧", "中文": "国产剧", "汉语普通话": "国产剧",
    "ko": "日韩剧", "ja": "日韩剧", "韩语": "日韩剧", "日语": "日韩剧",
    "th": "海外剧", "vi": "海外剧", "id": "海外剧", "ms": "海外剧", "si": "海外剧",
    "ta": "海外剧", "hi": "海外剧", "bn": "海外剧", "tl": "海外剧", "km": "海外剧",
    "my": "海外剧", "ne": "海外剧", "tr": "海外剧",
    "泰语": "海外剧", "越南语": "海外剧", "印尼语": "海外剧", "马来语": "海外剧",
    "印地语": "海外剧", "土耳其语": "海外剧", "泰卢固语": "海外剧", "泰米尔语": "海外剧",
    "en": "美剧", "fr": "美剧", "de": "美剧", "es": "美剧", "it": "美剧",
    "ru": "美剧", "pt": "美剧", "sv": "美剧", "no": "美剧", "da": "美剧",
    "fi": "美剧", "nl": "美剧", "pl": "美剧", "cs": "美剧", "el": "美剧",
    "英语": "美剧", "法语": "美剧", "德语": "美剧", "西班牙语": "美剧",
    "意大利语": "美剧", "俄语": "美剧", "葡萄牙语": "美剧", "瑞典语": "美剧",
    "荷兰语": "美剧", "波兰语": "美剧", "丹麦语": "美剧", "挪威语": "美剧",
}

# 源站 sub_category 回退用地区词 (按序匹配, 前面优先)
DEFAULT_SUB_REGION_WORDS = [
    ("日韩剧", ["韩", "日", "日本", "日剧"]),
    ("美剧", ["美", "欧美", "欧", "英", "西方"]),
    ("海外剧", ["泰", "马泰", "海外", "马", "新", "越", "印", "印尼", "俄"]),
    ("国产剧", ["内地", "国产", "大陆", "港台", "香港", "台湾", "港剧", "台剧"]),
]
# 注: 地区词用精确词 ("香港"/"台湾"/"港剧"/"台剧") 而非裸 "港"/"台",
#     避免误吞 "翡翠台"/"平台" 等非地区词; 无法识别一律 -> 其他剧 (不塞国产剧)

# 题材同义映射 (源站粗词 / 英文 genre -> 规范题材名), 用户第⑤条: 同义标签全局合并
DEFAULT_GENRE_SYNONYMS = {
    # 中文源站粗词 -> 规范
    "动作片": "动作", "喜剧片": "喜剧", "爱情片": "爱情", "科幻片": "科幻",
    "恐怖片": "恐怖", "剧情片": "剧情", "战争片": "战争", "悬疑片": "悬疑",
    "惊悚片": "惊悚", "犯罪片": "犯罪", "奇幻片": "奇幻", "灾难片": "灾难",
    "冒险片": "冒险", "纪录片": "纪录", "动画片": "动画", "故事片": "剧情",
    "古装剧": "古装", "武侠剧": "武侠", "偶像剧": "偶像", "都市剧": "都市",
    "家庭剧": "家庭", "历史剧": "历史", "军旅剧": "军事", "谍战剧": "谍战",
    "刑侦剧": "刑侦", "仙侠剧": "仙侠", "权谋剧": "权谋",
    # TMDB zh-CN genres (个别 TMDB 译名归一)
    "动作冒险": "冒险", "肥皂剧": "都市", "儿童": "家庭", "纪录": "纪录",
    "现实": "真人秀", "Talk": "脱口秀", "Reality": "真人秀",
    # OMDb/IMDb 英文 genres -> 规范
    "comedy": "喜剧", "drama": "剧情", "action": "动作", "romance": "爱情",
    "sci-fi": "科幻", "thriller": "惊悚", "horror": "恐怖", "crime": "犯罪",
    "adventure": "冒险", "animation": "动画", "fantasy": "奇幻",
    "mystery": "悬疑", "family": "家庭", "war": "战争", "history": "历史",
    "documentary": "纪录", "music": "音乐", "musical": "歌舞", "biography": "传记",
    "sport": "运动", "sports": "运动", "western": "西部", "talk-show": "脱口秀",
    "reality-tv": "真人秀", "news": "新闻", "game-show": "游戏", "short": "短片",
}

# 规范题材白名单 (group_name 只允许落在这里)
DEFAULT_GENRE_CANONICAL = [
    "剧情", "喜剧", "动作", "爱情", "科幻", "动画", "悬疑", "惊悚", "恐怖",
    "犯罪", "冒险", "历史", "战争", "奇幻", "音乐", "歌舞", "家庭", "灾难",
    "西部", "传记", "运动", "纪录", "古装", "武侠", "偶像", "都市", "军事",
    "谍战", "刑侦", "仙侠", "权谋", "真人秀", "脱口秀", "游戏", "新闻", "短片",
]

# category -> region 兜底桶
REGION_DEFAULT = {
    "movies": "电影",
    "tv": "其他剧",      # 兜底原则: 宁可"其他剧", 不塞"国产剧"
    "anime": "动漫",
    "variety": "综艺",
}
GROUP_UNCLASSIFIED = "未分类"

# TMDB tv genres 中的类目改判信号
_ANIME_GENRE_HINTS = ("动画", "Animation", "Comedy Animation")
_VARIETY_GENRE_HINTS = ("真人秀", "脱口秀", "Reality", "Talk")

# 历史产物遗留 region 的归一映射 (用户第②④⑤条: 全局合并)
LEGACY_REGION_ALIASES = {
    "港台剧": "国产剧",
    "内地剧": "国产剧",
    "国产": "国产剧",
}

# ---------------- 词表加载 (config.json 覆盖内置默认) ----------------

@lru_cache(maxsize=1)
def _rules():
    cfg = load_config() or {}
    region_rules = cfg.get("REGION_RULES") or {}
    genre_rules = cfg.get("GENRE_RULES") or {}
    country_map = dict(DEFAULT_COUNTRY_REGION_MAP)
    country_map.update({str(k): v for k, v in (region_rules.get("country_map") or {}).items()})
    language_map = dict(DEFAULT_LANGUAGE_REGION_MAP)
    language_map.update({str(k): v for k, v in (region_rules.get("language_map") or {}).items()})
    sub_words = list(DEFAULT_SUB_REGION_WORDS)
    for bucket, words in (region_rules.get("sub_region_words") or {}).items():
        merged = False
        for i, (b, _w) in enumerate(sub_words):
            if b == bucket:
                sub_words[i] = (b, list(words))
                merged = True
                break
        if not merged:
            sub_words.append((bucket, list(words)))
    synonyms = dict(DEFAULT_GENRE_SYNONYMS)
    synonyms.update({str(k): v for k, v in (genre_rules.get("synonyms") or {}).items()})
    canonical = list(genre_rules.get("canonical") or DEFAULT_GENRE_CANONICAL)
    return {"country_map": country_map, "language_map": language_map,
            "sub_region_words": sub_words, "genre_synonyms": synonyms,
            "genre_canonical": canonical}


def clear_rules_cache():
    """运行期改写 config.json 后调用, 使词表重新加载"""
    _rules.cache_clear()

# ---------------- category 权威校正 ----------------

def authoritative_category(source_provider: str, meta: dict) -> tuple[str, str]:
    """
    刮削命中后用权威数据复核四类归属。
    返回 (category, reason); category 为 None 表示不在四类 -> 条目丢弃。
    """
    source = (source_provider or "").strip()
    genres = [str(g) for g in (meta or {}).get("genres") or []]
    genre_blob = " ".join(genres)

    def _is_documentary() -> bool:
        # 纪录片不在四类, 命中即丢弃 (用户: 只要电影/电视剧/综艺/动漫)
        return ("纪录" in genre_blob) or ("documentary" in genre_blob.lower())

    def _split_tv_by_genres():
        if _is_documentary():
            return None, f"tv genres 命中纪录片 ({genre_blob}), 丢弃"
        if any(h in genre_blob for h in _ANIME_GENRE_HINTS):
            return "anime", f"tv genres 命中动画类 ({genre_blob})"
        if any(h in genre_blob for h in _VARIETY_GENRE_HINTS):
            return "variety", f"tv genres 命中综艺类 ({genre_blob})"
        return "tv", "tv 端点命中, genres 无动漫/综艺信号"

    if source == "TMDB":
        endpoint = (meta or {}).get("tmdb_endpoint") or ""
        if not endpoint:
            return "", "TMDB 旧缓存无端点信号, 保持入口分类"
        if endpoint == "movie":
            if _is_documentary():
                return None, f"movie genres 命中纪录片 ({genre_blob}), 丢弃"
            if "动画" in genre_blob:
                # 动画电影仍属电影类 (如皮克斯/宫崎骏), 不改判 anime
                return "movies", "movie 端点命中 (动画电影保留 movies)"
            return "movies", "movie 端点命中"
        return _split_tv_by_genres()
    if source == "豆瓣":
        kind = (meta or {}).get("douban_kind") or ""
        if not kind:
            return "", "豆瓣旧缓存无类型信号, 保持入口分类"
        if _is_documentary():
            return None, f"豆瓣 genres 命中纪录片 ({genre_blob}), 丢弃"
        if kind == "tv":
            return _split_tv_by_genres()
        return "movies", "豆瓣 movie 端点命中"
    if source == "OMDb":
        otype = (meta or {}).get("omdb_type") or ""
        if not otype:
            return "", "OMDb 旧缓存无 Type 信号, 保持入口分类"
        if _is_documentary():
            return None, f"OMDb Genre 命中 Documentary ({genre_blob}), 丢弃"
        if otype == "series":
            return _split_tv_by_genres()
        if otype == "movie":
            if "动画" in genre_blob or "Animation" in genre_blob:
                return "anime", "OMDb movie 但 Genre 含 Animation"
            return "movies", "OMDb Type=movie"
        return None, f"OMDb Type={otype} 不在四类 (丢弃)"
    if source == "Bilibili":
        return "anime", "Bilibili 番剧条目"
    if source == "TheTVDB":
        return _split_tv_by_genres()
    # 其它/未知来源: 不校正
    return "", "无权威信号, 保持入口分类"

# ---------------- region (地区桶) ----------------

def region_from_country(country: str) -> str:
    """国家名/ISO 代码 (支持 '泰国' / 'USA, France' / 'CN' / TMDB zh-CN 国名) -> 地区桶"""
    if not country:
        return ""
    for token in re.split(r'[,，/、;；\s]+', str(country)):
        t = token.strip()
        if not t:
            continue
        bucket = _rules()["country_map"].get(t) or _rules()["country_map"].get(t.lower())
        if bucket:
            return bucket
    return ""

def region_from_language(lang: str) -> str:
    """语言 (ISO 639-1 / 中文语言名, 支持逗号分隔取首个可判定者) -> 地区桶"""
    if not lang:
        return ""
    for token in re.split(r'[,，/、;；\s]+', str(lang)):
        t = token.strip()
        if not t:
            continue
        bucket = _rules()["language_map"].get(t) or _rules()["language_map"].get(t.lower())
        if bucket:
            return bucket
    return ""

def region_from_sub_category(sub_category: str) -> str:
    """无刮削命中时回退: 源站 sub_category 地区词 -> 地区桶; 无法识别 -> 其他剧 (不塞国产剧)"""
    sub = str(sub_category or "")
    for bucket, words in _rules()["sub_region_words"]:
        if any(w in sub for w in words):
            return bucket
    return REGION_DEFAULT["tv"]

def resolve_region(category: str, item: dict) -> str:
    """
    region 判定优先级:
    1) 刮削命中: country (豆瓣/OMDb 国家名、TMDB production_countries) -> language 辅助
    2) 未命中: 源站 sub_category 地区词
    3) 兜底: movies/anime/variety 用固定桶; tv 无法识别 -> 其他剧
    """
    cat = (category or "").strip()
    if cat != "tv":
        return REGION_DEFAULT.get(cat, REGION_DEFAULT["movies"])
    if item.get("matched"):
        region = region_from_country(item.get("country", ""))
        if region:
            return region
        region = region_from_language(item.get("original_language", ""))
        if region:
            return region
    return region_from_sub_category(item.get("sub_category", ""))

# ---------------- group_name (题材) ----------------

def normalize_genre(word: str) -> str:
    """题材词规范化 (同义映射 + 白名单过滤); 地区词直接判死"""
    w = str(word or "").strip()
    if not w:
        return ""
    if any(rw in w for rw in ("内地", "国产", "港", "台", "泰", "韩", "日", "美", "欧", "海外", "大陆")):
        return ""
    mapped = _rules()["genre_synonyms"].get(w) or _rules()["genre_synonyms"].get(w.lower()) or w
    return mapped if mapped in _rules()["genre_canonical"] else ""

def genres_to_group(genres) -> str:
    """刮削 genres -> 主要题材 (取首个规范化成功项, TMDB 按相关性排序即首要题材)"""
    for g in (genres or []):
        n = normalize_genre(g)
        if n:
            return n
    return ""

def group_from_sub_category(sub_category: str) -> str:
    """无 genres 回退: 从源站 sub_category 拆题材词 ('国产爱情片'->爱情); 拆不出 -> 未分类"""
    sub = str(sub_category or "").strip()
    # 逐段尝试: 去地区词/类别词后查同义表
    for token in re.split(r'[\s/、,，|]+', sub):
        t = token.strip()
        if not t:
            continue
        n = normalize_genre(t)
        if n:
            return n
        # 剥掉地区前缀与'片/剧'后缀再试 ('国产爱情片'->'爱情')
        stripped = re.sub(r'^(内地|国产|大陆|港台|香港|台湾|港|台|泰国|韩|日|美|欧美|海外|马泰|马|新|越|印)+', '', t)
        stripped = re.sub(r'(片|剧|大全|推荐|热门)+$', '', stripped).strip()
        if stripped:
            n = normalize_genre(stripped)
            if n:
                return n
    return GROUP_UNCLASSIFIED

def resolve_group(category: str, item: dict) -> str:
    """
    group_name 判定优先级:
    1) 刮削命中且 genres 可用 -> 首个规范题材
    2) 回退源站 sub_category 拆题材词
    3) 拆不出 -> 未分类
    地区词永不作 group_name。
    """
    cat = (category or "").strip()
    if item.get("matched"):
        g = genres_to_group(item.get("genres"))
        if g:
            return g
    return group_from_sub_category(item.get("sub_category", ""))

# ---------------- 冲突样本 (权威校正 vs 入口粗判) ----------------

_CONFLICT_SAMPLES = []
_CONFLICT_CAP = 500

def record_conflict(entity_title: str, entry_category: str, corrected: str, reason: str):
    if len(_CONFLICT_SAMPLES) < _CONFLICT_CAP:
        _CONFLICT_SAMPLES.append({
            "title": entity_title,
            "entry_category": entry_category,
            "corrected_category": corrected,
            "reason": reason,
        })

def get_conflict_samples() -> list:
    return list(_CONFLICT_SAMPLES)

def clear_conflict_samples():
    _CONFLICT_SAMPLES.clear()

# ---------------- 历史数据 region 归一 ----------------

def normalize_legacy_region(region: str) -> str:
    """历史产物 region 别名归一 (港台剧/内地剧 -> 国产剧); 未命中原样返回"""
    return LEGACY_REGION_ALIASES.get(str(region or "").strip(), str(region or "").strip())
