import os
import json
import re
import hashlib
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from common import load_settings, clean_overview
import taxonomy

SCHEMA_VERSION = "2.1"
GENERATOR_VERSION = "suenmedia/2.1.0"
_COVER_CACHE_FILE = "cache/cover_hosts.json"

def _derive_status(raw_status, remarks, category) -> str:
    """状态推导单一来源: 严格规范化为 ended / ongoing / upcoming"""
    s = str(raw_status or "").lower()
    rem = str(remarks or "")
    if "ended" in s or "完结" in s or "完结" in rem or "全集" in rem or category == "movies":
        return "ended"
    if "upcoming" in s or "未播" in s or "预告" in rem:
        return "upcoming"
    return "ongoing"

def resolve_url_type(url: str) -> str:
    """根据 URL 特征推断播放类型，供 player 消费端免推断"""
    u = (url or "").split("?")[0].lower()
    if not (url or "").startswith(("http://", "https://")):
        return ""
    if ".m3u8" in u:
        return "m3u8"
    if u.endswith(".ts") or ".ts?" in (url or "").lower():
        return "ts"
    if ".mp4" in u:
        return "mp4"
    return "page"  # 无流扩展名的 http 链接按网页源标记, 由播放端 fresh-url 解析

def check_cover_hosts(urls: list, max_age_days: int = 7) -> dict:
    """
    封面域名探活 (域名级, 结果缓存 max_age_days 天)。
    连接失败时 DoH 仲裁防 DNS 污染误杀; 403/404 视为存活 (防盗链/单图失效不算)。
    返回 {host: True(存活)/False(死亡)}
    """
    import time as _time
    import requests
    from common import get_session

    cache = {}
    if os.path.exists(_COVER_CACHE_FILE):
        try:
            with open(_COVER_CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception as e:
            print(f"[警告] 封面域名缓存加载失败, 将全量重探: {type(e).__name__}: {e}")
            cache = {}

    hosts = {u.split("/")[2] for u in urls if u and "://" in u}
    now = _time.time()
    to_check = [h for h in hosts
                if h not in cache or now - cache[h].get("ts", 0) > max_age_days * 86400]
    if to_check:
        session = get_session()

        def _probe(h: str):
            alive = True
            try:
                resp = session.head(f"https://{h}/", timeout=3, allow_redirects=True)
                resp.close()
            except requests.exceptions.RequestException:
                # head 失败再试一次 GET, 避免误杀不支持 HEAD 的图床
                try:
                    session.get(f"https://{h}/", timeout=3, stream=True, allow_redirects=True).close()
                except Exception:
                    # 直连失败: DoH 仲裁, 防本机 DNS 污染误杀 (DoH 有 A 记录 = 域名活着)
                    try:
                        r = requests.get("https://dns.alidns.com/resolve",
                                         params={"name": h, "type": "A"}, timeout=5)
                        answers = r.json().get("Answer") or []
                        alive = any(a.get("type") == 1 and a.get("data") for a in answers)
                    except Exception:
                        alive = False
            return h, alive

        cache_lock = threading.Lock()

        def _probe_and_store(h: str):
            host, alive = _probe(h)
            with cache_lock:
                cache[host] = {"alive": alive, "ts": now}

        # 并行探活: 域名数可达数百个, 串行 3s 超时累积过慢
        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(_probe_and_store, to_check))
        try:
            os.makedirs(os.path.dirname(_COVER_CACHE_FILE), exist_ok=True)
            with open(_COVER_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False)
        except Exception as e:
            print(f"[警告] 封面域名缓存写入失败: {type(e).__name__}: {e}")
    return {h: v.get("alive", True) for h, v in cache.items()}

def _natural_episode_key(ep: dict):
    """提取集数中的数字用于自然排序，如 '第01集' -> 1"""
    name = ep.get("name", "")
    m = re.search(r'\d+', name)
    return int(m.group(0)) if m else 9999

def resolve_region_and_group(category: str, sub_category: str, item: dict | None = None) -> tuple[str, str]:
    """计算 v2.1 规范所需的 region (一级分类) 与 group_name (二级分组)

    子任务 5「刮削权威数据优先」: matched 条目的 region/group_name 来自刮削权威
    字段 (TMDB/豆瓣/OMDb/Bilibili 的 country/original_language/genres), 源站
    sub_category 仅作未命中回退; 地区词永不作 group_name; 词表集中在 taxonomy.py
    (可经 config.json 的 REGION_RULES/GENRE_RULES 运营期调整)。
    """
    it = dict(item) if item else {}
    # item 未带 sub_category 时回落到函数参数 (兼容仅传 sub 的旧调用)
    it.setdefault("sub_category", sub_category or "")
    cat = (category or "").strip()

    if cat == "movies":
        # 电影 region 保留"电影"大桶 (地区维度由 group_name 题材承担)
        return "电影", taxonomy.resolve_group(cat, it)
    region = taxonomy.resolve_region(cat, it)
    group = taxonomy.resolve_group(cat, it)
    return region, group

class MediaAggregator:
    def __init__(self):
        self.settings = load_settings()
        self.categories = ["movies", "tv", "anime", "variety"]  # 综艺暂保留, 短剧弃
        self.catalog = {cat: {} for cat in self.categories}
        self.unmatched = {}
        # 增量导出脏标记 (进程内): _load_existing_data 加载后先全量置脏
        self._dirty_cats = set(self.categories)
        self._dirty_cats.add("unmatched")
        self._lock = threading.Lock()
        self._load_existing_data()

    def _restore_item_from_v21(self, item: dict, fallback_cat: str = ""):
        """从 v2.1 格式条目还原内部 catalog 对象"""
        cat = item.get("category")
        reg = item.get("region", "")
        # 历史产物 region 归一 (子任务5 第②④⑤条): 港台剧/内地剧 -> 国产剧
        reg = taxonomy.normalize_legacy_region(reg)
        if reg:
            item["region"] = reg
        if not cat:
            if reg == "电影":
                cat = "movies"
            elif reg in ["国产剧", "美剧", "日韩剧", "港台剧", "海外剧", "其他剧"]:
                cat = "tv"
            elif reg == "动漫":
                cat = "anime"
            elif reg == "综艺":
                cat = "variety"
            elif reg == "微短剧":
                cat = "short_tv"
            else:
                cat = fallback_cat or "movies"

        if cat not in self.catalog:
            return  # 历史产物中的非保留类目, 不再载入

        entity_id = item.get("bangou") or item.get("id")
        if not entity_id:
            return

        if "sources" not in item:
            if item.get("type") == "video" and item.get("url"):
                item["sources"] = [{
                    "site": item.get("site", "网络源"),
                    "line_name": "默认线路",
                    "from": "default",
                    "episodes": [{"name": "正片", "url": item["url"]}]
                }]
                for alt in item.get("alt_urls", []):
                    if alt.get("url"):
                        item["sources"].append({
                            "site": alt.get("source", "备用源"),
                            "line_name": alt.get("label", "备用"),
                            "from": "alt",
                            "episodes": [{"name": "正片", "url": alt["url"]}]
                        })
            elif item.get("seasons"):
                primary_eps = []
                alt_lines = {}
                for s in item.get("seasons", []):
                    for ep in s.get("episodes", []):
                        primary_eps.append({"name": ep.get("ep_title", "正片"), "url": ep.get("url", "")})
                        for alt in ep.get("alt_urls", []):
                            src_name = alt.get("source", "备用源")
                            alt_lines.setdefault(src_name, []).append({"name": ep.get("ep_title", "正片"), "url": alt.get("url", "")})
                item["sources"] = [{
                    "site": item.get("site", "网络源"),
                    "line_name": "默认线路",
                    "from": "default",
                    "episodes": primary_eps
                }]
                for src_name, a_eps in alt_lines.items():
                    item["sources"].append({
                        "site": src_name,
                        "line_name": f"{src_name}-线路",
                        "from": "alt",
                        "episodes": a_eps
                    })
            else:
                item["sources"] = []

        item["total_episodes"] = max((len(s.get("episodes", [])) for s in item.get("sources", [])), default=0)
        item["category"] = cat
        item.setdefault("sub_category", item.get("group_name", ""))
        item.setdefault("remarks", "")
        item.setdefault("poster", item.get("cover", ""))
        item.setdefault("season", 1)
        for _k, _d in (("popularity", 0.0), ("runtime", 0), ("homepage", ""), ("logo", ""),
                       ("certification", ""), ("cast_structured", []), ("director_structured", []),
                       ("season_episodes", {}), ("season_meta", {}), ("season", 1)):
            item.setdefault(_k, _d)
        self.catalog[cat][entity_id] = item

    def _load_existing_data(self):
        """加载已持久化的历史数据（优先加载 product/videos.json，平滑兼容旧路径）"""
        loaded = False
        candidates = ["product/videos.json", "videos.json", "json/all/videos.json"]
        for cand in candidates:
            if os.path.exists(cand):
                try:
                    with open(cand, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        items_array = data.get("items", []) if isinstance(data, dict) else data
                        for item in items_array:
                            self._restore_item_from_v21(item)
                        if items_array:
                            loaded = True
                            break
                except Exception as e:
                    print(f"[警告] 加载历史归档失败 {cand}: {e}")

        if not loaded:
            for cat in self.categories:
                cat_dir = f"json/{cat}"
                if not os.path.exists(cat_dir):
                    continue
                for fname in os.listdir(cat_dir):
                    if fname.startswith("part_") and fname.endswith(".json"):
                        fpath = os.path.join(cat_dir, fname)
                        try:
                            with open(fpath, "r", encoding="utf-8") as f:
                                data = json.load(f)
                                items_array = data.get("items", []) if isinstance(data, dict) else data
                                for item in items_array:
                                    self._restore_item_from_v21(item, fallback_cat=cat)
                        except Exception as e:
                            print(f"[警告] 加载历史存档失败 {fpath}: {e}")

        unmatched_candidates = ["product/unmatched.json", "json/unmatched/unmatched.json"]
        unmatched_file = next((c for c in unmatched_candidates if os.path.exists(c)), None)
        if unmatched_file:
            try:
                with open(unmatched_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    items_array = data.get("items", []) if isinstance(data, dict) else data
                    for item in items_array:
                        eid = item.get("bangou") or item.get("id")
                        if eid:
                            item.setdefault("sources", [])
                            item.setdefault("category", "unmatched")
                            item.setdefault("sub_category", "")
                            item.setdefault("remarks", "")
                            item["total_episodes"] = max((len(s.get("episodes", [])) for s in item.get("sources", [])), default=0)
                            self.unmatched[eid] = item
            except Exception as e:
                print(f"[警告] 加载 unmatched 桶失败 {unmatched_file}: {type(e).__name__}: {e}")

    def add_item(self, item: dict):
        """归并单个采集项，进行多源跨线路去重与集数合并"""
        category = item.get("category", "movies")
        if category not in self.catalog:
            return  # 白名单外类目(discard/short_tv 等)直接丢弃, 不进库不进 unmatched
        with self._lock:
            self._dirty_cats.add(category)

        canonical_title = item.get("canonical_title") or item.get("title", "未知标题")
        year = str(item.get("year") or "").strip()
        bangou = item.get("bangou")

        if bangou:
            entity_id = bangou
        elif item.get("tmdb_id"):
            entity_id = f"tmdb_{item['tmdb_id']}"
        elif item.get("douban_id"):
            entity_id = f"douban_{item['douban_id']}"
        else:
            base_str = f"{category}:{canonical_title}:{year}"
            entity_id = "v_" + hashlib.md5(base_str.encode("utf-8")).hexdigest()[:12]

        is_matched = item.get("matched", False)
        target_dict = self.catalog[category] if is_matched else self.unmatched
        if not is_matched:
            with self._lock:
                self._dirty_cats.add("unmatched")

        if entity_id not in target_dict:
            region, group_name = resolve_region_and_group(category, item.get("sub_category", ""), item)
            target_dict[entity_id] = {
                "bangou": entity_id,
                "title": canonical_title,
                "season": item.get("season") or 1,
                "original_title": item.get("original_title", ""),
                "category": category,
                "sub_category": item.get("sub_category", ""),
                "region": region,
                "group_name": group_name,
                "year": year,
                "first_air_date": item.get("first_air_date", ""),
                "site": item.get("site", "网络源"),
                "poster": item.get("poster", ""),
                "backdrop": item.get("backdrop", ""),
                "overview": clean_overview(item.get("overview"))[:2000],
                "actor": item.get("actor", ""),
                "director": item.get("director", []),
                "cast": item.get("cast", []),
                "genres": item.get("genres", []),
                "tags": item.get("tags", []),
                "rating": item.get("rating"),
                "rating_source": item.get("rating_source"),
                "vote_count": item.get("vote_count", 0),
                "country": item.get("country", ""),
                "studio": item.get("studio", ""),
                "original_language": item.get("original_language", ""),
                "popularity": item.get("popularity", 0.0),
                "runtime": item.get("runtime", 0),
                "homepage": item.get("homepage", ""),
                "logo": item.get("logo", ""),
                "certification": item.get("certification", ""),
                "cast_structured": item.get("cast_structured", []),
                "director_structured": item.get("director_structured", []),
                "season_episodes": item.get("season_episodes", {}),
                "season_meta": item.get("season_meta", {}),
                "remarks": item.get("remarks", ""),
                "total_episodes": 0,
                "status": "ongoing",
                "last_updated": item.get("update_time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "sources": []
            }
            if not is_matched:
                # 子任务 6 (决策 2/3): unmatched 审计字段 —— reason 区分丢弃原因
                # (all_sources_miss / not_in_whitelist / retryable_exhausted / 历史遗留 unmatched)
                _ent = target_dict[entity_id]
                _ent["scrape_status"] = item.get("scrape_status") or "unmatched"
                _ent["reason"] = item.get("scrape_status_reason") or item.get("scrape_status") or "unmatched"
                _first_url = ""
                for _ln in item.get("lines", []):
                    for _ep in (_ln.get("episodes") or []):
                        if _ep.get("url"):
                            _first_url = _ep["url"]
                            break
                    if _first_url:
                        break
                _ent["url"] = _first_url

        entity = target_dict[entity_id]

        # 补全更优元数据
        if not entity.get("poster") and item.get("poster"):
            entity["poster"] = item["poster"]
        if not entity.get("backdrop") and item.get("backdrop"):
            entity["backdrop"] = item["backdrop"]
        if not entity.get("overview") and item.get("overview"):
            entity["overview"] = clean_overview(item["overview"])[:2000]
        if not entity.get("cast") and item.get("cast"):
            entity["cast"] = item["cast"]
        if not entity.get("director") and item.get("director"):
            entity["director"] = item["director"]
        if not entity.get("genres") and item.get("genres"):
            entity["genres"] = item["genres"]
        if not entity.get("studio") and item.get("studio"):
            entity["studio"] = item["studio"]
        if not entity.get("rating") and item.get("rating"):
            entity["rating"] = item["rating"]
            entity["rating_source"] = item.get("rating_source") or "TMDB"
        for _k in ("popularity", "runtime", "homepage", "logo", "certification",
                   "cast_structured", "director_structured", "season_episodes", "season_meta"):
            if not entity.get(_k) and item.get(_k):
                entity[_k] = item[_k]

        # 多线路与分集合并
        existing_lines = {s.get("line_name"): s for s in entity.get("sources", [])}
        for line in item.get("lines", []):
            line_name = line.get("line_name")
            new_episodes = line.get("episodes", [])
            if not new_episodes:
                continue

            if line_name in existing_lines:
                curr_line = existing_lines[line_name]
                ep_map = {ep["name"]: ep["url"] for ep in curr_line.get("episodes", [])}
                for ep in new_episodes:
                    ep_map[ep["name"]] = ep["url"]
                merged_eps = [{"name": k, "url": v} for k, v in ep_map.items()]
                merged_eps.sort(key=_natural_episode_key)
                curr_line["episodes"] = merged_eps
            else:
                sorted_eps = list(new_episodes)
                sorted_eps.sort(key=_natural_episode_key)
                new_source = {
                    "site": item.get("site", "网络源"),
                    "line_name": line_name,
                    "from": line.get("from", ""),
                    "episodes": sorted_eps
                }
                entity["sources"].append(new_source)
                existing_lines[line_name] = new_source

        # 计算最大集数与状态，并将更新最全、集数最多的线路智能置顶为主线路
        max_eps = 0
        for s in entity["sources"]:
            max_eps = max(max_eps, len(s.get("episodes", [])))
        entity["total_episodes"] = max_eps
        entity["sources"].sort(key=lambda s: len(s.get("episodes", [])), reverse=True)

        remarks = item.get("remarks", "")
        # 与原逻辑一致: 每轮按最新 remarks 重推导, 不继承历史 status
        entity["status"] = _derive_status("", remarks, entity["category"])

        entity["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _convert_to_v21_item(self, entity: dict) -> dict:
        """将内部实体转换为 suenmedia v2.1 规范条目对象"""
        cat = entity.get("category", "movies")
        sources = entity.get("sources", [])
        total_eps = entity.get("total_episodes") or 0
        if not total_eps and sources:
            total_eps = max((len(s.get("episodes", [])) for s in sources), default=0)

        # 判定 type: 电影且总集数<=1 为 video，其余默认 series
        is_video = (cat == "movies" and total_eps <= 1)
        media_type = "video" if is_video else "series"

        bangou = entity.get("bangou") or entity.get("id") or "vid_unknown"
        title = entity.get("title", "未知标题")
        cover = entity.get("poster") or entity.get("cover") or ""
        backdrop = entity.get("backdrop") or ""
        region = entity.get("region") or resolve_region_and_group(
            cat, entity.get("sub_category", ""), entity)[0]
        # 历史 region 别名归一 (港台剧/内地剧 -> 国产剧), 保证产物全局合并
        region = taxonomy.normalize_legacy_region(region)
        group_name = entity.get("group_name") or resolve_region_and_group(
            cat, entity.get("sub_category", ""), entity)[1]
        # 子任务 6 (决策 1) 加固: 历史 first_air_date 可能为 None, 不能直接切片
        date_str = str(entity.get("year") or (entity.get("first_air_date") or "")[:4] or "")
        site_str = sources[0].get("site", "网络源") if sources else entity.get("site", "suenmedia")

        # 整理标签
        tags = list(entity.get("tags") or [])
        if entity.get("genres"):
            tags.extend(entity["genres"])
        if group_name and group_name not in tags:
            tags.append(group_name)
        tags = list(dict.fromkeys(t for t in tags if t))

        # 严格规范化 status (单一来源 _derive_status)
        status_val = _derive_status(entity.get("status"), entity.get("remarks"), cat)

        item_obj = {
            "type": media_type,
            "bangou": bangou,
            "title": title,
            "cover": cover,
            "category": cat,
            "region": region,
            "group_name": group_name,
            "date": date_str,
            "site": site_str,
            "tags": tags,
            "overview": entity.get("overview") or "",
            "year": date_str,
            "status": status_val,
        }

        # 基础元数据 —— 完整对齐 player 消费端全字段，无条件输出
        # 子任务 6 (决策 1) 加固: 类型化兜底 —— 数值字段容错解析, 字符串字段 or "" ,
        # 列表字段 or [] —— 产物中严禁 null / 缺失 / 类型错误
        def _int_or_0(v):
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return 0

        def _num_or_0(v):
            try:
                return round(float(v), 2)
            except (TypeError, ValueError):
                try:  # 脏数据容错: '8.5分' → 8.5 (剥 "分" 后缀后重试)
                    sv = str(v or "").strip().rstrip("分").strip()
                    return round(float(sv), 2) if sv else 0.0
                except (TypeError, ValueError):
                    return 0.0

        r_num = _num_or_0(entity.get("rating"))
        has_rating = r_num > 0

        item_obj.update({
            "original_title": str(entity.get("original_title") or ""),
            "backdrop": str(entity.get("backdrop") or ""),
            "rating": round(r_num, 1) if has_rating else 0.0,
            "rating_source": str(entity.get("rating_source") or ("TMDB" if has_rating else "")),
            "vote_count": _int_or_0(entity.get("vote_count")),
            "first_air_date": str(entity.get("first_air_date") or ""),
            "runtime": _int_or_0(entity.get("runtime")),
            "original_language": str(entity.get("original_language") or ""),
            "homepage": str(entity.get("homepage") or ""),
            "certification": str(entity.get("certification") or ""),
            "country": str(entity.get("country") or ""),
            "studio": str(entity.get("studio") or ""),
            "logo": str(entity.get("logo") or ""),
            "popularity": _num_or_0(entity.get("popularity")),
            "view_count": _int_or_0(entity.get("view_count")),
            "trending_rank": _int_or_0(entity.get("trending_rank")),
            "cast_structured": entity.get("cast_structured") or [],
            "director_structured": entity.get("director_structured") or [],
        })
        # 类型 genres (子任务 6: 无条件输出, 空值兜底 [])
        _g_val = entity.get("genres") or []
        item_obj["genres"] = [str(g) for g in _g_val] if isinstance(_g_val, (list, tuple)) else [str(_g_val)]
        # 演员 cast (推荐级, 最多10人, array[string]) —— 无条件输出, 空值兜底 []
        cast_list = []
        if entity.get("cast"):
            c_val = entity["cast"]
            cast_list = [x.strip() for x in re.split(r'[,，/、\s]+', c_val) if x.strip()] if isinstance(c_val, str) else list(c_val)
        elif entity.get("actor"):
            cast_list = [x.strip() for x in re.split(r'[,，/、\s]+', entity["actor"]) if x.strip()]
        item_obj["cast"] = [str(x) for x in cast_list][:10]

        # 导演 director (array[string]) —— 无条件输出, 空值兜底 []
        dir_list = []
        if entity.get("director"):
            d_val = entity["director"]
            dir_list = [x.strip() for x in re.split(r'[,，/、\s]+', d_val) if x.strip()] if isinstance(d_val, str) else list(d_val)
        item_obj["director"] = [str(x) for x in dir_list]
        if entity.get("country"):
            item_obj["country"] = str(entity["country"])
        if entity.get("studio"):
            item_obj["studio"] = str(entity["studio"])
        if entity.get("original_language"):
            item_obj["original_language"] = str(entity["original_language"])
        if entity.get("first_air_date"):
            item_obj["first_air_date"] = str(entity["first_air_date"])

        # 构造播放线路与分集列表
        # 归一化 URL: 同一视频的 "裸路径" 与 "/index.m3u8" 视为同集 (源站常给网页+m3u8两组)
        seen_canon = set()

        def _canon(u):
            u = (u or "").split("?")[0].strip().lower()
            return re.sub(r'/index\.m3u8$|\.m3u8$|\.mp4$|/$', '', u)

        if is_video:
            primary_url = ""
            alt_urls = []
            for s in sources:
                for ep in s.get("episodes", []):
                    u = ep.get("url")
                    if not u:
                        continue
                    key = _canon(u)
                    if key in seen_canon:
                        continue
                    seen_canon.add(key)
                    if not primary_url:
                        primary_url = u
                    else:
                        alt_urls.append({
                            "source": s.get("site", "备用源"),
                            "url": u,
                            "label": s.get("line_name", "备用"),
                            "resolution": ""
                        })
            if not primary_url and entity.get("url"):
                primary_url = entity["url"]
            if not primary_url:
                return None  # 无有效 URL，按 3.1 规范跳过

            item_obj["url"] = primary_url
            item_obj["url_type"] = resolve_url_type(primary_url)
            item_obj["qualities"] = []
            for _alt in alt_urls:
                _alt["url_type"] = resolve_url_type(_alt.get("url", ""))
            item_obj["alt_urls"] = alt_urls
        else:
            season_episodes = entity.get("season_episodes") or {}
            # 按条目实际季号取分集元数据; 该季无数据时回退第 1 季
            season_num = int(entity.get("season") or 1)
            s_meta = (season_episodes.get(str(season_num))
                      or season_episodes.get(season_num)
                      or season_episodes.get("1") or {})
            episodes_list = []
            for ep_idx in range(total_eps):
                # 收集该槽位各线路候选 (URL, 站点, 线路名, 集名)
                candidates = []
                for s in sources:
                    s_eps = s.get("episodes", [])
                    if ep_idx < len(s_eps):
                        u = s_eps[ep_idx].get("url", "")
                        if u:
                            candidates.append((u, s.get("site", "备用源"),
                                               s.get("line_name", "备用"),
                                               s_eps[ep_idx].get("name") or ""))
                # 剔除与全局已出分集重复的候选 (同视频裸路径/m3u8 双组、源站脏数据)
                fresh = [c for c in candidates if _canon(c[0]) not in seen_canon]
                if not fresh:
                    continue
                # 主源优先 m3u8 形态
                fresh.sort(key=lambda c: 0 if ".m3u8" in c[0].lower() else 1)
                primary_ep_url, p_site, p_line, p_name = fresh[0]
                seen_canon.add(_canon(primary_ep_url))
                primary_ep_title = p_name or f"第{len(episodes_list) + 1}集"

                ep_alt_urls = []
                seen_alt = {_canon(primary_ep_url)}
                for u, si, ln, _nm in fresh[1:]:
                    k = _canon(u)
                    if k in seen_alt:
                        continue
                    seen_alt.add(k)
                    ep_alt_urls.append({
                        "source": si,
                        "url": u,
                        "label": ln,
                        "resolution": ""
                    })

                ep_num = len(episodes_list) + 1
                ep_meta = s_meta.get(ep_num) or {}
                ep_rating = ep_meta.get("rating")
                for _alt in ep_alt_urls:
                    _alt["url_type"] = resolve_url_type(_alt.get("url", ""))
                episodes_list.append({
                    "ep_id": f"{bangou}_{season_num}_{ep_num}",
                    "ep_title": primary_ep_title or ep_meta.get("name") or f"第{ep_num}集",
                    "ep_number": ep_num,
                    "air_date": str(ep_meta.get("air_date") or ""),
                    "url": primary_ep_url,
                    "url_type": resolve_url_type(primary_ep_url),
                    "duration": _int_or_0(ep_meta.get("runtime")),
                    "ep_overview": str(ep_meta.get("overview") or ""),
                    "ep_rating": round(float(ep_rating), 1) if isinstance(ep_rating, (int, float)) else 0.0,
                    "ep_rating_source": "TMDB" if isinstance(ep_rating, (int, float)) and ep_rating > 0 else "",
                    "ep_still": str(ep_meta.get("still") or ""),
                    "qualities": [],
                    "alt_urls": ep_alt_urls
                })

            episodes_list.sort(key=lambda x: x["ep_number"])

            if episodes_list:
                # 季级元数据优先: 每季独立的海报/简介/首播日期/客串演员
                sm = (entity.get("season_meta") or {}).get(str(season_num)) or {}
                if sm.get("cast"):
                    item_obj["cast"] = sm["cast"]
                item_obj["number_of_seasons"] = 1
                item_obj["number_of_episodes"] = len(episodes_list)
                item_obj["seasons"] = [
                    {
                        "season_number": season_num,
                        "season_title": sm.get("name") or f"第 {season_num} 季",
                        "season_cover": sm.get("cover") or cover,
                        "season_overview": sm.get("overview") or entity.get("overview") or "",
                        "season_date": sm.get("air_date") or date_str,
                        "episode_count": len(episodes_list),
                        "episodes": episodes_list
                    }
                ]
            else:
                # 若无分集列表，尝试退化为 video 单视频
                fallback_url = entity.get("url") or (sources[0]["episodes"][0]["url"] if (sources and sources[0].get("episodes")) else "")
                if fallback_url:
                    item_obj["type"] = "video"
                    item_obj["url"] = fallback_url
                    item_obj["url_type"] = resolve_url_type(fallback_url)
                    item_obj["qualities"] = []
                    item_obj["alt_urls"] = []
                    # 子任务 6 (决策 1): 退化 video 后补齐 series 专属字段的空兜底
                    item_obj["number_of_seasons"] = 0
                    item_obj["number_of_episodes"] = 0
                    item_obj["seasons"] = []
                else:
                    return None  # 无有效播放源，按 3.1 规范跳过

        return item_obj

    def apply_category_gate(self):
        """分类闸门: 兜住历史产物里的垃圾条目 (源头过滤只管新爬数据)。

        子任务 4 白名单语义: 除原有黑名单关键词外，增加"白名单外历史数据"剔除——
        sub_category 在 categorize_type 下归为 discard 的条目 (如历史别名误归入
        movies 的纪录片) 一并丢弃，保证 videos.json 只含四类。
        """
        from common import load_settings as _ls, categorize_type as _categorize
        _s = _ls()
        _cats = set(_s.get("crawl_skip_categories") or ["short_tv"])
        _tkws = [k for k in (_s.get("crawl_skip_type_keywords") or ["短剧", "解说", "微电影"]) if k]
        _title_kws = [k for k in (_s.get("crawl_skip_title_keywords") or ["微电影"]) if k]
        for cat in list(self.catalog.keys()):
            dropped_eids = []
            for eid, e in self.catalog[cat].items():
                sub = (e.get("sub_category") or "") + (e.get("group_name") or "")
                tags = " ".join(e.get("tags") or [])
                if (_categorize(e.get("sub_category") or "") == "discard"
                        or cat in _cats or e.get("category") in _cats
                        or any(k in sub for k in _tkws)
                        or any(k in tags for k in _tkws)
                        or any(k in (e.get("title") or "") for k in _title_kws)):
                    dropped_eids.append(eid)
            for eid in dropped_eids:
                dropped_item = self.catalog[cat].pop(eid)
                print(f"[过滤] 分类闸门丢弃: {dropped_item.get('title')} ({dropped_item.get('sub_category', '')})")
            if dropped_eids:
                with self._lock:
                    self._dirty_cats.add(cat)
                print(f"[导出] {cat} 分类闸门丢弃 {len(dropped_eids)} 条")

    def save_all(self):
        """严格按照 suenmedia v2.1 规范生成 ./product/ 结构产物"""
        print("\n[导出] 正在依据 suenmedia v2.1 规范生成 ./product/ 产物与 M3U8 软备份...")
        os.makedirs("product/m3u8", exist_ok=True)
        stats = {}
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        # 封面域名探活: 死图床的链接直接丢弃 (宁缺毋滥, player 端显示占位图)
        img_urls = []
        for cat in self.categories:
            for e in self.catalog[cat].values():
                if e.get("poster"):
                    img_urls.append(e["poster"])
                if e.get("backdrop"):
                    img_urls.append(e["backdrop"])
        host_status = check_cover_hosts(img_urls)
        dead_hosts = {h for h, alive in host_status.items() if not alive}
        if dead_hosts:
            dropped = 0
            for cat in self.categories:
                for e in self.catalog[cat].values():
                    host = (e.get("poster") or "").split("/")[2] if "://" in (e.get("poster") or "") else ""
                    if host in dead_hosts:
                        e["poster"] = ""
                        dropped += 1
                    host = (e.get("backdrop") or "").split("/")[2] if "://" in (e.get("backdrop") or "") else ""
                    if host in dead_hosts:
                        e["backdrop"] = ""
                with self._lock:
                    self._dirty_cats.add(cat)
            print(f"[导出] 封面探活: 死亡图床 {len(dead_hosts)} 个 ({', '.join(sorted(dead_hosts))}), 丢弃 {dropped} 张死链图")

        # 分类闸门: 兜住历史产物里的垃圾条目 (源头过滤只管新爬数据)
        self.apply_category_gate()

        # 子任务 6 (决策 2): 删除旧"无封面硬性丢弃"判据 —— 丢弃判据统一升级为
        # "刮削是否命中": 全源 MISS/权威丢弃在 enrich 阶段已分流 unmatched.json,
        # 命中条目即使源站无封面也保留 (刮削源通常已提供权威封面)

        all_unified_items = []
        date_str = datetime.now().strftime("%m-%d")

        # 1. 汇总全部分类条目并导出 M3U8 软备份 (仅重写本轮有变更的类目)
        for cat in self.categories:
            raw_entities = list(self.catalog[cat].values())
            raw_entities.sort(key=lambda x: x.get("last_updated", ""), reverse=True)

            v21_items = [self._convert_to_v21_item(e) for e in raw_entities]
            v21_items = [it for it in v21_items if it is not None]
            stats[cat] = len(v21_items)
            all_unified_items.extend(v21_items)

            with self._lock:
                is_dirty = cat in self._dirty_cats
            if not is_dirty:
                continue

            # 导出扁平兼容 M3U8 软备份文件 (内存缓冲, 一次 write)
            m3u8_path = f"product/m3u8/{cat}.m3u8"
            buf = ["#EXTM3U\n"]
            for it in v21_items:
                poster = it.get("cover", "")
                title = it.get("title", "")
                group = it.get("group_name", cat)
                if it["type"] == "video":
                    url = it.get("url")
                    if url:
                        buf.append(f'#EXTINF:-1 group-title="{group}" tvg-logo="{poster}",{title} [{date_str}]\n')
                        buf.append(f"{url}\n")
                else:
                    for s in it.get("seasons", []):
                        for ep in s.get("episodes", []):
                            ep_url = ep.get("url")
                            if ep_url:
                                ep_title = ep.get("ep_title", "正片")
                                buf.append(f'#EXTINF:-1 group-title="{group}" tvg-logo="{poster}",{title} {ep_title} [{date_str}]\n')
                                buf.append(f"{ep_url}\n")
            with open(m3u8_path, "w", encoding="utf-8") as f:
                f.write("".join(buf))

        # 1.5 导出 unmatched 桶 (修 main.py 恒 0: 统计与落盘双通道)
        unmatched_payload = list(self.unmatched.values())
        os.makedirs("product", exist_ok=True)
        tmp_unmatched = "product/unmatched.json.tmp"
        with open(tmp_unmatched, "w", encoding="utf-8") as f:
            json.dump({"schema_version": SCHEMA_VERSION,
                       "generated_at": now_iso,
                       "items": unmatched_payload}, f, ensure_ascii=False)
        os.replace(tmp_unmatched, "product/unmatched.json")
        stats["unmatched"] = len(unmatched_payload)

        # 1.6 导出 category 权威校正冲突样本 (子任务 5: 入口粗判 vs 刮削权威)
        conflicts = taxonomy.get_conflict_samples()
        if conflicts:
            with open("product/category_conflicts.json", "w", encoding="utf-8") as f:
                json.dump({"schema_version": SCHEMA_VERSION,
                           "generated_at": now_iso,
                           "samples": conflicts}, f, ensure_ascii=False, indent=1)
            print(f"[导出] category 权威校正冲突样本 {len(conflicts)} 条 -> product/category_conflicts.json")

        # 2. 导出核心交付单文件 product/videos.json (紧凑输出 + tmp+os.replace 原子替换)
        full_doc = {
            "schema_version": SCHEMA_VERSION,
            "project": {"name": "影视仓", "slug": "suenmedia"},
            "generated_at": now_iso,
            "source": "suenmedia",
            "generator": GENERATOR_VERSION,
            "items": all_unified_items
        }
        tmp_doc = "product/videos.json.tmp"
        with open(tmp_doc, "w", encoding="utf-8") as f:
            json.dump(full_doc, f, ensure_ascii=False)
        os.replace(tmp_doc, "product/videos.json")

        # 本轮已落盘, 清空脏标记 (进程内增量基线)
        with self._lock:
            self._dirty_cats.clear()

        print("[完成] suenmedia 产物收敛至 ./product/ 成功:")
        for cat, cnt in stats.items():
            print(f"  - {cat}: {cnt} 部")
        print(f"  - 唯一全量导入文件 (product/videos.json): 共 {len(all_unified_items)} 部影视")
        print(f"  - 软备份目录: product/m3u8/ (仅重写脏类目)")
        return stats
