import os
import json
import re
import time
import hashlib
import urllib.parse
from bs4 import BeautifulSoup
import requests
from common import load_settings, get_session, clean_overview, has_cjk
from taxonomy import authoritative_category, normalize_genre, record_conflict
import retry_queue as rq
import progress_log

_CACHE_FILE = "cache/metadata_cache.json"
_TVDB_TOKEN_FILE = "cache/tvdb_token.json"

# ---------------- 刮削结果三态 (子任务 6 · 决策 3) ----------------
# 各 _query_* 返回 (meta|None, status):
#   命中            -> (dict, "hit")
#   真未命中        -> (None, ST_MISS)      源响应正常但查无此片 (200 空结果/404)
#   限流/不可达     -> (None, ST_RETRYABLE) 429/403/5xx/超时/连接失败/反爬
#   源未启用/无 Key -> (None, ST_SKIPPED)   不参与三分类判定
ST_MISS = "miss"
ST_RETRYABLE = "retryable"
ST_SKIPPED = "skipped"

import threading
_CACHE_LOCK = threading.Lock()

def _load_cache() -> dict:
    if os.path.exists(_CACHE_FILE):
        try:
            with open(_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            # 缓存损坏不可静默: 意味着下轮将全量重刮, 必须留下线索
            progress_log.event(f"[警告] 元数据缓存加载失败, 将全量重刮: {type(e).__name__}: {e}")
    return {}

def _save_cache(cache_data: dict):
    """原子写 (tmp + os.replace) + 紧凑输出: 进程被杀不损坏缓存, 体积减约 40%

    锁覆盖「内存快照 -> 写 tmp -> os.replace」完整临界区, 且 tmp 带线程 id:
    同进程多线程并发保存时不再共用同一 tmp 路径, 避免 os.replace 竞态
    (线程 A 把 tmp replace 走后, 线程 B 对同一路径再 replace 报 FileNotFoundError)。
    """
    os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
    tmp = f"{_CACHE_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with _CACHE_LOCK:
            snapshot = dict(cache_data)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, ensure_ascii=False)
            os.replace(tmp, _CACHE_FILE)
    except Exception as e:
        progress_log.event(f"[警告] 保存元数据缓存失败: {type(e).__name__}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _safe_float(v) -> float:
    """容错解析评分: 支持 8.5 / '8.5' / '8.5分' / 'N/A' / None, 失败返回 0.0"""
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v or "").strip().rstrip("分").strip()
        return float(s) if s else 0.0
    except (TypeError, ValueError):
        return 0.0

class MetadataScraper:
    def __init__(self):
        self.settings = load_settings()
        # 环境变量优先 (GitHub Actions secrets), 其次 settings.json
        self.api_key = (os.environ.get("TMDB_API_KEY") or self.settings.get("tmdb_api_key", "")).strip()
        self.api_base = self.settings.get("tmdb_api_base", "https://api.themoviedb.org/3").rstrip('/')
        self.image_base = self.settings.get("tmdb_image_base", "https://image.tmdb.org/t/p/w500").rstrip('/')
        self.enable_tmdb = self.settings.get("enable_tmdb", True) and bool(self.api_key)
        self.enable_douban = self.settings.get("enable_douban_fallback", True)
        self.session = get_session(use_proxy=True)
        self.cache = _load_cache()
        # 节流写盘基线: 新增缓存条数距上次落盘 >= 500 才写, 避免全量重写 I/O 放大
        self._last_saved = len(self.cache)
        # 豆瓣自动熔断: 海外 runner 上豆瓣接口不可用, 连续失败 5 次后直接跳过,
        # 避免 TMDB 未命中条目在必败请求上每条烧 30s+
        self._douban_fail = 0
        # 静默异常治理: 各数据源异常计数, close() 时汇总输出
        self._err_counts = {}
        # 全局共享限速: 多线程并发时各源 QPS 仍受 settings 间隔约束
        self._rate_lock = threading.Lock()
        self._last_call = {}
        # Bilibili wbi 密钥内存缓存 (子任务 8)
        self._bili_keys = ("", "")
        self._bili_keys_ts = 0.0
        # T8.3 日志改造: 置 True 时逐条 [待办]/[丢弃]/[权威丢弃]/[权威校正] 打印静默,
        # 由调用方 (full_crawl) 聚合为错误行与进度总览; 默认 False 保持原逐条打印
        self.quiet_items = False

    def _log_err(self, where: str, e: Exception):
        """统一异常出口: 打印 + 计数, 不再静默吞掉"""
        self._err_counts[where] = self._err_counts.get(where, 0) + 1
        # T8.3: 经事件行输出, 先清空覆盖刷新的进度行再打印, 避免被进度行覆盖
        progress_log.event(f"[刮削警告] {where}: {type(e).__name__}: {e}")

    def _throttle(self, kind: str, interval: float):
        """跨线程共享限速: 同源请求间隔不小于 interval 秒"""
        if interval <= 0:
            return
        import time as _time
        while True:
            with self._rate_lock:
                now = _time.time()
                wait = self._last_call.get(kind, 0) + interval - now
                if wait <= 0:
                    self._last_call[kind] = now
                    return
            _time.sleep(min(wait, interval))

    def _tmdb_get(self, url: str, params: dict, retries: int = 3, timeout: int = 8) -> tuple:
        """带 429/5xx 指数退避与最小间隔限流的 TMDB GET。
        返回 (json|None, retryable):
          200            -> (json, False)
          429/5xx/超时退避耗尽 -> (None, True)   [限流/不可达]
          404 等明确失败  -> (None, False)        [非限流]"""
        import time as _time
        interval = float(self.settings.get("tmdb_min_interval", 0.15) or 0)
        retryable = False
        for i in range(retries):
            self._throttle("tmdb", interval)
            try:
                res = self.session.get(url, params=params, timeout=timeout)
                if res.status_code == 200:
                    return res.json(), False
                if res.status_code == 429:
                    retryable = True
                    retry_after = float(res.headers.get("Retry-After") or 0)
                    wait = max(retry_after, min(2 ** (i + 1) * 5, 120))
                    progress_log.event(f"[限流] TMDB 429, 退避 {wait:.0f}s")
                    _time.sleep(wait)
                    continue
                if res.status_code in (500, 502, 503, 504):
                    retryable = True
                    _time.sleep(min(2 ** (i + 1), 30))
                    continue
                return None, False
            except Exception as e:
                self._log_err("tmdb_get", e)
                retryable = True
                if i < retries - 1:
                    _time.sleep(min(2 ** (i + 1), 30))
        return None, retryable

    def _query_tmdb(self, title: str, category: str, year: str = None) -> tuple:
        """TMDB 主源刮削, 返回 (meta|None, status) 三态"""
        if not self.enable_tmdb:
            return None, ST_SKIPPED

        endpoint_type = "movie" if category == "movies" else "tv"
        url = f"{self.api_base}/search/{endpoint_type}"

        try:
            data = None
            search_retryable = False
            for variant in self._search_variants(title):
                params = {
                    "api_key": self.api_key,
                    "query": variant,
                    "language": "zh-CN",
                    "page": 1
                }
                if year and str(year).isdigit() and variant == title:
                    # 年份只用于原始标题查询 (第几季的播出年 ≠ 剧的首播年)
                    if endpoint_type == "movie":
                        params["primary_release_year"] = int(year)
                    else:
                        params["first_air_date_year"] = int(year)
                data, retryable = self._tmdb_get(url, params)
                if retryable:
                    search_retryable = True  # 429/5xx/超时退避耗尽
                if data and data.get("results"):
                    break
            if data:
                results = data.get("results", [])

                if results:
                    best = results[0]
                    poster_path = best.get("poster_path")
                    backdrop_path = best.get("backdrop_path")
                    first_date = best.get("first_air_date") or best.get("release_date") or ""

                    # 尝试拉取详情获取演职员、类型、片长、评分、分级、Logo 与分集数据
                    tmdb_id = best.get("id")
                    cast = []
                    director = []
                    cast_structured = []
                    director_structured = []
                    genres = []
                    runtime = 0
                    homepage = ""
                    popularity = float(best.get("popularity", 0.0) or 0.0)
                    tmdb_status = str(best.get("status") or "").strip()
                    number_of_seasons = 0
                    number_of_episodes = 0
                    logo = ""
                    certification = ""
                    season_episodes = {}
                    season_meta = {}
                    d_json = None  # 详情失败时保持 None, 权威国家字段可安全回退
                    try:
                        detail_url = f"{self.api_base}/{endpoint_type}/{tmdb_id}"
                        detail_params = {
                            "api_key": self.api_key,
                            "language": "zh-CN",
                            "append_to_response": "credits,images,release_dates,content_ratings",
                            "include_image_language": "zh,en,null",
                        }
                        d_json, _detail_st = self._tmdb_get(detail_url, detail_params, retries=2, timeout=6)
                        if d_json:
                            genres = [g.get("name") for g in d_json.get("genres", []) if g.get("name")]
                            credits = d_json.get("credits", {})
                            for c in credits.get("cast", [])[:10]:
                                if not c.get("name"):
                                    continue
                                cast.append(c["name"])
                                cast_structured.append({
                                    "name": c.get("name", ""),
                                    "character": c.get("character", ""),
                                    "profile_path": f"https://image.tmdb.org/t/p/w185{c['profile_path']}" if c.get("profile_path") else "",
                                })
                            for crew in credits.get("crew", []):
                                if crew.get("job") == "Director" and crew.get("name"):
                                    director.append(crew["name"])
                                    director_structured.append({
                                        "name": crew["name"],
                                        "job": "Director",
                                        "profile_path": f"https://image.tmdb.org/t/p/w185{crew['profile_path']}" if crew.get("profile_path") else "",
                                    })

                            popularity = float(d_json.get("popularity", popularity) or popularity)
                            homepage = str(d_json.get("homepage") or "")
                            tmdb_status = str(d_json.get("status") or tmdb_status)

                            if endpoint_type == "movie":
                                runtime = int(d_json.get("runtime") or 0)
                                for rd in d_json.get("release_dates", {}).get("results", []):
                                    iso = rd.get("iso_3166_1", "")
                                    if iso in ("CN", "US"):
                                        for r in rd.get("release_dates", []):
                                            cert = str(r.get("certification") or "").strip()
                                            if cert:
                                                certification = cert
                                                break
                                    if certification:
                                        break
                            else:
                                run_times = d_json.get("episode_run_time") or []
                                runtime = int(run_times[0]) if run_times else 0
                                number_of_seasons = int(d_json.get("number_of_seasons") or 0)
                                number_of_episodes = int(d_json.get("number_of_episodes") or 0)
                                for cr in d_json.get("content_ratings", {}).get("results", []):
                                    iso = cr.get("iso_3166_1", "")
                                    if iso in ("CN", "US"):
                                        rating_txt = str(cr.get("rating") or "").strip()
                                        if rating_txt:
                                            certification = rating_txt
                                            break

                            logos = d_json.get("images", {}).get("logos", []) or []
                            if logos:
                                logo = f"https://image.tmdb.org/t/p/w500{logos[0].get('file_path')}"

                            # 剧集拉取各季级数据: 每季独立的海报/简介/首播/演员 + 分集级明细
                            season_meta = {}
                            if endpoint_type == "tv":
                                for s_info in d_json.get("seasons", []):
                                    s_num = s_info.get("season_number")
                                    if not isinstance(s_num, int) or s_num < 1:
                                        continue
                                    try:
                                        s_json, _s_st = self._tmdb_get(
                                            f"{self.api_base}/tv/{tmdb_id}/season/{s_num}",
                                            {"api_key": self.api_key, "language": "zh-CN",
                                             "append_to_response": "credits"},
                                            retries=2, timeout=6,
                                        )
                                        if not s_json:
                                            continue
                                        ep_map = {}
                                        for ep in s_json.get("episodes", []):
                                            ep_map[int(ep.get("episode_number") or 0)] = {
                                                "name": ep.get("name") or "",
                                                "air_date": ep.get("air_date") or "",
                                                "overview": ep.get("overview") or "",
                                                "still": f"https://image.tmdb.org/t/p/w300{ep['still_path']}" if ep.get("still_path") else "",
                                                "runtime": int(ep.get("runtime") or 0),
                                                "rating": round(_safe_float(ep.get("vote_average")), 1),
                                                "vote_count": int(ep.get("vote_count") or 0),
                                            }
                                        if ep_map:
                                            season_episodes[str(s_num)] = ep_map
                                        s_cast = []
                                        s_credit = s_json.get("credits") or {}
                                        for c in (s_credit.get("cast") or [])[:10]:
                                            if c.get("name"):
                                                s_cast.append(c["name"])
                                        season_meta[str(s_num)] = {
                                            "name": s_info.get("name") or "",
                                            "overview": s_info.get("overview") or s_json.get("overview") or "",
                                            "cover": f"{self.image_base}{s_info.get('poster_path')}" if s_info.get("poster_path") else "",
                                            "air_date": s_info.get("air_date") or "",
                                            "cast": s_cast,
                                        }
                                    except Exception as e:
                                        self._log_err("tmdb_season_detail", e)
                                        continue
                    except Exception as e:
                        self._log_err("tmdb_seasons", e)

                    # 权威国家字段 (子任务 5): 详情 production_countries 中文名, 回退 tv 的 origin_country ISO
                    country_val = ""
                    prod_countries = (d_json or {}).get("production_countries") or []
                    c_names = [c.get("name", "").strip() for c in prod_countries if c.get("name")]
                    if c_names:
                        country_val = ",".join(c_names)
                    else:
                        o_countries = best.get("origin_country") or []
                        if o_countries:
                            country_val = ",".join(str(c) for c in o_countries)

                    return {
                        "source": "TMDB",
                        "tmdb_id": tmdb_id,
                        "tmdb_endpoint": endpoint_type,
                        "country": country_val,
                        "title": best.get("name") or best.get("title") or title,
                        "original_title": best.get("original_name") or best.get("original_title") or "",
                        "overview": best.get("overview", ""),
                        "poster": f"{self.image_base}{poster_path}" if poster_path else "",
                        "backdrop": f"https://image.tmdb.org/t/p/original{backdrop_path}" if backdrop_path else "",
                        "rating": round(_safe_float(best.get("vote_average")), 1),
                        "rating_source": "TMDB",
                        "vote_count": int(best.get("vote_count", 0)),
                        "year": first_date[:4] if first_date else year,
                        "first_air_date": first_date,
                        "genres": genres,
                        "cast": cast,
                        "director": director,
                        "original_language": best.get("original_language", ""),
                        "popularity": round(popularity, 2),
                        "runtime": runtime,
                        "homepage": homepage,
                        "status": tmdb_status,
                        "number_of_seasons": number_of_seasons,
                        "number_of_episodes": number_of_episodes,
                        "logo": logo,
                        "certification": certification,
                        "cast_structured": cast_structured,
                        "director_structured": director_structured,
                        "season_episodes": season_episodes,
                        "season_meta": season_meta,
                    }, "hit"
            if search_retryable:
                return None, ST_RETRYABLE
            return None, ST_MISS
        except Exception as e:
            self._log_err("tmdb_query", e)
            return None, ST_RETRYABLE

    _SEASON_SUFFIX = re.compile(
        r'\s*(第[一二三四五六七八九十百0-9]+[季期部]|Season\s*\d+|Part\s*\d+|\(\d+\)|\d+rd Season|\d+nd Season|\d+st Season|\d+th Season)\s*$',
        re.IGNORECASE)

    def _search_variants(self, title: str) -> list:
        """生成搜索变体: 原标题 + 去季数后缀 + 去标点 + 去～副标题～ + 去尾缀数字"""
        t = (title or "").strip()
        variants = [t] if t else []
        stripped = self._SEASON_SUFFIX.sub('', t).strip()
        if stripped and stripped not in variants:
            variants.append(stripped)
        no_punct = re.sub(r'[：:·—\-]', ' ', stripped or t).strip()
        if no_punct and no_punct not in variants:
            variants.append(no_punct)
        # 去 ~副标题~ (如 "吃饱睡足等幸福～早春养生篇～")
        no_sub = re.sub(r'[~～][^~～]+[~～]', '', (stripped or t)).strip()
        if no_sub and no_sub not in variants:
            variants.append(no_sub)
        # 去尾缀数字 (如 "姐姐家的产地直送3" / "秘书镇2")
        no_digit = re.sub(r'\s*\d+$', '', (stripped or t)).strip()
        if no_digit and no_digit not in variants:
            variants.append(no_digit)
        return variants

    # ---------------- 冗余元数据源矩阵 ----------------
    # TMDB (主源) -> TheTVDB (剧集, 需Key) -> 豆瓣 (国内网络)
    #   -> Bilibili (动漫, 免Key, 全中文) -> OMDb/IMDb (评分兜底·已收窄, 需Key)
    # 子任务 8: 原 GraphQL 动漫源已移除 (实测 title 落日文 native / genres 与简介为英文,
    # 与中文产物契约冲突), 动漫兜底改由 Bilibili 公开 API 提供
    # 子任务 8: OMDb 收窄为「评分+票数」兜底, 英文 title/overview/cast 等不再返回 (T7 P1-3)

    # ---- Bilibili 番剧公开 API (免 Key, 全中文) ----
    # wbi 签名为 B 站前端公开同款算法 (mixin key 置换表), 实测 2026-09-07 可用
    _BILI_MIXIN_TAB = (
        46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
        33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
        61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
        36, 20, 34, 44, 52)

    def _bili_wbi_keys(self) -> tuple:
        """nav 接口获取 wbi 密钥 (免登录取 img/sub key), 内存缓存 6h; 失败返回 ("","")"""
        now = time.time()
        if self._bili_keys[0] and now < self._bili_keys_ts + 6 * 3600:
            return self._bili_keys
        try:
            res = self.session.get("https://api.bilibili.com/x/web-interface/nav", timeout=8)
            wbi = (res.json().get("data") or {}).get("wbi_img") or {}
            # nav 实测返回 img_url/sub_url (key 为 URL 末段); 兼容直接给 img_key/sub_key 的形态
            img_url, sub_url = str(wbi.get("img_url") or ""), str(wbi.get("sub_url") or "")
            keys = (
                str(wbi.get("img_key") or (img_url.rsplit("/", 1)[-1].split(".")[0] if img_url else "")),
                str(wbi.get("sub_key") or (sub_url.rsplit("/", 1)[-1].split(".")[0] if sub_url else "")),
            )
            if keys[0] and keys[1]:
                self._bili_keys, self._bili_keys_ts = keys, now
            return keys
        except Exception as e:
            self._log_err("bilibili_nav", e)
            return ("", "")

    def _bili_wbi_sign(self, params: dict, img_key: str, sub_key: str) -> dict:
        """wbi 签名 (B 站前端公开同款算法, 已实测可用): 参数按 key 排序拼接,
        与置换后的 mixin key 串联取 md5 得 w_rid"""
        raw = img_key + sub_key
        mixin = "".join(raw[i] for i in self._BILI_MIXIN_TAB)[:32]
        signed = {k: str(v) for k, v in sorted(params.items())}
        signed["wts"] = int(time.time())
        query = urllib.parse.urlencode(signed)
        signed["w_rid"] = hashlib.md5((query + mixin).encode("utf-8")).hexdigest()
        return signed

    def _query_bilibili(self, title: str) -> tuple:
        """Bilibili 番剧公开 API (免 Key, 返回全中文, 仅用于 anime 类目)。
        实测 2026-09-07 全链路通过 (wbi 搜索 -> pgc 详情), 输出见交付留档。
        三态: nav 不可达/限流/业务码异常 -> RETRYABLE; 查无此番 -> MISS; 命中 -> (meta, "hit")"""
        if not self.settings.get("bilibili_enable", True):
            return None, ST_SKIPPED
        if not (title or "").strip():
            return None, ST_MISS
        self._throttle("bilibili", float(self.settings.get("bilibili_min_interval", 1.0) or 0))
        img_key, sub_key = self._bili_wbi_keys()
        if not img_key or not sub_key:
            return None, ST_RETRYABLE  # nav 不可达无法签名, 属源暂时不可用
        params = self._bili_wbi_sign(
            {"keyword": title.strip(), "search_type": "media_bangumi", "page": 1},
            img_key, sub_key)
        try:
            res = self.session.get(
                "https://api.bilibili.com/x/web-interface/wbi/search/type",
                params=params, timeout=8,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                         "Referer": "https://www.bilibili.com/"})
            if res.status_code == 429 or res.status_code >= 500:
                return None, ST_RETRYABLE
            payload = res.json()
        except Exception as e:
            self._log_err("bilibili_search", e)
            return None, ST_RETRYABLE
        if payload.get("code") != 0:
            # -412 风控等业务码异常: 视作源暂时不可用, 不判真未命中
            return None, ST_RETRYABLE
        results = (payload.get("data") or {}).get("result") or []
        target = next((r for r in results if r.get("season_id")), None)
        if not target:
            return None, ST_MISS  # 200 正常返回但 B 站未收录该番剧
        try:
            res2 = self.session.get(
                "https://api.bilibili.com/pgc/view/web/season",
                params={"season_id": target.get("season_id")}, timeout=8,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                         "Referer": "https://www.bilibili.com/"})
            if res2.status_code == 429 or res2.status_code >= 500:
                return None, ST_RETRYABLE
            d2 = (res2.json() or {}).get("result") or {}
        except Exception as e:
            self._log_err("bilibili_season", e)
            return None, ST_RETRYABLE
        if not str(d2.get("title") or "").strip():
            return None, ST_RETRYABLE
        rating = d2.get("rating") or {}
        pub_time = str((d2.get("publish") or {}).get("pub_time") or "")[:10]

        def _clean(s):
            return re.sub(r"<[^>]+>", "", str(s or "")).strip()

        # cast: 搜索结果 cv 字段按行取"："后声优名, 去（）括注, 最多 10 个
        cast = []
        for line in str(target.get("cv") or "").splitlines():
            if "：" not in line:
                continue
            name = re.sub(r"[（(].*?[)）]", "", line.split("：", 1)[1]).strip()
            if name and name not in cast:
                cast.append(name)
            if len(cast) >= 10:
                break
        # director: pgc 详情 staff 多行文本中取"导演/监督："后内容
        director = ""
        for line in str(d2.get("staff") or "").splitlines():
            if ("导演" in line or "监督" in line) and "：" in line:
                director = re.sub(r"[（(].*?[)）]", "", line.split("：", 1)[1]).strip()
                break
        # genres: 详情 styles 为字符串列表; 详情缺失时回退搜索 styles ("漫画改/奇幻/热血")
        styles = d2.get("styles")
        if isinstance(styles, list):
            genres = [str(g).strip() for g in styles if str(g).strip()]
        else:
            genres = [g.strip() for g in str(target.get("styles") or "").split("/") if g.strip()]
        areas = d2.get("areas") or target.get("areas") or []
        meta = {
            "source": "Bilibili",
            "bilibili_season_id": target.get("season_id"),
            "title": _clean(d2.get("title") or target.get("title")),
            "original_title": str(d2.get("jp_title") or "").strip() or None,
            "overview": _clean(d2.get("evaluate"))[:2000] or None,
            "poster": str(d2.get("cover") or target.get("cover") or "").strip() or None,
            "rating": round(_safe_float(rating.get("score")), 1) or None,
            "rating_source": "哔哩哔哩",
            "vote_count": int(rating.get("count") or 0) or None,
            "year": pub_time[:4] or None,
            "first_air_date": pub_time or None,
            "genres": genres,
            "number_of_episodes": d2.get("total") or None,
            "cast": cast or None,
            "director": director or None,
            "country": (areas[0].get("name") if areas and isinstance(areas[0], dict) else None),
        }
        meta = {k: v for k, v in meta.items() if v not in (None, "", [], 0.0)}
        return meta, "hit"

    def _tvdb_token(self) -> str:
        """获取/缓存 TheTVDB v4 登录 token (有效期约1个月)"""
        if os.path.exists(_TVDB_TOKEN_FILE):
            try:
                tok = json.load(open(_TVDB_TOKEN_FILE))
                if tok.get("token") and tok.get("ts", 0) > time.time() - 86400 * 25:
                    return tok["token"]
            except Exception as e:
                self._log_err("tvdb_token_read", e)
        try:
            res = self.session.post(
                "https://api4.thetvdb.com/v4/login",
                json={"apikey": self.settings.get("tvdb_api_key", "")},
                headers={"Content-Type": "application/json"}, timeout=8)
            token = (res.json().get("data") or {}).get("token") if res.status_code == 200 else ""
            if token:
                os.makedirs(os.path.dirname(_TVDB_TOKEN_FILE), exist_ok=True)
                json.dump({"token": token, "ts": time.time()}, open(_TVDB_TOKEN_FILE, "w"))
            return token
        except Exception as e:
            self._log_err("tvdb_token_login", e)
            return ""

    def _tvdb_translation(self, series_id, headers) -> tuple:
        """TheTVDB v4 官方翻译 (子任务 8·D): /v4/series/{id}/translations/{lang}。
        依次尝试 settings.tvdb_translation_langs (默认 ["zho", "chi"]), 取首个有 name 的翻译。
        返回 (翻译dict, 语言码); 翻译缺失/接口失败返回 ({}, "") -> 调用方回退英文, 不判 MISS。
        注意: 沙箱未配 TheTVDB Key, 此路径「待配 Key 实测」; 端点出处: TVDB v4 官方 swagger"""
        for lang in (self.settings.get("tvdb_translation_langs") or ["zho", "chi"]):
            try:
                res = self.session.get(
                    f"https://api4.thetvdb.com/v4/series/{series_id}/translations/{lang}",
                    headers=headers, timeout=8)
                if res.status_code != 200:
                    continue
                tr = res.json().get("data") or {}
                if isinstance(tr, list):
                    tr = tr[0] if tr else {}
                if str(tr.get("name") or "").strip():
                    return tr, lang
            except Exception as e:
                self._log_err("tvdb_translation", e)
        return {}, ""

    def _tvdb_episode_translations(self, series_id, lang, headers) -> dict:
        """批量分集译名 (子任务 8·D): /v4/series/{id}/episodes/official/{lang}
        -> {episode_id: {"name":…, "overview":…}}; 失败返回 {} (保留英文分集名, 不判 MISS)"""
        if not lang:
            return {}
        try:
            res = self.session.get(
                f"https://api4.thetvdb.com/v4/series/{series_id}/episodes/official/{lang}",
                headers=headers, timeout=10)
            if res.status_code != 200:
                return {}
            data = res.json().get("data") or {}
            eps = data.get("episodes") if isinstance(data, dict) else data
            out = {}
            for ep in eps or []:
                if isinstance(ep, dict) and ep.get("id"):
                    out[ep["id"]] = {"name": ep.get("name") or "",
                                     "overview": ep.get("overview") or ""}
            return out
        except Exception as e:
            self._log_err("tvdb_episode_translations", e)
            return {}

    def _query_tvdb(self, title: str) -> tuple:
        """TheTVDB v4 剧集元数据 (需免费 API Key)。返回 (meta|None, status) 三态。"""
        apikey = self.settings.get("tvdb_api_key", "").strip()
        if not apikey:
            return None, ST_SKIPPED
        token = self._tvdb_token()
        if not token:
            # 登录接口不可达/凭据被拒 -> 无法访问, 可重试
            return None, ST_RETRYABLE
        try:
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            res = None
            for variant in self._search_variants(title):
                res = self.session.get(
                    "https://api4.thetvdb.com/v4/search",
                    params={"query": variant, "type": "series", "limit": 3},
                    headers=headers, timeout=8)
                if res.status_code == 200 and res.json().get("data"):
                    break
            if res is None:
                return None, ST_MISS  # 标题为空无可查变体
            if res.status_code == 401:  # token 失效, 重取一次
                if os.path.exists(_TVDB_TOKEN_FILE):
                    os.remove(_TVDB_TOKEN_FILE)
                token = self._tvdb_token()
                headers["Authorization"] = f"Bearer {token}"
                res = self.session.get(
                    "https://api4.thetvdb.com/v4/search",
                    params={"query": title, "type": "series", "limit": 3},
                    headers=headers, timeout=8)
            if res.status_code != 200:
                # 401 重取后仍失败 / 429 / 5xx -> 不可达, 可重试
                return None, ST_RETRYABLE
            results = res.json().get("data") or []
            if not results:
                return None, ST_MISS  # 200 但查无此剧
            series_id = results[0].get("tvdb_id") or results[0].get("id")
            # 完整版 extended (非 short): 含 seasons[].episodes[], 用于季级元数据
            res2 = self.session.get(
                f"https://api4.thetvdb.com/v4/series/{series_id}/extended?short=false",
                headers=headers, timeout=10)
            if res2.status_code != 200:
                # 搜索已确认条目存在, 详情不可达 -> 可重试 (不判 MISS)
                return None, ST_RETRYABLE
            ser = res2.json().get("data") or {}
            # 子任务 8·D: 官方中文翻译优先 (translations 端点), 缺失/失败回退英文原数据,
            # 不判 MISS (TheTVDB 原始数据为英文, 翻译接口为唯一中文化路径)
            tr, tr_lang = self._tvdb_translation(series_id, headers)
            tr_eps = self._tvdb_episode_translations(series_id, tr_lang, headers) if tr else {}
            first_date = str(ser.get("firstAired") or "")[:10]
            seasons_list = ser.get("seasons") or []
            season_meta = {}
            season_episodes = {}
            for s in seasons_list:
                if not isinstance(s, dict):
                    continue
                s_num = s.get("number")
                if not isinstance(s_num, int) or s_num < 1:
                    continue
                season_meta[str(s_num)] = {
                    "name": s.get("name") or "",
                    "overview": s.get("overview") or "",
                    "cover": s.get("image") or "",
                    "air_date": str(s.get("year") or s.get("firstAired") or ""),
                    "cast": [],
                }
                ep_map = {}
                for ep in s.get("episodes") or []:
                    if not isinstance(ep, dict):
                        continue
                    ep_num = ep.get("number")
                    if not isinstance(ep_num, int):
                        continue
                    # 分集中文译名优先 (批量端点按 episode id 匹配), 缺失回退英文
                    _ep_tr = tr_eps.get(ep.get("id")) or {}
                    ep_map[ep_num] = {
                        "name": _ep_tr.get("name") or ep.get("name") or "",
                        "air_date": ep.get("aired") or "",
                        "overview": _ep_tr.get("overview") or ep.get("overview") or "",
                        "still": ep.get("image") or "",
                        "runtime": 0,
                        "rating": _safe_float(ep.get("rating")) if ep.get("rating") else 0.0,
                        "vote_count": 0,
                    }
                if ep_map:
                    season_episodes[str(s_num)] = ep_map
            seasons = {"totalCount": len(seasons_list)}
            status = ser.get("status") if isinstance(ser.get("status"), dict) else {}
            tvdb_meta = {
                "source": "TheTVDB",
                "tvdb_id": series_id,
                "title": str(tr.get("name") or "").strip() or ser.get("name") or title,
                "original_title": ser.get("originalName") or "",
                "overview": (str(tr.get("overview") or "").strip() or ser.get("overview") or "")[:2000],
                "poster": ser.get("image") or "",
                "rating": round(_safe_float(ser.get("score")), 1),
                "rating_source": "TheTVDB",
                "vote_count": 0,
                "year": first_date[:4],
                "first_air_date": first_date,
                "genres": [g.get("name") for g in ser.get("genres") or [] if g.get("name")],
                "number_of_seasons": seasons.get("totalCount") or 0,
                "status": str(status.get("name") or "").lower(),
                "season_meta": season_meta,
                "season_episodes": season_episodes,
            }
            return tvdb_meta, "hit"
        except Exception as e:
            self._log_err("tvdb_query", e)
            return None, ST_RETRYABLE

    def _query_omdb(self, title: str, year: str = None) -> tuple:
        """OMDb (IMDb 代理) 评分兜底 (需免费 Key)。返回 (meta|None, status) 三态。
        子任务 8·D (T7 P1-3): OMDb 实测全英文 -> 收窄为「评分+票数」兜底,
        英文 title/overview/poster/genres/cast/director 等一律不返回, 避免英文污染中文产物;
        canonical_title 由 enrich 的 CJK 保护保持条目原题。
        已知取舍: 收窄后无 genres, OMDb 命中无法判纪录片 (标注假设, 交主 AD 复核)。"""
        apikey = self.settings.get("omdb_api_key", "").strip()
        if not apikey:
            return None, ST_SKIPPED
        any_retryable = False
        try:
            d = None
            for variant in self._search_variants(title):
                params = {"apikey": apikey, "t": variant}
                if year and str(year).isdigit():
                    params["y"] = str(year)
                res = self.session.get("https://www.omdbapi.com/", params=params, timeout=8)
                if res.status_code != 200:
                    any_retryable = True  # 429/5xx/网关异常
                    continue
                j = res.json()
                if j.get("Response") == "True":
                    d = j
                    break
                err = str(j.get("Error") or "").lower()
                if "limit" in err or "rate" in err:
                    any_retryable = True  # 配额超限
            if d is None:
                return (None, ST_RETRYABLE) if any_retryable else (None, ST_MISS)
            v_str = (d.get("imdbVotes") or "").replace(",", "")
            omdb_meta = {
                "source": "OMDb",
                "omdb_type": d.get("Type") or "",
                "rating": _safe_float(d.get("imdbRating")),
                "rating_source": "IMDb",
                "vote_count": int(v_str) if v_str.isdigit() else 0,
                "year": str(d.get("Year") or "")[:4],
            }
            return omdb_meta, "hit"
        except Exception as e:
            self._log_err("omdb_query", e)
            return None, ST_RETRYABLE

    _DOUBAN_UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                  "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")

    def _query_douban(self, title: str) -> tuple:
        """
        豆瓣刮削 (移动端 rexxar 接口, 免 JS 挑战):
        search/subjects 定位条目 -> movie|tv/{id} 深度详情
        (评分/人数/简介/演员/导演/国家/语言/片长/集数/大图)

        三态返回 (决策 3): (meta, "hit") / (None, ST_MISS) / (None, ST_RETRYABLE)
        - 熔断开启 = 该源整体不可达 -> RETRYABLE (而非未命中)
        - 搜索接口非 200 / 异常 -> search_retry, 无结果时归 RETRYABLE
        - 搜索成功但查无此片 -> MISS
        """
        if not self.enable_douban:
            return None, ST_SKIPPED
        # 自动熔断: 连续失败达阈值后本轮直接跳过豆瓣 (海外 runner 常态性不可达)
        # 熔断 = 该源不可达, 按"限流/不可达"归入可重试, 不算未命中
        if self._douban_fail >= 5:
            return None, ST_RETRYABLE
        headers = {
            "User-Agent": self._DOUBAN_UA,
            "Referer": "https://m.douban.com/search/"
        }
        try:
            target = None
            search_retry = False
            for variant in self._search_variants(title)[:3]:
                q = urllib.parse.quote(variant)
                try:
                    res = self.session.get(
                        f"https://m.douban.com/rexxar/api/v2/search/subjects?q={q}&count=3",
                        headers=headers, timeout=8)
                    if res.status_code != 200:
                        search_retry = True
                        continue
                    subs = ((res.json().get("subjects") or {}).get("items")) or []
                except Exception as e:
                    self._log_err("douban_search", e)
                    search_retry = True
                    continue
                if subs:
                    target = (subs[0].get("target") or {})
                    break
            # 兜底通路: subject_suggest (rexxar 搜索召回差时常用)
            if not target:
                for variant in self._search_variants(title)[:2]:
                    try:
                        r2 = self.session.get(
                            f"https://movie.douban.com/j/subject_suggest?q={urllib.parse.quote(variant)}",
                            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                                     "Referer": "https://movie.douban.com/"},
                            timeout=6)
                        sug = r2.json() if r2.status_code == 200 else []
                        if sug and isinstance(sug, list) and sug[0].get("id"):
                            f0 = sug[0]
                            target = {"id": str(f0.get("id", "")), "title": f0.get("title") or variant,
                                      "original_title": f0.get("sub_title") or "",
                                      "cover_url": f0.get("img") or "", "year": f0.get("year") or "",
                                      "type_name": "电视剧" if f0.get("type") == "tv" else "电影"}
                            break
                    except Exception as e:
                        self._log_err("douban_suggest", e)
                        continue
            if not target:
                # 搜索通路均响应成功但查无此片 -> 真未命中; 存在限流/异常 -> 可重试
                if not search_retry:
                    self._douban_fail = 0
                    return None, ST_MISS
                return None, ST_RETRYABLE
            # 至少一条搜索通路响应成功, 熔断计数清零
            self._douban_fail = 0
            douban_id = str(target.get("id") or "")
            if not douban_id:
                return None, ST_MISS

            meta = {
                "source": "豆瓣",
                "douban_id": douban_id,
                "title": target.get("title") or title,
                "original_title": target.get("original_title") or "",
                "poster": target.get("cover_url") or "",
                "year": str(target.get("year") or ""),
                "first_air_date": "",
                "rating": 0.0,
                "rating_source": "豆瓣",
                "overview": ""
            }

            self._throttle("douban", float(self.settings.get("douban_min_interval", 1.0) or 1.0))
            det_headers = {
                "User-Agent": self._DOUBAN_UA,
                "Referer": f"https://m.douban.com/movie/subject/{douban_id}/"
            }
            # is_tv 时用 tv 端点, 否则 movie; 两端点结构一致
            detail_path = f"tv/{douban_id}" if target.get("type_name") == "电视剧" or target.get("is_tv") else f"movie/{douban_id}"
            det = None
            try:
                d_res = self.session.get(
                    f"https://m.douban.com/rexxar/api/v2/{detail_path}",
                    headers=det_headers, timeout=8)
                if d_res.status_code == 200:
                    det = d_res.json()
            except Exception as e:
                # 详情拉取失败不致命: 保留搜索阶段部分元数据, 仍按命中返回
                self._log_err("douban_detail", e)
            if det:
                rating = det.get("rating") or {}
                meta["rating"] = _safe_float(rating.get("value"))
                meta["vote_count"] = int(rating.get("count") or 0)
                meta["overview"] = (det.get("intro") or "")[:2000]
                meta["cast"] = [a.get("name") for a in det.get("actors") or [] if a.get("name")][:10]
                meta["director"] = [d0.get("name") for d0 in det.get("directors") or [] if d0.get("name")]
                if det.get("countries"):
                    meta["country"] = det["countries"][0]
                if det.get("languages"):
                    lang_map = {"汉语普通话": "普通话", "英语": "英语"}
                    meta["original_language"] = lang_map.get(det["languages"][0], det["languages"][0])
                if det.get("episodes_count"):
                    meta["number_of_episodes"] = int(det["episodes_count"])
                if det.get("durations"):
                    m = re.search(r'(\d+)', det["durations"][0])
                    if m:
                        meta["runtime"] = int(m.group(1))
                if det.get("pubdate"):
                    meta["first_air_date"] = det["pubdate"][0].split("(")[0]
                if det.get("year"):
                    meta["year"] = str(det["year"])
                if det.get("title"):
                    meta["title"] = det["title"]
                if det.get("original_title"):
                    meta["original_title"] = det["original_title"]
                if det.get("cover_url"):
                    meta["poster"] = det["cover_url"].replace("m_ratio_poster", "l_ratio_poster")
            # 权威类型标记 (子任务 5): 豆瓣 movie/tv 端点即豆瓣对条目类型的权威判定
            meta["douban_kind"] = "tv" if detail_path.startswith("tv/") else "movie"
            if det and det.get("genres"):
                meta["genres"] = det["genres"]
            return meta, "hit"
        except Exception as e:
            # 网络异常/超时计入熔断计数, 连续 5 次后跳过豆瓣; 按"不可达"归入可重试
            self._douban_fail += 1
            self._log_err("douban_query", e)
            return None, ST_RETRYABLE

    def enrich(self, item: dict) -> dict:
        """
        对电影/电视剧采集项进行深度元数据丰富与对齐 (TMDB + 豆瓣)。
        """
        search_title = item.get("search_title") or item.get("title")
        category = item.get("category", "movies")
        year = item.get("year")

        # 三态队列键: 必须在权威校正改写 item["category"] 之前计算
        qkey = rq.key_of(item)
        # 子任务 8: v4 -> v5, 作废旧缓存 (已移除源结构 / OMDb 英文字段 / None 投毒条目)
        cache_key = f"v5:{category}:{search_title}:{year or ''}"
        meta = self.cache.get(cache_key)
        attempted = []  # 本轮实际尝试过的源: [(源名, status)]
        if meta is None:
            # 缓存未命中 (或历史版本遗留的 None 投毒条目) -> 走全链查询
            # 冗余源矩阵: TMDB -> TheTVDB(剧集) -> 豆瓣 -> Bilibili(动漫) -> OMDb(评分兜底)
            # 三态聚合 (决策 3): 任一源命中即短路; 未命中才聚合各源状态做 MISS/RETRYABLE 判定
            def _attempt(label, fn):
                m, st = fn()
                attempted.append((label, st))
                return m

            meta = _attempt("TMDB", lambda: self._query_tmdb(search_title, category, year))
            if not meta and category in ("tv", "anime", "variety", "short_tv"):
                meta = _attempt("TheTVDB", lambda: self._query_tvdb(search_title))
            if not meta:
                meta = _attempt("豆瓣", lambda: self._query_douban(search_title))
            if not meta and category == "anime":
                meta = _attempt("Bilibili", lambda: self._query_bilibili(search_title))
            if not meta:
                meta = _attempt("OMDb", lambda: self._query_omdb(search_title, year))
            if not meta and item.get("douban_id"):
                meta = {
                    "source": "豆瓣",
                    "douban_id": item["douban_id"],
                    "title": item["title"],
                    "rating": _safe_float(item.get("douban_score")),
                    "rating_source": "豆瓣",
                    "poster": item.get("poster", "")
                }
                attempted.append(("源站豆瓣id", "hit"))

            # 只缓存成功命中的 meta: 失败返回 None 不落缓存,
            # 避免网络故障/限流期间的 None 永久污染缓存导致下轮全量跳过
            if meta is not None:
                self.cache[cache_key] = meta
            # 节流写盘: 新增条数距上次落盘 >= 500 才全量写一次
            if len(self.cache) - self._last_saved >= 500:
                _save_cache(self.cache)
                self._last_saved = len(self.cache)

        if meta:
            # 命中 (HIT): 从待办队列移除 (上轮限流条目本轮重试成功的出口)
            rq.resolve(qkey)
            item["scrape_status"] = "hit"
            item.pop("scrape_status_reason", None)
            item["matched"] = True
            # T7 P0-1: canonical_title CJK 优先保护 - 源标题非中文而条目原题已是中文时
            # 不改写 (挡 OMDb 英文标题等非中文源覆盖; 豆瓣/TMDB zh-CN 天然中文不受影响)
            _ct = meta.get("title") or item["title"]
            if _ct != item["title"] and not has_cjk(str(_ct)) and has_cjk(str(item["title"])):
                _ct = item["title"]
            item["canonical_title"] = _ct
            item["original_title"] = meta.get("original_title", "")
            # 季级条目标识: 同一部剧的不同季各自独立, 避免跨季混流
            season_num = int(item.get("season") or 1)
            base_bangou = (
                f"tmdb_{meta['tmdb_id']}" if meta.get("tmdb_id") else
                f"bilibili_{meta['bilibili_season_id']}" if meta.get("bilibili_season_id") else
                f"tvdb_{meta['tvdb_id']}" if meta.get("tvdb_id") else
                f"douban_{meta['douban_id']}" if meta.get("douban_id") else None)
            item["bangou"] = f"{base_bangou}_s{season_num}" if (base_bangou and season_num > 1) else base_bangou
            item["tmdb_id"] = meta.get("tmdb_id")
            item["douban_id"] = meta.get("douban_id") or item.get("douban_id")
            item["source_provider"] = meta.get("source", "")

            # 权威校正 (子任务 5): category 以刮削权威数据复核, 不再默认信任入口源站分类
            corrected, reason = authoritative_category(item.get("source_provider"), meta)
            if corrected is None:
                # 校正后不在四类 (纪录片/成人/单集素材等) -> 丢弃, 进 unmatched 审计
                # (与 all_sources_miss 区分: scrape_status_reason=not_in_whitelist)
                if not getattr(self, "quiet_items", False):
                    print(f"[权威丢弃] {item.get('title')}: 刮削判定非四类 ({reason})")
                item["matched"] = False
                item["scrape_status"] = "category_discard"
                item["scrape_status_reason"] = "not_in_whitelist"
                item["discard_detail"] = reason or ""  # taxonomy 描述性细节留审计
                return item
            if corrected and corrected != category:
                record_conflict(item.get("title") or item.get("canonical_title") or "",
                                category, corrected, reason)
                if not getattr(self, "quiet_items", False):
                    print(f"[权威校正] {item.get('title')}: 入口 {category} -> {corrected} ({reason})")
                item["category"] = corrected

            r_val = meta.get("rating")
            if r_val is not None and _safe_float(r_val) > 0:
                item["rating"] = round(_safe_float(r_val), 1)
                item["rating_source"] = meta.get("rating_source") or "TMDB"
            elif item.get("douban_score") and _safe_float(item["douban_score"]) > 0:
                item["rating"] = round(_safe_float(item["douban_score"]), 1)
                item["rating_source"] = "豆瓣"
            else:
                item.pop("rating", None)
                item.pop("rating_source", None)

            if meta.get("poster"):
                item["poster"] = meta["poster"]
            if meta.get("backdrop"):
                item["backdrop"] = meta["backdrop"]
            if meta.get("overview"):
                m_ov = clean_overview(meta["overview"])
                cur_ov = clean_overview(item.get("overview", ""))
                # TMDB 中文翻译缺失时会返回英文原文: 有中文就优先中文
                if has_cjk(m_ov) or not cur_ov:
                    item["overview"] = m_ov
            if meta.get("year"):
                item["year"] = meta["year"]
            if meta.get("first_air_date"):
                item["first_air_date"] = meta["first_air_date"]
            if meta.get("genres"):
                # T7 P0-2: genres 落库统一归中 (taxonomy.normalize_genre 同义映射 +
                # lower() 兜底; 映射失败保留原词, 不缩字段)
                item["genres"] = [normalize_genre(g) or str(g) for g in meta["genres"]]
            if meta.get("cast"):
                item["cast"] = meta["cast"]
            if meta.get("director"):
                item["director"] = meta["director"]
            if meta.get("original_language"):
                item["original_language"] = meta["original_language"]
            if meta.get("vote_count"):
                item["vote_count"] = meta["vote_count"]
            for _k in ("popularity", "runtime", "homepage", "status",
                       "number_of_seasons", "number_of_episodes",
                       "logo", "certification", "cast_structured",
                       "director_structured", "season_episodes", "season_meta",
                       "vote_count", "country", "original_language"):
                if meta.get(_k):
                    item[_k] = meta[_k]
        else:
            # 全源未命中 (决策 3): 先区分"真未命中"与"限流/不可达"
            retryable_sources = [lbl for lbl, st in attempted if st == ST_RETRYABLE]
            if retryable_sources:
                # RETRYABLE: 进待办队列, 不进 videos.json 也不进 unmatched,
                # 等网络恢复或下一轮任务重新刮削
                failed_sources = [{"source": lbl, "reason": st} for lbl, st in attempted if st != "hit"]
                rq.record(qkey, item, failed_sources)
                if not getattr(self, "quiet_items", False):
                    print(f"[待办] {item.get('title')}: 限流/不可达 ({', '.join(retryable_sources)}), 进重试队列")
                return None
            # MISS (决策 2): 全部源真未命中 = 资源无价值, 不进 videos.json,
            # 仅记入 unmatched.json 审计 (scrape_status_reason=all_sources_miss)
            # 子任务 8: 动漫源移除后为五源全 miss 判定 (TMDB/TheTVDB/豆瓣/Bilibili/OMDb 全部真未命中)
            if not getattr(self, "quiet_items", False):
                print(f"[丢弃] 全源未命中: {item.get('title')}")
            item["matched"] = False
            item["scrape_status"] = "all_sources_miss"
            item["scrape_status_reason"] = "all_sources_miss"
            item["canonical_title"] = item["title"]
            rq.resolve(qkey)  # 上轮待办条目本轮确认未命中 -> 从队列移除, 正常丢弃

        return item

    def close(self):
        if self._err_counts:
            summary = ", ".join(f"{k}={v}" for k, v in sorted(self._err_counts.items()))
            progress_log.event(f"[刮削异常汇总] {summary}")
        _save_cache(self.cache)
