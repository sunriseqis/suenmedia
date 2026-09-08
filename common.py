import os
import json
import subprocess
from functools import lru_cache
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import html as _html
import re as _re

def clean_overview(text: str) -> str:
    """简介清洗: HTML实体解码 → 去标签 → 去&nbsp;/零宽字符 → 压缩空白"""
    if not text:
        return ""
    t = _html.unescape(str(text))
    t = _re.sub(r'<[^>]+>', ' ', t)
    t = t.replace('\xa0', ' ').replace('\u200b', '')
    t = _re.sub(r'\s+', ' ', t).strip()
    return t

def has_cjk(text: str) -> bool:
    return bool(_re.search(r'[\u4e00-\u9fff]', text or ''))

_DEFAULT_SETTINGS = {
    "tmdb_api_key": "",
    "tmdb_api_base": "https://api.themoviedb.org/3",
    "tmdb_image_base": "https://image.tmdb.org/t/p/w500",
    "enable_tmdb": True,
    "enable_douban_fallback": True,
    "enable_m3u8_check": True,
    "m3u8_timeout": 5,
    "max_workers": 5,
    "crawl_hours": 24,
    "max_pages_per_site": 20,
    "proxy": ""
}

def _load_dotenv():
    """加载本地 .env (不入库): 简单 KEY=VALUE 格式, 不覆盖已有环境变量"""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, encoding="utf-8") as env_f:
            for line in env_f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    except Exception:
        pass

_load_dotenv()

@lru_cache(maxsize=1)
def load_settings():
    """加载 settings.json 配置，包含安全默认值与环境变量覆盖 (进程内缓存, 配置运行期不变)"""
    cfg = dict(_DEFAULT_SETTINGS)
    if os.path.exists("settings.json"):
        try:
            with open("settings.json", "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception as e:
            print(f"[警告] 读取 settings.json 失败: {e}")
    # 环境变量优先级最高
    env_tmdb = os.getenv("TMDB_API_KEY")
    if env_tmdb:
        cfg["tmdb_api_key"] = env_tmdb
    env_proxy = os.getenv("CRAWL_PROXY")
    if env_proxy:
        cfg["proxy"] = env_proxy
    return cfg

@lru_cache(maxsize=1)
def load_config():
    """加载 config.json 采集站点与分类规则 (进程内缓存; 若运行期改写 config.json 需调 load_config.cache_clear())"""
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[警告] 读取 config.json 失败: {e}")
    return {"SITES": [], "CATEGORY_RULES": {}}

def get_session(use_proxy=False):
    """创建带有连接池和自动重试的 requests.Session

    重试策略: session 层仅保留 total=1 兜底层 (连接抖动恢复), 调用级
    (_fetch_page / _tmdb_get) 的重试循环是唯一业务重试层, 避免双层重试放大。
    连接池按管道并发归位: pipeline_workers*6, 消除 pool_maxsize 偏小导致的
    "Connection pool is full, discarding connection" 连接重建开销。
    """
    session = requests.Session()
    retries = Retry(total=1, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504],
                    respect_retry_after_header=True)
    pipeline_workers = int(load_settings().get("pipeline_workers", 20) or 20)
    pool_size = max(20, pipeline_workers * 6)
    adapter = HTTPAdapter(max_retries=retries, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })
    settings = load_settings()
    proxy = settings.get("proxy", "")
    if use_proxy and proxy:
        session.proxies = {"http": proxy, "https": proxy}
    return session

def categorize_type(raw_type: str, category_rules: dict = None) -> str:
    """根据源站分类名称归一化映射: movies / tv / anime / variety / short_tv / discard

    白名单语义 (子任务 4): 别名表与启发式规则都未命中的分类返回 "discard"，
    表示白名单外的垃圾分类 (纪录片/音乐/少儿/教育/新闻/体育等)，
    由源头 _skip_reason 直接丢弃——不进库、不进 unmatched、不占用刮削预算。
    空分类名: maccms 源站空 type_name 绝大多数为电影，仍归 "movies"，
    异常由黑名单关键词第二道防线兜底。
    """
    if not raw_type or not raw_type.strip():
        return "movies"
    raw_type = raw_type.strip()
    rules = category_rules or load_config().get("CATEGORY_RULES", {})
    
    for cat_key, aliases in rules.items():
        if raw_type in aliases:
            return cat_key
        for alias in aliases:
            if alias in raw_type or raw_type in alias:
                return cat_key
                
    # 启发式兜底
    if any(k in raw_type for k in ["短剧", "爽剧"]):
        return "short_tv"
    if any(k in raw_type for k in ["剧", "连续剧"]):
        return "tv"
    if any(k in raw_type for k in ["动漫", "动画", "新番"]):
        return "anime"
    if any(k in raw_type for k in ["综艺", "秀", "演艺"]):
        return "variety"
    # maccms 电影分类普遍以"片"结尾 (动作片/喜剧片/爱情片...), 未枚举的"XX片"
    # 按电影保留, 已知垃圾 (纪录片/预告片/音乐片...) 仍由黑名单关键词第二道防线剔除
    if raw_type.endswith("片"):
        return "movies"
    # 白名单外: 未识别分类一律返回 discard (子任务 4)，不再兜底为 movies
    return "discard"

def send_pushplus(title: str, content: str):
    """向 PushPlus 发送微信推送通知"""
    token = os.getenv("PUSHPLUS_TOKEN")
    if not token:
        print("[提示] 未检测到 PUSHPLUS_TOKEN 环境变量，跳过微信通知")
        return
    url = "https://www.pushplus.plus/send"
    data = {
        "token": token,
        "title": title,
        "content": content,
        "template": "html"
    }
    try:
        res = requests.post(url, json=data, timeout=10)
        res_json = res.json()
        if res_json.get("code") == 200:
            print("[通知] PushPlus 微信通知发送成功")
        else:
            print(f"[警告] PushPlus 发送失败: {res_json.get('msg')}")
    except Exception as e:
        print(f"[错误] PushPlus 推送异常: {e}")

def git_push_backup(commit_msg: str):
    """执行 Git commit 与 push 同步"""
    if os.getenv("GITHUB_ACTIONS") != "true" and not os.path.exists(".git"):
        print("[提示] 非 GitHub Actions 环境且无本地 Git 仓库，跳过推送")
        return
    try:
        subprocess.run(["git", "config", "--local", "user.email", "action@github.com"], check=True)
        subprocess.run(["git", "config", "--local", "user.name", "GitHub Action"], check=True)
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", commit_msg], check=False)
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("[完成] Git 增量推送成功")
    except Exception as e:
        # 仅当确实处于 rebase 冲突中断状态时才 abort, 避免对正常失败误执行 abort
        if os.path.exists(".git/rebase-merge"):
            subprocess.run(["git", "rebase", "--abort"], check=False)
        # 推送失败必须显性化: 进通知通道, 不再静默吞掉 (产物更新滞留 runner 磁盘会被回收)
        print(f"[错误] Git 推送失败: {e}")
        try:
            send_pushplus(
                title="⚠️ SuenMedia Git 推送失败",
                content=f"<b>产物提交/推送失败, 本轮更新滞留 runner 本地磁盘:</b><br/><pre>{e}</pre>"
            )
        except Exception as notify_err:
            print(f"[错误] 推送失败告警发送异常: {notify_err}")
