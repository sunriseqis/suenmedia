"""
自定义网页爬虫示例模板 (Custom Crawler Demo)

可针对特定影视网站编写 HTML 解析逻辑，提取播放地址与分集。

约定（与 crawlers/harvest.py 的 CRAWLER_DISPATCHER + 动态插件兼容）：
- 站点配置 `config.json` 里把 `type` 设为非 maccms_v10（如 "custom_page"），
  或显式指定 `crawler_plugin`，harvest 会动态 import `crawlers.<plugin_name>`，
  调用本模块的 `crawl()`。
- `crawl()` 可为 async（推荐，走并发管线；harvest 自动识别），
  也可保留 sync 签名（自动线程池兜底）。
- 返回 `list[RawItem]`（字段契约见 crawlers/maccms.py build_item 或设计文档 §2 P1）。
"""
from __future__ import annotations

from typing import Any, Dict, List


async def crawl(site_config: Dict[str, Any], client=None,
                hours: int = 24, max_pages: int = 5) -> List[Dict[str, Any]]:
    """示例爬虫入口：返回 RawItem 列表。

    Args:
        site_config: 站点配置 dict（config.json SITES 中该站）。
        client: 共享 AsyncHttpClient（鸭子类型）；不使用可忽略。
        hours: 增量窗口小时数。
        max_pages: 页数上限提示（契约保留）。
    """
    site_name = site_config.get("name", "自定义站点")
    items: List[Dict[str, Any]] = []
    # 在此处实现具体站点解析逻辑：
    #   1. 用 client.get(url, params=...) 抓取页面/接口（返回 FetchResult，data 自动解析 JSON）；
    #   2. 解析出每条作品 → build 为 RawItem（字段见 crawlers.maccms.build_item）；
    #   3. 追加进 items。
    _ = (site_name, client, hours, max_pages)
    return items