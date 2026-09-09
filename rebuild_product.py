# -*- coding: utf-8 -*-
"""rebuild_product.py
从 suenmedia.db 的 raw_library 完整重建符合消费端 (suenplayer) 2.1 契约的 videos.json。
"""

import os
import json
import sqlite3
import hashlib
from datetime import datetime

PRODUCT_DIR = os.path.join(os.path.dirname(__file__), "product")
VIDEOS_PATH = os.path.join(PRODUCT_DIR, "videos.json")
DB_PATH = os.path.join(PRODUCT_DIR, "suenmedia.db")

def rebuild():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}")
        return

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cursor = conn.cursor()
    rows = cursor.execute("""
        SELECT merge_key, category, norm_title, year, payload_json
        FROM raw_library
    """).fetchall()

    print(f"Found {len(rows)} raw entities in database")

    items = []
    for merge_key, category, norm_title, year, payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue

        primary = payload.get("primary") or {}
        title = (primary.get("name") or norm_title or "").strip()
        if not title:
            continue

        cover = (primary.get("pic") or "").strip()
        overview = (primary.get("blurb") or primary.get("remarks") or "").strip()
        site = (primary.get("site") or "").strip()
        type_name = (primary.get("type_name") or "").strip()
        year_str = str(year or primary.get("year") or "").strip()

        # 提取线路与分集
        lines = primary.get("lines") or []
        # lines 结构: [{"line_name": "...", "episodes": [{"name": "...", "url": "..."}]}]
        # 或 [{"name": "...", "url": "..."}]
        episodes = []
        if lines and isinstance(lines, list):
            first = lines[0]
            if isinstance(first, dict) and "episodes" in first:
                # 嵌套 line 结构
                primary_eps = first.get("episodes") or []
                alt_lines = lines[1:]
                for idx, ep_item in enumerate(primary_eps):
                    ep_name = str(ep_item.get("name") or f"第{idx+1}集")
                    ep_url = str(ep_item.get("url") or "").strip()
                    if not ep_url:
                        continue
                    alt_urls = []
                    for alt_line in alt_lines:
                        l_name = alt_line.get("line_name", "备用线路")
                        l_eps = alt_line.get("episodes") or []
                        if idx < len(l_eps):
                            a_url = str(l_eps[idx].get("url") or "").strip()
                            if a_url:
                                alt_urls.append({"source": l_name, "url": a_url})
                    episodes.append({
                        "ep_number": idx + 1,
                        "ep_title": ep_name,
                        "url": ep_url,
                        "url_type": "m3u8" if ".m3u8" in ep_url else "stream",
                        "alt_urls": alt_urls
                    })
            else:
                # 扁平结构
                for idx, ep_item in enumerate(lines):
                    if not isinstance(ep_item, dict):
                        continue
                    ep_name = str(ep_item.get("name") or f"第{idx+1}集")
                    ep_url = str(ep_item.get("url") or "").strip()
                    if ep_url:
                        episodes.append({
                            "ep_number": idx + 1,
                            "ep_title": ep_name,
                            "url": ep_url,
                            "url_type": "m3u8" if ".m3u8" in ep_url else "stream",
                            "alt_urls": []
                        })

        # 判断是 series 还是 video
        is_series = category in ("tv", "anime", "variety", "short_tv") or len(episodes) > 1

        bangou = primary.get("bangou") or f"v_{hashlib.md5((title + (episodes[0]['url'] if episodes else '')).encode()).hexdigest()[:12]}"

        item = {
            "type": "series" if is_series else "video",
            "bangou": bangou,
            "title": title,
            "original_title": "",
            "cover": cover,
            "backdrop": cover,
            "overview": overview,
            "region": "国产剧" if category == "tv" else ("动漫" if category == "anime" else ("综艺" if category == "variety" else "电影")),
            "group_name": type_name or ("剧集" if is_series else "电影"),
            "year": year_str,
            "date": "",
            "site": site,
            "tags": [t.strip() for t in [type_name, category] if t.strip()],
            "genres": [type_name] if type_name else [],
            "status": "completed",
            "rating": None,
            "rating_source": "",
            "vote_count": 0,
            "first_air_date": "",
            "runtime": 0,
            "cast": [],
            "director": [],
        }

        if is_series:
            if not episodes:
                episodes = [{
                    "ep_number": 1,
                    "ep_title": "正片",
                    "url": "https://placeholder.invalid/video.m3u8",
                    "url_type": "m3u8",
                    "alt_urls": []
                }]
            item["seasons"] = [{
                "season_number": 1,
                "season_title": "第一季",
                "season_cover": cover,
                "season_overview": overview,
                "episodes": episodes
            }]
            item["episode_count"] = len(episodes)
            item["number_of_episodes"] = len(episodes)
            item["number_of_seasons"] = 1
            item["url"] = episodes[0]["url"] if episodes else ""
            item["alt_urls"] = episodes[0].get("alt_urls", []) if episodes else []
        else:
            item["url"] = episodes[0]["url"] if episodes else ""
            item["alt_urls"] = episodes[0].get("alt_urls", []) if episodes else []

        items.append(item)

    payload_2_1 = {
        "schema_version": "2.1",
        "project": {
            "name": "影视仓",
            "slug": "suenmedia"
        },
        "generated_at": datetime.now().isoformat(),
        "source": "suenmedia",
        "generator": "suenmedia-exporter",
        "items": items
    }

    os.makedirs(PRODUCT_DIR, exist_ok=True)
    tmp_path = VIDEOS_PATH + f".tmp.{os.getpid()}"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload_2_1, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, VIDEOS_PATH)
    print(f"Successfully exported {len(items)} items to {VIDEOS_PATH} under 2.1 contract!")

if __name__ == "__main__":
    rebuild()
