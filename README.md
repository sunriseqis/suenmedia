# SuenMedia · 影视全自动采集 / 刮削 / 托管系统

从 10 大 MacCMS 采集源全自动抓取 **电影 / 电视剧 / 动漫 / 综艺** 四类内容，经前置过滤、域名级探活、素材库粗合并、置信度打分刮削、精合并后导出契约 v3 分片产物，并由 GitHub Actions 每 12 小时无人值守跑一轮，结果自动 git 同步 + PushPlus 微信汇报。

---

## 1. 系统链路

```
GitHub Actions（每 12 小时）/ Docker / 本地命令行
                    │
                    ▼
    main.py  harvest（建库/增量）  ·  run（端到端流水线，单轮 30 分钟预算）
                    │
    ├─ P1 采集       crawlers.harvest（跨站异步 + RawSeen 去重 + L1-L5 前置过滤）
    ├─ P2 探活       DomainProber（域名级：alive 0 请求 / 新域名探 1 次 / dead 丢线路）
    ├─ P3 粗合并     CoarseMerger → 素材库 raw_library（按归一化标题合并同片）
    ├─ P4 刮削       ScrapeOrchestrator（缓存 → 源链 → 置信度打分 → S8 ID 校验 → Tier B 补全）
    │                源链: TMDB → TheTVDB → 豆瓣 → Bilibili(动漫) → OMDb(评分兜底)
    ├─ P5 精合并     FineMerger（bangou 归并 + 跨站线路合并 + 入库门槛）
    └─ P6 导出       Exporter → product/（契约 v3 分片原子写）
                    │
                    ▼
        git commit & push  ·  PushPlus 微信运行汇报
```

一个素材库件事贯穿始终：`raw_library` 存归一化后的粗实体（同片多站线路合一、游标推进幂等），刮削命中即把富化结果写回库；中断 / 撞超时后再次运行自动从游标接续。

## 2. 核心特性

- **四类白名单，精而准**：产物只保留 `movies / tv / anime / variety`；L1–L5 前置过滤拦掉短剧、解说、微电影、体育、新闻、儿童、MV、相声小品等 70+ 黑词（`config.json` / `settings.json` 可增删）。
- **素材库热 / 冷双通道**：
  - 热通道：近 7 天新更新条目，Tier AB 全字段补全（search + detail，约 2 请求/条）；
  - 冷通道：素材库游标 FIFO 消费存量，Tier A 仅搜索（约 1 请求/条），`variety` 权重 ×2 优先补样本。
- **单轮 30 分钟预算调度**：crawl 420s / probe 90s / coarse 60s / scrape ≤1200s / fine 120s / export 240s / safety 90s；限速按 `rate × 0.9` 生效，每 200 条复核预计结束时间，超时即停派。
- **置信度打分匹配**：S1 中文名相似度 + S2 原名/译名 + S3 年份 + S4 类型 + S5 集数 + S6 热度 + S7 序号综合评分，翻译差异保护（中英译名对不误杀），S8 豆瓣 ID 硬证据校验；阈值 high 70（改写权威标题）/ medium 55（保留源站标题）/ low 40（须过 S8 才入库）。
- **入库门槛（精而准）**：`matched` + 封面非空 + 简介非空 + `confidence >= 55`，不达标进 `unmatched.json` 审计，正式库只见高质量条目。
- **负缓存四道防污染闸**：retryable 绝不写负缓存 / 全局错误率 >30% 熔断 / 429 暂停 / 人工清除，杜绝"刮一次失败永久黑名单"。
- **域名级探活**：线路按 CDN 域名聚合，活域名后续 0 请求放行，死域名丢线路——历史实测 449,935 条线路仅分布在 147 个域名。
- **契约 v3 分片产物**：`videos.json` 与 `episodes.jsonl.gz` 分离防单文件膨胀，全部原子写（`.tmp` → `os.replace`），杜绝半截文件。
- **产物防退变**：harvest 再入库不会覆盖已刮削的富化数据；热窗口出现新更新才重置状态触发再刮削。

## 3. 快速开始

### 3.1 本地运行

```bash
pip install -r requirements.txt

python main.py harvest --hours 24     # 增量采集入库（仅建素材库，不刮削不导出）
python main.py harvest --full         # 阶段 0 全量建库（翻到站点尽头，可反复派发接续）
python main.py run --hours 24         # 端到端流水线：采集+探活+刮削+合并+导出+推送
python main.py run --dry-run          # 干跑：不执行 git 推送与外部通知
python main.py run --scrape-only      # 排障：跳过采集，仅消费素材库存量刮削
python main.py run --no-check         # 跳过线路探活
```

首次跑之前：`settings.json` 填入 `tmdb_api_key`（必填，见 4.3）。

### 3.2 GitHub Actions 自动托管

| Workflow | 名称 | 触发 | 执行 |
|---|---|---|---|
| harvest.yml | SuenMedia 定时采集与同步 | cron 每 12 小时 + 手动 | `python main.py run --hours N` |
| full-crawl.yml | SuenMedia 全站建库（一次性全量任务） | 手动派发 | `python main.py harvest --full`，断点自动接续 |
| cleanup.yml | SuenMedia 十五日深度清洗 | 每月 1/16 日 03:00 UTC | `python cleanup.py --days N` 复检死链并精简产物 |

仓库 Secrets（Settings → Secrets and variables → Actions）：

- `TMDB_API_KEY`：**必填**，TheMovieDB 密钥（https://www.themoviedb.org/settings/api），注入给采集与刮削。
- `PUSHPLUS_TOKEN`：可选，PushPlus 微信推送（https://www.pushplus.plus/）。

### 3.3 Docker / VPS

```bash
export TMDB_API_KEY=xxx
export PUSHPLUS_TOKEN=xxx        # 可选
docker compose up -d --build
```

容器默认执行 `python main.py`（run 流水线），`product/`、`settings.json`、`config.json` 均已挂载卷保留数据。

## 4. 配置说明

### 4.1 `settings.json`（运行参数）

| 配置段 | 关键键 | 说明 |
|---|---|---|
| 顶层 | `tmdb_api_key` / `tmdb_api_base` / `tmdb_image_base` | TMDB 密钥与端点 |
| 爬取 | `crawl_hours`(24) `sites_per_run`(5) `max_pages_per_site`(20) `crawl_per_site_concurrency`(4) | 增量窗口、每轮站点数、单站页数、站内并发 |
| 刮削 | `enable_tmdb` / `enable_douban_fallback` / `bilibili_enable` / `tmdb_min_interval`(0.15) / `douban_min_interval`(0.5) / `tvdb_translation_langs` | 各源开关与间隔 |
| 预算 | `run_budget_seconds`(1800) `budget.{crawl_max,probe_max,coarse_max,scrape_max,fine_max,export_max,safety}` | 单轮时间分配（4.2 详述） |
| 素材库 | `library.{hot_window_days:7, max_attempts:3, category_weights.variety:2.0}` | 热通道窗口、重试上限、类目消费权重 |
| Tier | `scrape_tier.{hot:"AB", cold:"A", backfill_b:true}` | 热 AB / 冷 A / 存量 Tier B 回填 |
| 速率 | `rate_limits.{tmdb:4, douban:3, tvdb:8, bilibili:3, omdb:2}` | 各源软限速（req/s，生效 ×0.9） |
| 匹配 | `match.{high:70, medium:55, low:40, id_verify_enable:true, candidate_limit:5}` | 置信度阈值与 S8 开关 |
| 过滤 | `crawl_skip_categories` / `crawl_skip_title_keywords` / `crawl_skip_type_keywords` | 与 prefilter L2-L4 合并的黑名单 |

### 4.2 `config.json`（采集源）

预置 **10 大 MacCMS v10 源**（`type: maccms_v10`）：索尼 / 红牛 / 非凡 / 暴风 / 光速 / 360 / 最大化 / 天涯 / 金鹰 / 蓝光，其中 5 站配镜像 `fallbacks`，抓取失败自动轮换 + 指数退避。每轮按 `priority` 与 `sites_per_run` 轮换采集。`enabled: false` 即可禁用某站，也可追加新 MacCMS 接口。分类规则在 `CATEGORY_RULES`，四类白名单外的分类会被 L1 拦截。

### 4.3 刮削数据源（API）

元数据按优先级链自动刮削，命中即停：

```
TMDB（主源） → TheTVDB（剧集季级） → 豆瓣（正式兜底，每轮 ≤500 请求） → Bilibili（动漫，免 Key 全中文） → OMDb（评分+票数兜底）
```

| 数据源 | 覆盖 | 需要 Key | 说明 |
|---|---|---|---|
| TMDB | 电影/剧集/动漫，含分集 | 必须 | `tmdb_api_key`；Actions 需 Secrets `TMDB_API_KEY` |
| TheTVDB | 剧集季级（官方中文翻译优先） | 可选 | `tvdb_api_key`，不填自动跳过 |
| 豆瓣 | 国内影视/综节目 | 免 Key | **仅国内网络可用**（Actions 海外跑机不可用）；失败率 >30% 自动冷却 900s 半开 |
| Bilibili | 动漫（全中文） | 免 Key | wbi 签名公开接口，仅服务 `anime` 类目 |
| OMDb | IMDb 评分/票数兜底 | 可选 | `omdb_api_key`，不填自动跳过 |

TMDB 国内直连注意：`api.themoviedb.org` 存在 DNS 污染，本机运行需在 hosts 追加可用 IP（如 `18.239.199.108 api.themoviedb.org`，失效时用 `dns.alidns.com/resolve` 查新 IP）；GitHub Actions 海外跑机无需处理。

## 5. 产物目录结构（契约 v3）

```
product/
├── videos.json            # 元数据主文件（不含分集；cover 为唯一封面字段）
├── episodes.jsonl.gz      # 分集 gzip 分片，逐行 {bangou, season_number, title, episodes[]}
├── unmatched.json         # 未达入库门槛的隔离审计（含 reason）
├── m3u8/
│   └── {category}/{bangou}.m3u8   # 每作品一份播放列表
└── manifest.json          # 版本 / 条目数 / 各文件 sha256 / 生成时间

product/suenmedia.db         # SQLite(WAL)：meta_cache / title_index / domain_registry /
                           # raw_seen / retry_queue / raw_library（素材库）
product/progress.json       # 全量建库断点游标（跨派发续跑）
```

**契约硬性约束（消费端 suenplayer 强依赖）**：

- `bangou` 为唯一主键（缺失时 `md5(title+url)` 兜底）；
- **`cover` 是唯一封面字段，`poster` 不被识别**（产物禁止出现 poster）；
- `seasons[].season_number`、`episodes[].ep_number` 必须是 int；
- 多线路 → 主 `url` + `alt_urls[{source,url,label,resolution,url_type}]`，备用线路不丢。

## 6. 质量与容错机制

- **入库门槛**：命中 + 封面 + 简介 + `confidence >= 55`；不达标按原因（`all_sources_miss` / `low_confidence` / `no_cover` / `not_in_whitelist` 等）进 `unmatched.json`，下轮可重算。
- **源级熔断**：豆瓣失败率 >30%（样本 ≥5）冷却 900s 半开重试；TMDB 429 触发暂停（负缓存闸门同步进入暂停态，期间不计 miss）。
- **分类权威校正**：`taxonomy.authoritative_category()` 按采用源校正最终类目，非四类产物直接丢弃。
- **预算硬上限**：`projected_end > deadline` 即停止派发刮削，避免单轮超时。
- **产物防退变**：harvest 再入库保护已刮削富化数据；热窗口新更新才重置刮削状态（见 §2）。

## 7. 开发与测试

```
core/        基础设施：cache(SQLite WAL) / config / http / logging / ratelimit
normalize/   标题与集名归一化（同片异名合并、季集解析）
crawlers/    采集器：maccms（MacCMS 状态机）+ harvest（跨站编排）
pipeline/    prefilter(L1-L5) / probe(域名探活) / coarse_merge / scrape(刮削编排)
             fine_merge / export / budget
sources/     刮削源：scorer(置信度) / tmdb / tvdb / douban / bilibili / omdb
schema.py    v3 产物契约定义与校验
main.py      harvest / run 双子命令入口
cleanup.py   十五日深度清洗（产物级）
tests/       184 用例（含端到端冒烟：素材库→刮削→精合并→导出全链路）
```

```bash
.venv/Scripts/python.exe -m pytest tests/ -q    # Windows 虚拟环境
python -m pytest tests/ -q
```

旧版入口（`metadata_scraper.py` / `aggregator.py` / `progress_log.py` / `retry_queue.py` / 顶层 `harvest.py` / `full_crawl.py` 等）已由新分层取代，源码归档于 `docs/legacy/`。设计文档见 `docs/refactor-design.md`（v1.1）。