# -*- coding: utf-8 -*-
"""pipeline —— 管道层（设计文档 §1.1 分层中的"管道层"）。

```
入口层     main.py
管道层     pipeline/   ← 本包：prefilter → probe → coarse_merge → scrape → fine_merge → export
适配层     sources/ crawlers/
领域层     normalize/ taxonomy.py schema.py
基础设施   core/
```

职责边界：
- 本包只允许依赖 core / normalize / taxonomy / common / schema（领域层与基础设施层），
  **禁止反向依赖 sources/ / crawlers/**（适配层），保证无循环依赖。
- 每个模块面向"一条管道阶段"：输入输出均为普通 dict（RawItem / lines），
  不直接感知 HTTP 客户端类型（async/sync 由调用方注入）。

典型用法::

    from pipeline.prefilter import Prefilter
    from pipeline.probe import DomainProber

    keep, reason = Prefilter().keep(item)
    valid_lines = await DomainProber(registry).filter_lines(client, item["lines"])
"""

from __future__ import annotations

__version__ = "1.0.0"

__all__ = ["__version__"]