# -*- coding: utf-8 -*-
"""pytest 公共 fixture：把项目根目录加入 sys.path，便于 `import core` / `import schema`。"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
