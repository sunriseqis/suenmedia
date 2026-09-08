# -*- coding: utf-8 -*-
"""崩溃安全测试的子进程：写入 N 条缓存后打印 READY，然后长睡等待被 kill。

用法：`python tests/crash_child.py <db_path> <n>`
父进程读到 READY 后 `proc.kill()`（Windows 上等价于 TerminateProcess，
即硬杀），随后重新打开数据库验证数据完整。
"""
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.cache import MetaCache  # noqa: E402
import core.logging as _log  # noqa: E402

_log.set_quiet(True)  # 子进程协议：stdout 只承载 READY，日志不干扰


def main() -> None:
    """写 N 条命中缓存，打印 READY，长睡等待被父进程杀掉。"""
    path = sys.argv[1]
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    # buffer_size=1：每条写入立即落盘（自动提交 + WAL），模拟真实增量写
    cache = MetaCache(path=path, buffer_size=1)
    for i in range(count):
        cache.put_hit(f"v6|movies|测试片{i}|1|2024", {"title": f"测试片{i}", "id": i},
                      provider="TMDB", confidence=90)
    cache.flush()
    print("READY", flush=True)
    time.sleep(120)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        sys.exit(2)
