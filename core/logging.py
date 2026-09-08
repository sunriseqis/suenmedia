# -*- coding: utf-8 -*-
"""结构化日志与进度输出（设计文档 §11 T01，迁自 progress_log.py 的角色）。

注意：本模块名为 core.logging，包内 `import logging` 走绝对导入拿到标准库，
不会自引用（Python 3 默认绝对导入）。

提供的能力：
- `setup_logging()`：统一配置 root logger（TTY 用人类可读行，CI 可切 JSON 行）。
- `log_event()` / `event()`：结构化事件行，`kind=... k=v` 形式，便于 grep / 报表。
- `stage()`：阶段计时上下文管理器，自动打 start/end 与耗时，并汇总进 `STAGES`。
- `Progress`：单行覆盖刷新进度（TTY 用 `\r`，非 TTY 自动降级为定期换行），
  线程安全，支持错误条目聚合（迁移自 `progress_log.LiveProgress`）。

约定：事件名用小写点分（`cache.flush` / `stage.end`），字段值统一 `k=v`，
值内含空格时用引号包裹，保证可被简单解析器还原。
"""

from __future__ import annotations

import json
import logging as _stdlib_logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, TextIO

# ---------------------------------------------------------------- 常量

DEFAULT_LEVEL: str = "INFO"
DEFAULT_INTERVAL: float = 15.0     # 进度行刷新间隔（秒），需求 10~20s 取中
DEFAULT_TICKS: int = 100           # 非 TTY 降级：每 N 次更新保底打一行
MAX_ERROR_SAMPLE: int = 20         # 聚合错误行单次最多展示条数

_STDOUT_LOCK = threading.RLock()
_STD_LOGGER_NAME = "suenmedia"

#: 是否在事件行里额外输出 JSON（GA / 文件采集场景打开）
_JSON_MODE: bool = os.getenv("SUENMEDIA_LOG_JSON", "").lower() in ("1", "true", "yes")
_QUIET: bool = False


def set_quiet(quiet: bool = True) -> None:
    """全局静默：关闭 log_event 的事件行输出（子进程协议等场景用）。"""
    global _QUIET
    _QUIET = bool(quiet)


# ---------------------------------------------------------------- 工具

def _is_tty(stream: Optional[TextIO]) -> bool:
    """判断流是否可交互（TTY）。"""
    try:
        return bool(stream is not None and stream.isatty())
    except Exception:
        return False


def _dwidth(text: str) -> int:
    """终端显示宽度（CJK/全角按 2 列计），用于 `\\r` 覆盖时计算擦除宽度。"""
    return sum(2 if ord(ch) > 0x2E7F else 1 for ch in text)


def _fmt_value(value: Any) -> str:
    """把事件字段值格式化为可解析的字符串。"""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "" or any(ch in text for ch in " \t\"'\n\r"):
        return '"' + text.replace('"', '\\"') + '"'
    return text


def _now_str() -> str:
    """当前时间字符串（本地时区，秒精度）。"""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# ---------------------------------------------------------------- 初始化

def setup_logging(level: str = DEFAULT_LEVEL,
                  stream: Optional[TextIO] = None,
                  json_mode: Optional[bool] = None,
                  logger_name: str = _STD_LOGGER_NAME) -> _stdlib_logging.Logger:
    """配置并返回 suenmedia 主 logger。

    Args:
        level: 日志级别名（INFO / DEBUG / WARNING ...）。
        stream: 输出流；默认 sys.stdout（TTY 覆盖刷新需要 stdout）。
        json_mode: 是否输出 JSON 行；None 时读环境变量 SUENMEDIA_LOG_JSON。
        logger_name: logger 名称。

    Returns:
        已配置的 Logger 实例（不传播到 root，避免重复输出）。
    """
    global _JSON_MODE
    if json_mode is not None:
        _JSON_MODE = bool(json_mode)

    target = stream if stream is not None else sys.stdout
    logger = _stdlib_logging.getLogger(logger_name)
    logger.setLevel(getattr(_stdlib_logging, str(level).upper(), _stdlib_logging.INFO))
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    handler = _stdlib_logging.StreamHandler(target)
    handler.setFormatter(_stdlib_logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                                   datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    return logger


def get_logger(name: Optional[str] = None) -> _stdlib_logging.Logger:
    """取 logger；未指定名字时返回 suenmedia 主 logger（保证已配置）。"""
    if not name:
        logger = _stdlib_logging.getLogger(_STD_LOGGER_NAME)
        if not logger.handlers:
            setup_logging()
        return logger
    return _stdlib_logging.getLogger(f"{_STD_LOGGER_NAME}.{name}")


def _emit(stream: Optional[TextIO], text: str) -> None:
    """线程安全地写一行到流（进度行与事件行共用同一把锁，避免交织）。"""
    target = stream if stream is not None else sys.stdout
    with _STDOUT_LOCK:
        try:
            target.write(text + "\n")
            target.flush()
        except (ValueError, OSError):
            pass  # 流已关闭（如进程退出阶段），静默忽略


# ---------------------------------------------------------------- 结构化事件

def log_event(kind: str, level: str = "INFO", stream: Optional[TextIO] = None,
              **fields: Any) -> None:
    """输出一条结构化事件行。

    人类模式： `2026-09-08 15:04:05 [INFO] cache.flush rows=200 pending=0`
    JSON 模式： `{"ts":"...","level":"INFO","event":"cache.flush","rows":200,...}`

    Args:
        kind: 事件名（小写点分）。
        level: 级别名。
        stream: 输出流；默认 sys.stdout。
        **fields: 事件字段。
    """
    if _QUIET:
        return
    message = fields.pop("message", None)
    if _JSON_MODE:
        payload: Dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "level": str(level).upper(),
            "event": kind,
        }
        payload.update(fields)
        if message is not None:
            payload["message"] = message
        _emit(stream, json.dumps(payload, ensure_ascii=False, default=str))
        return
    parts = " ".join(f"{k}={_fmt_value(v)}" for k, v in fields.items())
    line = f"{_now_str()} [{str(level).upper()}] {kind}"
    if message is not None:
        line += " " + str(message)
    elif parts:
        line += " " + parts
    _emit(stream, line)


def event(kind: str, **fields: Any) -> None:
    """`log_event` 的简写（INFO 级别），对齐旧 progress_log.event() 调用习惯。"""
    log_event(kind, "INFO", None, **fields)


# ---------------------------------------------------------------- 阶段计时

class StageTracker:
    """阶段耗时汇总器：记录 name → [duration...]，可出报表。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: Dict[str, List[float]] = {}

    def record(self, name: str, seconds: float) -> None:
        """记录一次阶段耗时。"""
        with self._lock:
            self._records.setdefault(name, []).append(float(seconds))

    def total(self, name: str) -> float:
        """某阶段累计耗时（秒）。"""
        with self._lock:
            return float(sum(self._records.get(name, [])))

    def count(self, name: str) -> int:
        """某阶段执行次数。"""
        with self._lock:
            return len(self._records.get(name, []))

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        """快照：{stage: {"count": n, "total": sec, "avg": sec}}。"""
        with self._lock:
            out: Dict[str, Dict[str, float]] = {}
            for name, values in self._records.items():
                total = float(sum(values))
                out[name] = {
                    "count": float(len(values)),
                    "total": round(total, 3),
                    "avg": round(total / len(values), 3) if values else 0.0,
                }
            return out

    def reset(self) -> None:
        """清空记录。"""
        with self._lock:
            self._records.clear()


#: 全局阶段计时器（供 report.py 汇总）
STAGES = StageTracker()


class StageHandle:
    """阶段上下文句柄：`elapsed` 实时耗时，可在阶段内打点。"""

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.started_at: float = time.monotonic()
        self._finished: bool = False

    def elapsed(self) -> float:
        """已耗时（秒）。"""
        return time.monotonic() - self.started_at

    def mark(self, note: str, **fields: Any) -> None:
        """阶段内打点事件。"""
        log_event("stage.mark", "INFO", None, stage=self.name, note=note,
                  elapsed=round(self.elapsed(), 3), **fields)


@contextmanager
def stage(name: str, level: str = "INFO", **fields: Any) -> Iterator[StageHandle]:
    """阶段计时上下文管理器。

    用法：
        with stage("scrape", quota=1200) as st:
            ...
            st.mark("halfway")
    """
    handle = StageHandle(name)
    log_event("stage.start", level, None, stage=name, **fields)
    try:
        yield handle
    except Exception as exc:  # 阶段异常也要记录耗时，便于定位超时点
        log_event("stage.error", "ERROR", None, stage=name,
                  error=f"{type(exc).__name__}: {exc}",
                  elapsed=round(handle.elapsed(), 3))
        raise
    finally:
        seconds = handle.elapsed()
        STAGES.record(name, seconds)
        log_event("stage.end", level, None, stage=name, seconds=round(seconds, 3))


# ---------------------------------------------------------------- 进度输出

class Progress:
    """单行覆盖刷新进度（线程安全，TTY 自适应）。

    迁移自 `progress_log.LiveProgress`，保留三个关键行为：
    1. 按时间戳节流刷新（默认 15s），与行数无关；
    2. 非 TTTY（GA / 重定向）自动降级为定期换行，不依赖 `\\r`；
    3. 错误条目只记账不刷屏，随刷新以聚合行输出（单行最多 20 条）。
    """

    def __init__(self, name: str = "progress", total: int = 0,
                 interval: float = DEFAULT_INTERVAL,
                 every_ticks: int = DEFAULT_TICKS,
                 stream: Optional[TextIO] = None) -> None:
        self.name: str = name
        self.total: int = int(total or 0)
        self._interval: float = max(float(interval), 0.001)
        self._every_ticks: int = max(int(every_ticks), 1)
        self._stream: TextIO = stream if stream is not None else sys.stdout
        self._tty: bool = _is_tty(self._stream)
        self._lock = threading.RLock()
        self._done: int = 0
        self._ok: int = 0
        self._failed: int = 0
        self._skipped: int = 0
        self._started_at: float = time.monotonic()
        self._last_render: float = 0.0
        self._ticks: int = 0
        self._text: str = ""
        self._width: int = 0
        self._active: bool = False
        self._errors: List[str] = []
        self._closed: bool = False

    # ---------------------------------------------------------- 计数

    def update(self, n: int = 1, ok: bool = True, text: Optional[str] = None) -> None:
        """推进进度 n 条。

        Args:
            n: 完成条数。
            ok: True 计成功，False 计失败。
            text: 附加文案（放在进度行尾部）。
        """
        with self._lock:
            self._done += int(n)
            if ok:
                self._ok += int(n)
            else:
                self._failed += int(n)
            self._ticks += 1
            if text:
                self._text = text
        self._maybe_render()

    def add_error(self, title: str) -> None:
        """登记一条错误条目（聚合展示，不立即打印）。"""
        with self._lock:
            self._errors.append(str(title))
        self._maybe_render()

    def skip(self, n: int = 1) -> None:
        """登记跳过条数。"""
        with self._lock:
            self._skipped += int(n)
            self._done += int(n)
            self._ticks += 1
        self._maybe_render()

    @property
    def done(self) -> int:
        """已完成条数（含跳过）。"""
        with self._lock:
            return self._done

    @property
    def ok(self) -> int:
        """成功条数。"""
        with self._lock:
            return self._ok

    @property
    def failed(self) -> int:
        """失败条数。"""
        with self._lock:
            return self._failed

    def stats(self) -> Dict[str, Any]:
        """进度统计快照。"""
        with self._lock:
            return {
                "name": self.name,
                "done": self._done,
                "ok": self._ok,
                "failed": self._failed,
                "skipped": self._skipped,
                "total": self.total,
                "errors": list(self._errors),
                "elapsed": round(time.monotonic() - self._started_at, 3),
            }

    # ---------------------------------------------------------- 渲染

    def _maybe_render(self, force: bool = False) -> None:
        """按节流策略渲染（TTY 覆盖 / 非 TTY 定期换行）。"""
        with self._lock:
            if self._closed:
                return
            now = time.monotonic()
            due_time = (now - self._last_render) >= self._interval
            due_ticks = self._ticks >= self._every_ticks
            if not (force or due_time or (not self._tty and due_ticks)):
                return
            self._last_render = now
            self._ticks = 0
            line = self._line_locked()
            errors = list(self._errors)
            self._errors = []
        if not self._tty:
            self._emit_line(line)
            if errors:
                self._emit_line(self._error_line(errors))
            return
        self._render_tty(line, errors)

    def _line_locked(self) -> str:
        """构造进度行文案（调用方持锁）。"""
        elapsed = time.monotonic() - self._started_at
        rate = (self._done / elapsed) if elapsed > 0 else 0.0
        pct = (self._done * 100.0 / self.total) if self.total > 0 else 0.0
        base = (f"{self.name}: {self._done}/{self.total}" if self.total > 0
                else f"{self.name}: {self._done}")
        line = (f"{base} ({pct:.1f}%) ok={self._ok} fail={self._failed} "
                f"skip={self._skipped} {rate:.1f}/s {elapsed:.0f}s")
        if self._text:
            line += f" | {self._text}"
        if self.total > 0 and rate > 0:
            eta = (self.total - self._done) / rate
            line += f" eta={eta:.0f}s"
        return line

    @staticmethod
    def _error_line(errors: List[str]) -> str:
        """构造聚合错误行（最多 MAX_ERROR_SAMPLE 条）。"""
        shown = errors[:MAX_ERROR_SAMPLE]
        more = len(errors) - len(shown)
        suffix = f" ……等 {len(errors)} 条" if more > 0 else ""
        return "失败条目：《" + "》《".join(shown) + "》" + suffix

    def _render_tty(self, line: str, errors: List[str]) -> None:
        """TTY：先擦除旧进度行，必要时打事件行，再重画进度行。"""
        with _STDOUT_LOCK:
            try:
                if self._active and self._width:
                    self._stream.write("\r" + " " * self._width + "\r")
                if errors:
                    self._stream.write(self._error_line(errors) + "\n")
                self._stream.write(line)
                self._stream.flush()
                self._width = _dwidth(line)
                self._active = True
            except (ValueError, OSError):
                self._active = False

    def _emit_line(self, line: str) -> None:
        """非 TTY：直接换行输出。"""
        with _STDOUT_LOCK:
            try:
                self._stream.write(line + "\n")
                self._stream.flush()
            except (ValueError, OSError):
                pass

    def clear(self) -> None:
        """擦除屏幕上的进度行（打印事件行前调用，避免被覆盖吃掉）。"""
        with _STDOUT_LOCK:
            if not (self._tty and self._active and self._width):
                return
            try:
                self._stream.write("\r" + " " * self._width + "\r")
                self._stream.flush()
            except (ValueError, OSError):
                pass
            self._active = False
            self._width = 0

    def finish(self, text: Optional[str] = None) -> Dict[str, Any]:
        """结束进度：强制渲染末行 + 换行收尾，返回统计快照。"""
        with self._lock:
            if text:
                self._text = text
            self._closed = True
        self._maybe_render(force=True)
        if self._tty:
            with _STDOUT_LOCK:
                try:
                    self._stream.write("\n")
                    self._stream.flush()
                except (ValueError, OSError):
                    pass
                self._active = False
                self._width = 0
        return self.stats()

    def close(self) -> None:
        """等价于 finish()。"""
        self.finish()

    def __enter__(self) -> "Progress":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.finish()
        return False
