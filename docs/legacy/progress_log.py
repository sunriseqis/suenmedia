# -*- coding: utf-8 -*-
"""T8.3 日志进度改造: 覆盖刷新进度行 + 关键事件行协调 + 错误条目聚合

设计要点:
- 覆盖刷新用 \\r (回车不换行): 进度行刷新前后与事件行协调 —— 打印事件前先清空
  进度行, 打完再恢复, 事件行不会被进度行覆盖吃掉 (参考 cleanup.py 的 \\r 用法,
  补齐了「先刷新进度 -> 打事件行 -> 恢复进度」的顺序保证)。
- 刷新频率用时间戳控制 (默认 15s, 落在需求 10~20s 区间), 与行数无关: 爬取每页
  耗时短则自动合并, 刮削每条耗时长则按时间刷新。
- 终端非 TTY (GitHub Actions 日志 / 重定向文件) 自动降级: 不依赖 \\r 交互特性,
  每隔 interval 秒或每 every_ticks 次更新打一行普通换行日志。
- 线程安全: 所有流写入都持有模块级 _event_lock + 实例锁, 爬取多站点并发
  (ThreadPoolExecutor) 共享一块看板, 刮削并发计数在调用方持锁聚合。
- 错误条目聚合: add_error 只记账不刷屏, 随进度刷新以「失败剧集：《a》《b》」
  聚合行输出 (单行最多 20 条), 避免逐条打印淹没终端。
"""
import sys
import threading
import time
from collections import deque

# 模块级写流互斥: 保证「清行 -> 打事件 -> 恢复行」序列与并发进度刷新不交织
_EVENT_LOCK = threading.RLock()
_BOARDS = []            # 当前注册的进度行实例 (供无看板引用的模块发事件行)
_BOARDS_LOCK = threading.RLock()   # 可重入: get_crawl_board 持锁构造看板时会再进 LiveProgress 注册

DEFAULT_INTERVAL = 15.0   # 覆盖刷新间隔 (秒), 需求 10~20s 取中
DEFAULT_TICKS = 100       # 非 TTY 降级: 每 N 次更新保底打一行


def _dwidth(s: str) -> int:
    """终端显示宽度 (CJK/全角按 2 列计), 用于 \\r 覆盖时计算擦除宽度"""
    return sum(2 if ord(ch) > 0x2E7F else 1 for ch in s)


def _is_tty(stream) -> bool:
    try:
        return bool(stream.isatty())
    except Exception:
        return False


class LiveProgress:
    """单行覆盖刷新进度。

    update(text) 更新进度文案并按时间戳节流渲染 (TTY 用 \\r 覆盖, 非 TTY 定期换行);
    add_error(title) 聚合错误条目, 随下次刷新以聚合行输出;
    事件行走模块级 event(), 保证不被进度行覆盖。
    """

    def __init__(self, interval: float = DEFAULT_INTERVAL, every_ticks: int = DEFAULT_TICKS,
                 stream=None, register: bool = True):
        self._stream = stream if stream is not None else sys.stdout
        self._tty = _is_tty(self._stream)
        self._interval = max(float(interval), 0.001)
        self._every_ticks = int(every_ticks)
        self._lock = threading.RLock()
        self._text = ""           # 最近一次进度文案
        self._width = 0           # 屏上进度行显示宽度 (擦除用)
        self._active = False      # 终端上是否正显示进度行
        self._last_render = 0.0
        self._ticks = 0
        self._last_tick_render = 0
        self._batch = []                  # 距上次聚合输出新增的错误标题
        self._recent = deque(maxlen=20)   # 最近 20 条错误标题 (收尾汇总用)
        self._err_total = 0
        self._closed = False
        if register:
            with _BOARDS_LOCK:
                _BOARDS.append(self)

    # ---------- 内部: 流写路径 (调用方需持有 _EVENT_LOCK) ----------

    def _wipe_line(self):
        """清空屏上进度行 (\\r + 空格覆盖 + \\r 回行首)"""
        with self._lock:
            if self._tty and self._active:
                self._stream.write("\r" + " " * self._width + "\r")
                self._stream.flush()
                self._active = False

    def _draw_line(self, text: str = None):
        """在当前行画进度行并停在行首, 便于下次覆盖"""
        with self._lock:
            text = self._text if text is None else text
            if not self._tty or not text:
                return
            self._stream.write(text + "\r")
            self._stream.flush()
            self._width = max(_dwidth(text), 1)
            self._active = True

    def _write_plain(self, s: str):
        with self._lock:
            self._stream.write(s)
            self._stream.flush()

    def _is_live(self) -> bool:
        with self._lock:
            return self._tty and self._active and not self._closed

    # ---------- 内部: 渲染 ----------

    def _render(self):
        if self._tty:
            self._wipe_line()
            self._draw_line()
        else:
            self._write_plain(self._text + "\n")
        self._last_render = time.monotonic()
        self._last_tick_render = self._ticks

    def _emit_error_line(self):
        """输出聚合错误行 (调用方需持有 _EVENT_LOCK; 输出后进度行未恢复, 由随后的 _render 恢复)"""
        with self._lock:
            batch, self._batch = self._batch, []
        if not batch:
            return
        shown = batch[-20:]
        line = "失败剧集：" + "".join(f"《{t}》" for t in shown)
        if len(batch) > len(shown):
            line += f" (本批共 {len(batch)} 条, 仅显示最近 {len(shown)} 条)"
        if self._tty:
            self._wipe_line()
        self._write_plain(line + "\n")

    # ---------- 对外接口 ----------

    def update(self, text: str, force: bool = False):
        """更新进度文案; 按时间戳 (TTY/非TTY) 与 every_ticks (非TTY保底) 节流渲染"""
        with self._lock:
            if self._closed:
                return
            self._text = str(text)
            self._ticks += 1
            do_render = force
            if not do_render:
                do_render = (time.monotonic() - self._last_render) >= self._interval
            if not do_render and not self._tty and self._every_ticks > 0:
                do_render = (self._ticks - self._last_tick_render) >= self._every_ticks
            need_err_flush = do_render and bool(self._batch)
        if not do_render:
            return
        with _EVENT_LOCK:
            if need_err_flush:
                self._emit_error_line()
            self._render()

    def add_error(self, title):
        """记账一条失败条目 (线程安全, 不直接打印, 随进度刷新聚合输出)"""
        with self._lock:
            self._err_total += 1
            self._batch.append(str(title))
            self._recent.append(str(title))

    def event(self, msg: str):
        """关键事件行: 经模块级协调 (清进度行 -> 打事件 -> 恢复进度行)"""
        progress_event(msg)

    def reset(self):
        """清空进度行与节流状态 (新一轮阶段开始时复用)"""
        with _EVENT_LOCK:
            with self._lock:
                if self._tty and self._active:
                    self._stream.write("\r" + " " * self._width + "\r")
                    self._stream.flush()
                self._text = ""
                self._width = 0
                self._active = False
                self._last_render = 0.0
                self._ticks = 0
                self._last_tick_render = 0
                self._batch = []
                self._err_total = 0

    def close(self):
        """阶段结束: 输出残余错误聚合行, 清空进度行, 注销看板"""
        with _EVENT_LOCK:
            with self._lock:
                if self._closed:
                    return
                has_batch = bool(self._batch)
            if has_batch:
                self._emit_error_line()
            self._wipe_line()
            with self._lock:
                self._closed = True
                self._text = ""
        with _BOARDS_LOCK:
            if self in _BOARDS:
                _BOARDS.remove(self)

    @property
    def error_total(self) -> int:
        with self._lock:
            return self._err_total

    @property
    def recent_errors(self) -> list:
        with self._lock:
            return list(self._recent)


class CrawlBoard:
    """爬取阶段多站点进度看板: 并发站点各占一段, 拼成一行覆盖刷新。

    单站点时即需求格式:
      进度：XXX源 目标页数3000 总页数5000 当前页数1700 进度N%。已过滤377条
    多站点并发时以「 | 」拼接各站点段落。
    """

    def __init__(self, interval: float = DEFAULT_INTERVAL, every_ticks: int = 20, stream=None):
        self._lock = threading.Lock()
        self._sites = {}   # name -> {budget, total, page, filtered}
        self._closed = False
        self._line = LiveProgress(interval=interval, every_ticks=every_ticks, stream=stream)

    def reset(self):
        with self._lock:
            self._sites.clear()
            self._closed = False
        self._line.reset()

    def site_init(self, name: str, budget: int):
        """站点开始抓取: 登记目标页数 (本次派发的绝对页上限)"""
        with self._lock:
            self._sites[name] = {"budget": budget, "total": None, "page": 0, "filtered": 0}
        self.refresh()

    def site_update(self, name: str, page: int, total: int = None, filtered: int = None):
        with self._lock:
            s = self._sites.setdefault(name, {"budget": None, "total": None,
                                              "page": 0, "filtered": 0})
            s["page"] = page
            if total is not None:
                s["total"] = total
            if filtered is not None:
                s["filtered"] = filtered
        self.refresh()

    def site_finish(self, name: str):
        """站点结束: 从看板移除段落; 无站点时直接清行"""
        with self._lock:
            self._sites.pop(name, None)
            empty = not self._sites
        if empty:
            self._line.reset()
        else:
            self.refresh(force=True)

    def refresh(self, force: bool = False):
        with self._lock:
            parts = []
            for name, s in self._sites.items():
                budget, total, page = s.get("budget"), s.get("total"), s.get("page", 0)
                pct = min(100, page * 100 // budget) if budget else 0
                seg = f"{name} 目标页数{budget if budget else '?'}"
                if total:
                    seg += f" 总页数{total}"
                seg += f" 当前页数{page} 进度{pct}%。已过滤{s.get('filtered', 0)}条"
                parts.append(seg)
            text = "进度：" + " | ".join(parts) if parts else "进度："
        self._line.update(text, force=force)

    def event(self, msg: str):
        progress_event(msg)

    def close(self):
        with self._lock:
            self._closed = True
        self._line.close()


_default_board = None


def get_crawl_board() -> CrawlBoard:
    """模块级爬取看板 (crawl_maccms 内部使用; full_crawl 阶段开始时 reset, 结束时 close)"""
    global _default_board
    with _BOARDS_LOCK:
        if _default_board is None or _default_board._closed:
            _default_board = CrawlBoard()
        return _default_board


def progress_event(msg: str):
    """关键事件行: 协调所有活跃进度行 —— 清行 -> 打事件 -> 恢复行。

    无活跃进度行时等价于普通 print; 有多个活跃看板时去重同一流只打一次。
    供 metadata_scraper / full_crawl 等无看板引用处使用。
    """
    msg = str(msg)
    with _BOARDS_LOCK:
        boards = [b for b in _BOARDS if not getattr(b, "_closed", False)]
    with _EVENT_LOCK:
        live, seen = [], set()
        for b in boards:
            if b._is_live() and id(b._stream) not in seen:
                live.append(b)
                seen.add(id(b._stream))
        if not live:
            sys.stdout.write(msg + "\n")
            sys.stdout.flush()
            return
        for b in live:
            b._wipe_line()
        for b in live:
            b._write_plain(msg + "\n")
        for b in live:
            b._draw_line()


# 简短别名 (业务模块 import 后调用形如 progress_log.event(...))
event = progress_event
