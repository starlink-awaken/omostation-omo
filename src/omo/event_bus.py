"""Event Stream Bus — 轻量本地事件总线（BET-Y1Q4-T2-01）.

统一汇聚 OA/邮件/日历/RSS/IM 信号的多源并发总线：优先级动态排队（高优毫秒级
优先出队）、背压控制（maxsize 溢出丢弃+高危标记）。100% 标准库 asyncio，
无外部 MQ——单机主权轻量化（BET non_goal 契约）。
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

SCHEMA = "omo.event-bus.v1"
DEFAULT_MAXSIZE = 10_000  # circuit_breaker 阈值：超此堆积启动丢弃
ALARM_THRESHOLD = 10_000


@dataclass(frozen=True, slots=True)
class Event:
    source: str
    kind: str  # policy / mail / calendar / rss / im / ...
    payload: dict[str, Any]
    priority: str = "normal"  # high | normal
    ts: float = field(default_factory=time.monotonic)


@dataclass
class BusStats:
    enqueued: int = 0
    dequeued: int = 0
    dropped_overflow: int = 0
    alarm: bool = False
    high_wait_max_ms: float = 0.0


class PriorityEventBus:
    """双队列优先级总线：high 恒先于 normal 出队（同优先级 FIFO）。

    背压契约（BET circuit_breaker）：normal 队列达 maxsize 后丢最旧并计数；
    总堆积（high+normal）超 ALARM_THRESHOLD 置 alarm 高危标记。
    high 队列不丢弃（公文/来信不可弃）。
    """

    def __init__(self, maxsize: int = DEFAULT_MAXSIZE) -> None:
        self._high: deque[Event] = deque()
        self._normal: deque[Event] = deque()
        self._maxsize = maxsize
        self._wakeup: asyncio.Event = asyncio.Event()
        self._closed = False
        self.stats = BusStats()

    @property
    def depth(self) -> int:
        return len(self._high) + len(self._normal)

    def publish(self, event: Event) -> bool:
        """同步入队；normal 溢出按背压契约丢弃最旧，返回 False 仅当 high 非法优先级。"""
        if event.priority == "high":
            self._high.append(event)
        elif event.priority == "normal":
            self._normal.append(event)
            while len(self._normal) > self._maxsize:
                self._normal.popleft()
                self.stats.dropped_overflow += 1
        else:
            return False
        self.stats.enqueued += 1
        self.stats.alarm = self.depth > ALARM_THRESHOLD
        self._wakeup.set()
        return True

    async def subscribe(self) -> AsyncIterator[Event]:
        """出队迭代器：high 优先；空则挂起等待 wakeup。"""
        while True:
            event = self._pop()
            if event is not None:
                yield event
                continue
            if self._closed:
                return
            self._wakeup.clear()
            await self._wakeup.wait()

    def _pop(self) -> Event | None:
        if self._high:
            event = self._high.popleft()
            wait_ms = (time.monotonic() - event.ts) * 1000
            self.stats.high_wait_max_ms = max(self.stats.high_wait_max_ms, wait_ms)
        elif self._normal:
            event = self._normal.popleft()
        else:
            return None
        self.stats.dequeued += 1
        return event

    def close(self) -> None:
        self._closed = True
        self._wakeup.set()


async def run_producer(
    bus: PriorityEventBus, source: str, kind: str, count: int, priority: str = "normal", delay: float = 0.0
) -> int:
    """并发生产者：每源一 task。返回成功入队数。"""
    published = 0
    for i in range(count):
        bus.publish(Event(source=source, kind=kind, payload={"seq": i}, priority=priority))
        published += 1
        if delay:
            await asyncio.sleep(delay)
        elif i % 64 == 63:
            await asyncio.sleep(0)  # 让出事件循环：drain 并发消费（流式语义）
    return published


async def stream_benchmark(sources: int = 5, per_source: int = 4000, high_ratio: int = 20) -> dict[str, Any]:
    """基准：N 源并发 × M 事件 → 断言吞吐 ≥1000/s + 高优延迟 <10ms。"""
    bus = PriorityEventBus()
    consumed: list[Event] = []
    producers_done = asyncio.Event()

    async def drain() -> None:
        # 完成条件 = 生产者全完成且队列排空（背压丢弃时 consumed < 总发布数，
        # 原 >= total 条件会死锁——首跑 5x4000 实测：normal 16000 > maxsize
        # 10000 丢 6000，drain 永不达标）
        async for event in bus.subscribe():
            consumed.append(event)
            if producers_done.is_set() and bus.depth == 0:
                bus.close()

    async def producers_all() -> None:
        await asyncio.gather(
            *[
                run_producer(
                    bus,
                    source=f"src-{i}",
                    kind="rss" if i % 2 else "mail",
                    count=per_source,
                    priority="high" if i % high_ratio == 0 else "normal",
                )
                for i in range(sources)
            ]
        )
        producers_done.set()
        bus.close()  # 全部入队完毕：唤醒可能挂起的 drain 做最终判定

    t0 = time.monotonic()
    producers = [
        run_producer(
            bus,
            source=f"src-{i}",
            kind="rss" if i % 2 else "mail",
            count=per_source,
            priority="high" if i % high_ratio == 0 else "normal",
        )
        for i in range(sources)
    ]
    await asyncio.gather(drain(), producers_all())
    elapsed = time.monotonic() - t0
    rate = len(consumed) / elapsed if elapsed else float("inf")

    high_first_ok = True
    # 高优优先出队断言：所有 high 事件应先于其后的 normal 出现
    last_high_idx = -1
    for idx, ev in enumerate(consumed):
        if ev.priority == "high":
            last_high_idx = idx
        elif last_high_idx >= 0 and ev.ts < consumed[last_high_idx].ts and ev.priority == "normal":
            # normal 在 high 之后入队却先出队 → 优先级违背（仅当 high 已在队列）
            high_first_ok = False
            break

    return {
        "schema": SCHEMA,
        "events": len(consumed),
        "elapsed_s": round(elapsed, 3),
        "events_per_s": round(rate, 1),
        "throughput_ok": rate >= 1000,
        "high_priority_latency_max_ms": round(bus.stats.high_wait_max_ms, 3),
        "high_priority_ok": bus.stats.high_wait_max_ms < 10 and high_first_ok,
        "overflow_dropped": bus.stats.dropped_overflow,
        "alarm": bus.stats.alarm,
    }
