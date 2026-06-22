"""MaiBot Reminder 插件。

提供三个 LLM 可调用的工具：
- set_reminder: 设置提醒
- list_reminders: 列出当前流的所有提醒
- cancel_reminder: 取消提醒
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from maibot_sdk import MaiBotPlugin, Tool
from maibot_sdk.config import Field, PluginConfigBase

logger = logging.getLogger(__name__)


class _PluginSection(PluginConfigBase):
    """插件元信息配置节。"""

    config_version: str = "1.0.0"


class ReminderConfig(PluginConfigBase):
    """提醒插件配置。"""

    plugin: _PluginSection = Field(default_factory=_PluginSection)
    max_reminders_per_stream: int = Field(default=20, ge=1, le=1000)


@dataclass
class Reminder:
    """单个提醒对象。"""

    stream_id: str
    trigger_time: datetime
    message: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class ReminderStore:
    """按 stream_id 索引的内存提醒存储。"""

    def __init__(self, max_per_stream: int = 20) -> None:
        self._max_per_stream = max_per_stream
        self._reminders: dict[str, dict[str, Reminder]] = {}

    def add(self, reminder: Reminder) -> str:
        """添加提醒，返回 reminder_id。"""
        stream_id = reminder.stream_id
        if self.count_by_stream(stream_id) >= self._max_per_stream:
            raise ValueError(
                f"stream {stream_id!r} 的活跃提醒数量已达上限 {self._max_per_stream}"
            )
        self._reminders.setdefault(stream_id, {})[reminder.id] = reminder
        return reminder.id

    def list_by_stream(self, stream_id: str) -> list[Reminder]:
        """返回某流的所有提醒，按触发时间排序。"""
        return sorted(
            self._reminders.get(stream_id, {}).values(),
            key=lambda r: r.trigger_time,
        )

    def get(self, reminder_id: str) -> Reminder | None:
        """按 ID 获取提醒（不区分 stream）。"""
        for stream in self._reminders.values():
            if reminder_id in stream:
                return stream[reminder_id]
        return None

    def cancel(self, reminder_id: str) -> bool:
        """取消提醒，返回是否成功。"""
        for stream_id, reminders in list(self._reminders.items()):
            if reminder_id in reminders:
                del reminders[reminder_id]
                if not reminders:
                    del self._reminders[stream_id]
                return True
        return False

    def clear(self) -> None:
        """清空所有提醒。"""
        self._reminders.clear()

    def count_by_stream(self, stream_id: str) -> int:
        """返回某流当前提醒数量。"""
        return len(self._reminders.get(stream_id, {}))


class ReminderScheduler:
    """基于 asyncio 的秒级提醒调度器。"""

    def __init__(
        self,
        store: ReminderStore,
        callback: Callable[[Reminder], Awaitable[None]],
    ) -> None:
        self._store = store
        self._callback = callback
        self._task: asyncio.Task[None] | None = None
        self._heap: list[tuple[float, str]] = []
        self._wake_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """启动调度后台任务。"""
        if self._task is not None and not self._task.done():
            return
        self._shutdown_event.clear()
        self._wake_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """停止调度任务。"""
        if self._task is None or self._task.done():
            self._task = None
            return
        self._shutdown_event.set()
        self._wake_event.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    def schedule(self, reminder: Reminder) -> None:
        """将提醒加入调度堆。"""
        ts = reminder.trigger_time.astimezone(timezone.utc).timestamp()
        heapq.heappush(self._heap, (ts, reminder.id))
        self._wake_event.set()

    def unschedule(self, reminder_id: str) -> bool:
        """从调度堆中移除提醒（通过存储层标记失效）。"""
        removed = self._store.cancel(reminder_id)
        if removed:
            self._wake_event.set()
        return removed

    async def _run(self) -> None:
        """后台调度循环，至少每秒评估一次截止时间。"""
        try:
            while not self._shutdown_event.is_set():
                now = datetime.now(timezone.utc)
                await self._process_due(now)

                timeout = self._next_sleep_seconds(now)
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(),
                        timeout=timeout,
                    )
                    self._wake_event.clear()
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    def _next_sleep_seconds(self, now: datetime) -> float:
        """计算下一次评估前需要睡眠的秒数。"""
        next_ts = self._next_due_timestamp()
        if next_ts is None:
            return 1.0
        return min(1.0, max(0.0, next_ts - now.timestamp()))

    def _next_due_timestamp(self) -> float | None:
        """返回下一个仍然有效的提醒时间戳。"""
        while self._heap:
            ts, reminder_id = self._heap[0]
            if self._store.get(reminder_id) is not None:
                return ts
            heapq.heappop(self._heap)
        return None

    async def _process_due(self, now: datetime) -> None:
        """处理所有已到期的提醒。"""
        now_ts = now.timestamp()
        while self._heap and self._heap[0][0] <= now_ts:
            ts, reminder_id = heapq.heappop(self._heap)
            reminder = self._store.get(reminder_id)
            if reminder is None:
                continue
            try:
                await self._callback(reminder)
            except Exception:
                logger.exception(
                    "提醒回调执行失败: reminder_id=%s stream_id=%s",
                    reminder_id,
                    reminder.stream_id,
                )
            finally:
                self._store.cancel(reminder_id)


class ReminderPlugin(MaiBotPlugin):
    """MaiBot 提醒插件主类。"""

    config_model = ReminderConfig

    def __init__(self) -> None:
        super().__init__()
        self._store: ReminderStore | None = None
        self._scheduler: ReminderScheduler | None = None

    async def on_load(self) -> None:
        """加载插件：初始化存储与调度器。"""
        max_per_stream = self.config.max_reminders_per_stream
        self._store = ReminderStore(max_per_stream=max_per_stream)
        self._scheduler = ReminderScheduler(self._store, self._on_reminder_trigger)
        await self._scheduler.start()

    async def on_unload(self) -> None:
        """卸载插件：停止调度并清空提醒。"""
        if self._scheduler is not None:
            await self._scheduler.stop()
        if self._store is not None:
            self._store.clear()
        self._store = None
        self._scheduler = None

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """处理配置热更新。"""
        pass

    async def _on_reminder_trigger(self, reminder: Reminder) -> None:
        """提醒到期时调用，主动向消息流发送文本。"""
        await self.ctx.send.text(reminder.message, reminder.stream_id)

    @Tool("set_reminder", description="设置一个未来某个时间触发的提醒")
    async def set_reminder(self, **kwargs: Any) -> dict[str, Any]:
        """LLM 工具：设置提醒。"""
        stream_id = str(kwargs.get("stream_id", "")).strip()
        message = str(kwargs.get("message", "")).strip()
        trigger_time_raw = kwargs.get("trigger_time", "")

        if not stream_id:
            return {"success": False, "error": "stream_id 不能为空"}
        if not message:
            return {"success": False, "error": "message 不能为空"}

        try:
            trigger_time = datetime.fromisoformat(str(trigger_time_raw))
        except Exception:
            return {"success": False, "error": "trigger_time 不是有效的 ISO-8601 时间"}

        if trigger_time.tzinfo is None:
            return {"success": False, "error": "trigger_time 必须带有时区信息"}

        if trigger_time <= datetime.now(timezone.utc):
            return {"success": False, "error": "trigger_time 必须是未来时间"}

        if self._store is None or self._scheduler is None:
            return {"success": False, "error": "插件尚未完成初始化"}

        reminder = Reminder(
            stream_id=stream_id,
            trigger_time=trigger_time,
            message=message,
        )
        try:
            reminder_id = self._store.add(reminder)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}

        self._scheduler.schedule(reminder)
        logger.info("已设置提醒: stream_id=%s reminder_id=%s", stream_id, reminder_id)
        return {"success": True, "reminder_id": reminder_id}

    @Tool("list_reminders", description="列出指定消息流的所有活跃提醒")
    async def list_reminders(self, **kwargs: Any) -> dict[str, Any]:
        """LLM 工具：列出提醒。"""
        stream_id = str(kwargs.get("stream_id", "")).strip()
        if not stream_id:
            return {"success": False, "error": "stream_id 不能为空"}
        if self._store is None:
            return {"success": False, "error": "插件尚未完成初始化"}

        reminders = self._store.list_by_stream(stream_id)
        return {
            "success": True,
            "reminders": [
                {
                    "id": r.id,
                    "trigger_time": r.trigger_time.isoformat(),
                    "message": r.message,
                }
                for r in reminders
            ],
        }

    @Tool("cancel_reminder", description="取消一个已设置的提醒")
    async def cancel_reminder(self, **kwargs: Any) -> dict[str, Any]:
        """LLM 工具：取消提醒。"""
        reminder_id = str(kwargs.get("reminder_id", "")).strip()
        if not reminder_id:
            return {"success": False, "error": "reminder_id 不能为空"}
        if self._store is None or self._scheduler is None:
            return {"success": False, "error": "插件尚未完成初始化"}

        if self._scheduler.unschedule(reminder_id):
            logger.info("已取消提醒: reminder_id=%s", reminder_id)
            return {"success": True}
        return {"success": False, "error": f"未找到提醒 {reminder_id}"}


def create_plugin() -> ReminderPlugin:
    """插件工厂函数。"""
    return ReminderPlugin()
