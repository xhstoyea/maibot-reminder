"""MaiBot Reminder 插件。

提供三个 LLM 可调用的工具：
- set_reminder: 设置提醒
- list_reminders: 列出当前流的所有提醒
- cancel_reminder: 取消提醒

提醒到期时不会直接控制 Bot 发消息，而是通过 ``maisaka.proactive.trigger``
把意图交给 AI，由 AI 自行决定如何回复。
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from maibot_sdk import MaiBotPlugin, Tool
from maibot_sdk.config import Field, PluginConfigBase
from maibot_sdk.types import ToolParameterInfo, ToolParamType

try:
    import tomlkit
except ImportError:
    tomlkit = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class _PluginSection(PluginConfigBase):
    """插件元信息配置节。"""

    config_version: str = "1.0.0"


class ReminderItemConfig(PluginConfigBase):
    """单个提醒在配置页面中的结构。"""

    __ui_label__ = "提醒项"

    stream_id: str = Field(default="", description="目标聊天流 ID")
    trigger_time: str = Field(default="", description="触发时间（ISO-8601，带时区）")
    message: str = Field(default="", description="提醒内容，到期后作为意图交给 AI")


class ReminderConfig(PluginConfigBase):
    """提醒插件配置。"""

    plugin: _PluginSection = Field(default_factory=_PluginSection)
    max_reminders_per_stream: int = Field(default=20, ge=1, le=1000)
    reminders: dict[str, ReminderItemConfig] = Field(
        default_factory=dict,
        description="当前已设置的提醒，以 reminder_id 为键，可在插件配置页面查看、编辑和删除",
    )


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
        """加载插件：初始化存储与调度器，并从配置恢复提醒。"""
        max_per_stream = self.config.max_reminders_per_stream
        self._store = ReminderStore(max_per_stream=max_per_stream)
        self._scheduler = ReminderScheduler(self._store, self._on_reminder_trigger)
        await self._scheduler.start()
        self._load_reminders_from_config()

    async def on_unload(self) -> None:
        """卸载插件：停止调度并清空提醒。"""
        if self._scheduler is not None:
            await self._scheduler.stop()
        if self._store is not None:
            self._store.clear()
        self._store = None
        self._scheduler = None

    @property
    def _plugin_dir(self) -> Path:
        """返回插件目录路径。"""
        return Path(__file__).parent.resolve()

    @property
    def _config_path(self) -> Path:
        """返回插件配置文件路径。"""
        return self._plugin_dir / "config.toml"

    def _save_reminders_to_disk(self) -> None:
        """将当前内存中的提醒持久化到 ``config.toml``。

        仅更新 ``reminders`` 字段，保留文件中其它所有内容；
        当 ``tomlkit`` 不可用或文件不可写时静默跳过。
        """
        if tomlkit is None:
            return
        config_path = self._config_path
        try:
            if config_path.exists():
                with open(config_path, "r", encoding="utf-8") as f:
                    doc = tomlkit.load(f)
            else:
                doc = tomlkit.document()

            reminders_table = tomlkit.table()
            for reminder_id, item in self.config.reminders.items():
                table = tomlkit.table()
                table["stream_id"] = item.stream_id
                table["trigger_time"] = item.trigger_time
                table["message"] = item.message
                reminders_table.add(reminder_id, table)

            doc["reminders"] = reminders_table
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, "w", encoding="utf-8") as f:
                tomlkit.dump(doc, f)
        except Exception:
            logger.exception("持久化提醒到 config.toml 失败")

    def _load_reminders_from_config(self) -> None:
        """从当前配置中恢复提醒并加入调度器。

        会跳过已过期或格式非法的条目；加载前先清空现有调度堆。
        """
        if self._store is None or self._scheduler is None:
            return

        self._store.clear()
        self._scheduler._heap.clear()

        now = datetime.now(timezone.utc)
        for reminder_id, item in self.config.reminders.items():
            try:
                trigger_time = datetime.fromisoformat(item.trigger_time)
                if trigger_time.tzinfo is None:
                    trigger_time = trigger_time.replace(tzinfo=timezone.utc)
            except Exception:
                logger.warning("配置文件中的提醒时间格式非法，已跳过: %s", reminder_id)
                continue

            if trigger_time <= now:
                logger.info("配置文件中的提醒已过期，跳过: %s", reminder_id)
                continue

            reminder = Reminder(
                id=reminder_id,
                stream_id=item.stream_id,
                trigger_time=trigger_time,
                message=item.message,
            )
            try:
                self._store.add(reminder)
            except ValueError:
                logger.warning("从配置恢复提醒时超出流上限，跳过: %s", reminder_id)
                continue
            self._scheduler.schedule(reminder)

        self._scheduler._wake_event.set()

    async def _append_persisted_reminder(self, reminder: Reminder) -> None:
        """将新提醒追加到配置并持久化。"""
        if getattr(self, "_plugin_config_instance", None) is None:
            return
        self.config.reminders[reminder.id] = ReminderItemConfig(
            stream_id=reminder.stream_id,
            trigger_time=reminder.trigger_time.isoformat(),
            message=reminder.message,
        )
        self._save_reminders_to_disk()

    async def _remove_persisted_reminder(self, reminder_id: str) -> None:
        """从配置中移除指定提醒并持久化。"""
        if getattr(self, "_plugin_config_instance", None) is None:
            return
        if reminder_id in self.config.reminders:
            del self.config.reminders[reminder_id]
            self._save_reminders_to_disk()

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """处理配置热更新。

        当用户在插件配置页面修改提醒列表后，Runner 会重新注入配置并调用
        本方法，此时需要从最新配置重新加载提醒。
        """
        if scope == "self" and self._store is not None and self._scheduler is not None:
            self._load_reminders_from_config()

    async def _on_reminder_trigger(self, reminder: Reminder) -> None:
        """提醒到期时调用，通过 ``maisaka.proactive.trigger`` 交给 AI 决策。

        插件不直接控制 Bot 发送消息，而是将提醒意图提交给 Maisaka，
        由 AI 根据当前上下文决定如何回复。
        """
        await self.ctx.maisaka.proactive.trigger(
            stream_id=reminder.stream_id,
            intent=f"提醒事件：{reminder.message}",
            reason="定时提醒到期",
            metadata={"reminder_id": reminder.id},
        )
        await self._remove_persisted_reminder(reminder.id)

    @Tool(
        "set_reminder",
        description=(
            "为当前聊天流设置一个未来某个时间触发的秒级提醒。"
            "当用户要求你在未来某个时刻提醒他/她、或希望你在指定时间后再做某事时使用本工具。"
            "参数说明："
            "stream_id（可选，string）目标消息流 ID，例如当前聊天流的 session_id；"
            "省略时会自动使用当前对话的流 ID。"
            "trigger_time（必填，string）ISO-8601 格式的未来触发时间，必须带时区且精确到秒，"
            "例如 2026-06-22T18:00:00+08:00；不要省略 +08:00 这类时区后缀。"
            "message（必填，string）提醒内容，到达触发时间后会作为意图交给 AI，由 AI 自行决定如何回复。"
            "注意：时间必须是未来时间，且每个聊天流同时存在的提醒数有上限。"
            "调用成功会返回 reminder_id，可用于后续取消或列出提醒。"
        ),
        parameters=[
            ToolParameterInfo(
                name="stream_id",
                param_type=ToolParamType.STRING,
                description="目标消息流 ID，例如当前聊天流的 session_id；省略时自动使用当前对话流 ID",
                required=False,
            ),
            ToolParameterInfo(
                name="trigger_time",
                param_type=ToolParamType.STRING,
                description="ISO-8601 格式的未来触发时间，必须带时区，精确到秒，例如 2026-06-22T18:00:00+08:00",
                required=True,
            ),
            ToolParameterInfo(
                name="message",
                param_type=ToolParamType.STRING,
                description="提醒内容，到达触发时间后会作为意图交给 AI，由 AI 决定如何回复",
                required=True,
            ),
        ],
        visibility="visible",
    )
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
        await self._append_persisted_reminder(reminder)
        logger.info("已设置提醒: stream_id=%s reminder_id=%s", stream_id, reminder_id)
        return {
            "success": True,
            "content": f"已设置提醒（reminder_id={reminder_id}），将在 {trigger_time.isoformat()} 触发",
            "reminder_id": reminder_id,
        }

    @Tool(
        "list_reminders",
        description=(
            "列出指定消息流当前所有尚未触发的提醒，按触发时间升序排列。"
            "当用户想查看已设置的提醒、或你需要确认某个聊天流有哪些待触发提醒时使用。"
            "参数说明："
            "stream_id（可选，string）目标消息流 ID，例如当前聊天流的 session_id；"
            "省略时会自动使用当前对话的流 ID。"
            "返回值包含 reminders 数组，每个元素包含 id、trigger_time、message。"
        ),
        parameters=[
            ToolParameterInfo(
                name="stream_id",
                param_type=ToolParamType.STRING,
                description="目标消息流 ID，例如当前聊天流的 session_id；省略时自动使用当前对话流 ID",
                required=False,
            ),
        ],
        visibility="visible",
    )
    async def list_reminders(self, **kwargs: Any) -> dict[str, Any]:
        """LLM 工具：列出提醒。"""
        stream_id = str(kwargs.get("stream_id", "")).strip()
        if not stream_id:
            return {"success": False, "error": "stream_id 不能为空"}
        if self._store is None:
            return {"success": False, "error": "插件尚未完成初始化"}

        reminders = self._store.list_by_stream(stream_id)
        count = len(reminders)
        lines = [f"当前聊天流共有 {count} 个待触发提醒："]
        lines.extend(
            f"- {r.trigger_time.isoformat()}: {r.message}（id={r.id}）"
            for r in reminders
        )
        return {
            "success": True,
            "content": "\n".join(lines),
            "reminders": [
                {
                    "id": r.id,
                    "trigger_time": r.trigger_time.isoformat(),
                    "message": r.message,
                }
                for r in reminders
            ],
        }

    @Tool(
        "cancel_reminder",
        description=(
            "通过 reminder_id 取消一个已设置但尚未触发的提醒。"
            "当用户要求取消某个提醒、或你希望撤销之前设置的提醒时使用。"
            "参数说明："
            "reminder_id（必填，string）要取消的提醒 ID，即 set_reminder 返回的 reminder_id，"
            "也可通过 list_reminders 查询得到。"
            "取消成功后会同时移除持久化配置中的该提醒。"
        ),
        parameters=[
            ToolParameterInfo(
                name="reminder_id",
                param_type=ToolParamType.STRING,
                description="要取消的提醒 ID，即 set_reminder 返回的 reminder_id",
                required=True,
            ),
        ],
        visibility="visible",
    )
    async def cancel_reminder(self, **kwargs: Any) -> dict[str, Any]:
        """LLM 工具：取消提醒。"""
        reminder_id = str(kwargs.get("reminder_id", "")).strip()
        if not reminder_id:
            return {"success": False, "error": "reminder_id 不能为空"}
        if self._store is None or self._scheduler is None:
            return {"success": False, "error": "插件尚未完成初始化"}

        if self._scheduler.unschedule(reminder_id):
            await self._remove_persisted_reminder(reminder_id)
            logger.info("已取消提醒: reminder_id=%s", reminder_id)
            return {"success": True, "content": f"已取消提醒 {reminder_id}"}
        return {"success": False, "error": f"未找到提醒 {reminder_id}"}


def create_plugin() -> ReminderPlugin:
    """插件工厂函数。"""
    return ReminderPlugin()
