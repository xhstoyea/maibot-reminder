"""MaiBot Reminder 插件测试。"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from maibot_sdk.context import PluginContext
from plugin import Reminder, ReminderPlugin, ReminderScheduler, ReminderStore


class TestReminderStore:
    def test_add_reminder_returns_uuid_and_stores(self) -> None:
        store = ReminderStore()
        reminder = Reminder(
            stream_id="stream_1",
            trigger_time=datetime.now(timezone.utc) + timedelta(minutes=5),
            message="hello",
        )
        rid = store.add(reminder)

        assert isinstance(uuid.UUID(rid), uuid.UUID)
        assert store.count_by_stream("stream_1") == 1
        assert store.get(rid) is reminder

    def test_list_reminders_sorted_by_trigger_time(self) -> None:
        store = ReminderStore()
        now = datetime.now(timezone.utc)
        r1 = Reminder(stream_id="s", trigger_time=now + timedelta(hours=2), message="later")
        r2 = Reminder(stream_id="s", trigger_time=now + timedelta(hours=1), message="sooner")
        store.add(r1)
        store.add(r2)

        reminders = store.list_by_stream("s")
        assert [r.message for r in reminders] == ["sooner", "later"]

    def test_cancel_reminder_removes_it(self) -> None:
        store = ReminderStore()
        r = Reminder(stream_id="s", trigger_time=datetime.now(timezone.utc) + timedelta(hours=1), message="m")
        rid = store.add(r)

        assert store.cancel(rid) is True
        assert store.get(rid) is None
        assert store.cancel(rid) is False

    def test_add_exceeds_cap_raises(self) -> None:
        store = ReminderStore(max_per_stream=2)
        now = datetime.now(timezone.utc)
        store.add(Reminder(stream_id="s", trigger_time=now + timedelta(minutes=1), message="1"))
        store.add(Reminder(stream_id="s", trigger_time=now + timedelta(minutes=2), message="2"))

        with pytest.raises(ValueError):
            store.add(Reminder(stream_id="s", trigger_time=now + timedelta(minutes=3), message="3"))


class TestReminderScheduler:
    @pytest.mark.asyncio
    async def test_scheduler_triggers_reminder(self) -> None:
        store = ReminderStore()
        triggered: list[Reminder] = []

        async def callback(reminder: Reminder) -> None:
            triggered.append(reminder)

        scheduler = ReminderScheduler(store, callback)
        await scheduler.start()
        try:
            reminder = Reminder(
                stream_id="s",
                trigger_time=datetime.now(timezone.utc) + timedelta(seconds=0.1),
                message="ping",
            )
            store.add(reminder)
            scheduler.schedule(reminder)

            async with asyncio.timeout(2.0):
                while len(triggered) < 1:
                    await asyncio.sleep(0.01)
        finally:
            await scheduler.stop()

        assert len(triggered) == 1
        assert triggered[0].message == "ping"
        assert triggered[0].stream_id == "s"
        assert store.get(reminder.id) is None

    @pytest.mark.asyncio
    async def test_scheduler_stop_cancels_task(self) -> None:
        store = ReminderStore()
        scheduler = ReminderScheduler(store, lambda r: None)
        await scheduler.start()
        assert scheduler._task is not None
        await scheduler.stop()
        assert scheduler._task is None or scheduler._task.done()


class TestSetReminderTool:
    @pytest.fixture
    def plugin(self) -> ReminderPlugin:
        p = ReminderPlugin()
        p.set_plugin_config({"plugin": {"config_version": "1.0.0"}})
        p._store = ReminderStore()
        p._scheduler = ReminderScheduler(p._store, lambda r: None)
        return p

    @pytest.mark.asyncio
    async def test_set_reminder_returns_valid_id(self, plugin: ReminderPlugin) -> None:
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        result = await plugin.set_reminder(
            stream_id="stream_1",
            trigger_time=future,
            message="记得喝水",
        )

        assert result["success"] is True
        assert isinstance(uuid.UUID(result["reminder_id"]), uuid.UUID)
        assert plugin._store.count_by_stream("stream_1") == 1

    @pytest.mark.asyncio
    async def test_set_reminder_rejects_past_time(self, plugin: ReminderPlugin) -> None:
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        result = await plugin.set_reminder(
            stream_id="stream_1",
            trigger_time=past,
            message="too late",
        )

        assert result["success"] is False
        assert "future" in result["error"].lower() or "未来" in result["error"]

    @pytest.mark.asyncio
    async def test_set_reminder_rejects_invalid_iso(self, plugin: ReminderPlugin) -> None:
        result = await plugin.set_reminder(
            stream_id="stream_1",
            trigger_time="not-a-time",
            message="bad time",
        )

        assert result["success"] is False
        assert "iso" in result["error"].lower() or "时间" in result["error"]

    @pytest.mark.asyncio
    async def test_set_reminder_enforces_cap(self, plugin: ReminderPlugin) -> None:
        plugin._store = ReminderStore(max_per_stream=2)
        plugin._scheduler = ReminderScheduler(plugin._store, lambda r: None)
        base = datetime.now(timezone.utc) + timedelta(minutes=1)
        for i in range(2):
            await plugin.set_reminder(
                stream_id="stream_cap",
                trigger_time=(base + timedelta(minutes=i)).isoformat(),
                message=f"r{i}",
            )

        result = await plugin.set_reminder(
            stream_id="stream_cap",
            trigger_time=(base + timedelta(minutes=5)).isoformat(),
            message="over",
        )
        assert result["success"] is False
        assert "上限" in result["error"] or "cap" in result["error"].lower()


class TestListRemindersTool:
    @pytest.fixture
    def plugin(self) -> ReminderPlugin:
        p = ReminderPlugin()
        p.set_plugin_config({"plugin": {"config_version": "1.0.0"}})
        p._store = ReminderStore()
        p._scheduler = ReminderScheduler(p._store, lambda r: None)
        return p

    @pytest.mark.asyncio
    async def test_list_reminders_returns_sorted_active_reminders(self, plugin: ReminderPlugin) -> None:
        base = datetime.now(timezone.utc) + timedelta(minutes=1)
        await plugin.set_reminder(
            stream_id="stream_list",
            trigger_time=(base + timedelta(minutes=2)).isoformat(),
            message="later",
        )
        await plugin.set_reminder(
            stream_id="stream_list",
            trigger_time=(base + timedelta(minutes=1)).isoformat(),
            message="sooner",
        )

        result = await plugin.list_reminders(stream_id="stream_list")

        assert result["success"] is True
        assert [r["message"] for r in result["reminders"]] == ["sooner", "later"]
        assert all("id" in r and "trigger_time" in r for r in result["reminders"])

    @pytest.mark.asyncio
    async def test_list_reminders_empty_stream(self, plugin: ReminderPlugin) -> None:
        result = await plugin.list_reminders(stream_id="empty_stream")
        assert result["success"] is True
        assert result["reminders"] == []


class TestCancelReminderTool:
    @pytest.fixture
    def plugin(self) -> ReminderPlugin:
        p = ReminderPlugin()
        p.set_plugin_config({"plugin": {"config_version": "1.0.0"}})
        p._store = ReminderStore()
        p._scheduler = ReminderScheduler(p._store, lambda r: None)
        return p

    @pytest.mark.asyncio
    async def test_cancel_existing_reminder(self, plugin: ReminderPlugin) -> None:
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        set_result = await plugin.set_reminder(
            stream_id="s",
            trigger_time=future,
            message="to be cancelled",
        )
        rid = set_result["reminder_id"]

        result = await plugin.cancel_reminder(reminder_id=rid)
        assert result["success"] is True
        assert plugin._store.get(rid) is None

    @pytest.mark.asyncio
    async def test_cancel_unknown_reminder(self, plugin: ReminderPlugin) -> None:
        result = await plugin.cancel_reminder(reminder_id="not-real")
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_cancel_prevents_trigger(self, plugin: ReminderPlugin) -> None:
        store = ReminderStore()
        triggered: list[Reminder] = []

        async def callback(reminder: Reminder) -> None:
            triggered.append(reminder)

        plugin = ReminderPlugin()
        plugin.set_plugin_config({"plugin": {"config_version": "1.0.0"}})
        plugin._store = store
        plugin._scheduler = ReminderScheduler(store, callback)
        await plugin._scheduler.start()
        try:
            future = (datetime.now(timezone.utc) + timedelta(seconds=0.2)).isoformat()
            set_result = await plugin.set_reminder(
                stream_id="s",
                trigger_time=future,
                message="should not trigger",
            )
            rid = set_result["reminder_id"]
            cancel_result = await plugin.cancel_reminder(reminder_id=rid)
            assert cancel_result["success"] is True

            await asyncio.sleep(0.3)
        finally:
            await plugin._scheduler.stop()

        assert triggered == []


class MockMaisakaProactive:
    def __init__(self) -> None:
        self.trigger_calls: list[dict[str, object]] = []

    async def trigger(self, *, stream_id: str, intent: str, reason: str = "", priority: str = "", metadata: dict[str, object] | None = None, **kwargs: object) -> dict[str, object]:
        self.trigger_calls.append({
            "stream_id": stream_id,
            "intent": intent,
            "reason": reason,
            "priority": priority,
            "metadata": metadata or {},
            **kwargs,
        })
        return {"success": True}


class MockMaisaka:
    def __init__(self) -> None:
        self.proactive = MockMaisakaProactive()


class TestLifecycle:
    @pytest.fixture
    def plugin(self) -> ReminderPlugin:
        p = ReminderPlugin()
        p.set_plugin_config({"plugin": {"config_version": "1.0.0"}})
        ctx = PluginContext("com.example.maibot-reminder")
        ctx.maisaka = MockMaisaka()
        p._set_context(ctx)
        return p

    @pytest.mark.asyncio
    async def test_on_load_starts_scheduler(self, plugin: ReminderPlugin) -> None:
        await plugin.on_load()
        assert plugin._store is not None
        assert plugin._scheduler is not None
        assert plugin._scheduler._task is not None
        await plugin.on_unload()

    @pytest.mark.asyncio
    async def test_on_unload_clears_reminders(self, plugin: ReminderPlugin) -> None:
        await plugin.on_load()
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        await plugin.set_reminder(stream_id="s", trigger_time=future, message="m")
        assert plugin._store.count_by_stream("s") == 1

        await plugin.on_unload()
        assert plugin._store is None or plugin._store.count_by_stream("s") == 0
        assert plugin._scheduler is None or plugin._scheduler._task is None

    @pytest.mark.asyncio
    async def test_reminder_triggers_proactive_task(self, plugin: ReminderPlugin) -> None:
        await plugin.on_load()
        try:
            future = (datetime.now(timezone.utc) + timedelta(seconds=0.1)).isoformat()
            await plugin.set_reminder(
                stream_id="stream_int",
                trigger_time=future,
                message="time is up",
            )

            async with asyncio.timeout(2.0):
                while len(plugin.ctx.maisaka.proactive.trigger_calls) < 1:
                    await asyncio.sleep(0.01)
        finally:
            await plugin.on_unload()

        assert len(plugin.ctx.maisaka.proactive.trigger_calls) == 1
        call = plugin.ctx.maisaka.proactive.trigger_calls[0]
        assert call["stream_id"] == "stream_int"
        assert "time is up" in str(call["intent"])
        assert call["reason"] == "定时提醒到期"
        assert "reminder_id" in call.get("metadata", {})
