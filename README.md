# MaiBot Reminder

让 MaiBot 能够在指定时间主动发送提醒消息的定时器插件。

## 安装

1. 将本仓库克隆到 MaiBot 的 `plugins/` 目录下：

```bash
cd /path/to/MaiBot/plugins
git clone https://github.com/xhstoyea/maibot-reminder.git maibot-reminder
```

2. 重启 MaiBot，插件会自动加载。

## 配置

`config.toml` 中提供了以下配置项：

```toml
[plugin]
config_version = "1.0.0"
max_reminders_per_stream = 20
```

- `max_reminders_per_stream`：每个消息流最多允许同时存在的活跃提醒数量，默认为 20。

## 工具说明

插件为 LLM 暴露了三个工具：

### set_reminder

设置一个提醒。

参数：
- `stream_id`（string，必填）：目标消息流 ID。
- `trigger_time`（string，必填）：ISO-8601 格式触发时间，例如 `2026-06-22T18:00:00+08:00`。
- `message`（string，必填）：触发时发送的消息内容。

返回值：
- 成功时返回 `{ "success": true, "reminder_id": "<uuid>" }`。
- 若时间不是未来时间或已超过单流上限，则返回 `{ "success": false, "error": "..." }`。

### list_reminders

列出指定消息流的所有活跃提醒。

参数：
- `stream_id`（string，必填）：目标消息流 ID。

返回值：
- `{ "success": true, "reminders": [ { "id": "...", "trigger_time": "...", "message": "..." }, ... ] }`
- 结果按 `trigger_time` 升序排列。

### cancel_reminder

取消一个已设置的提醒。

参数：
- `reminder_id`（string，必填）：要取消的提醒 ID。

返回值：
- 成功返回 `{ "success": true }`；若 ID 不存在返回 `{ "success": false, "error": "..." }`。

## 使用示例

当 LLM 调用：

```json
{
  "tool": "set_reminder",
  "args": {
    "stream_id": "stream_123",
    "trigger_time": "2026-06-22T18:00:00+08:00",
    "message": "该吃晚饭啦！"
  }
}
```

到达 18:00 时，插件会主动通过 `ctx.send.text` 向 `stream_123` 发送消息“该吃晚饭啦！”。
