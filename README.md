# MaiBot Reminder

让 MaiBot 能够根据聊天内容自行设定秒级提醒的定时器插件。

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

[reminders]
[reminders."<reminder_id>"]
stream_id = "stream_123"
trigger_time = "2026-06-22T18:00:00+08:00"
message = "提醒内容"
```

- `max_reminders_per_stream`：每个消息流最多允许同时存在的活跃提醒数量，默认为 20。
- `reminders`：当前已设置的提醒，以 `reminder_id` 为键。插件会在 WebUI 配置页面中以 JSON 编辑器的形式暴露该字段，支持查看、编辑和删除 AI 设置的提醒。

## 工具说明

插件为 LLM 暴露了三个工具：

### set_reminder

设置一个提醒。

参数：
- `stream_id`（string，必填）：目标消息流 ID。
- `trigger_time`（string，必填）：ISO-8601 格式触发时间，精确到秒，例如 `2026-06-22T18:00:00+08:00`。
- `message`（string，必填）：提醒内容，到期后会作为意图交给 AI，由 AI 决定如何回复。

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
    "message": "提醒用户该吃晚饭了"
  }
}
```

到达 18:00 时，插件会通过 `maisaka.proactive.trigger` 向当前流提交意图“提醒事件：提醒用户该吃晚饭了”，由 AI 自行决定如何回复，而不是直接控制 Bot 发送消息。

## 在插件配置页面管理提醒

插件配置模型中声明了 `reminders` 字段，因此可以在 MaiBot 的插件配置页面中：

- 查看 AI 当前设置的所有提醒。
- 手动编辑提醒的 `trigger_time` 或 `message`。
- 删除不需要的提醒。

配置保存后会自动热更新到插件，无需重启 MaiBot。
