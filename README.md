# QQ 连接（文字、引用、图片、表情与语音）

### 插件版本 0.9.2

在支持工具集的 Core 中，进入「QQ 连接 → 工具集」可查询 QQ 自带工具的用途和运行注册状态。本版随插件提供 15 项工具的分组和名称，主动分享的两项按配置注册。生图工具由生图插件展示。此版本沿用随 Core 分发的 QQ 接入代码，更新内容集中在工具说明和独立更新源。


此包通过 OneBot v11 HTTP 动作发送文字、表情、图片与语音，并通过正向 WebSocket 接收私聊、群聊事件。收到的消息按 QQ 消息 ID 去重，写入 TelyinCore 事件库并唤醒主循环。原生 `face` 表情会保留编号；普通图片与 GIF/动画表情会分开标记，并缓存可再次发送的原始文件。语音消息先尝试本地 SenseVoice 识别（配置模型目录时），失败后请求 SnowLuma 转写，完成后才进入事件流；两者均失败时保留消息并标明转写不可用。

1. 在扩展页从内置源安装 `core-infinity-qq`。
2. 在安装目录 `extensions/core-infinity-qq/` 中，将 `config.example.json` 复制为 `config.json`。填写 SnowLuma 的 HTTP、正向 WebSocket 地址与访问令牌，以及允许接收和发送的 QQ 号、群号。`privates` 与 `groups` 至少填一个；名单外会话不会入站，也无法发送。令牌只留在安装副本中，不写入内置源码。
3. 在扩展页启用插件。文字与普通模型回复可发回原 QQ 会话。它提供 `qq_send_text`、`qq_send_reply`、`qq_send_face`、`qq_list_custom_faces`、`qq_send_custom_face`、`qq_send_saved_image` 与 `qq_send_voice` 工具。收藏表情必须已存在于当前 QQ 账号；先列出其 `emoji_id`，再选择发送。要缓存收到的图片和 GIF，先安装 `pip install -e '.[qq-media]'`。语音模型与协议端进程在 QQ 插件页管理；TTS 和 ASR 首次部署默认关闭。启用 QQ 插件后，保存的 TTS 开关会在 Bot 唤醒时启动服务。发送成功以 OneBot 回包的 `message_id` 为准，超时或失败会成为工具错误。入站连接失败会后台重连；不填写 `wsUrl` 时只启用发送。

戳一戳沿用私聊和群聊名单。通知里分别标明发起人和被戳者的 QQ 身份。有人戳 Bot 时，她可以在原会话回文字或收藏表情，也可以保持安静。仅有群友互戳或目标不明时只观察。戳一戳没有可引用的消息编号。

入站消息正文的前缀包含 `消息#<编号>`。`qq_send_reply` 只允许引用本次插件运行中从同一 QQ 会话接收、且编号仍在最近 512 条记录内的消息；它发送 `reply` 段加文字段。普通模型回复仍走原有文字输出，不自动变成引用。2026-10-14 已用真实私聊入站消息调用此工具，发送回执成功；随后用 SnowLuma `get_msg` 回读，发出消息含 `reply` 段，引用编号对应原消息的 `message_seq`。用户已在 QQ 测试私聊客户端确认该消息显示为引用回复；协议回读与客户端展示均通过。

示例配置：

```json
{
  "httpUrl": "http://127.0.0.1:3000",
  "wsUrl": "ws://127.0.0.1:3001",
  "token": "你的访问令牌",
  "privates": ["123456789"],
  "groups": ["987654321"],
  "proactiveTarget": "",
  "transcribeVoice": false,
  "asrModelDir": "",
  "asrLanguage": "auto",
  "asrUseItn": true,
  "asrThreads": 3,
  "historyBackfillMinutes": 10,
  "ttsUrl": "",
  "ttsEnabled": false,
  "ttsModel": "",
  "ttsVoice": "default",
  "ttsSendMode": "on_demand"
}
```

`proactiveTarget` 可选。留空时 `send_private` 只写入本地出站匣；填入 `privates` 名单中的一个 QQ 号后，Agent 主动发起的消息会投递到该私聊，工具回执区分协议端已接受与投递未确认。它不影响收到消息后的普通回复路由。

图片与 GIF 在后台从 QQ 媒体域名下载；私聊中作为文件发送的 PNG、JPEG、GIF、WebP 则通过 SnowLuma 的 `get_private_file_url` 获取下载链接。每条消息最多处理前四张；仅接受不超过 8 MB、尺寸不超过 4096×4096 且像素数不超过 1600 万的图片。文件下载链接仅接受公开 IP 的既有 QQ 文件下载路径，或 HTTPS 的 `njc-download.ftn.qq.com/ftn_handler/` 十六进制文件路径；不访问内网地址或跟随重定向。缓存放在核心根目录的 `data/media/qq/`，事件正文显示 24 位媒体编号。缓存图片同时以持久媒体引用写入事件；当前主模型档案启用视觉时，请求才读取缓存并把图片送给该主模型（动画格式会转换为静态图片）。主模型不支持视觉或关闭图片输入时，只保留正文中的图片类别与编号，不自动调用其他视觉模型。下载失败时消息照常入站，但仅显示图片占位。`qq_send_saved_image` 只可将缓存图片发回原会话，并检查 OneBot 消息 ID。QQ 标为“动画表情”的消息有时只提供 JPEG 图像，缓存会保留实际收到的格式，不能保证动画。其他文件仅标记为 `[文件]`；群聊中作为文件发送的图片已接入 SnowLuma 的 `get_group_file_url`，默认 busid 为 102；目前仅通过模拟测试，真实群文件仍待验收。可选的 `core-infinity-qq-vision-overlay` 已提供独立图片理解工具，默认停用且不配置模型；只有在插件页明确选择视觉档案并启用插件后，Agent 才可调用它，主路径不会自动回退到第二模型。

`transcribeVoice` 默认关闭，启用后才转写收到的语音。当前默认选用项目内 `models/voice/asr/SenseVoiceSmall-onnx`；要用本地识别，核心环境需安装 `pip install -e '.[qq-voice]'`。插件通过 `get_record` 读取 SnowLuma 的 WAV；只接受不超过 30 秒、10 MB 的音频，不保存音频文件。本地识别失败仍会尝试 SnowLuma 的 `fetch_ptt_text`。TTS 默认关闭且无选定模型；Infinity 页选择并启用模型后才写入本机 `ttsUrl`，已启用的 QQ 插件会随保存的 TTS 开关启动服务。停用插件会停止本 Bot 托管的 TTS，并保留两项语音设置。`qq_send_voice` 将完整文字合成为 WAV，经 OneBot `record` 段发送。2026-10-14 已实测私聊入站语音、本地转写、Agent 处理及文字回复经 SnowLuma 发出；语音发送也已由协议端回包与 `record` 消息核对。语音处理队列最多容纳 16 条等待任务，超出时保留“转写暂不可用”的事件。

`ttsVoice` 默认 `default`，使用 CosyVoice 官方运行目录中的通用示例参考音频；只有本机 `references/Telyin.wav` 存在时，配置页才显示私有 `telyin` 预设。私有参考音频不随此插件包、模型权重或示例配置发布。TTS 服务必须在生成结果中回报实际音色 ID；旧服务没有回报时，插件拒绝发送，避免把私有音色误当通用默认音色。当前语速固定 1.0，卡片不提供无效的调速操作。试听文字可在页面临时修改（1—200 字），不写入私有配置。

`ttsSendMode=on_demand` 为默认：普通 QQ 回复仍只发文字，只有 Agent 显式调用 `qq_send_voice` 才发语音。选 `reply_with_voice` 且启用 TTS 时，来自单一允许 QQ 会话的普通回复先发文字，再附发同文语音；超过 1000 字或语音合成失败时仅保留已送达的文字。它不改变网页回复、混合会话或主动私聊的行为。

ASR 使用本地 ONNX int8。`asrLanguage` 支持 `auto`、`zh`、`en`，默认自动；`asrUseItn` 控制数字与标点规范化，默认开启；`asrThreads` 仅允许 CPU 2 或 3 线程，新配置默认 3，已有的显式 2 线程设置继续生效。保存后重载 QQ 插件，下一次创建识别器时这些参数传给 sherpa-onnx。3 线程档来自离线基准，尚未在真实直播负载下验收；VAD 使用独立的单线程检测。

插件配置页现在可编辑允许的私聊 QQ 号、群号、主动私聊目标和断线补读时长；保存后会重载已启用的插件。目标名单至少保留一项，主动私聊目标必须在私聊名单内，补读时长限 0—30 分钟。页面不会显示令牌，保存时保留令牌和语音等其他字段；若重载失败则恢复原配置。直接修改 `config.json` 后仍需重新启用插件。`wsToken` 可单独设置 WebSocket 令牌；省略时使用 `token`。SnowLuma 的 HTTP、WebSocket 端口和令牌应以本机实际设置为准。

`historyBackfillMinutes` 可设为 0 到 30，默认 0；示例启用 10 分钟。每次 WebSocket 连接建立后，插件只从允许的私聊和群聊各读取最近最多 30 条记录，补回重启或短暂断线期间漏掉的消息；事件库按原 QQ 消息 ID 去重。设为 0 则关闭补读。

## 经历页上的前缀（`telyincore.channels`）

2026-10-17 起，经历页上"哪一路来的"那一小截前缀**由插件自己声明**，核心只按它渲染：

```json
"telyincore": {
  "channels": [
    {"label": "QQ",
     "markers": [
       {"scope": "群", "filter": "answered",
        "pattern": "\\[QQ群(?<room>-?\\d+)·消息#(?<id>-?\\d+)\\]\\s*(?<who>[^：]+)："},
       {"scope": "私",
        "pattern": "\\[QQ私聊(?::(?<room>\\d+))?·消息#(?<id>-?\\d+)\\]\\s*(?<who>[^：]+)："}
     ],
     "speech": {"tools": ["qq_send_text", "qq_send_reply", "qq_send_voice"], "text": "text"}}
  ]
}
```

- `label` 是平台名（`QQ`）；`scope` 是收起来时显示的那一个字（`群` ／ `私`）；
  `pattern` 必须**从正文开头**匹配，命名组 `who` 必填，`room` 选了就给。
- 页上画成 `[QQ·群]某某说：`；点「群」就地展开成 `[QQ群·123456789]某某说：`。
  `room` 空（老记录没带账号）⇒ 不做链接。
- **`filter: answered`**：群这一类**只在她参与对话时进经历** —— 她发言工具调用里的
  `trigger_message_id` 指哪一条就是接住了哪一条；某一轮她说了话却没有锚点（自主插话），
  那一轮她看到的来话全算。**没声明 filter 的来话照旧全收**（私聊就是这一类）。
- **`speech`**：哪几件工具算"她发言"、正文在哪个字段。只活在工具调用里、
  没有对应 `assistant` 消息的那几句（`send_private` 这种主动开口）据此补进经历轴。
- **别的平台（微信 ／ B站…）要上经历页 ＝ 那个插件在它自己的 `package.json` 里多写一条**，
  核心和这一页一个字都不用改。
- 私聊那一条的正文标记自 2026-10-17 起带账号（`[QQ私聊:123456789·消息#…]`），
  因为私聊的"会话号"就是对方账号，而它只在正文里露得出来。

## 许可

本插件采用 [《TelyinCore 非商业使用与受限分发许可》1.0](LICENSE)
（`LicenseRef-TelyinCore-NonCommercial-1.0`）。这是源码可见的受限许可，不是 MIT、GPL 等 OSI 标准开源许可，也不是 `Unlicense`。
它不覆盖本插件使用的模型、依赖或运行时；这些组件仍按各自许可。
商业使用、未经授权的修改与再分发需要另行授权。


## 认可素材库（通用功能，按 Bot 隔离）

在 QQ 插件的 TTS 卡片展开 **认可素材库 · 当前 Bot**，手动开启“收集待评审语音”。默认关闭，不改变 QQ 原有配置。所有 Bot 均可使用；每个 Bot 独立开关、独立库，不共享特琳音色或其他 Bot 的素材。

- 只复制本 Bot 经当前插件合成、且 OneBot 已返回成功发送回执的完整 PCM16 WAV；不是对方实际听见的证明。不保存 QQ 目标账号、群号、访问令牌或用户 ASR 录音，也不扫描共享 TTS 输出目录。
- 开启即同意在本机保存输出语音及其文字。待评审 → 试听 → 认可 / 排除，可重新评审。标记不会删除音频；没有自动训练、自动上传或自动清理。
- 原 WAV 与文字、请求参数、来源、音频 SHA256 同事务落入 SQLite。模型具体版本若服务未提供则标为 `unreported_by_service`，不能视作已验证的模型指纹。文字是发起合成的原始文字，训练前仍需检查实际读音与对齐。
- 路径为 `<Core>/data/qq-voice-corpus/<有效配置路径的 SHA256>/corpus.sqlite3`，不在插件安装目录内。默认 Bot 和独立配置 Bot 分离。迁移时需连同 Bot 配置路径映射管理；不要按文件名跨 Bot 合并数据库。
- 每库最多 5000 条、512 MiB WAV 载荷（数据库/元数据另有少量开销）。达到容量后不再收集，不删除旧素材，不重试已发送的消息。关闭开关后旧素材仍可评审；取消发送/回执不明确不会入库。
- 页面每次最多显示 20 条，试听按需加载。仅本地 WebUI 操作者能认可，不给 Agent 工具或 QQ 消息下放评审权限。沿用 Core 回环/同源边界，不是同机其他进程之间的安全沙箱。
- 已认可仅表示操作者觉得值得保留，不保证无漂移、无错音或适合训练。训练导出、清洗与独立验证集留待后续；不自动拿整库训练。

升级须同时更新 Core 的 `telyincore/voice_corpus.py`、控制入口及前端；不能只拷贝插件 index.py 到旧版 Core。Core 控制入口需重启 Core 后生效，插件可在此后按原流程加载。
