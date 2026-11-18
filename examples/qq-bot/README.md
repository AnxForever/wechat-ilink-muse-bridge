# QQ 变体：把同一套 hook 契约接到 QQ 官方机器人

[中文](README.md) | [English](README.en.md)

`examples/muse-hook/` 接的是微信 iLink。本目录把同样的契约接到 **QQ 官方机器人
平台（QQ 开放平台 / bot API v2）**，Muse worker 的作业契约不用改一行——它看到
的还是同一个 `qq-pending.json`、同样的 `wake`/`silent`、同样的认领与提交规则。

## 先说清楚它做不到什么

接之前必须知道三件事，它们会改变这个适配器能承诺的东西：

1. **没有主动推送。** 平台自 2025-04-21 起停止提供主动消息，调用会报错。
   微信版最招牌的能力（定时提醒、主动报平安）在 QQ 上没有对应物。
   本适配器因此不提供 `proactive` 动词——不是没写，是平台不给。
2. **被动回复窗口很短。** 单聊：用户消息后 **60 分钟内**、最多回 **4 条**；
   群聊：**5 分钟内**、最多 5 条。超时或超频直接发送失败。所以 worker
   必须快，群聊尤其不能挂着慢慢想。
3. **生产需要固定公网 IP 白名单。** 开放平台要求把服务器出口 IP 加进白名单，
   家宽动态 IP 会被挡。**沙箱环境不要求白名单**，所以开发期用沙箱即可；
   正式上线前要么换机器，要么上固定 IP。

另外：消息里的 URL 需要先在后台「开发设置 → 消息 URL 配置」登记（要备案域名），
否则发送失败；Markdown 需要单独申请；单聊支持「正在输入」，群聊不支持。

## 结构

```
QQ 用户 ──消息──> QQ 网关
                    │  WebSocket 推送（平台主动推，不是我们去轮询）
                    ▼
              qqbot.py serve       ← 常驻进程，持有网关连接
                    │  事件先落盘 inbox/（tmp→fsync→rename），再持久化 seq
                    ▼
              data/inbox/*.json    ← 至少一次：崩溃重连会重推，不会丢
                    │  qq-watch.sh 每 5s 跑一轮
                    ▼
              qqbot.py poll        ← 只读 inbox，不推进任何状态
                    │
              qq-pending.json      ← 原子落盘，然后才 commit
                    │  worker 用 mv 原子认领 → qq-processing.json
                    ▼
              Muse worker agent    ← 发送前复查队列、合并连发、只回一条
```

和微信版唯一的结构差异：微信是我们去长轮询，QQ 是平台推给我们。所以
「拉取」这一步拆成了 `serve`（收）和 `poll`（取），但 `poll` 的契约完全没变：
**只读、不推进、可重复调用**。

## 为什么 `serve` 和 `poll` 要分开

平台的 Node SDK 有一个已经核实的缺陷：网关事件在派发前就把 `lastSeq` 落盘了，
而且不 await 异步 handler。也就是说进程在 handler 执行到一半崩掉，这个事件
既没处理、也不会重推——**SDK 的 `sessionPersistence` 保证不了至少一次**。

本实现把顺序反过来：先把事件写进 `inbox/`（tmp 文件 → fsync → rename，
原子可见），**然后**才持久化 seq。崩在任何中间点，重连时网关会补推，
`inbox_write()` 按 seq 幂等去重，所以最坏是重复处理（worker 有幂等键兜底），
绝不会静默丢消息。

同一个理由下，`msg_seq` 不是 SDK 那种随机 16 位数：它由幂等键的气泡后缀
（`<msg_id>-1`、`<msg_id>-2`）确定性推导，失败重试落在**同一对**
`(msg_id, msg_seq)` 上，服务端直接判重。账本在调用前就记下这对值，
所以「发成功但没收到响应」也不会变成第二条气泡。

## 安装

```bash
export QQ_APP_ID=<你的 AppID>
export QQ_APP_SECRET=<你的 AppSecret>
export QQ_DATA_DIR=$HOME/qq-bot-data      # 凭据、队列、inbox 都放这
export QQ_SANDBOX=1                        # 开发期用沙箱 API

cp examples/qq-bot/qqbot.py "$QQ_DATA_DIR/"
python3 "$QQ_DATA_DIR/qqbot.py" health     # 检查凭据 + 接收端状态
```

也可以把凭据写进 `$QQ_DATA_DIR/config.json`（`{"app_id": ..., "app_secret": ...}`），
权限记得收成 600。**`data/` 里全是敏感物，别提交、别外发。**

## 绑定主人（必做，且只有一次）

和微信版一样，owner 是**代码强制**的：不在 `owner.json` 里的发信人一律不进队列。
新部署的机器人还不知道谁是主人，所以先开一个绑定窗口：

```bash
python3 qqbot.py serve &          # 先让接收端跑起来
python3 qqbot.py bind 600         # 600 秒窗口，第一个单聊发信人成为主人
```

窗口期内给机器人发任意一条单聊消息即可。成功打印 `BOUND:<openid>` 并回一条
确认。之后 `owner.json` 里就有主人了。

群聊默认一条都不处理。要让某个群可用，显式加白：

```json
{"owner_id": "...", "groups": ["<group_openid>", "..."]}
```

## 接进 Muse

`qq-watch.sh` 就是 `ilink-watch.sh` 的同构替换：同样的两段式提交、同样的
pending 认领、同样的告警状态机、同样的热态守候。把它挂成每 5 秒跑一轮的 hook：

```
script_path = examples/qq-bot/qq-watch.sh
env: QQ_DATA_DIR=..., QQ_HOOK_STATE_DIR=~/.muse/hooks/state
```

同时**必须**让 `qqbot.py serve` 常驻（它是唯一的数据源；它挂了 `poll` 会
明确报 `POLL_ERROR`，走同一套告警，而不是安静地什么都不发生）。

QQ 的事件 topic 与微信版平行：

| topic | 含义 |
|---|---|
| `new_qq_messages` | 有新批次，payload 带 `count` / `pending_file` |
| `pending_qq_messages` | 上一批还没处理完 |
| `token_expired` | 凭据缺失或失效（首次立即报，之后每小时复报） |
| `not_bound` | 还没绑定主人（首次立即报，之后每小时复报） |
| `poll_failing` | 连续失败满 10 分钟才报 |
| `recovered` | 曾告警的异常恢复正常（`kind`: token / bind / poll_error） |

## worker 侧的差异（只有这些）

契约本身不变，但两个平台的能力不同，worker 需要知道：

- **发回复必须带 msg_id。** QQ 只允许被动回复：
  `qqbot.py send <reply_to> <msg_id> - --key <msg_id>-1`。
  `reply_to` 在批次条目里（单聊是 sender 的 openid，群聊是 group_openid）。
- **气泡数有硬上限。** 单聊一条用户消息最多回 4 条，群聊最多 5 条
  （官方「消息收发概述」）。越界的第 N 条 `send` 会直接返回
  `SEND_BUBBLE_CAP` 而不发出去。所以「2–3 条短气泡」的约定在这里是硬约束，
  不是风格建议。
- **群聊要加 `--group`。** 单聊不用加（默认）。频道/私信分别是 `--channel` / `--dm`。
- **群聊不点亮正在输入。** 平台只支持单聊；`qq-watch.sh` 会自动跳过。
- **热态守候要缩短。** 群聊窗口只有 5 分钟；建议 `QQ_WARM_IDLE=60`。
  单聊可以保持默认 180。

## 自检

```bash
python3 qqbot.py status     # token / 网关 session / inbox 积压 / 账本，全 JSON
python3 qqbot.py health     # exit 0 正常 / 1 警告 / 2 故障
```

`status` 里 `serve_heartbeat_age` 是接收端的存活证据——和微信版的
`ilink-heartbeat` 同一个角色：安静但心跳新鲜是健康的空闲，安静且心跳过期是死掉的接收端。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

QQ 相关的有三个文件：

- `tests/test_qqbot.py` — poll/commit 两段式、owner 白名单、去重、
  发送幂等与 `msg_seq` 推导
- `tests/test_qq_hook_integration.py` — `qq-watch.sh` 的端到端契约
  （需要 `bash` + `jq` + `flock` + `timeout`）
- `tests/test_qqbot_ws.py` — 最小 WebSocket 客户端与网关循环，
  用一个本地回环网关验证「先落盘、后记 seq」的顺序

Git Bash 没有 `flock`，所以 hook 集成测试在纯 Windows 上会 skip；
用 WSL 或 Linux 跑即可。

## 已知限制

- 主动推送没有（见开头第 1 条），别再设计依赖它的功能。
- 富媒体（图片/文件）目前只做**接收方向**的下载（落 `media/`，带
  `media_path`）；发送方向还没做——`send` 只发文本。
- 单聊 openid 是 per-bot 的，换机器人就换 id；迁移时要重新绑定。
- 平台对消息内容有审核，审核事件走 `MESSAGE_AUDIT`（本适配器暂未订阅）。
