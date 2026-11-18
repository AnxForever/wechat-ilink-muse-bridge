# 飞书变体：把同一套 hook 契约接到飞书自建应用

[中文](README.md) | [English](README.en.md)

`examples/muse-hook/` 接的是微信 iLink，`examples/qq-bot/` 接的是 QQ 官方机器人。
本目录把同一份契约接到 **飞书（Lark）自建应用**。Muse worker 的作业契约不用改
一行——它看到的还是同样的 pending 队列、同样的 `wake`/`silent`、同样的认领与
提交规则，只是 topic 和发信 id 换了前缀。

## 先说清楚它做不到什么

接之前必须知道这几件事，它们会改变这个适配器能承诺的东西：

1. **没有「正在输入」指示。** 飞书没有给机器人用的 typing 接口（QQ 单聊有）。
   本适配器不提供 `typing` 动词，worker 也不要假装点亮过它。
2. **群聊默认只收 @ 机器人的消息。** 这是权限层面的设计：订阅
   `im.message.receive_v1` 时，群聊消息需要 `im:message.group_at_msg` 一类权限，
   而它只投递被 @ 的消息。想收群里所有消息属于敏感权限（`im:message.group_msg`），
   一般不要申请。所以群消息的 `@_user_1` 前缀由适配器剥掉。
3. **推送必须在 3 秒内应答。** 超时平台按失败处理，并会在 15 秒、5 分钟、1 小时、
   6 小时后重试（最多 4 次），之后丢弃。这一步由 `feishubot.py serve` 保证：
   先把事件落盘，再回 ack，正常耗时是毫秒级。
4. **至少一次，不是恰好一次。** 平台会重推，而且官方明确要求**按
   `message_id` 去重**（不是 `event_id`）。本适配器在落盘和 poll 两处都按
   `message_id` 去重。
5. **限流。** 单聊/群聊维度 5 QPS，应用维度 1000 次/分钟。所以回复只做一次
   短重试（命中限流码时），不做指数退避队列。
6. **主动消息受「可用范围」约束。** `send` 在 ctx 为空时走 create 接口，
   只能发给应用可用范围内的用户/群；范围外会直接报错，不会静默丢弃。

反过来，飞书比 QQ 省心的地方：**长连接不需要公网 IP、域名和隧道**，鉴权只在
建连时做一次，之后的事件推送是明文的（不走 Encrypt Key / Verification Token，
也正因为如此，长连接模式下没有 webhook 那套签名校验）。这也是本适配器选择
长连接而不是 webhook 的原因——一台家用机器就能跑。

## 结构

```
飞书用户 ──消息──> 飞书开放平台
                     │  长连接推送（我们主动建连，平台往这条连接上推）
                     ▼
               feishubot.py serve    ← 常驻进程，持有长连接
                     │  事件先落盘 inbox/（tmp→fsync→rename），再回 ack
                     ▼
               data/inbox/*.json     ← 至少一次：未 ack 会被重推，不会丢
                     │  feishu-watch.sh 每 5s 跑一轮
                     ▼
               feishubot.py poll     ← 只读 inbox，不推进任何状态
                     │
               feishu-pending.json   ← 原子落盘，然后才 commit
                     │  worker 用 mv 原子认领 → feishu-processing.json
                     ▼
               Muse worker agent     ← 发送前复查队列、合并连发、只回一条
```

和微信版的结构差异只有「谁来发起连接」：微信是我们长轮询，飞书是我们建一条
长连接、由平台往上面推。所以「收」和「取」拆成了 `serve` 与 `poll`，而 `poll`
的契约完全没变：**只读、不推进、可重复调用**。

## 为什么自己实现长连接，而不是用官方 SDK

`feishubot.py` 只用标准库：`urllib` 调 REST，`socket` + `ssl` 实现最小
RFC 6455 客户端，帧格式 `pbbp2.Frame` 用二十来行手写 protobuf 编解码。
这样做换来三件事：

- 和 `ilink.py` / `qqbot.py` 一样，部署时不需要 `pip install` 任何东西；
- ack 的时机完全由我们决定（SDK 是 handler 同步执行完再 ack，handler 一慢
  就会吃掉 3 秒预算，我们的 ack 只等一次 fsync）；
- 去重键是我们自己选的 `message_id`，不受 SDK 事件分发层的影响。

代价是协议升级要自己跟。`tests/test_feishubot_ws.py` 里有一个**独立的**编解码
实现和一个本地回环服务端，所以「自己跟自己一致」这种假通过不会发生。

## 安装

1. 在 [飞书开放平台](https://open.feishu.cn) 建一个**自建应用**，拿到
   `App ID` / `App Secret`。
2. 开通机器人能力，订阅事件 `im.message.receive_v1`（接收消息），
   权限至少要有：`im:message.p2p_msg:readonly`（单聊）、
   `im:message.group_at_msg:readonly`（群里 @ 机器人）、
   `im:message:send_as_bot`（以机器人身份发消息）。
3. **事件接收方式选「长连接」**，不要配 webhook——配了 webhook 反而要求公网地址。
4. 发布版本并把应用可用范围设成包含你自己。

```bash
export FEISHU_APP_ID=<你的 App ID>
export FEISHU_APP_SECRET=<你的 App Secret>
export FEISHU_DATA_DIR=$HOME/feishu-bot-data    # 凭据、队列、inbox 都放这

cp examples/feishu-bot/feishubot.py "$FEISHU_DATA_DIR/"
python3 "$FEISHU_DATA_DIR/feishubot.py" health  # 凭据 + 接收端状态
```

凭据也可以写进 `$FEISHU_DATA_DIR/config.json`
（`{"app_id": ..., "app_secret": ...}`），权限记得收成 600。
**`data/` 里全是敏感物，别提交、别外发。**

国际版 Lark 租户设置 `FEISHU_DOMAIN=https://open.larksuite.com` 即可，
其余逻辑相同。

## 绑定主人（必做，且只有一次）

和微信/QQ 版一样，owner 是**代码强制**的：不在 `owner.json` 里的发信人一律
不进队列。新部署还不知道谁是主人，先开一个绑定窗口：

```bash
python3 feishubot.py serve &          # 先让接收端跑起来
python3 feishubot.py bind 600         # 600 秒窗口，第一个单聊发信人成为主人
```

窗口期内给机器人发任意一条单聊消息即可。成功打印 `BOUND:<open_id>` 并回一条
确认（用的就是被绑定那条消息的回复接口，所以不占主动消息额度）。

群聊默认一条都不处理。要让某个群可用，显式加白：

```json
{"owner_id": "ou_xxx", "groups": ["oc_xxx", "..."]}
```

注意群消息默认只有 @ 机器人时才会推过来，所以群场景下 @ 是必须的，
加白解决的是「这个群允不允许用」，不是「能不能收到」。

（`allow_all_groups: true` 能关掉整个群白名单，但它等于让任何拉得动机器人的群
都能进队列，除非你明确想要，否则别开。）

## 接进 Muse

`feishu-watch.sh` 是 `qq-watch.sh` 的同构替换：同样的两段式提交、同样的
pending 认领、同样的告警状态机、同样的热态守候（少了 typing 分支）。
把它挂成每 5 秒跑一轮的 hook：

```
script_path = examples/feishu-bot/feishu-watch.sh
env: FEISHU_DATA_DIR=..., FEISHU_HOOK_STATE_DIR=~/.muse/hooks/state
```

同时**必须**让 `feishubot.py serve` 常驻（它是唯一的数据源；它挂了 `poll` 会
明确报 `POLL_ERROR`，走同一套告警，而不是安静地什么都不发生）。

topic 与其它渠道平行：

| topic | 含义 |
|---|---|
| `new_feishu_messages` | 有新批次，payload 带 `count` / `pending_file` |
| `pending_feishu_messages` | 上一批还没处理完 |
| `token_expired` | 凭据缺失或失效（首次立即报，之后每小时复报） |
| `not_bound` | 还没绑定主人（首次立即报，之后每小时复报） |
| `poll_failing` | 连续失败满 10 分钟才报 |
| `recovered` | 曾告警的异常恢复正常（`kind`: token / bind / poll_error） |

## worker 侧的差异（只有这些）

- **回复用 ctx（message_id），不要用 chat_id 猜。** 批次条目里有
  `reply_to` 和 `message_id`：

  ```bash
  python3 feishubot.py send <reply_to> <message_id> - --key <message_id>-1
  ```

  这是被动回复，走 `/im/v1/messages/:message_id/reply`，比 proactive 稳。
- **主动消息把 ctx 传空**（例如定时提醒）：

  ```bash
  python3 feishubot.py send <oc_ chat_id 或 ou_ open_id> "" - --key <自定义键>
  ```

  接收方类型由前缀推断（`oc_` → `chat_id`，`ou_` → `open_id`），也可以用
  `--chat` / `--p2p` 显式指定。范围外的目标会报错。
- **幂等键照旧。** 同一 key 第二次调用返回 `SEND_ALREADY_SENT`；key 同时
  被用作请求的 `uuid`（飞书按 `uuid` 做 1 小时服务端去重），所以「发成功但
  没收到响应」的重试不会变成第二条消息。多气泡时用 `<message_id>-1`、
  `-2` 后缀，和其它渠道一致。
- **富媒体只在接收方向处理。** 图片/文件/语音/视频会下载到 `data/media/`，
  条目里带 `media_path`（失败时是 `media_error`）；卡片、表情包只做占位文本。
  发送方向目前只发文本。
- **群聊不用加任何 flag**，`reply_to` 已经是 chat_id，适配器自己走群路径。
- **热态守候保持默认即可。** 没有 QQ 那种 5 分钟群窗口，180 秒没问题。

## 自检

```bash
python3 feishubot.py status     # token / 长连接 / inbox 积压 / 账本，全 JSON
python3 feishubot.py health     # exit 0 正常 / 1 警告 / 2 故障
```

`status` 里的 `serve_heartbeat_age` 是接收端的存活证据：安静但心跳新鲜是
健康的空闲，安静且心跳过期是死掉的接收端。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

飞书相关三个文件：

- `tests/test_feishubot.py` — poll/commit 两段式、owner 白名单、按 `message_id`
  去重、文本/富文本/媒体解析、发送幂等与 `uuid` 推导、限流重试
- `tests/test_feishu_hook_integration.py` — `feishu-watch.sh` 的端到端契约，
  外加一条「永远不发 typing」（需要 `bash` + `jq` + `flock` + `timeout`）
- `tests/test_feishubot_ws.py` — 长连接循环：手写 protobuf 的字节级断言、
  分片重组、ack 前后顺序、ping/pong、以及「鉴权失败/致命错误码不重连」

Git Bash 没有 `flock`，所以 hook 集成测试在纯 Windows 上会 skip；
用 WSL 或 Linux 跑即可。

## 已知限制

- 没有正在输入指示（见开头第 1 条），别再设计依赖它的交互。
- 群聊默认只有 @ 机器人的消息能收到；要收全量得申请敏感权限，不建议。
- 主动消息不能发给可用范围之外的人，也不能保证对方会看到（取决于对方的
  消息设置）。
- 发送方向只有文本；图片/文件要发得先走 `im/v1/image`、`im/v1/file` 上传，
  本适配器还没做。
- 用户的 open_id 是 per-app 的，换应用就换 id；迁移时要重新绑定。
