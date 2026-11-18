#!/usr/bin/env bash
# QQ 官方机器人：把 Muse hook 契约接到 QQ。
#
# 与 ilink-watch.sh 的结构一一对应，只是传输层换了：
#   - 微信是长轮询：每轮脚本 hold 最多 35s 去 getupdates
#   - QQ 是推送：qqbot.py serve 常驻持有 WebSocket，事件落 inbox/ 后就返回
#   所以本脚本每轮只是把 inbox/ 里已落盘的事件取出来（qqbot.py poll），
#   走同一套两段式提交：先原子落盘 pending，再调 commit 释放 inbox 文件。
#
# 两段式提交（与微信版同样的理由，同样的不变量）：
#   qqbot.py poll 只读 inbox、不推进游标也不记去重 id；本脚本把批次
#   原子落盘到 ilink 同构的 pending.json 之后才调 qqbot.py commit。
#   任何一步崩溃，事件都还在 inbox 里，下一轮原样再取——宁可重复，绝不丢失。
#
# 告警闭环（与微信版同一状态机）：
#   - 凭据失效：首次立即 wake 报一次，之后每小时复报一次
#   - 接收端失联：serve 心跳超过 120s 视为故障，连续失败满 10 分钟才报
#   - 恢复：曾告警过的异常恢复正常后 wake 一次 recovered
#   - worker 卡住：processing 超 10 分钟被回收时记 stuck 起点
#
# 与微信版的差别（QQ 平台限制，不是实现取舍）：
#   - 没有主动推送（2025-04-21 起下线），所以没有 proactive 分支
#   - 被动回复窗口短：单聊 60 分钟 / 群聊 5 分钟；条数上限单聊 4 条、群聊 5 条
#   - typing 只在单聊可用，群聊直接跳过
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE=${QQ_DATA_DIR:-$HOME/qq-bot-data}
STATE_DIR=${QQ_HOOK_STATE_DIR:-${QQ_HOOK_STATE:-$HOME/hooks/state}}
mkdir -p "$STATE_DIR"

# 单实例锁：防 hook 自己重叠。
exec 9>"$STATE_DIR/qq-watch.lock"
if ! flock -n 9; then
  exit 0
fi

# 驱动锁：与常驻 daemon（daemon.py 那类角色）二选一。QQ 版的 serve 是
# 数据源而不是驱动，所以它不抢这把锁；这把锁仍然留给可能出现的
# 常驻驱动，保持与微信版一致的行为。
exec 8>"$STATE_DIR/qq-driver.lock"
if ! flock -n 8; then
  exit 0
fi

touch "$STATE_DIR/qq-heartbeat"
PENDING=$STATE_DIR/qq-pending.json
PROC=$STATE_DIR/qq-processing.json
WARM=$STATE_DIR/qq-warm.json
COMMIT_PENDING=$STATE_DIR/qq-commit-pending.json
POLLLOG=$BRIDGE/poll.log
ALERT=$BRIDGE/alert-state.json
CLIENT=${QQ_CLIENT:-$BRIDGE/qqbot.py}
DRY=${HATCH_HOOK_DRY_RUN:-0}
NOW=$(date +%s)

now() { date '+%Y-%m-%d %H:%M:%S'; }

log_line() {
  [ "$DRY" = "1" ] && return 0
  echo "[$(now)] $1" >> "$POLLLOG"
  trim_poll_log
}

trim_poll_log() {
  [ "$DRY" = "1" ] && return 0
  [ -f "$POLLLOG" ] || return 0
  local n
  n=$(wc -l < "$POLLLOG" 2>/dev/null || echo 0)
  if [ "$n" -gt 500 ] 2>/dev/null; then
    tail -n 500 "$POLLLOG" > "$POLLLOG.trim.$$" && mv "$POLLLOG.trim.$$" "$POLLLOG"
  fi
}

# 队列三态：空 -> 0；合法队列 -> 条数；损坏（含结构不对）-> 隔离保留现场。
# 与微信版同一条理由：把损坏文件当 0 会被覆盖（丢消息）或静默卡死。
queue_count() {
  local path=$1 n iso
  [ -s "$path" ] || { echo 0; return; }
  if ! jq -e 'type == "object" and (.msgs | type == "array")' "$path" >/dev/null 2>&1; then
    iso="$path.corrupt-$(date +%s)-$$"
    if mv "$path" "$iso" 2>/dev/null; then
      log_line "PENDING_CORRUPT: $path unparseable, quarantined as $iso"
      log "PENDING_CORRUPT: quarantined $path as $iso"
    fi
    echo 0
    return
  fi
  n=$(jq '[.msgs[]?] | length' "$path" 2>/dev/null || echo 0)
  echo "${n:-0}"
}

# commit 待提交账本补交：上一轮 commit 失败时 payload 留在这里。
# 补交失败本轮不 poll——旧批次未提交前拉新批次会交叠。
flush_commit_pending() {
  [ -f "$COMMIT_PENDING" ] || return 0
  [ "$DRY" = "1" ] && return 0
  local payload iso
  if ! jq -e 'type == "object"' "$COMMIT_PENDING" >/dev/null 2>&1; then
    iso="$COMMIT_PENDING.corrupt-$(date +%s)"
    mv "$COMMIT_PENDING" "$iso" 2>/dev/null || rm -f "$COMMIT_PENDING"
    log_line "COMMIT_PENDING_CORRUPT: quarantined as $iso; its batch will be re-fetched"
    log "COMMIT_PENDING_CORRUPT: quarantined $COMMIT_PENDING as $iso"
    return 0
  fi
  payload=$(jq -c '.' "$COMMIT_PENDING")
  if printf '%s' "$payload" | python3 "$CLIENT" commit >/dev/null 2>&1; then
    rm -f "$COMMIT_PENDING"
    log "retried pending commit OK"
    return 0
  fi
  log_line "COMMIT_RETRY_FAILED: $COMMIT_PENDING still uncommitted, poll skipped this round"
  log "commit retry failed, poll skipped this round"
  return 1
}

warm_alive() {
  [ -f "$WARM" ] || return 1
  local hb until_n
  hb=$(jq -r '.heartbeat // 0' "$WARM" 2>/dev/null || echo 0)
  until_n=$(jq -r '.until // 0' "$WARM" 2>/dev/null || echo 0)
  case "$hb" in ''|*[!0-9]*) return 1 ;; esac
  case "$until_n" in ''|*[!0-9]*) return 1 ;; esac
  [ $((NOW - hb)) -le 45 ] && [ "$until_n" -gt "$NOW" ]
}

astate() { jq -r ".$1 // 0" "$ALERT" 2>/dev/null || echo 0; }
aset() {
  [ "$DRY" = "1" ] && return 0
  local tmp="$ALERT.tmp.$$"
  if [ -f "$ALERT" ]; then
    jq --argjson v "$2" ".$1 = \$v" "$ALERT" > "$tmp" 2>/dev/null || echo '{}' > "$tmp"
  else
    jq -n --argjson v "$2" "{\"$1\": \$v}" > "$tmp" 2>/dev/null || echo '{}' > "$tmp"
  fi
  mv "$tmp" "$ALERT" 2>/dev/null || true
}

# 点亮「对方正在输入」：QQ 只在单聊支持，群聊/频道一律跳过。
# 平台要求带上被回复的消息 id，所以有 message_id 就一起传。
# 失败绝不影响主流程（纯装饰）。
lit_typing() {
  local first_json=$1 to chat mid
  to=$(printf '%s' "$first_json" | jq -r '.reply_to // empty' 2>/dev/null || true)
  chat=$(printf '%s' "$first_json" | jq -r '.chat_type // "c2c"' 2>/dev/null || true)
  mid=$(printf '%s' "$first_json" | jq -r '.message_id // empty' 2>/dev/null || true)
  [ -n "$to" ] || return 0
  [ "$chat" = "c2c" ] || return 0
  if [ -n "$mid" ]; then
    python3 "$CLIENT" typing "$to" start --msg-id "$mid" >/dev/null 2>&1 || true
  else
    python3 "$CLIENT" typing "$to" start >/dev/null 2>&1 || true
  fi
}

# 崩溃恢复：上一个 worker 超 10 分钟没处理完，把消息退回 pending。
# 用 -cmin 而非 -mmin：mv 认领会刷新 ctime，mtime 保留落盘时间；
# 积压超过 10 分钟的消息认领瞬间就会被误判过期、形成乒乓循环。
# pending 里还有批次时不回收，避免 mv 覆盖掉新批次（覆盖即永久丢失）。
pending_msgs=$(queue_count "$PENDING")
if [ -f "$PROC" ] && [ "${pending_msgs:-0}" -eq 0 ] && [ -n "$(find "$PROC" -cmin +10 2>/dev/null || true)" ]; then
  mv "$PROC" "$PENDING"
  log "recovered stale processing file to pending"
  if [ "$(astate stuck_since)" -eq 0 ]; then
    aset stuck_since $((NOW - 600))
  fi
fi

if [ -s "$PENDING" ]; then
  cnt=$(queue_count "$PENDING")
  if [ "$cnt" -gt 0 ] 2>/dev/null; then
    log "pending $cnt messages waiting"
    first=$(jq -c '.msgs[0] // {}' "$PENDING" 2>/dev/null || echo '{}')
    lit_typing "$first"
    extra=""
    stuck_since=$(astate stuck_since)
    stuck_last=$(astate stuck_last)
    if [ "$stuck_since" -gt 0 ] && [ $((NOW - stuck_since)) -ge 600 ] \
       && [ $((NOW - stuck_last)) -ge 3600 ]; then
      aset stuck_last "$NOW"
      extra=",\"stuck_alert\":true,\"stuck_minutes\":$(( (NOW - stuck_since) / 60 ))"
    fi
    if warm_alive; then
      log "warm worker on duty, $cnt pending left to it"
      silent "warm worker on duty"
    else
      wake "pending_qq_messages" "{\"count\":$cnt,\"pending_file\":\"$PENDING\"$extra}"
    fi
  else
    silent "pending file empty"
  fi
elif ! flush_commit_pending; then
  silent "commit retry pending, poll skipped"
else
  # 纯本地读取（inbox 目录），不会阻塞；timeout 30 只是防止病态情况下
  # 卡住锁。inbox 里没东西时它立刻返回空批次。
  out=$(timeout 30 python3 "$CLIENT" poll 2>/dev/null || true)

  if [[ "$out" == NO_CREDENTIALS* ]] || [[ "$out" == TOKEN_ERROR* ]] || [[ "$out" == *"NO_TOKEN"* ]]; then
    exp_since=$(astate exp_since)
    exp_last=$(astate exp_last)
    if [ "$exp_since" -eq 0 ]; then
      aset exp_since "$NOW"
      aset exp_last "$NOW"
      if [ "$DRY" != "1" ]; then
        wake "token_expired" "{}"
      else
        silent "dry-run: would wake token_expired"
      fi
    elif [ $((NOW - exp_last)) -ge 3600 ]; then
      aset exp_last "$NOW"
      if [ "$DRY" != "1" ]; then
        wake "token_expired" "{\"repeat\":true,\"hours\":$(( (NOW - exp_since) / 3600 ))}"
      else
        silent "dry-run: would repeat token_expired alert"
      fi
    else
      silent "token error, repeat alert throttled"
    fi
  elif [[ "$out" == NO_OWNER* ]]; then
    # 没绑定 owner：一条消息都不处理，这是配置问题不是故障。报一次，
    # 之后每小时复报，直到 bind 完成（bind 后自动走 recovered）。
    if [ "$(astate noowner_last)" -eq 0 ] || [ $((NOW - $(astate noowner_last))) -ge 3600 ]; then
      aset noowner_last "$NOW"
      if [ "$DRY" != "1" ]; then
        wake "not_bound" "{\"hint\":\"给机器人发一条单聊消息，然后运行 qqbot.py bind\"}"
      else
        silent "dry-run: would wake not_bound"
      fi
    else
      silent "no owner bound"
    fi
  elif [[ "$out" == POLL_ERROR* ]] || ! printf '%s' "$out" | jq -e 'type == "object" and has("msgs")' >/dev/null 2>&1; then
    if [ "$DRY" != "1" ]; then
      echo "[$(now)] $(printf '%s' "${out:-POLL_CRASH:empty-or-invalid-output}" | head -c 200)" >> "$POLLLOG"
      trim_poll_log
    fi
    err_since=$(astate err_since)
    if [ "$err_since" -eq 0 ]; then
      aset err_since "$NOW"
      err_since=$NOW
    fi
    err_last=$(astate err_last)
    err_dur=$((NOW - err_since))
    if [ "$err_dur" -ge 600 ] && { [ "$err_last" -eq 0 ] || [ $((NOW - err_last)) -ge 3600 ]; }; then
      aset err_last "$NOW"
      if [ "$DRY" != "1" ]; then
        wake "poll_failing" "{\"minutes\":$((err_dur / 60))}"
      else
        silent "dry-run: would wake poll_failing"
      fi
    else
      silent "poll error"
    fi
  else
    recovered=""
    exp_since=$(astate exp_since)
    err_since=$(astate err_since)
    if [ ! -f "$PROC" ] && [ "$(astate stuck_since)" -gt 0 ]; then
      aset stuck_since 0
      aset stuck_last 0
    fi
    if [ "$exp_since" -gt 0 ]; then
      recovered="{\"kind\":\"token\",\"minutes\":$(( (NOW - exp_since) / 60 ))}"
      aset exp_since 0
      aset exp_last 0
    elif [ "$(astate noowner_last)" -gt 0 ]; then
      aset noowner_last 0
      recovered="{\"kind\":\"bind\",\"minutes\":0}"
    elif [ "$err_since" -gt 0 ]; then
      if [ "$(astate err_last)" -gt 0 ]; then
        recovered="{\"kind\":\"poll_error\",\"minutes\":$(( (NOW - err_since) / 60 ))}"
      fi
      aset err_since 0
      aset err_last 0
    fi
    cnt=$(printf '%s' "$out" | jq '[.msgs[]?] | length' 2>/dev/null || echo 0)
    commit_json=$(printf '%s' "$out" | jq -c '.commit // empty' 2>/dev/null || true)

    do_commit() {
      if [ "$DRY" != "1" ] && [ -n "$commit_json" ]; then
        local ctmp="$COMMIT_PENDING.tmp.$$"
        printf '%s' "$commit_json" > "$ctmp" && mv "$ctmp" "$COMMIT_PENDING"
        if printf '%s' "$commit_json" | python3 "$CLIENT" commit >/dev/null 2>&1; then
          rm -f "$COMMIT_PENDING"
        else
          echo "[$(now)] COMMIT_FAILED: payload kept in $COMMIT_PENDING, retried next round" >> "$POLLLOG"
          trim_poll_log
        fi
      fi
    }
    if [ "${cnt:-0}" -gt 0 ] 2>/dev/null; then
      if [ "$DRY" = "1" ]; then
        silent "dry-run: $cnt messages fetched, left uncommitted for live poll"
      else
        tmp="$PENDING.tmp.$$"
        printf '%s' "$out" | jq -c '{msgs}' > "$tmp"
        mv "$tmp" "$PENDING"
        log "received $cnt messages"
        first=$(printf '%s' "$out" | jq -c '.msgs[0] // {}' 2>/dev/null || echo '{}')
        lit_typing "$first"
        do_commit
        rec_extra=""
        if [ -n "$recovered" ]; then rec_extra=",\"recovered\":$recovered"; fi
        if [ -z "$recovered" ] && warm_alive; then
          log "warm worker on duty, batch left to it"
          silent "warm worker on duty"
        else
          wake "new_qq_messages" "{\"count\":$cnt,\"pending_file\":\"$PENDING\"$rec_extra}"
        fi
      fi
    elif [ -n "$recovered" ]; then
      do_commit
      if [ "$DRY" != "1" ]; then
        wake "recovered" "$recovered"
      else
        silent "dry-run: would wake recovered"
      fi
    else
      do_commit
      silent "no new messages"
    fi
  fi
fi
