#!/usr/bin/env bash
# QQ 版热态守候：回复完不退出，短时间守住下一个批次。
#
# 与 ilink-warm-wait.sh 行为完全一致，只是命名空间换成 qq-*：
# 它维护 $STATE_DIR/qq-warm.json（{since, activity, heartbeat, until}）。
# 心跳新鲜时脚本不再重复 wake，由本进程第一时间发现新批次；worker 死掉
# 心跳过期，脚本自动恢复 wake（自愈）。
#
# 每次回复后运行一次。阻塞刷新心跳，最后只打印一行：
#   NEW_MESSAGES  pending 队列里有批次——直接认领处理，不要重读上下文
#   IDLE_TIMEOUT  连续 ILINK_WARM_IDLE 秒没人说话——退出
#   TOTAL_CAP     本次热态会话到了 ILINK_WARM_TOTAL 秒上限——退出
#
# Env: QQ_HOOK_STATE_DIR（旧名 QQ_HOOK_STATE）、
#      QQ_WARM_IDLE（默认 180）、QQ_WARM_TOTAL（默认 1800）、
#      QQ_WARM_POLL（默认 5）。
#
# QQ 注意：群聊被动回复窗口只有 5 分钟。群聊场景建议把 QQ_WARM_IDLE
# 调到 60 秒以内，否则守候期会吃掉回复窗口。
set -uo pipefail

STATE_DIR=${QQ_HOOK_STATE_DIR:-${QQ_HOOK_STATE:-$HOME/hooks/state}}
WARM=$STATE_DIR/qq-warm.json
PENDING=$STATE_DIR/qq-pending.json
IDLE=${QQ_WARM_IDLE:-180}
TOTAL=${QQ_WARM_TOTAL:-1800}
POLL=${QQ_WARM_POLL:-5}

now_s=$(date +%s)
# `since` 从已有标记延续，这样总上限覆盖整段热态会话；死会话留下的
# 标记其上限在过去，本次调用立即结束。
since=$now_s
if [ -f "$WARM" ]; then
  prev=$(jq -r '.since // empty' "$WARM" 2>/dev/null || true)
  [ -n "$prev" ] && since=$prev
fi
activity=$now_s
cap=$((since + TOTAL))
deadline=$((activity + IDLE))
[ "$deadline" -gt "$cap" ] && deadline=$cap

write_marker() {
  local tmp="$WARM.tmp.$$"
  jq -n --argjson s "$since" --argjson a "$activity" \
        --argjson h "$(date +%s)" --argjson u "$cap" \
        '{since: $s, activity: $a, heartbeat: $h, until: $u}' \
        > "$tmp" 2>/dev/null && mv "$tmp" "$WARM" 2>/dev/null || true
}
write_marker

while :; do
  n=$(date +%s)
  if [ -s "$PENDING" ]; then
    cnt=$(jq '[.msgs[]?] | length' "$PENDING" 2>/dev/null || echo 0)
    if [ "${cnt:-0}" -gt 0 ] 2>/dev/null; then
      # 保留标记：心跳过期正是 worker 死掉后脚本重新 wake 的依据。
      echo NEW_MESSAGES
      exit 0
    fi
  fi
  if [ "$n" -ge "$deadline" ]; then
    rm -f "$WARM"
    if [ "$n" -ge "$cap" ]; then echo TOTAL_CAP; else echo IDLE_TIMEOUT; fi
    exit 0
  fi
  write_marker
  sleep "$POLL"
done
