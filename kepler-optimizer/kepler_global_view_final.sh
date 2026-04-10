#!/bin/bash
# =============================================================================
# kepler_global_view_final.sh - 最终版全局任务健康检查 (使用 call-webapi)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPI_SCRIPT="/Users/sansi.xy/.qoder/skills/call-webapi/scripts/webapi.sh"

# 检查 webapi 脚本是否存在
if [ ! -f "$WEBAPI_SCRIPT" ]; then
    echo "错误: call-webapi 脚本不存在: $WEBAPI_SCRIPT"
    echo "请先安装 call-webapi skill"
    exit 1
fi

NCE_API="https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query"
KEPLER_API="https://kepler.alipay.com/api"
DURATION=${2:-60}

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     Kepler Global View Final - 全局任务健康检查               ║"
echo "║        (使用 call-webapi skill 查询)                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

TASK_IDS="$1"
[ -z "$TASK_IDS" ] && echo "用法: $0 <id1,id2,id3> [duration_min]" && exit 1

START_MS=$(( ($(date +%s) - DURATION * 60) * 1000 ))
START_H=$(python3 -c "import time; print(time.strftime('%H:%M', time.localtime($START_MS/1000)))")

echo "分析周期: ${START_H} → now (${DURATION}min)"
echo "任务列表: $TASK_IDS"
echo ""

# 日志函数
log_info() { echo -e "\033[36m$1\033[0m"; }
log_good() { echo -e "\033[32m$1\033[0m"; }
log_warn() { echo -e "\033[33m$1\033[0m"; }
log_err() { echo -e "\033[31m$1\033[0m"; }

# 使用 call-webapi 发送请求
call_webapi() {
    local api_url=$1
    local method=$2
    local body=$3
    
    local params_json
    if [ -n "$body" ]; then
        # POST 请求 - 使用 jq 构造 JSON 避免转义问题
        params_json=$(echo "$body" | jq -Rs --arg api "$api_url" --arg method "$method" '{
            api: $api,
            method: $method,
            webHost: ($api | split("/api")[0]),
            headers: {"Content-Type": "application/json"},
            params: (. | fromjson)
        }')
    else
        # GET 请求
        params_json=$(jq -n --arg api "$api_url" --arg method "$method" '{
            api: $api,
            method: $method,
            webHost: ($api | split("/api")[0])
        }')
    fi
    
    "$WEBAPI_SCRIPT" "$params_json" 2>/dev/null
}

# 获取任务名
get_task_name() {
    local id=$1
    local resp=$(call_webapi "${KEPLER_API}/sql/$id" "GET" "")
    echo "$resp" | jq -r '.data.name // empty'
}

# 查询单个任务
check_task() {
    local id=$1
    local name=$(get_task_name "$id")
    
    if [ -z "$name" ] || [ "$name" = "null" ]; then
        echo "✗ 任务 $id: 无法获取配置"
        return
    fi
    
    echo "═══════════════════════════════════════════════════════════"
    echo "  任务ID: $id"
    echo "  任务名: $name"
    echo "═══════════════════════════════════════════════════════════"
    
    # 查询 JobOffset
    local offset_body="{\"start\":$START_MS,\"queries\":[{\"metric\":\"JobOffset\",\"aggregator\":\"sum\",\"downsample\":\"5m-avg\",\"tags\":{\"topologyName\":\"$name\",\"type\":\"*\"}}],\"msResolution\":false}"
    local offset_resp=$(call_webapi "$NCE_API" "POST" "$offset_body")
    
    local offset_values=$(echo "$offset_resp" | jq -r '[.[0].dps // {} | to_entries[] | .value]')
    local offset_count=$(echo "$offset_values" | jq 'length')
    
    if [ "$offset_count" = "0" ] || [ "$offset_count" = "null" ]; then
        log_warn "  无监控数据 (任务可能未运行或监控未开)"
        return
    fi
    
    local offset_first=$(echo "$offset_values" | jq 'first // 0')
    local offset_last=$(echo "$offset_values" | jq 'last // 0')
    
    echo "  JobOffset: ${offset_first} → ${offset_last}"
    
    # 查询 FullGc
    local gc_body="{\"start\":$START_MS,\"queries\":[{\"metric\":\"FullGc\",\"aggregator\":\"sum\",\"downsample\":\"5m-avg\",\"tags\":{\"metaType\":\"TOPOLOGY\",\"topology\":\"$id\"}}],\"msResolution\":false}"
    local gc_resp=$(call_webapi "$NCE_API" "POST" "$gc_body")
    local gc_values=$(echo "$gc_resp" | jq -r '[.[0].dps // {} | to_entries[] | .value]')
    local gc_max=$(echo "$gc_values" | jq 'max // 0')
    local gc_avg=$(echo "$gc_values" | jq 'if length > 0 then (add // 0) / length else 0 end')
    
    echo "  FullGc: max=${gc_max}, avg=${gc_avg}"
    
    # 查询 write_records
    local write_body="{\"start\":$START_MS,\"queries\":[{\"metric\":\"write_records\",\"aggregator\":\"sum\",\"downsample\":\"5m-avg\",\"tags\":{\"metaType\":\"COMPONENT\",\"topology\":\"$id\"}}],\"msResolution\":false}"
    local write_resp=$(call_webapi "$NCE_API" "POST" "$write_body")
    local write_values=$(echo "$write_resp" | jq -r '[.[0].dps // {} | to_entries[] | .value]')
    local write_latest=$(echo "$write_values" | jq 'last // 0')
    local write_min=$(echo "$write_values" | jq 'min // 0')
    local write_zeros=$(echo "$write_values" | jq '[.[] | select(. == 0)] | length')
    
    echo "  Write: latest=${write_latest}, min=${write_min}, zeros=${write_zeros}"
    
    echo ""
    echo "【诊断结论】"
    
    # 决策逻辑
    if [ "$offset_first" -eq 0 ] 2>/dev/null || [ -z "$offset_first" ]; then
        echo "  ❓ 数据不完整"
        return
    fi
    
    # 计算延时下降比例
    local drop_pct=$(echo "$offset_first $offset_last" | awk '{printf "%d", ($1 - $2) * 100 / $1}')
    
    # 写入量为0的特殊情况（任务可能暂停消费但checkpoint在推进）
    if [ "$write_latest" -eq 0 ] 2>/dev/null && awk "BEGIN{exit !($offset_last < $offset_first)}"; then
        log_info "  ℹ️  写入为0但延时下降 ${drop_pct}%，任务可能暂停消费"
        if [ "$drop_pct" -gt 10 ]; then
            log_good "  ✅ [可接受] 延时下降 ${drop_pct}%，暂不处理"
        else
            log_warn "  ⚠️ [关注] 延时下降缓慢 ${drop_pct}%，建议检查任务状态"
        fi
        return
    fi
    
    # 正常决策（写入有数据）
    if [ "$drop_pct" -gt 10 ]; then
        log_good "  ✅ [正常] 延时快速下降 ${drop_pct}%，无需干预"
    else
        # 延时下降缓慢或上涨
        gc_max_int=$(echo "$gc_max" | cut -d. -f1)
        gc_avg_int=$(echo "$gc_avg" | cut -d. -f1)
        
        if [ "$write_latest" -gt 100000 ] && [ "$write_zeros" -lt 3 ] 2>/dev/null; then
            # 写入正常
            if [ "$gc_max_int" -gt 50 ]; then
                log_warn "  ⚠️ [内存不足] GC高导致写入不稳，建议:内存 4→5→6GB"
            else
                log_warn "  ⚠️ [参数过小] 写入正常但延时追不上，建议: 调参对齐partition"
            fi
        else
            # 写入异常
            if [ "$gc_max_int" -gt 40 ]; then
                log_warn "  ⚠️ [内存不足] GC高且写入低/跌0，建议:内存 4→6GB"
            elif [ "$write_zeros" -gt 5 ]; then
                log_err "  ❓[写入异常] GC正常但频繁跌0，需人工排查日志"
            else
                log_warn "  ⚠️ [参数过小] 配置低写不动，建议: normal-opt 调参"
            fi
        fi
    fi
    echo ""
}

# 处理任务
IFS=',' read -ra IDS <<< "$TASK_IDS"

for id in "${IDS[@]}"; do
    check_task "$id"
done

echo "═══════════════════════════════════════════════════════════"
echo "                        诊断完成                             "
echo "═══════════════════════════════════════════════════════════"
