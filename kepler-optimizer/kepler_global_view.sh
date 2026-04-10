#!/bin/bash
# =============================================================================
# kepler_global_view.sh - Kepler 全局任务健康检查脚本
# =============================================================================
# 功能：批量分析一批任务的健康状态，给出是否需要调参的建议
#
# 分析逻辑:
# 1. JobOffset 趋势:
#    - 下降明显 (>10% in 1h) → 正常 ✓
#    - 下降缓慢 (<10%) 或上升 → 进入下一步分析
#
# 2. write_records (TPS) 分析:
#    - 正常 (>1000 且稳定) → 写入正常，可能是消费能力不足
#    - 低 (<1000) 或不稳定 → 写入有问题，继续看GC
#
# 3. FullGc 分析:
#    - GC高 (>50) + 写入低/不稳定 → 内存不足
#    - GC低 (<20) + 写入正常但延时涨 → 参数过小
#
# 输出分类:
#   [正常] - 延时快速下降，无需干预
#   [内存不足] - GC导致，建议增大内存 4→5→6GB
#   [参数过小] - 配置低导致追不上，建议调参 (对齐partition)
#   [写入异常] - 其他写入问题需人工排查
#
# 使用方法:
# ./kepler_global_view.sh -t 75917,123456,78901
# ./kepler_global_view.sh -f tasks.txt    # 文本文件每行一个ID
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# API 配置
NCE_API="https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query"
KEPLER_API="https://kepler.alipay.com/api"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'
GRAY='\033[0;90m'

# 配置
COOKIE_FILE="${SCRIPT_DIR}/cookie.txt"
DURATION_MIN=60
JOBS_FILE=""

# 统计
TOTAL=0
NORMAL=0
MEMORY_ISSUE=0
PARAM_TOO_SMALL=0
WRITE_PROBLEM=0
UNKNOWN=0

usage() {
    cat << EOF
╔══════════════════════════════════════════════════════════╗
║ Kepler Global View 全局任务健康检查脚本                       ║
╚══════════════════════════════════════════════════════════╝

用法:
  $0 -t <id1,id2,id3,...>    # 指定多个任务ID，逗号分隔
  $0 -f jobs.txt              # 从文件读取任务ID列表

参数:
  -t topologyIds   # 逗号分隔的任务ID列表
  -f filename      # 包含任务ID的文件 (每行一个)
  -c cookie文件     # 默认: cookie.txt
  -d 分钟数         # 分析时长 (默认: 60)
  -h               # 帮助

输出分类:
  [正常]        JobOffset快速下降，无需干预
  [内存不足]    FullGc高导致写入不稳定，需增大内存 4→5→6GB
  [参数过小]    配置低写入追不上生产，需调参对齐partition
  [写入异常]    其他写入问题需人工排查

示例:
  $0 -t 75917,123456,78901
  $0 -f /path/to/task_list.txt -d 120

EOF
    exit 1
}

log_section() { echo -e "${BLUE}$1${NC}"; }
log_normal() { echo -e "${GREEN}✓ $1${NC}"; }
log_warn() { echo -e "${YELLOW}⚠ $1${NC}"; }
log_error() { echo -e "${RED}✗ $1${NC}"; }
log_info() { echo -e "${CYAN}ℹ $1${NC}"; }
log_gray() { echo -e "${GRAY}$1${NC}"; }

# 解析参数
TOPOLOGY_IDS=""
while getopts "t:f:c:d:h" opt; do
    case $opt in
        t) TOPOLOGY_IDS="$OPTARG" ;;
        f) JOBS_FILE="$OPTARG" ;;
        c) COOKIE_FILE="$OPTARG" ;;
        d) DURATION_MIN="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# 检查输入
if [ -z "$TOPOLOGY_IDS" ] && [ -z "$JOBS_FILE" ]; then
    log_error "必须提供 -t 任务列表 或 -f 任务文件"
    usage
fi

# 读取任务列表
if [ -n "$JOBS_FILE" ]; then
    if [ ! -f "$JOBS_FILE" ]; then
        log_error "文件不存在: $JOBS_FILE"
        exit 1
    fi
    TOPOLOGY_IDS=$(cat "$JOBS_FILE" | tr '\n' ',' | sed 's/,$//')
fi

# 检查 cookie
if [ -n "$COOKIE" ]; then
    COOKIE_DATA="$COOKIE"
elif [ -f "$COOKIE_FILE" ]; then
    COOKIE_DATA=$(cat "$COOKIE_FILE")
else
    log_error "未找到 Cookie 文件: $COOKIE_FILE"
    echo "使用: export COOKIE=\"...\" 或创建 $COOKIE_FILE"
    exit 1
fi

START_TIME=$(( ($(date +%s) - DURATION_MIN * 60) * 1000 ))

log_section ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     Kepler Global View - 全局任务健康检查                    ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "分析时长: ${DURATION_MIN} 分钟"
echo "任务列表: $TOPOLOGY_IDS"
echo ""

# 初始化结果数组
declare -A RESULTS

# 查询单个指标
query_metric() {
    local metric=$1
    local tags=$2
    local body="{\"start\":$START_TIME,\"queries\":[{\"metric\":\"$metric\",\"aggregator\":\"sum\",\"downsample\":\"1m-avg\",\"tags\":$tags}],\"msResolution\":false}"
    
    curl -s -X POST "$NCE_API" \
        -H "Content-Type: application/json" \
        -b "$COOKIE_DATA" \
        --connect-timeout 10 \
        -d "$body" 2>/dev/null
}

# 分析指标
analyze_joboffset() {
    local data=$1
    echo "$data" | jq -r '
        if (.[0].dps // empty) != empty then
            (.[0].dps | to_entries | map(.value)) as $v |
            if ($v | length) > 1 then
                {
                    first: $v[0],
                    last: $v[-1],
                    trend: ($v[-1] - $v[0]),
                    drop_pct: (($v[0] - $v[-1]) / $v[0] * 100)
                } | @json
            else "{\"error\":\"insufficient_data\"}" end
        else "{\"error\":\"no_data\"}" end'
}

analyze_fullgc() {
    local data=$1
    echo "$data" | jq -r '
        if (.[0].dps // empty) != empty then
            (.[0].dps | to_entries | map(.value)) as $v |
            if ($v | length) > 0 then
                {
                    max: ($v | max),
                    avg: (($v | add) / ($v | length)),
                    total: ($v | add),
                    zeros: ([$v[] | select(. == 0)] | length)
                } | @json
            else "{\"error\":\"no_data\"}" end
        else "{\"error\":\"no_data\"}" end'
}

analyze_write_records() {
    local data=$1
    echo "$data" | jq -r '
        if (.[0].dps // empty) != empty then
            (.[0].dps | to_entries | map(.value)) as $v |
            if ($v | length) > 0 then
                {
                    latest: $v[-1],
                    min: ($v | min),
                    max: ($v | max),
                    avg: (($v | add) / ($v | length)),
                    zeros: ([$v[] | select(. == 0)] | length),
                    stable: (($v | max) - ($v | min) < ($v | add) / ($v | length) * 0.5)
                } | @json
            else "{\"error\":\"no_data\"}" end
        else "{\"error\":\"no_data\"}" end'
}

# 诊断单个任务
diagnose_task() {
    local id=$1
    local idx=$2
    
    printf "\n[%s/%s] 检查任务 %s " "$idx" "$TOTAL" "$id"
    
    # 查询指标
    local offset_data=$(query_metric "JobOffset" "{\"topologyName\":\"$id\",\"type\":\"*\"}")
    local gc_data=$(query_metric "FullGc" "{\"metaType\":\"TOPOLOGY\",\"topology\":\"$id\"}")
    local write_data=$(query_metric "write_records" "{\"metaType\":\"COMPONENT\",\"topology\":\"$id\"}")
    
    local offset=$(analyze_joboffset "$offset_data")
    local gc=$(analyze_fullgc "$gc_data")
    local write=$(analyze_write_records "$write_data")
    
    # 检查错误
    if echo "$offset$gc$write" | grep -q "error"; then
        echo ""
        log_warn "任务 $id: 数据获取失败，可能任务不存在或无权限"
        RESULTS[$id]="UNKNOWN|数据获取失败"
        ((UNKNOWN++))
        return
    fi
    
    # 解析数值
    local offset_first=$(echo "$offset" | jq -r '.first // 0')
    local offset_last=$(echo "$offset" | jq -r '.last // 0')
    local offset_drop=$(echo "$offset" | jq -r '.drop_pct // 0')
    local gc_max=$(echo "$gc" | jq -r '.max // 0')
    local gc_avg=$(echo "$gc" | jq -r '.avg // 0')
    local write_avg=$(echo "$write" | jq -r '.avg // 0')
    local write_zeros=$(echo "$write" | jq -r '.zeros // 0')
    local write_stable=$(echo "$write" | jq -r '.stable // false')
    
    echo ""
    log_info "JobOffset: ${offset_first}→${offset_last} (↓${offset_drop}%)"
    log_info "FullGc: max=${gc_max}, avg=${gc_avg}"
    log_info "Write: avg=${write_avg}, zeros=${write_zeros}, stable=${write_stable}"
    
    # 决策逻辑
    local result=""
    local reason=""
    
    # Step 1: 延时是否快速下降？
    if (( $(echo "$offset_drop > 20" | bc -l) )); then
        # 延时下降明显 -> 正常
        result="NORMAL"
        reason="延时快速下降 ${offset_drop}%"
    else
        # 延时下降缓慢或上升 -> 分析写入
        if (( $(echo "$write_avg > 10000" | bc -l) )) && [ "$write_stable" = "true" ]; then
            # 写入正常 -> 参数过小
            result="PARAM_TOO_SMALL"
            reason="延时涨但写入正常(stable)，配置过小追不上生产"
        else
            # 写入异常 -> 看GC
            if (( $(echo "$gc_max > 40" | bc -l) )); then
                result="MEMORY_ISSUE"
                if [ "$write_zeros" -gt 5 ]; then
                    reason="GC高(max=${gc_max})导致写入跌0(${write_zeros}次)，内存不足"
                else
                    reason="GC高(max=${gc_max})导致写入不稳定，内存不足"
                fi
            else
                if [ "$write_zeros" -gt 5 ]; then
                    result="WRITE_PROBLEM"
                    reason="GC正常但写入频繁跌0，需排查其他问题"
                else
                    result="PARAM_TOO_SMALL"
                    reason="延时缓慢下降，建议调参对齐partition"
                fi
            fi
        fi
    fi
    
    RESULTS[$id]="$result|$reason"
    
    case $result in
        NORMAL) ((NORMAL++)) ;;
        MEMORY_ISSUE) ((MEMORY_ISSUE++)) ;;
        PARAM_TOO_SMALL) ((PARAM_TOO_SMALL++)) ;;
        WRITE_PROBLEM) ((WRITE_PROBLEM++)) ;;
        *) ((UNKNOWN++)) ;;
    esac
}

# 主流程
log_section "开始批量分析..."
echo ""

# 计数
IFS=',' read -ra IDS <<< "$TOPOLOGY_IDS"
TOTAL=${#IDS[@]}

# 逐个分析
for i in "${!IDS[@]}"; do
    diagnose_task "${IDS[$i]}" "$((i+1))"
done

# 输出汇总
log_section ""
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                    分析结果汇总                            ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "总计任务: $TOTAL"
echo ""

# 按分类输出
if [ $NORMAL -gt 0 ]; then
    echo -e "${GREEN}[正常] $NORMAL 个${NC}: 延时快速下降，无需干预"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == NORMAL* ]]; then
            reason=${RESULTS[$id]#*|}
            echo "  $id - $reason"
        fi
    done
    echo ""
fi

if [ $MEMORY_ISSUE -gt 0 ]; then
    echo -e "${MAGENTA}[内存不足] $MEMORY_ISSUE 个${NC}: 增大内存 4→5→6GB + buffer=80"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == MEMORY_ISSUE* ]]; then
            reason=${RESULTS[$id]#*|}
            echo "  $id - $reason"
        fi
    done
    echo ""
fi

if [ $PARAM_TOO_SMALL -gt 0 ]; then
    echo -e "${YELLOW}[参数过小] $PARAM_TOO_SMALL 个${NC}: 执行调参对齐partition"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == PARAM_TOO_SMALL* ]]; then
            reason=${RESULTS[$id]#*|}
            echo "  $id - $reason"
        fi
    done
    echo ""
fi

if [ $WRITE_PROBLEM -gt 0 ]; then
    echo -e "${RED}[写入异常] $WRITE_PROBLEM 个${NC}: GC正常但写入问题，需人工排查"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == WRITE_PROBLEM* ]]; then
            reason=${RESULTS[$id]#*|}
            echo "  $id - $reason"
        fi
    done
    echo ""
fi

if [ $UNKNOWN -gt 0 ]; then
    echo -e "${GRAY}[未知/失败] $UNKNOWN 个${NC}: 数据获取失败或无权限"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == UNKNOWN* ]]; then
            reason=${RESULTS[$id]#*|}
            echo "  $id - $reason"
        fi
    done
    echo ""
fi

# 操作建议
echo "═══════════════════════════════════════════════════════════"
echo "                        操作建议                              "
echo "═══════════════════════════════════════════════════════════"
echo ""

if [ $MEMORY_ISSUE -gt 0 ]; then
    echo "【内存不足任务】执行命令:"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == MEMORY_ISSUE* ]]; then
            echo "  ./kepler_expert_opt.sh -t $id  # 选择 OOM 选项"
        fi
    done
    echo ""
fi

if [ $PARAM_TOO_SMALL -gt 0 ]; then
    echo "【参数过小任务】执行命令:"
    for id in "${!RESULTS[@]}"; do
        if [[ ${RESULTS[$id]} == PARAM_TOO_SMALL* ]]; then
            echo "  ./kepler_normal_opt.sh -t $id"
        fi
    done
    echo ""
fi

echo "分析完成！"
