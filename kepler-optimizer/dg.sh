#!/bin/bash
# =============================================================================
# dg.sh - Kepler 智能诊断主控脚本 (Diagnosis & Guidance)
# =============================================================================
# 功能：自动诊断 Kepler 任务并执行相应的优化策略
#
# 诊断流程:
#   1. Global View → 获取任务指标 (JobOffset/FullGc/Write)
#   2. 检查并发一致性 (worker.size vs parallelismConfig vs Partition)
#   3. 如果不一致 → Simply-Opt (无脑对齐)
#   4. 如果一致但有问题 → Normal-Opt (智能诊断)
#   5. 如果仍未解决 → 提示 Expert-Opt (需人工确认)
#
# 依赖:
#   - call-webapi skill (必需)
#   - antlogs MCP (仅 expert-opt 需要)
#
# 用法:
#   ./dg.sh <topologyId> [options]
#   ./dg.sh 197182              # 诊断并自动优化
#   ./dg.sh 197182 -n           # 仅诊断，不执行优化
#   ./dg.sh 197182 -f           # 强制模式，跳过确认
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPI_SCRIPT="/Users/sansi.xy/.qoder/skills/call-webapi/scripts/webapi.sh"

# API 配置
KEPLER_API="https://kepler.alipay.com/api"
ANTC_API="https://antc.alipay.com/api"
NCE_API="https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# 默认值
DRY_RUN=false
FORCE=false
DURATION_MIN=60

# 检查依赖
check_dependencies() {
    if [ ! -f "$WEBAPI_SCRIPT" ]; then
        echo -e "${RED}错误: call-webapi skill 未安装${NC}"
        echo "请安装 call-webapi skill:"
        echo "  方法1: 在 Qoder 中执行 /find-skills call-webapi"
        echo "  方法2: 手动安装到 ~/.qoder/skills/call-webapi/"
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        echo -e "${RED}错误: jq 未安装${NC}"
        echo "请安装 jq: brew install jq 或 apt-get install jq"
        exit 1
    fi
}

# call-webapi 请求函数
call_webapi() {
    local api_url=$1
    local method=$2
    local body=$3

    local params_json
    if [ -n "$body" ]; then
        params_json=$(echo "$body" | jq -Rs --arg api "$api_url" --arg method "$method" '{
            api: $api,
            method: $method,
            webHost: ($api | split("/api")[0]),
            headers: {"Content-Type": "application/json"},
            params: (. | fromjson)
        }')
    else
        params_json=$(jq -n --arg api "$api_url" --arg method "$method" '{
            api: $api,
            method: $method,
            webHost: ($api | split("/api")[0])
        }')
    fi

    "$WEBAPI_SCRIPT" "$params_json" 2>/dev/null
}

# 日志函数
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[⚠]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_dry() { echo -e "${CYAN}[DRY-RUN]${NC} $1"; }
log_analysis() { echo -e "${CYAN}[分析]${NC} $1"; }
log_step() { echo -e "${MAGENTA}[步骤 $1]${NC} $2"; }

# 使用说明
usage() {
    cat << EOF
╔══════════════════════════════════════════════════════════╗
║ Kepler DG - 智能诊断与优化主控脚本                          ║
╚══════════════════════════════════════════════════════════╝

用法: $0 <topologyId> [选项]

诊断流程:
  1. Global View → 获取任务指标
  2. 检查并发一致性 (worker.size vs parallelismConfig vs Partition)
  3. 如果不一致 → 执行 Simply-Opt (无脑对齐)
  4. 如果一致但有问题 → 执行 Normal-Opt (智能诊断)
  5. 如果仍未解决 → 提示 Expert-Opt (需人工确认)

依赖:
  - call-webapi skill (必需)
  - antlogs MCP (仅 expert-opt 需要)

参数:
  -t topologyId    任务 ID (必需，也可作为第一个参数)
  -n               仅诊断，不执行优化 (dry-run)
  -f               强制模式，跳过确认
  -d 分钟数        分析时长 (默认: 60)
  -h               显示帮助

示例:
  $0 197182              # 诊断并自动优化
  $0 197182 -n           # 仅诊断
  $0 197182 -f           # 强制模式，不确认
  $0 -t 197182 -d 120    # 分析最近2小时

EOF
    exit 1
}

# 解析参数
TOPOLOGY_ID=""
while getopts "t:nfd:h" opt; do
    case $opt in
        t) TOPOLOGY_ID="$OPTARG" ;;
        n) DRY_RUN=true ;;
        f) FORCE=true ;;
        d) DURATION_MIN="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# 如果没有 -t，第一个参数作为 topologyId
if [ -z "$TOPOLOGY_ID" ] && [ $# -gt 0 ]; then
    TOPOLOGY_ID="$1"
fi

if [ -z "$TOPOLOGY_ID" ]; then
    log_error "必须提供 topologyId"
    usage
fi

# 检查依赖
check_dependencies

# 打印头部
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Kepler DG - 智能诊断与优化                               ║"
echo "║   任务: $TOPOLOGY_ID                                       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

[ "$DRY_RUN" = true ] && log_dry "Dry-run 模式，仅诊断不执行"
[ "$FORCE" = true ] && log_warn "强制模式，将跳过确认"
echo ""

# =============================================================================
# 步骤 1: 获取任务基本信息
# =============================================================================
log_step "1/5" "获取任务配置..."

CONFIG=$(call_webapi "${KEPLER_API}/sql/${TOPOLOGY_ID}" "GET" "")

if ! echo "$CONFIG" | jq -e '.success == true' >/dev/null 2>&1; then
    log_error "无法获取任务配置"
    exit 1
fi

TASK_NAME=$(echo "$CONFIG" | jq -r '.data.name')
WORKER_SIZE=$(echo "$CONFIG" | jq -r '.data.globalConfig["kepler.worker.size"] // "0"')
MEMORY_SIZE=$(echo "$CONFIG" | jq -r '.data.globalConfig["kepler.worker.memory.size"] // "0"')
JAR_ID=$(echo "$CONFIG" | jq -r '.data.jarId')
CLUSTER_NAME=$(echo "$CONFIG" | jq -r '.data.clusterName')
STATUS=$(echo "$CONFIG" | jq -r '.data.status')

# 获取 parallelismConfig 的值
PARALLELISM_VALUES=$(echo "$CONFIG" | jq -r '.data.parallelismConfig | to_entries | map(.value) | unique | .[]')
PARALLELISM_COUNT=$(echo "$CONFIG" | jq -r '.data.parallelismConfig | keys | length')
FIRST_PARALLELISM=$(echo "$CONFIG" | jq -r '.data.parallelismConfig | to_entries[0].value // 0')

# 查询 partition 数量
PARTITION_COUNT=$(call_webapi "${ANTC_API}/kepler/engine/${TOPOLOGY_ID}/offset" "GET" "" | \
    jq -r '.data | length' 2>/dev/null || echo "0")

log_success "任务名: $TASK_NAME"
log_info "集群: $CLUSTER_NAME | 状态: $STATUS"
log_info "当前配置:"
echo "  - worker.size: $WORKER_SIZE"
echo "  - worker.memory: $((MEMORY_SIZE / 1073741824))GB"
echo "  - parallelismConfig: $PARALLELISM_COUNT 个算子"
echo "  - partition 数: $PARTITION_COUNT"
echo ""

# =============================================================================
# 步骤 2: Global View - 获取监控指标
# =============================================================================
log_step "2/5" "Global View 指标诊断..."

START_MS=$(( ($(date +%s) - DURATION_MIN * 60) * 1000 ))

# 查询 JobOffset
offset_body="{\"start\":$START_MS,\"queries\":[{\"metric\":\"JobOffset\",\"aggregator\":\"sum\",\"downsample\":\"5m-avg\",\"tags\":{\"topologyName\":\"$TASK_NAME\",\"type\":\"*\"}}],\"msResolution\":false}"
offset_resp=$(call_webapi "$NCE_API" "POST" "$offset_body")
offset_values=$(echo "$offset_resp" | jq -r '[.[0].dps // {} | to_entries[] | .value]')
offset_count=$(echo "$offset_values" | jq 'length')

if [ "$offset_count" -gt 0 ] 2>/dev/null; then
    offset_first=$(echo "$offset_values" | jq 'first // 0')
    offset_last=$(echo "$offset_values" | jq 'last // 0')
    if [ "$offset_first" -gt 0 ] 2>/dev/null; then
        drop_pct=$(echo "$offset_first $offset_last" | awk '{printf "%d", ($1 - $2) * 100 / $1}')
    else
        drop_pct=0
    fi
else
    offset_first="N/A"
    offset_last="N/A"
    drop_pct=0
fi

# 查询 FullGc
gc_body="{\"start\":$START_MS,\"queries\":[{\"metric\":\"FullGc\",\"aggregator\":\"sum\",\"downsample\":\"5m-avg\",\"tags\":{\"metaType\":\"TOPOLOGY\",\"topology\":\"$TOPOLOGY_ID\"}}],\"msResolution\":false}"
gc_resp=$(call_webapi "$NCE_API" "POST" "$gc_body")
gc_values=$(echo "$gc_resp" | jq -r '[.[0].dps // {} | to_entries[] | .value]')
gc_max=$(echo "$gc_values" | jq 'max // 0')
gc_avg=$(echo "$gc_values" | jq 'if length > 0 then (add // 0) / length else 0 end')

# 查询 write_records
write_body="{\"start\":$START_MS,\"queries\":[{\"metric\":\"write_records\",\"aggregator\":\"sum\",\"downsample\":\"5m-avg\",\"tags\":{\"metaType\":\"COMPONENT\",\"topology\":\"$TOPOLOGY_ID\"}}],\"msResolution\":false}"
write_resp=$(call_webapi "$NCE_API" "POST" "$write_body")
write_values=$(echo "$write_resp" | jq -r '[.[0].dps // {} | to_entries[] | .value]')
write_latest=$(echo "$write_values" | jq 'last // 0')
write_zeros=$(echo "$write_values" | jq '[.[] | select(. == 0)] | length')

# 输出指标
echo "┌─────────────────────────────────────────────────────────┐"
echo "│ Global View 指标                                         │"
echo "├─────────────────────────────────────────────────────────┤"
printf "│ JobOffset:  %-10s → %-10s (变化: %s%%)       │\n" "$offset_first" "$offset_last" "$drop_pct"
printf "│ FullGc:      max=%-6s avg=%-6s                      │\n" "$gc_max" "$gc_avg"
printf "│ Write:       latest=%-8s zeros=%-3s                 │\n" "$write_latest" "$write_zeros"
echo "└─────────────────────────────────────────────────────────┘"
echo ""

# =============================================================================
# 步骤 3: 并发一致性检查
# =============================================================================
log_step "3/5" "并发一致性检查..."

echo ""
echo "[黄金法则检查]"
echo "  worker.size = $WORKER_SIZE"
echo "  parallelismConfig 所有值 = $(echo "$PARALLELISM_VALUES" | tr '\n' ' ')"
echo "  partition 数 = $PARTITION_COUNT"
echo ""

# 检查是否一致
CONSISTENT=true

# 检查 worker.size 是否等于 partition
if [ "$WORKER_SIZE" != "$PARTITION_COUNT" ]; then
    log_warn "worker.size ($WORKER_SIZE) ≠ partition ($PARTITION_COUNT)"
    CONSISTENT=false
else
    log_success "worker.size = partition ✓"
fi

# 检查 parallelismConfig 是否都等于 partition
for val in $PARALLELISM_VALUES; do
    if [ "$val" != "$PARTITION_COUNT" ]; then
        log_warn "parallelismConfig 存在值 $val ≠ partition ($PARTITION_COUNT)"
        CONSISTENT=false
        break
    fi
done

if [ "$CONSISTENT" = true ]; then
    log_success "parallelismConfig 全部一致 ✓"
fi

echo ""

# =============================================================================
# 步骤 4: 决策与执行
# =============================================================================
log_step "4/5" "诊断决策..."

echo ""

# 决策逻辑
if [ "$CONSISTENT" = false ]; then
    # 不一致 → Simply-Opt
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║ 诊断结论: 并发配置不一致                                   ║"
    echo "║ 推荐方案: Simply-Opt (无脑对齐)                           ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    log_analysis "原因: worker.size 或 parallelismConfig 与 partition 数量不一致"
    log_analysis "方案: 执行 simply-opt 将所有并发参数对齐到 partition 数"
    echo ""

    if [ "$DRY_RUN" = true ]; then
        log_dry "Dry-run 模式，跳过执行"
        exit 0
    fi

    if [ "$FORCE" = false ]; then
        read -r -p "是否执行 simply-opt? [Y/n] " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]] && [ -n "$confirm" ]; then
            log_info "用户取消"
            exit 0
        fi
    fi

    echo ""
    log_info "执行 simply-opt..."
    bash "${SCRIPT_DIR}/kepler_simply_opt_simple.sh" "$TOPOLOGY_ID"

else
    # 一致但可能有其他问题 → Normal-Opt
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║ 诊断结论: 并发配置一致                                     ║"
    echo "║ 推荐方案: Normal-Opt (智能诊断)                           ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # 分析是否有问题
    NEED_OPTIMIZE=false

    # 检查延时是否上涨
    if [ "$drop_pct" -lt -10 ] 2>/dev/null; then
        log_warn "JobOffset 上涨 ${drop_pct#-}%，需要优化"
        NEED_OPTIMIZE=true
    fi

    # 检查 GC
    gc_max_int=$(echo "$gc_max" | cut -d. -f1)
    if [ "$gc_max_int" -gt 50 ] 2>/dev/null; then
        log_warn "FullGc 峰值 $gc_max，可能存在内存问题"
        NEED_OPTIMIZE=true
    fi

    # 检查写入
    if [ "$write_zeros" -gt 5 ] 2>/dev/null; then
        log_warn "写入频繁跌0 ($write_zeros 次)，需要优化"
        NEED_OPTIMIZE=true
    fi

    # 检查内存配置
    MEMORY_GB=$((MEMORY_SIZE / 1073741824))
    if [ "$MEMORY_GB" -lt 4 ] 2>/dev/null; then
        log_warn "当前内存 ${MEMORY_GB}GB 较小，建议优化"
        NEED_OPTIMIZE=true
    fi

    if [ "$NEED_OPTIMIZE" = false ]; then
        log_success "任务状态良好，无需优化"
        exit 0
    fi

    echo ""

    if [ "$DRY_RUN" = true ]; then
        log_dry "Dry-run 模式，跳过执行"
        exit 0
    fi

    if [ "$FORCE" = false ]; then
        read -r -p "是否执行 normal-opt? [Y/n] " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]] && [ -n "$confirm" ]; then
            log_info "用户取消"
            exit 0
        fi
    fi

    echo ""
    log_info "执行 normal-opt..."
    bash "${SCRIPT_DIR}/kepler_normal_opt.sh" -t "$TOPOLOGY_ID" -d "$DURATION_MIN"
fi

echo ""

# =============================================================================
# 步骤 5: 后续建议
# =============================================================================
log_step "5/5" "后续建议..."

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║ 优化完成！                                                 ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "建议后续操作:"
echo "  1. 等待 15 分钟后复查任务状态"
echo "  2. 使用以下命令监控:"
echo "     ./dg.sh $TOPOLOGY_ID -n"
echo ""
echo "如果问题仍未解决:"
echo "  - 执行 expert-opt 进行深度诊断:"
echo "    ./kepler_expert_opt.sh -t $TOPOLOGY_ID"
echo "  - 注意: expert-opt 需要 antlogs MCP 支持"
echo ""
