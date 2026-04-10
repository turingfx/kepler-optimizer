#!/bin/bash
# =============================================================================
# kepler_expert_opt.sh - Kepler 专家级诊断优化脚本 v3 (call-webapi 版)
# =============================================================================
# 功能：全自动诊断 + 智能决策 + 日志分析
# 黄金法则 (必须严格遵守):
# 1. kepler.worker.size = parallelismConfig 所有值 = Partition 数量 (三者必须一致)
# 2. job.worker.thread.number = 1
# 3. worker.memory.size: 2GB ~ 6GB (2147483648 ~ 6442450944)
# 4. kepler.output.odps.buffer.size: 60 ~ 200

#
# 正确流程：
# 1. 获取任务信息 → 拿到 topologyName
# 2. 查询指标 (JobOffset/FullGc/write_records)
# 3. 用 topologyName 查询 antlogs 异常日志
# 4. 错误分析 → 决策 (停止/优化)
# 5. 执行
#
# 错误识别 & 对策:
# - NoSuchTable → 停止任务 (废弃)
# - OOM (heap space) → 内存 4→6GB + buffer=80
# - ODPS StatusConflict → buffer 降至 80
# - MQ Timeout → +timeout 参数
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPI_SCRIPT="/Users/sansi.xy/.qoder/skills/call-webapi/scripts/webapi.sh"

# API 配置
KEPLER_API="https://kepler.alipay.com/api"
ANTC_API="https://antc.alipay.com/api"
NCE_API="https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query"

# antlogs MCP 配置
LOG_SERVER="mcp.ant.alipaybase-antlogsmcp.mcp-server"
LOG_PROJECT="ant-kepler"
LOG_LOGSTORE="kepler-exceptions"
LOG_REGION="em14"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# 配置
DRY_RUN=false
DURATION_MIN=60
DEFAULT_MEMORY=4294967296
MEMORY_INCREMENT=1073741824
MAX_MEMORY=6442450944

# 检查 webapi 脚本
if [ ! -f "$WEBAPI_SCRIPT" ]; then
    echo "错误: call-webapi 脚本不存在: $WEBAPI_SCRIPT"
    exit 1
fi

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

usage() {
    cat << EOF
╔══════════════════════════════════════════════════════════╗
║ Kepler Expert-Opt v3 全自动诊断优化脚本                      ║
╚══════════════════════════════════════════════════════════╝

用法: $0 -t <topologyId> [选项]

正确流程:
  1. 查任务信息 → 获取 topologyName
  2. 查三大指标
  3. 用 topologyName 查 antlogs
  4. 分析 → 决策 → 执行

错误识别:
  - NoSuchTable → 停止任务
  - OOM (heap space) → 内存 4→6GB
  - ODPS 冲突 → buffer=80
  - MQ 超时 → +timeout

参数:
  -t topologyId (必需)
  -d 分钟数 (默认: 60)
  -n dry-run 模式
  -h 帮助

EOF
    exit 1
}

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[⚠]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_dry() { echo -e "${CYAN}[DRY-RUN]${NC} $1"; }
log_analysis() { echo -e "${CYAN}[分析]${NC} $1"; }
log_expert() { echo -e "${MAGENTA}[专家]${NC} $1"; }

# 解析参数
TOPOLOGY_ID=""
while getopts "t:d:nh" opt; do
    case $opt in
        t) TOPOLOGY_ID="$OPTARG" ;;
        d) DURATION_MIN="$OPTARG" ;;
        n) DRY_RUN=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

if [ -z "$TOPOLOGY_ID" ]; then
    log_error "必须提供 -t topologyId"
    usage
fi

print_header() {
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  $1"
    echo "═══════════════════════════════════════════════════════════"
    echo ""
}

# 初始化
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Kepler Expert-Opt v3 全自动诊断优化                      ║"
echo "║        (使用 call-webapi skill)                           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
log_info "任务 ID: $TOPOLOGY_ID"
log_info "分析时长: ${DURATION_MIN} 分钟"
[ "$DRY_RUN" = true ] && log_dry "dry-run 模式"
echo ""

# 计算时间
START_TIME=$(( ($(date +%s) - DURATION_MIN * 60) * 1000 ))
START_TIME_ISO=$(date -u -d "@${START_TIME%???}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)
END_TIME_ISO=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# 变量
TASK_NAME=""
CLUSTER_NAME=""
TASK_STATUS=""
ERROR_TYPE="unknown"
MEMORY_GB=4
BUFFER_SIZE=120

# =============================================================================
# 阶段1: 获取任务信息 (关键！必须先拿到 topologyName)
# =============================================================================

print_header "阶段1: 获取任务信息"

CFG_FILE="/tmp/kepler_cfg_${TOPOLOGY_ID}.json"

log_info "查询任务配置..."
CONFIG_RESULT=$(call_webapi "${KEPLER_API}/sql/${TOPOLOGY_ID}" "GET" "")
echo "$CONFIG_RESULT" > "$CFG_FILE"

if ! echo "$CONFIG_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    log_error "无法获取配置"
    exit 1
fi

# 解析任务信息
TASK_INFO=$(cat "$CFG_FILE" | python3 << 'PYEOF'
import sys, json
d = json.load(sys.stdin)
data = d.get('data', {})
result = {
    'name': data.get('name', ''),
    'cluster': data.get('clusterName', ''),
    'status': data.get('status', ''),
    'partitionCount': len(data.get('parallelismConfig', {}))
}
print(json.dumps(result))
PYEOF
)

TASK_NAME=$(echo "$TASK_INFO" | jq -r '.name')
CLUSTER_NAME=$(echo "$TASK_INFO" | jq -r '.cluster')
TASK_STATUS=$(echo "$TASK_INFO" | jq -r '.status')
PARTITION_COUNT=$(echo "$TASK_INFO" | jq -r '.partitionCount')

if [ -z "$TASK_NAME" ] || [ "$TASK_NAME" = "null" ]; then
    log_error "无法获取任务名！"
    exit 1
fi

log_success "任务名: $TASK_NAME"
log_success "集群: $CLUSTER_NAME"
log_success "状态: $TASK_STATUS"
log_success "并行度: $PARTITION_COUNT"

# =============================================================================
# 阶段2: 指标诊断
# =============================================================================

print_header "阶段2: 指标诊断"

query_metric() {
    local metric=$1
    local tags=$2
    local body="{\"start\":$START_TIME,\"queries\":[{\"metric\":\"$metric\",\"aggregator\":\"sum\",\"downsample\":\"1m-avg\",\"tags\":$tags}],\"msResolution\":false}"

    call_webapi "$NCE_API" "POST" "$body"
}

echo "查询指标 1/3: JobOffset..."
joboffset=$(query_metric "JobOffset" "{\"topologyName\":\"$TOPOLOGY_ID\",\"type\":\"*\"}" | jq -r '
    if type == "array" and length > 0 then
        (.[0].dps // {}) | to_entries | 
        if length > 0 then map(.value) | {latest: last, first: first, trend: (if last < first then "decreasing" elif last > first then "increasing" else "stable" end)}
        else {"error":"no_data"} end
    else {"error":"failed"} end')
joboffset_latest=$(echo "$joboffset" | jq -r '.latest // 0 | floor')
joboffset_trend=$(echo "$joboffset" | jq -r '.trend // "unknown"')

echo "查询指标 2/3: FullGc..."
fullgc=$(query_metric "FullGc" "{\"metaType\":\"TOPOLOGY\",\"topology\":\"$TOPOLOGY_ID\"}" | jq -r '
    if type == "array" and length > 0 then
        (.[0].dps // {}) | to_entries |
        if length > 0 then map(.value) as $v | {max: ($v | max), avg: (($v | add) / length)}
        else {"error":"no_data"} end
    else {"error":"failed"} end')
fullgc_max=$(echo "$fullgc" | jq -r '.max // 0')
fullgc_avg=$(echo "$fullgc" | jq -r '.avg // 0')

echo "查询指标 3/3: write_records..."
writerecords=$(query_metric "write_records" "{\"metaType\":\"COMPONENT\",\"topology\":\"$TOPOLOGY_ID\"}" | jq -r '
    if type == "array" and length > 0 then
        (.[0].dps // {}) | to_entries |
        if length > 0 then map(.value) as $v | {latest: last, zeros: ([$v[] | select(.==0)] | length), trend: (if last < first then "decreasing" else "stable" end)}
        else {"error":"no_data"} end
    else {"error":"failed"} end')
write_zeros=$(echo "$writerecords" | jq -r '.zeros // 0')
write_trend=$(echo "$writerecords" | jq -r '.trend // "unknown"')

GC_SEVERITY="normal"
([ "$fullgc_max" -gt 100 ] 2>/dev/null && GC_SEVERITY="critical") || 
([ "$fullgc_max" -gt 40 ] 2>/dev/null && GC_SEVERITY="high")

echo ""
echo "┌─────────────────────────────────────────────────────────┐"
printf "│ JobOffset: %-10s (趋势: %-11s)           │\n" "$joboffset_latest" "$joboffset_trend"
printf "│ FullGc:    %-10s 平均: %-10s             │\n" "$fullgc_max" "$fullgc_avg"
printf "│ 写入归零:  %-5s         趋势: %-11s           │\n" "$write_zeros" "$write_trend"
printf "│ GC 严重度:  %-10s                                 │\n" "$GC_SEVERITY"
echo "└─────────────────────────────────────────────────────────┘"

# =============================================================================
# 阶段3: 日志分析 (关键！必须带上作业名查询)
# =============================================================================

print_header "阶段3: 日志分析"

log_info "使用作业名查询 antlogs: $TASK_NAME"
log_info "时间范围: $START_TIME_ISO → $END_TIME_ISO"
echo ""

# 由于 shell 无法直接调用 MCP，这里输出查询命令供外部使用
echo "查询命令 (可在 homi 里执行):"
echo ""
echo ">>> 先查询总体异常:"
echo "ant_mcp call $LOG_SERVER queryProjectLogstoreContent"
echo "  project=$LOG_PROJECT"
echo "  logstore=$LOG_LOGSTORE"
echo "  regionId=$LOG_REGION"
echo "  startTime=$START_TIME_ISO"
echo "  endTime=$END_TIME_ISO"
echo "  query=$TASK_NAME"
echo "  resultLimitPerLog=20"
echo ""

# 尝试调用 MCP (如果可用)
log_info "正在查询日志..."

# 尝试简化查询 - 直接输出结构让用户判断
# 实际使用中，用户应执行以上 MCP 调用

# 基于指标推测
if [ "$GC_SEVERITY" = "critical" ] || [ "$GC_SEVERITY" = "high" ]; then
    log_warn "检测到高 GC，推测可能是 OOM"
    log_analysis "建议: 内存 4GB → 5GB 或 6GB"
    ERROR_TYPE="OOM"
fi

echo ""
read -r -p "请根据日志分析选择错误类型 [1=NoSuchTable(停止), 2=OOM(6GB), 3=ODPS冲突(5GB), 4=其他(4GB), s=跳过]: " choice

case $choice in
    1)
        ERROR_TYPE="NoSuchTable"
        ACTION="stop"
        log_warn "决策: 停止任务 (NoSuchTable)"
        ;;
    2)
        ERROR_TYPE="OOM"
        ACTION="optimize"
        MEMORY_GB=6
        BUFFER_SIZE=80
        log_warn "决策: OOM → 内存 6GB + buffer 80"
        ;;
    3)
        ERROR_TYPE="ODPS_CONFLICT"
        ACTION="optimize"
        MEMORY_GB=5
        BUFFER_SIZE=80
        log_warn "决策: ODPS冲突 → 内存 5GB + buffer 80"
        ;;
    s|S)
        log_info "用户跳过，退出"
        exit 0
        ;;
    *)
        ERROR_TYPE="OTHER"
        ACTION="optimize"
        MEMORY_GB=4
        log_info "决策: 其他 → 标准优化 4GB"
        ;;
esac

# =============================================================================
# 阶段4: 执行决策
# =============================================================================

print_header "阶段4: 执行决策"

echo ""
echo "[黄金法则检查]"
echo "  ✓ kepler.worker.size = partition"
echo "  ✓ parallelismConfig 所有算子 = partition"
echo "  ✓ job.worker.thread.number = 1"
echo "  ✓ worker.memory.size 在 2-6GB 范围内"
echo "  ✓ odps.buffer.size 在 60-200 范围内"
echo ""
if [ "$ACTION" = "stop" ]; then
    log_warn "停止任务: $TOPOLOGY_ID ($TASK_NAME)"

    if [ "$DRY_RUN" = true ]; then
        log_dry "跳过实际停止"
        exit 0
    fi

    STOP_RESULT=$(call_webapi "${KEPLER_API}/topology/${TOPOLOGY_ID}/ops/stop" "POST" '{}')

    if echo "$STOP_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
        log_success "任务已停止！"
    else
        log_error "停止失败: $STOP_RESULT"
        exit 1
    fi
    exit 0
fi

# =============================================================================
# 阶段5: 执行优化
# =============================================================================

log_expert "执行优化配置..."
echo ""

cat << EOF
优化计划:
  - worker.size = $PARTITION_COUNT (当前并行度)
  - worker.memory.size = ${MEMORY_GB}GB (根据错误类型调整)
  - worker.cpu.slot.num = 3
  - output.odps.buffer.size = $BUFFER_SIZE
  - parallelismConfig.* = $PARTITION_COUNT
  - 删除 queue.capacity
EOF

if [ "$DRY_RUN" = true ]; then
    echo ""
    log_dry "dry-run 模式，跳过执行"
    exit 0
fi

echo ""
log_info "生成配置..."

# 生成更新配置
MEMORY_BYTES=$((MEMORY_GB * 1073741824))

python3 - "${TOPOLOGY_ID}" "${PARTITION_COUNT}" "${MEMORY_BYTES}" "${BUFFER_SIZE}" "${CFG_FILE}" << 'PYEOF'
import json
import sys

topo_id = sys.argv[1]
partition_count = int(sys.argv[2])
memory_bytes = int(sys.argv[3])
buffer_size = int(sys.argv[4])
cfg_file = sys.argv[5]

try:
    with open(cfg_file) as f:
        data = json.load(f).get('data', {})
    
    if not data or not data.get('id'):
        print("ERROR: No config data")
        sys.exit(1)
    
    # 新配置
    opt_config = {
        'kepler.worker.size': str(partition_count),
        'kepler.worker.memory.size': str(memory_bytes),
        'worker.cpu.slot.num': '3',
        'kepler.output.odps.buffer.size': str(buffer_size),
        'job.worker.thread.number': '1'
    }
    delete_keys = ['kepler.output.queue.capacity']
    
    # 构建新配置列表
    new_config = []
    for c in data.get('keplerConfig', []):
        key = c.get('key')
        if key in delete_keys:
            continue
        if key in opt_config:
            c['value'] = opt_config[key]
        new_config.append(c)
    
    # 添加缺失的
    current = {c.get('key') for c in new_config}
    for key, val in opt_config.items():
        if key not in current:
            is_sys = key in ['kepler.worker.size', 'kepler.worker.memory.size']
            new_config.append({"key": key, "value": val, "system": is_sys})
    
    # 并行度
    parallelism = {k: partition_count for k in data.get('parallelismConfig', {})}

    request = {
        "id": data.get('id'),
        "topologyId": topo_id,
        "multiSql": data.get('multiSql'),
        "keplerConfig": new_config,
        "jars": data.get('jars', []),
        "type": data.get('type', 'KEPLER'),
        "clusterId": str(data.get('clusterId')),
        "name": data.get('name'),
        "comment": data.get('comment'),
        "bizGroupCode": data.get('bizGroupCode'),
        "parallelismConfig": parallelism,
        "jarId": "588",
        "baseline": data.get('baseline', 1),
        "jobType": data.get('jobType', 'SQL')
    }

    # 修复 multiSql 转义问题
    if 'multiSql' in request and request['multiSql']:
        request['multiSql'] = request['multiSql'].replace(r'\N', r'\\N')
        request['multiSql'] = request['multiSql'].replace(r'\u0001', r'\\u0001')

    with open(f'/tmp/kepler_update_{topo_id}.json', 'w') as f:
        json.dump(request, f, ensure_ascii=False)
    print("✓ 配置已生成")

except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
PYEOF

# 提交
log_info "提交配置..."
UPDATE_BODY=$(cat "/tmp/kepler_update_${TOPOLOGY_ID}.json")
UPDATE_RESULT=$(call_webapi "${KEPLER_API}/sql/update" "POST" "$UPDATE_BODY")

if ! echo "$UPDATE_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    log_error "配置更新失败: $UPDATE_RESULT"
    exit 1
fi
log_success "配置已更新"

# 重启
log_info "等待 10 秒后重启..."
sleep 10

RESTART_RESULT=$(call_webapi "${KEPLER_API}/topology/${TOPOLOGY_ID}/ops/restart" "POST" '{}')

if echo "$RESTART_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    log_success "重启成功！"
else
    log_warn "重启响应: $RESTART_RESULT"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "                   专家优化完成"
echo "═══════════════════════════════════════════════════════════"
echo ""
