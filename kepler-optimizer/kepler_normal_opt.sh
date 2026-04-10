#!/bin/bash
# =============================================================================
# kepler_normal_opt.sh - Kepler 智能诊断优化脚本 (call-webapi 版)
# =============================================================================
# 功能：基于监控指标智能决策的调参方案
#
# 黄金法则 (必须严格遵守):
# 1. kepler.worker.size = parallelismConfig 所有值 = Partition 数量 (三者必须一致)
# 2. job.worker.thread.number = 1
# 3. worker.memory.size: 2GB ~ 6GB (2147483648 ~ 6442450944)
# 4. kepler.output.odps.buffer.size: 60 ~ 200

# 策略:
# 1. 查询三大指标: JobOffset | FullGc | write_records
# 2. 无脑调整: worker.size = partition, cpu.slot = 3, buffer = 120
# 3. 智能内存决策:
#    - 默认 4GB
#    - 如果 FullGc 高且影响写入 -> +1GB
#    - 上限 6GB 封顶
#
# 使用方法:
# ./kepler_normal_opt.sh -t 168838
# ./kepler_normal_opt.sh -t 168838 -c /path/cookie
# COOKIE="xxx" ./kepler_normal_opt.sh -t 168838
#
# 参数:
# -t : topologyId (作业 ID)
# -c : cookie 文件路径
# -n : dry-run 模式
# -h : 显示帮助
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
NC='\033[0m'

# 默认值
DRY_RUN=false
DURATION_MIN=60  # 查询最近60分钟数据

# 内存配置规则
DEFAULT_MEMORY=4294967296      # 4GB
MEMORY_INCREMENT=1073741824    # 1GB
MAX_MEMORY=6442450944          # 6GB 封顶

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
║ Kepler Normal-Opt 智能诊断优化脚本                       ║
╚══════════════════════════════════════════════════════════╝

用法:
  $0 -t <topologyId> [选项]

智能策略:
  1. 查询三大指标 (JobOffset/FullGc/write_records)
  2. 无脑调整: worker.size=partition, cpu.slot=3, buffer=120
  3. 智能内存:
     - 默认 4GB
     - FullGc高且写入受影响 → 5GB
     - 上限 6GB 封顶

必需参数:
  -t topologyId (作业 ID)

可选参数:
  -d 查询时长(分钟) (默认: 60)
  -n dry-run 模式，仅分析不执行
  -h 显示帮助

示例:
  $0 -t 168838                    # 正常执行
  $0 -t 168838 -n                 # 仅分析预览
  $0 -t 168838 -d 120             # 分析最近2小时数据

EOF
    exit 1
}

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[⚠]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_dry() { echo -e "${CYAN}[DRY-RUN]${NC} $1"; }
log_analysis() { echo -e "${CYAN}[分析]${NC} $1"; }

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

# 参数校验
if [ -z "$TOPOLOGY_ID" ]; then
    log_error "必须提供 -t topologyId"
    usage
fi

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║ Kepler Normal-Opt 智能诊断优化                            ║"
echo "║        (使用 call-webapi skill)                           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
log_info "任务: $TOPOLOGY_ID"
log_info "分析时长: ${DURATION_MIN}分钟"
[ "$DRY_RUN" = true ] && log_dry "dry-run 模式"
echo ""

START_TIME=$(( ($(date +%s) - DURATION_MIN * 60) * 1000 ))

# =============================================================================
# 阶段1: 诊断分析 - 查询三大指标
# =============================================================================

echo "═══════════════════════════════════════════════════════════"
echo "                     阶段1: 诊断分析                         "
echo "═══════════════════════════════════════════════════════════"
echo ""

# 查询单个指标
query_metric() {
    local metric=$1
    local tags=$2

    local body="{\"start\":$START_TIME,\"queries\":[{\"metric\":\"$metric\",\"aggregator\":\"sum\",\"downsample\":\"1m-avg\",\"tags\":$tags}],\"msResolution\":false}"

    call_webapi "$NCE_API" "POST" "$body"
}

# 分析指标数据
analyze_metric() {
    local metric=$1
    local data=$2
    
    echo "$data" | jq -c --arg metric "$metric" '
        if type == "array" and length > 0 then
            .[0] |
            if .dps and (.dps | keys | length) > 0 then
                .dps | to_entries |
                map(.value) as $values |
                {
                    metric: $metric,
                    count: ($values | length),
                    latest: $values[-1],
                    first: $values[0],
                    min: ($values | min),
                    max: ($values | max),
                    avg: (($values | add) / ($values | length)),
                    trend: (if $values[-1] < $values[0] then "decreasing" elif $values[-1] > $values[0] then "increasing" else "stable" end),
                    zeros: ([$values[] | select(. == 0)] | length)
                }
            else
                {"metric":$metric,"error":"no_data"}
            end
        else
            {"metric":$metric,"error":"invalid_response"}
        end'
}

# 查询 JobOffset
echo "查询指标 1/3: JobOffset..."
joboffset_data=$(query_metric "JobOffset" "{\"topologyName\":\"$TOPOLOGY_ID\",\"type\":\"*\"}")
joboffset=$(analyze_metric "JobOffset" "$joboffset_data")

# 查询 FullGc
echo "查询指标 2/3: FullGc..."
fullgc_data=$(query_metric "FullGc" "{\"metaType\":\"TOPOLOGY\",\"topology\":\"$TOPOLOGY_ID\"}")
fullgc=$(analyze_metric "FullGc" "$fullgc_data")

# 查询 write_records
echo "查询指标 3/3: write_records..."
writerecords_data=$(query_metric "write_records" "{\"metaType\":\"COMPONENT\",\"topology\":\"$TOPOLOGY_ID\"}")
writerecords=$(analyze_metric "write_records" "$writerecords_data")

echo ""

# =============================================================================
# 阶段2: 智能分析
# =============================================================================

echo "═══════════════════════════════════════════════════════════"
echo "                     阶段2: 智能分析                         "
echo "═══════════════════════════════════════════════════════════"
echo ""

# 提取关键指标
if [ -n "$joboffset" ] && ! echo "$joboffset" | jq -e '.error' >/dev/null 2>&1; then
    joboffset_latest=$(echo "$joboffset" | jq -r '.latest // 0 | floor')
    joboffset_trend=$(echo "$joboffset" | jq -r '.trend // "unknown"')
    joboffset_avg=$(echo "$joboffset" | jq -r '.avg // 0 | floor')
else
    joboffset_latest="N/A"
    joboffset_trend="unknown"
fi

if [ -n "$fullgc" ] && ! echo "$fullgc" | jq -e '.error' >/dev/null 2>&1; then
    fullgc_max=$(echo "$fullgc" | jq -r '.max // 0')
    fullgc_total=$(echo "$fullgc" | jq -r '(.first // 0) + (.latest // 0)')
    fullgc_count=$(echo "$fullgc" | jq -r '.count // 0')
    # 计算平均GC频率
    if [ "$fullgc_count" -gt 0 ] 2>/dev/null; then
        fullgc_avg=$(echo "$fullgc" | jq -r '.avg // 0 | floor')
    else
        fullgc_avg=0
    fi
else
    fullgc_max=0
    fullgc_avg=0
fi

if [ -n "$writerecords" ] && ! echo "$writerecords" | jq -e '.error' >/dev/null 2>&1; then
    write_latest=$(echo "$writerecords" | jq -r '.latest // 0 | floor')
    write_trend=$(echo "$writerecords" | jq -r '.trend // "unknown"')
    write_zeros=$(echo "$writerecords" | jq -r '.zeros // 0')
    write_count=$(echo "$writerecords" | jq -r '.count // 0')
else
    write_latest=0
    write_trend="unknown"
    write_zeros=0
    write_count=0
fi

# 分析 GC 是否严重
# 标准: 最近60分钟内 max FullGc > 50 或 avg > 10 视为严重
GC_SEVERITY="normal"
if [ "$fullgc_max" -gt 100 ] 2>/dev/null; then
    GC_SEVERITY="critical"
elif [ "$fullgc_max" -gt 50 ] 2>/dev/null || [ "$fullgc_avg" -gt 10 ] 2>/dev/null; then
    GC_SEVERITY="high"
fi

# 分析写入是否受影响
# 标准: 如果 GC 高且写入为0或下降趋势
WRITE_AFFECTED=false
if [ "$GC_SEVERITY" != "normal" ]; then
    if [ "$write_zeros" -gt 0 ] 2>/dev/null || [ "$write_trend" = "decreasing" ]; then
        WRITE_AFFECTED=true
    fi
fi

# 额外规则: 持续低GC (avg>0，即使值很小) + 写入跌0 → 同样视为内存不足
# 场景: GC 虽小但持续存在，最终导致写入跌0，本质仍是内存压力
if [ "$GC_SEVERITY" = "normal" ] && [ "$write_zeros" -gt 0 ] 2>/dev/null; then
    if awk "BEGIN{exit !($fullgc_avg > 0)}" 2>/dev/null; then
        GC_SEVERITY="high"
        WRITE_AFFECTED=true
        log_warn "持续低GC (avg=${fullgc_avg}) 导致写入跌0 (${write_zeros}次)，视为内存不足"
    fi
fi

# 输出诊断结果
echo "┌─────────────────────────────────────────────────────────┐"
echo "│ 诊断结果                                                  │"
echo "├─────────────────────────────────────────────────────────┤"
printf "│ JobOffset 延迟: %-10s (趋势: %-10s)      │\n" "$joboffset_latest" "$joboffset_trend"
printf "│ FullGc 峰值: %-10s (平均: %-10s)          │\n" "$fullgc_max" "$fullgc_avg"
printf "│ GC 严重程度: %-10s                               │\n" "$GC_SEVERITY"
printf "│ 写入量最新: %-10s (趋势: %-10s)           │\n" "$write_latest" "$write_trend"
printf "│ 写入受影响: %-10s                                 │\n" "$([ "$WRITE_AFFECTED" = true ] && echo "是 ⚠️" || echo "否 ✓")"
echo "└─────────────────────────────────────────────────────────┘"
echo ""

# =============================================================================
# 阶段3: 内存决策
# =============================================================================

echo "═══════════════════════════════════════════════════════════"
echo "                     阶段3: 内存决策                         "
echo "═══════════════════════════════════════════════════════════"
echo ""

# 决策逻辑
if [ "$GC_SEVERITY" = "critical" ] && [ "$WRITE_AFFECTED" = true ]; then
    # 严重GC + 写入受影响 -> 大幅增加内存
    TARGET_MEMORY=$((DEFAULT_MEMORY + 2 * MEMORY_INCREMENT))
    log_warn "FullGc 严重 (max=$fullgc_max) 且写入受影响"
    log_analysis "内存策略: 4GB → 6GB (增加 2GB)"
elif [ "$GC_SEVERITY" != "normal" ] && [ "$WRITE_AFFECTED" = true ]; then
    # GC高 + 写入受影响 -> 增加1GB
    TARGET_MEMORY=$((DEFAULT_MEMORY + MEMORY_INCREMENT))
    log_warn "FullGc 偏高 (max=$fullgc_max) 且写入受影响"
    log_analysis "内存策略: 4GB → 5GB (增加 1GB)"
else
    # 正常情况或不影响写入 -> 保持4GB
    TARGET_MEMORY=$DEFAULT_MEMORY
    log_success "GC状态良好 或 写入未受影响"
    log_analysis "内存策略: 保持 4GB"
fi

# 确保不超过上限
if [ "$TARGET_MEMORY" -gt "$MAX_MEMORY" ] 2>/dev/null; then
    TARGET_MEMORY=$MAX_MEMORY
    log_warn "内存达到上限 6GB 封顶"
fi

# 转换为GB显示
TARGET_GB=$((TARGET_MEMORY / 1073741824))
echo ""
echo "决策结果: ${TARGET_GB}GB"
echo ""

# =============================================================================
# 阶段4: 获取 partition 并准备配置
# =============================================================================

echo "═══════════════════════════════════════════════════════════"
echo "                     阶段4: 配置准备                         "
echo "═══════════════════════════════════════════════════════════"
echo ""

# 获取 partition 数量
log_info "查询 partition 数量..."
PARTITION_COUNT=$(call_webapi "${ANTC_API}/kepler/engine/${TOPOLOGY_ID}/offset" "GET" "" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo "0")

if [ "$PARTITION_COUNT" = "0" ] || [ -z "$PARTITION_COUNT" ]; then
    log_error "无法获取 partition 数据"
    exit 1
fi

log_success "Partition 数量: $PARTITION_COUNT"
echo ""

# 完整配置计划
echo "┌─────────────────────────────────────────────────────────┐"
echo "│ Normal-Opt 配置计划                                        │"
echo "├─────────────────────────────────────────────────────────┤"
echo ""
echo "[黄金法则检查]"
printf "│ %-50s │\n" "✓ kepler.worker.size = partition"
printf "│ %-50s │\n" "✓ parallelismConfig 所有算子 = partition"
printf "│ %-50s │\n" "✓ job.worker.thread.number = 1"
printf "│ %-50s │\n" "✓ worker.memory.size 在 2-6GB 范围内"
printf "│ %-50s │\n" "✓ odps.buffer.size 在 60-200 范围内"
echo ""
printf "│ %-50s │\n" "kepler.worker.size = $PARTITION_COUNT (无脑对齐)"
printf "│ %-50s │\n" "kepler.worker.memory.size = ${TARGET_GB}GB (智能决策)"
printf "│ %-50s │\n" "worker.cpu.slot.num = 3 (无脑设置)"
printf "│ %-50s │\n" "kepler.output.odps.buffer.size = 120 (无脑设置)"
printf "│ %-50s │\n" "parallelismConfig.* = $PARTITION_COUNT (无脑对齐)"
printf "│ %-50s │\n" "删除 kepler.output.queue.capacity"
echo "└─────────────────────────────────────────────────────────┘"
echo ""

# dry-run 模式直接退出
if [ "$DRY_RUN" = true ]; then
    log_dry "dry-run 模式，跳过执行"
    exit 0
fi

# =============================================================================
# 阶段5: 执行调参
# =============================================================================

echo "═══════════════════════════════════════════════════════════"
echo "                     阶段5: 执行调参                         "
echo "═══════════════════════════════════════════════════════════"
echo ""

# 获取当前配置
log_info "获取当前配置..."
CFG_FILE="/tmp/kepler_cfg_${TOPOLOGY_ID}.json"

CONFIG_RESULT=$(call_webapi "${KEPLER_API}/sql/${TOPOLOGY_ID}" "GET" "")
echo "$CONFIG_RESULT" > "$CFG_FILE"

if ! echo "$CONFIG_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    log_error "无法获取配置"
    exit 1
fi

log_success "配置已获取"
echo ""

# 生成更新配置
log_info "生成 normal-opt 配置..."

python3 - "${TOPOLOGY_ID}" "${PARTITION_COUNT}" "${TARGET_MEMORY}" "${CFG_FILE}" << 'PYEOF'
import json
import sys

topo_id = sys.argv[1]
partition_count = int(sys.argv[2])
target_memory = int(sys.argv[3])
cfg_file = sys.argv[4]

try:
    with open(cfg_file) as f:
        resp = json.load(f)
    
    if not resp.get('success'):
        print(f"ERROR: API返回失败")
        sys.exit(1)
    
    d = resp.get('data', {})
    if not d or not d.get('id'):
        print("ERROR: 无法获取配置数据")
        sys.exit(1)
    
    # Normal-opt 配置
    OPT_CONFIG = {
        'kepler.worker.size': str(partition_count),
        'kepler.worker.memory.size': str(target_memory),
        'worker.cpu.slot.num': '3',
        'kepler.output.odps.buffer.size': '120',
        'job.worker.thread.number': '1'
    }
    DELETE_KEYS = ['kepler.output.queue.capacity']
    
    # 处理配置
    new_config = []
    for c in d.get('keplerConfig', []):
        key = c.get('key')
        if key in DELETE_KEYS:
            continue
        if key in OPT_CONFIG:
            c['value'] = OPT_CONFIG[key]
        new_config.append(c)
    
    # 添加缺失配置
    current_keys = {c.get('key') for c in new_config}
    for key, value in OPT_CONFIG.items():
        if key not in current_keys:
            is_system = key in ['kepler.worker.size', 'kepler.worker.memory.size']
            new_config.append({"key": key, "value": value, "system": is_system})
    
    # 更新 parallelismConfig
    parallelism = d.get('parallelismConfig', {})
    new_parallelism = {k: partition_count for k in parallelism}
    
    # 构建请求
    request = {
        "id": d.get('id'),
        "topologyId": topo_id,
        "multiSql": d.get('multiSql'),
        "keplerConfig": new_config,
        "jars": d.get('jars', []),
        "type": d.get('type', 'KEPLER'),
        "clusterId": str(d.get('clusterId')),
        "name": d.get('name'),
        "comment": d.get('comment'),
        "bizGroupCode": d.get('bizGroupCode'),
        "parallelismConfig": new_parallelism,
        "jarId": "588",
        "baseline": d.get('baseline', 1),
        "jobType": d.get('jobType', 'SQL')
    }
    
    # 修复 multiSql 转义问题
    if 'multiSql' in request and request['multiSql']:
        request['multiSql'] = request['multiSql'].replace(r'\N', r'\\N')
        request['multiSql'] = request['multiSql'].replace(r'\u0001', r'\\u0001')

    with open(f'/tmp/kepler_update_{topo_id}.json', 'w') as f:
        json.dump(request, f, ensure_ascii=False)

    print("✓ 更新请求已生成")

except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
PYEOF

if [ $? -ne 0 ]; then
    log_error "配置生成失败"
    exit 1
fi

echo ""

# 提交更新
log_info "提交配置更新..."
UPDATE_BODY=$(cat "/tmp/kepler_update_${TOPOLOGY_ID}.json")
UPDATE_RESULT=$(call_webapi "${KEPLER_API}/sql/update" "POST" "$UPDATE_BODY")

if ! echo "$UPDATE_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    log_error "配置更新失败"
    log_error "响应: $UPDATE_RESULT"
    exit 1
fi

log_success "配置更新成功"
echo ""

# 等待
log_info "等待 10 秒..."
sleep 10

# 重启
log_info "重启任务..."
RESTART_RESULT=$(call_webapi "${KEPLER_API}/topology/${TOPOLOGY_ID}/ops/restart" "POST" '{}')

if echo "$RESTART_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    log_success "重启成功！"
elif echo "$RESTART_RESULT" | grep -q "RESTARTING"; then
    log_warn "任务已在重启中"
else
    log_warn "重启响应异常"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "                     优化完成                               "
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "建议后续监控:"
echo "  ./kepler_metrics_agent.sh ${TOPOLOGY_ID} -d 30"
echo ""
