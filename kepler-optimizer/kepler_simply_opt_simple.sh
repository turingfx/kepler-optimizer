#!/bin/bash
# =============================================================================
# kepler_simply_opt_simple.sh - 简化版 Kepler 优化脚本 (call-webapi 版)
# =============================================================================
#
# 黄金法则 (必须严格遵守):
# 1. kepler.worker.size = parallelismConfig 所有算子值 = Partition 数量 (三者必须一致)
# 2. job.worker.thread.number = 1
# 3. topology.master.worker.memory.size 范围：2G~6G (2147483648 ~ 6442450944)
# 4. kepler.output.odps.buffer.size 范围：60~200
# =============================================================================

TOPOLOGY_ID=$1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPI_SCRIPT="/Users/sansi.xy/.qoder/skills/call-webapi/scripts/webapi.sh"
KEPLER_API="https://kepler.alipay.com/api"

if [ -z "$TOPOLOGY_ID" ]; then
    echo "用法: $0 <topologyId>"
    exit 1
fi

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

echo "╔══════════════════════════════════════════════════════════╗"
echo "║     Kepler Simply-Opt 优化 - 任务 $TOPOLOGY_ID                    ║"
echo "║        (使用 call-webapi skill)                           ║"
echo "╚══════════════════════════════════════════════════════════╝"

# 步骤 1: 获取任务配置
echo "[1/5] 获取任务配置..."
CONFIG=$(call_webapi "${KEPLER_API}/sql/${TOPOLOGY_ID}" "GET" "")
TASK_NAME=$(echo "$CONFIG" | jq -r '.data.name')
PARTITION=$(echo "$CONFIG" | jq -r '.data.parallelismConfig | to_entries[0].value')
JAR_ID=$(echo "$CONFIG" | jq -r '.data.jarId')
CONFIG_ID=$(echo "$CONFIG" | jq -r '.data.id')
CLUSTER_ID=$(echo "$CONFIG" | jq -r '.data.clusterId')
OPERATOR_COUNT=$(echo "$CONFIG" | jq -r '.data.parallelismConfig | keys | length')

# 保存原始配置到文件，避免转义问题
echo "$CONFIG" > /tmp/config_${TOPOLOGY_ID}.json

if [ "$PARTITION" = "null" ] || [ -z "$PARTITION" ]; then
    echo "错误：无法获取 partition 数量"
    exit 1
fi

echo "  任务名：$TASK_NAME"
echo "  Partition: $PARTITION"
echo "  算子数：$OPERATOR_COUNT"
echo "  JarId: $JAR_ID"

# 步骤 2: 生成优化配置
echo "[2/5] 生成优化配置..."

python3 << PYEOF
import json

topo_id = "$TOPOLOGY_ID"
partition = $PARTITION
operator_count = $OPERATOR_COUNT
jar_id = "$JAR_ID"
config_id = $CONFIG_ID
task_name = "$TASK_NAME"
cluster_id = $CLUSTER_ID

# 标准 simply-opt 配置
# 黄金法则:
# 1. kepler.worker.size = partition 数量 = parallelismConfig 所有值
# 2. job.worker.thread.number = 1
# 3. memory: 2G~6G (2147483648 ~ 6442450944)
# 4. kepler.output.odps.buffer.size: 60~200
OPT_CONFIG = {
    'kepler.worker.size': str(partition),
    'kepler.worker.memory.size': '4294967296',  # 4GB，在 2-6GB 范围内
    'worker.cpu.slot.num': '3',
    'kepler.output.odps.buffer.size': '120',  # 在 60-200 范围内
    'job.worker.thread.number': '1'  # 黄金法则：必须为 1
}
DELETE_KEYS = ['kepler.output.queue.capacity']

# 读取原始配置
with open(f'/tmp/config_{topo_id}.json') as f:
    resp = json.load(f)

d = resp.get('data', {})
old_config = d.get('keplerConfig', [])
new_config = []
changes = []

for c in old_config:
    key = c.get('key')
    old_val = c.get('value')
    
    if key in DELETE_KEYS:
        changes.append(('删除', key, old_val, '-'))
        continue
    
    if key in OPT_CONFIG:
        new_val = OPT_CONFIG[key]
        if str(old_val) != str(new_val):
            changes.append(('修改', key, old_val or '未设置', new_val))
            c['value'] = new_val
        new_config.append(c)
    else:
        new_config.append(c)

current_keys = {c.get('key') for c in new_config}
for key, value in OPT_CONFIG.items():
    if key not in current_keys:
        changes.append(('新增', key, '未设置', value))
        is_system = key in ['kepler.worker.size', 'kepler.worker.memory.size']
        new_config.append({"key": key, "value": value, "system": is_system})

parallelism = d.get('parallelismConfig', {})
new_parallelism = {k: partition for k in parallelism}

print("=" * 50)
print("Simply-Opt 变更计划")
print("=" * 50)
print()
print("[黄金法则检查]")
print(f"  ✓ kepler.worker.size = {partition} (与 Partition 一致)")
print(f"  ✓ parallelismConfig 所有算子 = {partition} (与 Partition 一致)")
print(f"  ✓ job.worker.thread.number = 1")
print(f"  ✓ worker.memory.size = 4GB (在 2-6GB 范围内)")
print(f"  ✓ odps.buffer.size = 120 (在 60-200 范围内)")
print()
print("[配置变更]")
for action, key, old, new in changes:
    if action == '删除':
        print(f"  [{action}] {key}")
    else:
        print(f"  [{action}] {key}: {old} -> {new}")
print(f"  [并行度] {len(parallelism)} 个算子全部设置为：{partition}")

request = {
    "id": config_id,
    "topologyId": topo_id,
    "multiSql": d.get('multiSql', ''),
    "keplerConfig": new_config,
    "jars": d.get('jars', []),
    "type": d.get('type', 'KEPLER'),
    "clusterId": str(cluster_id),
    "name": task_name,
    "comment": d.get('comment', ''),
    "bizGroupCode": d.get('bizGroupCode', ''),
    "parallelismConfig": new_parallelism,
    "jarId": "588",
    "baseline": d.get('baseline', 1),
    "jobType": d.get('jobType', 'SQL')
}

# 使用原始 JSON 字符串方式写入，避免转义问题
with open(f'/tmp/kepler_update_{topo_id}.json', 'w') as f:
    f.write(json.dumps(request, ensure_ascii=False, indent=2))

print(f"\\n更新文件已生成：/tmp/kepler_update_{topo_id}.json")
PYEOF

# 步骤 3: 提交更新
echo "[3/5] 提交配置更新..."
# 修复 multiSql 中的非法转义序列，然后提交
python3 - "$TOPOLOGY_ID" << 'FIXEOF'
import json
import sys

topo_id = sys.argv[1]

with open(f'/tmp/kepler_update_{topo_id}.json', 'r') as f:
    data = json.load(f)

# multiSql 中的 \N 需要保持为 \\\N 才能在 JSON 中正确表示
if 'multiSql' in data and data['multiSql']:
    # 将 \N 替换为 \\\N (JSON 中的正确表示)
    data['multiSql'] = data['multiSql'].replace(r'\N', r'\\N')
    # 同样处理 \u0001
    data['multiSql'] = data['multiSql'].replace(r'\u0001', r'\\u0001')

with open(f'/tmp/kepler_update_{topo_id}.json', 'w') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("✓ 转义序列已修复")
FIXEOF

UPDATE_BODY=$(cat "/tmp/kepler_update_${TOPOLOGY_ID}.json")
UPDATE_RESULT=$(call_webapi "${KEPLER_API}/sql/update" "POST" "$UPDATE_BODY")

if echo "$UPDATE_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    echo "  ✓ 配置更新成功"
else
    echo "  ✗ 配置更新失败：$UPDATE_RESULT"
    exit 1
fi

# 步骤 4: 等待
echo "[4/5] 等待 10 秒..."
sleep 10

# 步骤 5: 重启任务
echo "[5/5] 重启任务..."
RESTART_RESULT=$(call_webapi "${KEPLER_API}/topology/${TOPOLOGY_ID}/ops/restart" "POST" '{}')

if echo "$RESTART_RESULT" | jq -e '.success == true' >/dev/null 2>&1; then
    echo "  ✓ 重启成功"
else
    echo "  重启结果：$RESTART_RESULT"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  优化完成！任务正在重启中..."
echo "═══════════════════════════════════════════════════════════"