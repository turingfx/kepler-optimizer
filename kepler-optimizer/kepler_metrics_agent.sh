#!/bin/bash
# Kepler Metrics Query Agent
# 统一查询三个核心指标: JobOffset | FullGc | write_records
# 用法: ./kepler_metrics_agent.sh <topology> [duration_minutes] [output_format]

set -e

TOPOLOGY="${1:-}"
DURATION="${2:-60}"
FORMAT="${3:-json}"

if [ -z "$TOPOLOGY" ]; then
    echo '{"error": "topology is required"}' >&2
    echo 'Usage: ./kepler_metrics_agent.sh <topology> [duration_minutes] [json|table|compact]' >&2
    exit 1
fi

NCE_API="https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query"
START_TIME=$(( ($(date +%s) - DURATION * 60) * 1000 ))

# 查询并分析指标
query_metric() {
    local metric=$1
    local tag_key=$2
    local tag_val=$3
    local extra_tags=$4

    local tags
    if [ -n "$extra_tags" ]; then
        tags="{$extra_tags}"
    else
        tags="{\"$tag_key\":\"$tag_val\",\"topology\":\"$TOPOLOGY\"}"
    fi

    local body="{\"start\":$START_TIME,\"queries\":[{\"metric\":\"$metric\",\"aggregator\":\"sum\",\"downsample\":\"1m-avg\",\"tags\":$tags}],\"msResolution\":false}"

    local raw_data
    raw_data=$(curl -s -X POST "$NCE_API" \
        -H "Content-Type: application/json" \
        --connect-timeout 10 \
        -d "$body" 2>/dev/null)

    if [ -z "$raw_data" ] || [ "$raw_data" = "[]" ] || [ "$raw_data" = "null" ]; then
        echo "{\"metric\":\"$metric\",\"error\":\"no_data\"}"
        return
    fi

    echo "$raw_data" | jq -c --arg metric "$metric" '
        if type == "array" and length > 0 then
            .[0] |
            if .dps and (.dps | keys | length) > 0 then
                .dps | to_entries |
                map(.value) as $values |
                {
                    metric: $metric,
                    topology: "'"$TOPOLOGY"'",
                    duration: '"$DURATION"',
                    sample_count: ($values | length),
                    latest: $values[-1],
                    first: $values[0],
                    min: ($values | min),
                    max: ($values | max),
                    avg: (($values | add) / ($values | length)),
                    trend: (if $values[-1] < $values[0] then "decreasing" elif $values[-1] > $values[0] then "increasing" else "stable" end),
                    zeros: ([$values[] | select(. == 0)] | length),
                    timestamp: now | todate
                }
            else
                {"metric":$metric,"error":"no_dps_data"}
            end
        else
            {"metric":$metric,"error":"invalid_response"}
        end'
}

# 查询三个指标
joboffset=$(query_metric "JobOffset" "topologyName" "$TOPOLOGY" "\"topologyName\":\"$TOPOLOGY\",\"type\":\"*\"")
fullgc=$(query_metric "FullGc" "metaType" "TOPOLOGY" "\"metaType\":\"TOPOLOGY\",\"topology\":\"$TOPOLOGY\"")
writerecords=$(query_metric "write_records" "metaType" "COMPONENT" "\"metaType\":\"COMPONENT\",\"topology\":\"$TOPOLOGY\"")

# 合并结果
results=$(jq -cs '.' <<< "$joboffset$fullgc$writerecords")

# 输出结果
case "$FORMAT" in
    json)
        echo "$results" | jq '{
            topology: .[0].topology,
            timestamp: .[0].timestamp,
            duration_minutes: .[0].duration,
            metrics: map(if .error then {metric,error} else del(.timestamp) end)
        }'
        ;;
    compact)
        for r in "$joboffset" "$fullgc" "$writerecords"; do
            if [ -n "$r" ]; then
                jq -r '[.metric, .latest // .error] | @tsv' <<< "$r"
            fi
        done
        ;;
    table)
        echo "========================================"
        echo "Topology: $TOPOLOGY"
        echo "Duration: ${DURATION}min"
        echo "========================================"
        printf "%-15s | %-12s | %-12s | %-10s\n" "METRIC" "LATEST" "AVG" "TREND"
        printf "%-15s-+-%-12s-+-%-12s-+-%-10s\n" "---------------" "------------" "------------" "----------"
        for r in "$joboffset" "$fullgc" "$writerecords"; do
            if [ -n "$r" ]; then
                if jq -e '.error' <<< "$r" >/dev/null 2>&1; then
                    printf "%-15s | %-12s | %-12s | %-10s\n" \
                        "$(jq -r '.metric' <<< "$r")" "N/A" "N/A" "$(jq -r '.error' <<< "$r")"
                else
                    printf "%-15s | %12s | %12s | %-10s\n" \
                        "$(jq -r '.metric' <<< "$r")" \
                        "$(jq -r '.latest | floor' <<< "$r")" \
                        "$(jq -r '.avg | floor' <<< "$r")" \
                        "$(jq -r '.trend' <<< "$r")"
                fi
            fi
        done
        ;;
    *)
        echo '{"error":"invalid_format"}'
        exit 1
        ;;
esac
