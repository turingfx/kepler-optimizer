# AGENTS.md

This file provides guidance to Qoder (qoder.com) when working with code in this repository.

## Overview

Kepler task optimization toolkit for Ant Group's Kepler stream computing engine. Provides intelligent diagnostics and optimization based on monitoring metrics (JobOffset/FullGc/write_records).

## Key Commands

```bash
# Query metrics for a task (30-60 minutes recommended for post-optimization monitoring)
./kepler_metrics_agent.sh <topologyId> [duration_min] [json|table|compact]

# Standard optimization (apply standard config without analysis)
./kepler_simply_opt_simple.sh <topologyId>

# Smart diagnostic optimization (recommended first choice)
./kepler_normal_opt.sh -t <topologyId> [-d duration_min] [-n dry-run]

# Expert-level diagnostics with log analysis
./kepler_expert_opt.sh -t <topologyId>

# Batch health check for multiple tasks
./kepler_global_view_final.sh "<id1,id2,id3>" [duration_min]
```

## Golden Rules (MUST follow strictly)

All optimization operations MUST comply with these rules. Any configuration violating these is forbidden:

1. **Worker count = Parallelism = Partition count** (三者必须一致)
   ```
   kepler.worker.size = parallelismConfig.* = Partition 数量
   ```

2. **Thread count fixed to 1**
   ```
   job.worker.thread.number = 1
   ```

3. **Memory range: 2GB ~ 6GB** (2147483648 ~ 6442450944 bytes)

4. **Buffer size range: 60 ~ 200**

5. **jarId must be 588 on update**
   ```
   jarId = "588"  (always override when submitting config update)
   ```

## Architecture

### Script Flow
- `kepler_metrics_agent.sh`: Base metrics query layer, used by other scripts
- `kepler_simply_opt_simple.sh`: Direct config application → restart
- `kepler_normal_opt.sh`: Query metrics → analyze GC severity → smart memory decision → apply config → restart
- `kepler_expert_opt.sh`: Normal-Opt + antlogs error analysis + human decision
- `kepler_global_view_final.sh`: Batch metrics query → classify health status for each task

### API Dependencies
- **Kepler API** (`https://kepler.alipay.com/api`): Task config, update, restart/stop operations
- **NCE Monitoring API** (`https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query`): Metrics queries

### Core Metrics
| Metric | Meaning | Tags |
|--------|---------|------|
| JobOffset | Consumption latency | topologyName, type |
| FullGc | Full GC count | metaType=TOPOLOGY, topology |
| write_records | Write record count | metaType=COMPONENT, topology |

## Cookie Authentication

All scripts require `cookie.txt` in the script directory. When API returns "请求验证失败", update cookie:
1. Login to https://kepler.alipay.com
2. Copy cookie from browser dev tools
3. Overwrite `cookie.txt`

## Memory Decision Logic (Normal-Opt)

```
GC normal → 4GB (default)
GC moderate + write affected → 5GB
GC severe + write affected → 6GB (max)
```

## Error Type Handling (Expert-Opt)

| Error Type | Action |
|------------|--------|
| NoSuchTable | Stop task (table deprecated) |
| OOM (heap space) | Memory 6GB + buffer 80 |
| ODPS StatusConflict | Memory 5GB + buffer 80 |
| MQ Timeout | Add timeout parameter |

## Monitoring Data Delay

NCE monitoring has 5-15 minute delay. After optimization, wait 15 minutes before re-checking metrics.
