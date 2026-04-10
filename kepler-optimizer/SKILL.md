# kepler-optimizer - Kepler 任务智能优化技能

## 技能描述

为蚂蚁集团 Kepler 流计算引擎提供智能诊断和优化能力。基于监控指标（JobOffset/FullGc/Write Records）自动分析任务延时原因，并应用标准化配置进行优化。

## 触发条件

当用户提到以下关键词时触发此技能：
- "Kepler 优化" / "kepler 调参"
- "任务延时" / "任务延迟" / "延时处理"
- "simply opt" / "normal opt" / "expert opt"
- "global view" / "全局查看"
- "内存调整" / "调大内存"
- 任务 ID 加上优化相关词汇

## 黄金法则

**所有优化操作必须严格遵守以下法则：**

1. **Worker 数与并发度必须一致**
   ```
   kepler.worker.size = parallelismConfig 所有算子值 = Partition 数量
   ```

2. **线程数固定为 1**
   ```
   job.worker.thread.number = 1
   ```

3. **内存范围限制**
   ```
   worker.memory.size: 2GB ~ 6GB (2147483648 ~ 6442450944)
   ```

4. **Buffer 大小范围**
   ```
   kepler.output.odps.buffer.size: 60 ~ 200
   ```

## 优化策略

### 1. Simply-Opt（标准优化）

**适用场景：** 快速应用标准配置，无需复杂分析

**配置标准：**
- `kepler.worker.size` = Partition 数量
- `kepler.worker.memory.size` = 4GB（可调整）
- `worker.cpu.slot.num` = 3
- `kepler.output.odps.buffer.size` = 120
- `job.worker.thread.number` = 1
- `parallelismConfig` 所有算子 = Partition 数量

**使用方式：**
```bash
./kepler_simply_opt_simple.sh <topologyId>
```

### 2. Normal-Opt（智能诊断优化）

**适用场景：** 需要基于监控指标进行智能决策

**诊断流程：**
1. 查询三大指标：JobOffset | FullGc | write_records
2. 分析 GC 严重程度
3. 智能内存决策：
   - 默认 4GB
   - FullGc 高且影响写入 → 5GB 或 6GB
   - 上限 6GB 封顶

**使用方式：**
```bash
./kepler_normal_opt.sh -t <topologyId> [-d duration_minutes]
```

### 3. Expert-Opt（专家级诊断）

**适用场景：** 复杂问题，需要日志分析和人工决策

**诊断流程：**
1. 获取任务指标
2. 查询 antlogs 异常日志
3. 错误识别：
   - NoSuchTable → 停止任务
   - OOM (heap space) → 内存 6GB + buffer 80
   - ODPS StatusConflict → 内存 5GB + buffer 80
   - MQ Timeout → 添加 timeout 参数
4. 人工确认执行方案

**使用方式：**
```bash
./kepler_expert_opt.sh -t <topologyId>
```

### 4. Global View（全局健康检查）

**适用场景：** 批量查看多个任务的健康状态

**输出内容：**
- JobOffset 趋势（判断是否在追赶）
- FullGc 情况（判断是否内存不足）
- Write Records（判断写入是否正常）
- 诊断结论和优化建议

**使用方式：**
```bash
./kepler_global_view_final.sh "<id1,id2,id3>" [duration_minutes]
```

## 脚本说明

### kepler_simply_opt_simple.sh

**功能：** 一键应用 Kepler 任务标准优化配置

**参数：**
- `<topologyId>` - 任务 ID（必需）

**执行流程：**
1. 获取任务配置（Partition 数量、算子数等）
2. 生成优化配置（遵守黄金法则）
3. 提交配置更新
4. 等待 10 秒
5. 重启任务

### kepler_normal_opt.sh

**功能：** 基于监控指标智能决策的调参方案

**参数：**
- `-t <topologyId>` - 任务 ID（必需）
- `-c <cookie 文件>` - Cookie 文件路径（可选）
- `-d <分钟数>` - 分析时长（可选，默认 60）
- `-n` - Dry-run 模式（可选）

**执行流程：**
1. 查询三大指标
2. 智能分析（GC 严重程度、写入影响）
3. 内存决策
4. 获取 Partition 数量
5. 提交配置更新并重启

### kepler_expert_opt.sh

**功能：** 全自动诊断 + 智能决策 + 日志分析

**参数：**
- `-t <topologyId>` - 任务 ID（必需）
- `-c <cookie 文件>` - Cookie 文件路径（可选）
- `-n` - Dry-run 模式（可选）

**执行流程：**
1. 获取任务指标
2. 查询 antlogs 异常日志
3. 错误识别和对策建议
4. 人工选择处理方案
5. 执行优化或停止

### kepler_global_view_final.sh

**功能：** 批量分析一批任务的健康状态

**参数：**
- `<id1,id2,id3>` - 逗号分隔的任务 ID 列表（必需）
- `[duration_minutes]` - 分析时长（可选，默认 60）

**输出分类：**
- `[正常]` - JobOffset 快速下降，无需干预
- `[内存不足]` - FullGc 高导致写入不稳定，需增大内存
- `[参数过小]` - 配置低写入追不上生产，需调参对齐 partition
- `[写入异常]` - 其他写入问题需人工排查

## API 依赖

### Kepler API
- `https://kepler.alipay.com/api/sql/{topologyId}` - 获取任务配置
- `https://kepler.alipay.com/api/sql/update` - 更新任务配置
- `https://kepler.alipay.com/api/topology/{topologyId}/ops/restart` - 重启任务
- `https://kepler.alipay.com/api/topology/{topologyId}/ops/stop` - 停止任务

### NCE 监控 API
- `https://nce-dashboard.alipay.com/api/datasources/proxy/5/api/query` - 查询监控指标

**监控指标：**
- `JobOffset` - 消费延迟（tags: topologyName, type）
- `FullGc` - Full GC 次数（tags: metaType=TOPOLOGY, topology）
- `write_records` - 写入记录数（tags: metaType=COMPONENT, topology）

## Cookie 管理

所有脚本依赖 `cookie.txt` 文件进行认证。

**Cookie 获取方式：**
1. 登录 Kepler 平台（https://kepler.alipay.com）
2. 从浏览器开发者工具复制 Cookie
3. 保存到脚本目录下的 `cookie.txt` 文件

**Cookie 更新：**
当 API 返回 "请求验证失败" 时，需要更新 Cookie。

## 使用示例

### 示例 1：单任务快速优化
```bash
# Simply-Opt 标准优化
./kepler_simply_opt_simple.sh 162804
```

### 示例 2：智能诊断优化
```bash
# Normal-Opt 智能诊断（分析最近 60 分钟指标）
./kepler_normal_opt.sh -t 126210

# Normal-Opt 智能诊断（分析最近 120 分钟指标）
./kepler_normal_opt.sh -t 126210 -d 120
```

### 示例 3：批量健康检查
```bash
# Global View 批量检查 6 个任务
./kepler_global_view_final.sh "162804,98328,126210,220003,216984,215152" 60
```

### 示例 4：调整内存
```bash
# 手动调整内存到 6GB（需修改脚本或使用 expert-opt）
# 或直接用 normal-opt 让系统根据 GC 情况智能决策
./kepler_normal_opt.sh -t 126210
```

### 示例 5：查询任务指标
```bash
# 查询单个任务的三大指标
./kepler_metrics_agent.sh 162804 60 table
```

## 输出说明

### 黄金法则检查
每次优化都会输出黄金法则检查清单：
```
[黄金法则检查]
  ✓ kepler.worker.size = partition
  ✓ parallelismConfig 所有算子 = partition
  ✓ job.worker.thread.number = 1
  ✓ worker.memory.size 在 2-6GB 范围内
  ✓ odps.buffer.size 在 60-200 范围内
```

### 优化完成输出
```
═══════════════════════════════════════════════════════════
  优化完成！任务正在重启中...
═══════════════════════════════════════════════════════════

建议后续监控:
  ./kepler_metrics_agent.sh <topologyId> -d 30
```

## 故障排查

### 问题：无法获取 partition 数据
**原因：** Cookie 过期或 topologyId 错误
**解决：** 更新 cookie.txt 或检查任务 ID

### 问题：请求验证失败
**原因：** Cookie 已过期
**解决：** 重新获取 Cookie 并更新 cookie.txt

### 问题：系统异常（类加载错误）
**原因：** Kepler API 临时问题
**解决：** 等待几分钟后重试

### 问题：JobOffset 持续上升
**原因：** 消费能力不足或源端数据量突增
**解决：**
1. 检查 FullGc 是否过高 → 增加内存
2. 检查 partition 是否过小 → 调整源端或使用 expert-opt
3. 检查是否有异常日志 → 使用 expert-opt 分析

## 相关文件

- `kepler_simply_opt_simple.sh` - Simply-Opt 脚本
- `kepler_normal_opt.sh` - Normal-Opt 脚本
- `kepler_expert_opt.sh` - Expert-Opt 脚本
- `kepler_global_view_final.sh` - Global View 脚本
- `kepler_metrics_agent.sh` - 指标查询脚本
- `kepler_golden_rules.md` - 黄金法则文档
- `cookie.txt` - 认证 Cookie（需要保护）

## 注意事项

1. **操作的是线上任务**，任何不符合流程的操作都是命令禁止的
2. 严格遵守**黄金法则**，禁止违反法则的配置
3. 优化后需要**监控 30-60 分钟**确认效果
4. 遇到问题**优先使用 normal-opt**进行智能诊断
5. 复杂问题使用**expert-opt**结合日志分析

---

*版本：v1.0 | 最后更新：2026-04-09*