# Kepler 优化脚本快速参考

## 脚本总览

| 脚本 | 用途 | 复杂度 | 自动化程度 |
|------|------|--------|------------|
| `kepler_simply_opt_simple.sh` | 标准优化 | 低 | 全自动 |
| `kepler_normal_opt.sh` | 智能诊断优化 | 中 | 全自动 |
| `kepler_expert_opt.sh` | 专家级诊断 | 高 | 半自动 |
| `kepler_global_view_final.sh` | 批量健康检查 | 低 | 全自动 |
| `kepler_metrics_agent.sh` | 指标查询 | 低 | 全自动 |

---

## 1. kepler_simply_opt_simple.sh

**一句话：** 快速应用标准配置，无需分析

**适用场景：**
- 新任务初始配置
- 已知需要标准化配置的任务
- 快速批量优化

**用法：**
```bash
./kepler_simply_opt_simple.sh <topologyId>
```

**配置标准：**
```
kepler.worker.size = Partition 数量
kepler.worker.memory.size = 4GB
worker.cpu.slot.num = 3
kepler.output.odps.buffer.size = 120
job.worker.thread.number = 1
parallelismConfig.* = Partition 数量
```

---

## 2. kepler_normal_opt.sh

**一句话：** 基于监控指标智能决策

**适用场景：**
- 任务延时需要分析原因
- 根据 GC 情况智能调整内存
- 推荐的首选优化方式

**用法：**
```bash
./kepler_normal_opt.sh -t <topologyId> [-d 分钟数]
```

**诊断指标：**
- JobOffset（延时趋势）
- FullGc（GC 严重程度）
- write_records（写入状态）

**内存决策逻辑：**
```
GC 正常 → 4GB
GC 中等 → 5GB
GC 严重 → 6GB
```

---

## 3. kepler_expert_opt.sh

**一句话：** 完整诊断 + 日志分析 + 人工决策

**适用场景：**
- 复杂问题排查
- 反复优化的顽疾
- 需要查看异常日志

**用法：**
```bash
./kepler_expert_opt.sh -t <topologyId>
```

**错误类型识别：**
| 错误类型 | 对策 |
|----------|------|
| NoSuchTable | 停止任务（表已废弃） |
| OOM (heap space) | 内存 6GB + buffer 80 |
| ODPS StatusConflict | 内存 5GB + buffer 80 |
| MQ Timeout | 添加 timeout 参数 |

---

## 4. kepler_global_view_final.sh

**一句话：** 批量查看任务健康状态

**适用场景：**
- 每日运维巡检
- 批量任务健康检查
- 快速识别问题任务

**用法：**
```bash
./kepler_global_view_final.sh "<id1,id2,id3>" [分钟数]
```

**输出分类：**
- `[正常]` - JobOffset 下降，无需干预
- `[内存不足]` - FullGc 高，需增大内存
- `[参数过小]` - 配置低，需调参
- `[写入异常]` - 其他问题需排查

---

## 5. kepler_metrics_agent.sh

**一句话：** 查询任务三大核心指标

**适用场景：**
- 优化前后效果对比
- 实时监控任务状态
- 故障诊断数据支撑

**用法：**
```bash
./kepler_metrics_agent.sh <topologyId> [分钟数] [json|table|compact]
```

**输出指标：**
- JobOffset（延时）
- FullGc（GC）
- write_records（写入）

---

## 黄金法则（必须遵守）

```
1. kepler.worker.size = parallelismConfig.* = Partition 数量
2. job.worker.thread.number = 1
3. worker.memory.size: 2GB ~ 6GB
4. kepler.output.odps.buffer.size: 60 ~ 200
```

**违反法则的配置禁止应用！**

---

## 典型工作流

### 日常巡检
```bash
# 1. 批量检查任务健康状态
./kepler_global_view_final.sh "162804,98328,126210" 60

# 2. 对异常任务进行智能诊断优化
./kepler_normal_opt.sh -t 162804

# 3. 监控优化效果
./kepler_metrics_agent.sh 162804 30 table
```

### 处理延时任务
```bash
# 1. 查看指标
./kepler_metrics_agent.sh 126210 60 table

# 2. 智能诊断优化（首选）
./kepler_normal_opt.sh -t 126210

# 3. 如果问题持续，使用 expert-opt 深入分析
./kepler_expert_opt.sh -t 126210
```

### 快速标准化
```bash
# 一键应用标准配置
./kepler_simply_opt_simple.sh 60369
```

---

## 依赖配置

### Cookie 文件
所有脚本依赖 `cookie.txt` 进行 API 认证。

**位置：** 脚本同目录

**更新方式：**
1. 登录 https://kepler.alipay.com
2. 复制浏览器 Cookie
3. 覆盖 `cookie.txt` 文件

### 监控数据
依赖 NCE 监控系统，指标采集有 5-15 分钟延迟。

---

## 故障快速排查

| 现象 | 可能原因 | 解决方案 |
|------|----------|----------|
| "请求验证失败" | Cookie 过期 | 更新 cookie.txt |
| "无法获取 partition" | Cookie 过期或 ID 错误 | 检查 Cookie 和任务 ID |
| "系统异常（类加载）" | API 临时问题 | 等待 2 分钟后重试 |
| JobOffset 持续上升 | 消费能力不足 | normal-opt 或直接增加内存 |
| 优化后无改善 | 监控未同步 | 等待 15 分钟再观察 |

---

## 文件清单

```
skills/kepler-optimizer/
├── SKILL.md                          # 技能文档
├── kepler_simply_opt_simple.sh       # Simply-Opt
├── kepler_normal_opt.sh              # Normal-Opt
├── kepler_expert_opt.sh              # Expert-Opt
├── kepler_global_view_final.sh       # Global View
├── kepler_metrics_agent.sh           # 指标查询
├── kepler_golden_rules.md            # 黄金法则
└── cookie.txt                        # 认证 Cookie（需保护）
```

---

*快速参考 | 版本 v1.0 | 2026-04-09*