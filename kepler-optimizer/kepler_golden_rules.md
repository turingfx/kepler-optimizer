# Kepler 优化黄金法则

**所有 Kepler 任务优化时必须严格遵守以下法则：**

## 法则 1：Worker 数与并发度必须一致
```
kepler.worker.size = parallelismConfig 所有算子值 = Partition 数量
```
三者必须完全一致！

## 法则 2：线程数固定为 1
```
job.worker.thread.number = 1
```

## 法则 3：内存范围限制
```
topology.master.worker.memory.size: 2GB ~ 6GB
即：2147483648 ~ 6442450944
```

## 法则 4：Buffer 大小范围
```
kepler.output.odps.buffer.size: 60 ~ 200
```

---

## Simply-Opt 标准配置
```json
{
  "kepler.worker.size": "<partition 数量>",
  "kepler.worker.memory.size": "4294967296",
  "worker.cpu.slot.num": "3",
  "kepler.output.odps.buffer.size": "120",
  "job.worker.thread.number": "1"
}
```

并行度设置：
```json
{
  "parallelismConfig": {
    "<operator-1>": <partition 数量>,
    "<operator-2>": <partition 数量>,
    ...
  }
}
```

---

## 检查清单

优化前必须验证：
- [ ] Partition 数量已确认
- [ ] kepler.worker.size = Partition 数量
- [ ] 所有 parallelismConfig 值 = Partition 数量
- [ ] job.worker.thread.number = 1
- [ ] 内存在 2-6GB 范围内
- [ ] buffer.size 在 60-200 范围内

**违反黄金法则的优化禁止执行！**