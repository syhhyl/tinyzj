# CHI-G 行为覆盖矩阵

基线：Arm IHI0050 G（Mar 2024）。此表区分已经验证的行为与待实现项；不是完整规范条目清单或合规认证。Python packet 模型不编码 RTL flit。

| 功能 | 当前实现与证据 | 未覆盖范围 |
| --- | --- | --- |
| TxnID/DBID/snoop ID | 独立编号、有界复用；`test_ids.py` | 全部 opcode 的 ID 规则审计 |
| CompAck | 一致性读按 HomeNID/DBID 确认；`test_ids.py` | 写 ExpCompAck、全部合法字段组合 |
| 写完成 | 分离响应双顺序、CompDBIDResp；`test_completion.py` | DBIDRespOrd、TagMatch、DWT |
| Retry | RN→HF 类型 0、提前 Grant、资源预留；`test_retry.py` | HF→S retry、多类型、PCrdReturn；已授信请求仍可能地址背压 |
| 网络信用 | packet 级每方向/通道 credit 守恒；`test_credits.py` | 链路激活、LCRDV/FLITPEND 信号 |
| 一致性读写 | ReadShared/ReadUnique/WriteUniqueFull；`test_zj.py` | 全部 CHI cache 状态及 snoop 组合 |
| dirty/copyback | UD、带数据 snoop、WriteBackFull、Home 驱逐；`test_dirty.py` | SD/UDP、WriteClean、自动容量替换、全部竞态 |
| 非一致性部分写 | 对齐 64-byte Normal memory 窗口及 BE；`test_data.py` | 子行 Size、Device memory、WriteUniquePtl |
| 多包 DAT | 64-byte 行、128/256/512-bit 宽度、DataID 重排收齐；`test_data.py` | 任意子行窗口、DataSepResp/RespSepData、压缩 |
| 错误 | RespErr、Poison→DERR、奇字节校验；`test_data.py` | 持久 Poison 存储、所有 snoop/error 状态组合 |
| Atomic | 自然对齐 AtomicSwap/Compare、八种 AtomicLoad/Store；1/2/4/8-byte，Compare 另支持16-byte；HF 行锁、signed/unsigned、并发 fetch-add、提前 CompData；`test_atomic.py` | 子行线上字节位置、Endian 字段 |
| Exclusive | 未实现 | monitor、成功/失败与失效关联 |
| Cache maintenance | 未实现 | Clean/Invalidate/同步完成与持久化 |
| DVM | 未实现 | 广播、同步与完成序列 |
| 系统验证 | 确定性混合压力和 byte-line scoreboard | 完整协议断言、所有必选条目审计 |

## 后续依赖

1. 统一整数地址、64-byte line key 与 Size 窗口，明确教学字符串地址的独立模式。
2. 为 Atomic/Exclusive 增加返回数据与写数据提交的联合完成记录；现有 read/write 分支不能直接复用作为完整原子语义。
3. 在相同地址锁内实现原子读改写及 snoop，加入双 RN 冲突验证。
4. 补维护/DVM 的拓扑能力配置和事务状态机，再完成规范逐项适用性审计。

任何未实现项在验收前保持“未覆盖”，不能仅因当前四节点示例没有使用就标为“不适用”。
