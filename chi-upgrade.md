# CHI 升级实施计划

## 目标与边界

升级 Python 行为模型，保留 2×2 Ring 与现有演示接口。以 CHI-G 为目标基线；实施具体功能前必须核对正式规范对应章节，记录字段、合法组合和顺序规则。当前没有完成这项规范核对，不能声称完整 CHI-G 合规。Chisel 移植单独立项。

“完整”以固定版本的覆盖矩阵为准：必选功能全部覆盖，可选功能明确支持或不适用，不能用增加 opcode 数量代替协议实现。每轮只完成一个可验证增量。

## 当前基线

- 四通道 REQ/RSP/DAT/SNP，最短路径、有限环缓冲、气泡注入和端点背压。
- ReadNoSnp、ReadShared、ReadUnique、WriteNoSnpFull；缓存 I/SC/UC、目录、无数据 snoop、CompAck。
- RN TxnID 独立；HF 内部 home_id 同时用于 DBID、下游 TxnID 和 snoop ID；SN DBID 独立。
- 当前 write API 是“写穿并失效”的教学行为，不能作为标准 WriteNoSnp 的完整语义。
- 单 payload，未建模多 beat、脏数据、链路 credit 或协议 retry。
- 基线验收：76 个测试，包括确定性并发压力与参考内存验证。

## 阶段与验收

| 阶段 | 修改内容 | 必须通过的验收 | 依赖 |
| --- | --- | --- | --- |
| A1 | 分离 HF 下游 TxnID 与内部 home_id；显式映射 | 不同编号起点、普通/一致性读、写、snoop 后续读、错误收尾均正确；映射最终清空 | 当前基线 |
| A2 | HF 写 DBID、snoop ID 独立分配；统一事务记录 | 相同数值跨空间不串事务，双 RN 同 TxnID 隔离，映射均清理 | A1 |
| A3 | 有界 ID 池、回收复用、资源耗尽背压 | 极小 ID 池反复复用；不覆盖活跃事务，无死锁；响应存档不误匹配新请求 | A2 |
| B | 核对 CompAck/ExpCompAck、DBID 与完成响应关联，显式状态机 | 正常/错误/重排响应完成条件与正式规范一致；请求完成与资源回收分开验证 | A |
| C | 链路 credit 与事务资源分离；RetryAck/PCrdGrant | 合法 retry/grant/reissue 顺序，有限信用无溢出，公平性与进展检查 | B |
| D | 明确标准非一致性写与一致性写 API；实现 dirty、带数据 snoop、回写/驱逐 | 最新数据唯一可追踪，唯一写者，脏数据不丢失，读写/驱逐/snoop 竞态 | B、C |
| E | 地址对齐、size、byte enable、DataID、多 beat、响应组合 | 部分写正确，多 beat 重排与收齐规则正确，错误/poison 传播 | B、D |
| F | 按覆盖矩阵实现原子、exclusive、维护、DVM 等适用功能 | 每个支持特性有规范条目、状态转移、正反向测试及竞态覆盖 | D、E |
| G | 系统验收与覆盖审计 | 独立 scoreboard、协议断言、可重放压力测试；所有目标条目有证据 | A–F |

## 实施规则

1. 先固定本增量验收测试，再改实现；失败必须定位，不靠放宽断言通过。
2. Ring 不解析 opcode、TxnID 或 DBID；协议映射由端点负责。
3. 身份转换使用映射，不修改已接收 Message 的字段；保留可追踪历史。
4. A1/A2 不改变网络时序：孤立普通读 10 步、写 20 步。之后如必须改变，先记录协议理由。
5. 每个映射和资源都列出分配、持有、释放时刻；正常和错误路径都验证。
6. 回归检查数据、响应归属、目录、缓存、唯一所有者及全部临时表，不能只检查网络空闲。
7. 全套测试及三个示例通过后记录进度。没有用户要求不自动提交。

## 本轮执行范围

从 **A1** 开始，仅分离下游 TxnID。HF 写 DBID 与 snoop ID 的分离留到 A2；有界回收留到 A3。先用独立单调计数器与下游 ID → home_id 映射，把结构分离和资源管理拆成可回归的增量。

## 执行记录

- F1：实现受限 `atomic_swap(line_address, bytes8)`，只允许 64-byte 对齐地址的前 8 字节；HF 先失效持有者，按 DBIDResp→操作数→CompData 顺序在同一地址锁内读取旧行并更新前 8 字节，返回旧值。核对 IHI0050 G B2.3.3 p.82；数据值操作已建模，子行 Size/自然字节位置线上编码仍待 E 完善。111 测试通过，含两个 RN 竞争 dirty 行的线性顺序验证。不是完整 Atomic 功能覆盖。

- G（阶段性证据）：新增固定种子的 byte-line scoreboard，3 种 DAT 宽度各 150 轮独占读、本地 store、copyback、Home eviction、随机部分写，共 2250 次组合操作，对比独立 bytes 参考值并检查收包表及错误表回收。110 测试通过。`chi-coverage.md` 列出已验证路径与明确缺口，F 与全量 G 验收尚未完成。

- E5：bytes64 数据包产生每字节奇校验 DataCheck，接收发现不匹配转为 DERR 并等待全部包收齐；IHI0050 G B9.2.2 p.371。109 测试通过，包含已知奇校验向量及中间包单 bit 损坏。

- E4：按 8-byte 粒度拆分/重组 Poison；端点选择不缓存 Poison 数据，将其转换为 DERR（IHI0050 G B9.2.1/B9.2.3 pp.371–372）。RN 不填充错误缓存；HF 接管 snoop/copyback dirty 数据时保存错误状态，并在后续读或驱逐中传播，不能重新作为 OK 数据使用。108 测试通过，覆盖所有数据宽度及 poisoned read。不是支持长期存储逐字节 Poison 标记的内存模型，DataCheck 尚未实现。

- E3：修复 HF 转发写数据丢失 RespErr，逐拍错误汇总后一直保留至 S；S 拒绝用错误数据覆盖原内存并返回 NDERR。106 测试通过，新增最后一拍前 RN TxnID/CompAck 持有、部分写失败不修改内存、任意写包错误的端到端检查。Poison 与 DataCheck 尚未实现，此条仅指 RespErr。

- E2：新增显式 `write_no_snp_partial(aligned_address, bytes64, byte_enable)`，经 HF→S 的 WriteNoSnpPtl 更新选中字节。BE 按 DAT 包拆分/合并，未使能的线上字节置零；支持零掩码（IHI0050 G B2.8.3 p.156，Normal memory）。103 测试通过，覆盖跨 16/32-byte 边界掩码、零掩码及输入拒绝。当前仅支持完整 64-byte 地址窗口；子行 Size/Device memory 尚未支持，REQ 中 BE 仅是本模型的传递元数据。

- E1：64-byte bytes 数据按可配置 16/32/64-byte DAT 宽度拆包，DataID 分别为 0/1/2/3、0/2、0（IHI0050 G B2.8 p.157）。所有 RN/HF/S 数据接收先按源/TxnID/opcode 收齐，才进入原完成逻辑；支持重排、RespErr 汇总，重复包与不一致固定字段拒绝。普通教学字符串保持单 payload。101 测试通过，涵盖写/读/dirty snoop/copyback/Home 驱逐和有限资源组合；line_bytes 是 Python 收包元数据，不是 CHI 线上字段。子行 Size/BE 与 poison 留到后续增量。

- D2d：新增 Home 显式 `evict(address)`，对无人持有、无在途事务的 Home dirty 行通过下游 WriteNoSnpFull 刷入 S。复用独立下游 TxnID/DBID 和写完成条件，Comp 成功前始终保留 dirty 数据，NDERR 后保留可重试；资源和地址锁最终回收。98 测试通过，包括失败后重新驱逐。此 API 是本地 Home cache 操作，不是新增 CHI opcode；自动容量替换策略尚未实现。

- D2c：RN 对同地址本地请求按提交顺序发射；回写在途时后续读不能命中即将驱逐的缓存行，必须等待并重新取数；显式 cached store 在本地同地址事务未完成时拒绝执行。不同地址仍可并行。97 测试通过，增加 writeback→本地 ReadUnique 与过早 store 的回归。

- D2b：实现 `writeback(address)` → WriteBackFull → CompDBIDResp → CopyBackWriteData（IHI0050 G B2.3.2.3 pp.66–67，CAH=0 路径）。RN 在收到 DBID 时采样当前缓存状态和数据，因而在途 snoop 已转移 dirty 数据时不会用旧快照覆盖 Home；RN 提交数据后完成，HF 收到数据后释放 DBID/事务/地址锁。dirty 数据交给 Home 保存，不等价于刷入 S。96 测试及三个示例通过，包括共享/独占 snoop 与回写两种先后次序、单 ID 和 retry 的组合。

- D2a：新增 UD 与显式 store_cached（仅在已有 UC/UD 所有权时允许）；SnpShared/SnpUnique 对 UD 返回 DAT/SnpRespData 并携带 pass_dirty，HF 接管最新数据至 dirty_data，从而后续一致性读不误取 S 的旧副本。成功一致性写后清除 Home dirty 副本，失败保留。94 测试通过。此增量仅覆盖 dirty 数据转交给 Home 的路径，尚无容量受限 Home cache、驱逐与 CopyBack Write；store_cached 是显式本地 CPU 操作，尚未提供与在途同地址请求的 CPU 排序语义。

- D1：将教学 write() 的一致性失效行为明确为 WriteUniqueFull，新增 write_unique()/write_no_snp() 两种显式 API。HF 仅对一致性写发失效 snoop，下游仍使用 WriteNoSnpFull；显式非一致性写不会替调用者维护缓存一致性。更新原测试中的 RN→HF opcode 期望，下游期望不变；增加非一致性写不失效与一致性写失效的对照测试。92 测试通过。D2 dirty 与数据 snoop 尚未完成。

- C2：有限环缓冲增加独立于协议 P-Credit 的每方向/通道发送 credit 计数；传输消耗 credit，接收缓冲释放后延迟归还。`credit_return_delay` 默认 1，保持原快照时序；保留气泡注入至少 2 个 credit 和快照占用保护。逐步验证 `可用credit + 占用 + 返回途中credit = 容量`，延迟 1/3/8 与 retry 组合均完成；90 测试通过。此为 packet 级网络信用模型，尚未模拟 CHI 链路激活、FLITPEND、LCRDV 信号和 flit 编码。

- C1：可选 `retry_enabled=True` 实现 RN→HF 的 RetryAck/PCrdGrant：首发 AllowRetry=1，无资源时交付请求并返回 RetryAck；HF FIFO 记录源节点，资源可用后预留槽并发类型 0 的 PCrdGrant；RN 按 `(源节点, credit类型)` 记账，兼容 Grant 先于 RetryAck，消耗一次 credit 后复制请求并设 AllowRetry=0 重发。原请求对象不被修改，保留历史关联。受同地址串行化约束的已授信请求仍可在网络等待，但不会再次 Retry。默认保持旧背压模式。
- C1 依据 IHI0050 G B2.9 p.170（Grant 与 RetryAck 可重排、重发使用 credit、DAT/RSP/SNP 不可 Retry）。89 测试通过，包括重试混合压力与信用清空。尚未支持 HF→SN retry、多 credit 类型、PCrdReturn；链路级 credit 与上述 P-Credit 不同，后续 C2 处理。

- B3b：已支持 RN/HF 接收 CompDBIDResp，同时处理 DBID 与完成语义，写数据仍只发送一次；OK/NDERR 与有界 ID 回收测试通过（87 测试）。正式依据现已固定为 IHI0050 G（Mar 2024），B2.3 pp.62–64；官方 PDF https://documentation-service.arm.com/static/69e73291379d3e7aaa33ab54 。默认 HF/S 仍产生分离响应，合并响应接收能力通过端点事件注入验证。

- B3a 已完成分离写响应重排：RN 与 HF 下游请求者均分别保存 Comp 和数据发送状态，只有收到 Comp 且单拍写数据已提交可靠源队列后，才对上层发布完成并释放映射/TxnID。Comp 提前到达不会丢掉 payload 或导致尚未收到 DBID 的事务提前复用。OK/NDERR 原样保存并在最终完成时传播。
- B3a 验收为端点级注入两种合法事件顺序（Comp→DBIDResp、DBIDResp→Comp），分别检查 RN 与 HF 数据、ID、资源持有/清理及响应历史；默认 SN 仍维持写数据接收后产生 Comp，未改变内存写入顺序。86 个测试通过。后续 B3b 补 CompDBIDResp 合并响应；多 beat 完成条件留 E 阶段。

- B2 已完成当前单拍顺序响应路径的 RN 状态与释放：请求状态记录 queued、await_data、await_dbid、await_comp、send_comp_ack、complete。收到读完成数据并把必需的 CompAck 放入可靠源队列后释放 RN TxnID；HF 仍等 CompAck 真正交付才释放 Home DBID/地址锁。删除 RN 的 pending_acks 全网扫描机制。参考 Arm 官方 Transaction flows： https://developer.arm.com/documentation/102407/0102/Transaction-flows ，RN 发送完成确认与 HN 接收后解锁分别是本地事件。
- `Ring.inject()` 当前无失败且源队列无限，提交队列视作发送责任转移；若后续 C 引入有界源队列/握手，释放条件必须改为明确的发送接受事件。send_comp_ack 在当前模型中为同步瞬态，不占额外 step。状态历史用于教学与验证，仍非所有 CHI opcode 的通用状态机；Comp 早于 DBIDResp 等写响应重排留在 B3。
- B2 验收：旧 CompAck 在途时容量 1 的 RN 复用 TxnID；人工延迟 CompAck 交付时 HF 仍持有原事务与地址锁，恢复后所有请求完成；84 测试通过。A3b 的交付后复用策略已由本条替代。

- B 拆为 B1 现有读路径字段关联、B2 完成状态机及 RN 释放条件、B3 合法响应组合与重排。B1 已实现：REQ 显式携带 `exp_comp_ack`（当前共享/独占读为 True）；CompData 携带 HomeNID 和以内部 home_id 为令牌的 DBID；RN 依据原请求的 ExpCompAck 发确认，CompAck 的目标为 HomeNID、TxnID 为返回的 DBID。HF 按 `(RN源节点, DBID)` 释放读事务。普通读保持不要求确认。
- B1 参考：Arm 官方《AMBA 5 CHI Architecture Specification》（2017 公开版本），Transaction identifier fields 2-73、Transaction structure 2-39、Ordering 2-63；URL https://documentation-service.arm.com/static/5f914e1cf86e16515cdc2b3b 。官方字段说明明确 HomeNID 为 CompAck 目标、DBID 为响应采用的 TxnID。此处核对的是已有读路径通用关联，不代表已完成 CHI-G 全部版本差异审计。CHI-G 的准确 issue/字段合法组合以及其他 opcode 的 ExpCompAck 约束仍需在扩展时核对。
- B1 测试覆盖 RN TxnID=0 与 Home DBID=100、正常/错误读均确认、HF 确认前持有资源、普通读不确认；RN 暂时保留 A3b 的保守交付后复用策略，B2 再完善。

- A3b 已完成：`id_capacity` 同时应用于各 RN 的 TxnID 空间。RN 用请求对象保存等待队列及未注入写数据，只有分配到 TxnID 才入环；响应历史按 `(channel, opcode, request对象)` 保存，重复编号不会覆盖旧请求结果。普通读/写在完成响应后释放；一致性读保守地等其 CompAck 从网络交付后释放。`Zhujiang.step()` 在环步进后推进 RN 队列，`run_until_idle()` 同时检查 RN 待处理状态。
- CompAck 的交付观察是当前 Python 模型的调度机制，不是新增 CHI 返回消息或真实硬件可见确认信号；阶段 B 需对照规范完善 CompAck 标识与 RN 的协议级释放条件。直接驱动 `ring.step()` 不代替系统级 RN 调度，应使用 `Zhujiang.step()`。
- A3b 验收：容量 1 下跨请求保留历史响应、CompAck 前禁止复用、排队写数据与错误响应不串事务；压力矩阵同时覆盖 ID 容量 1/2/3/无限。82 个测试通过。下一步 B 规范核对及完成状态机。

提交规则：以后每个可验证增量完成后，运行回归并创建本地 Conventional Commit，不自动推送。

- A3a 已完成：`Zhujiang(id_capacity=N)` 限制 HF 四个 ID 空间及 SN DBID，各空间独立循环分配并跳过活跃 ID；HF 按活跃内部事务数保守预留后续资源，仅对 REQ 背压，SN 对写 REQ 检查 DBID 容量。默认 None 保持原行为。容量 1/2/3 的逐步检查覆盖复用、并发、snoop 与错误清理；80 个测试及三个演示通过。RN TxnID 仍单调递增，下一步 A3b。

A3 分为两个独立验收增量：A3a 为 HF 内部/home、下游、写 DBID、snoop 及 SN DBID 增加可选容量，共用容量配置但各自独立分配；HF 接受请求时保守地按内部事务数预留后续阶段资源，避免收下请求后等待 ID 形成依赖环。A3b 再处理 RN TxnID 的本地等待队列、响应历史与 CompAck 安全复用。本轮先完成 A3a，不宣称已实现全部 ID 的有界化。

- A1 已完成：`HomeWrapper` 新增独立下游计数器和 `downstream_requests` 映射，统一下游请求构造。读数据返回后释放下游映射；写的 DBIDResp 保持映射，Comp 后释放。缓存读继续持有内部 home_id 到 CompAck。
- 测试刻意使用 home_id=100、下游 TxnID=1000、SN DBID=10000 起点，覆盖正常/错误与 snoop 后读；并发压力也使用错开的 ID 空间并检查映射清空。
- 77 个测试通过，孤立读 10 步、写 20 步仍通过；三个示例 CLI 运行完成。下一步 A2。
- A2 ID 分离已完成：HF 写 DBID 使用独立计数器及 `write_dbids[dbid] = home_id`；发 DBIDResp 时建立，收到完整单拍写数据时释放，数据继续由内部事务持有至下游 Comp。snoop 使用独立计数器，`pending_snoops[snoop_id]` 显式记录 home_id 和待响应节点集合，收齐全部 SnpResp 后释放，并按 home_id 继续读或写。
- A2 验收：覆盖各空间同起点/错开起点、两 RN 相同 TxnID、多持有者 snoop、无 snoop 的写入及错误路径压力；78 个测试和三个示例通过。统一记录当前先落实在 snoop 的 home_id 关联及写/下游映射，完整事务状态记录在 B 阶段实施。下一步 A3 有界分配与复用。
