# tinyzj Agent Guide

## Verify changes

- Full suite, from the repository root: `PYTHONDONTWRITEBYTECODE=1 ./test.sh`. It is stdlib `unittest`; there is no dependency manifest or configured lint, format, typecheck, or build step.
- Focused test: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest -v tests.test_cache_basics.CacheBasicsTest.test_full_line_store_uses_make_unique_and_discards_old_dirty_data`. Test files use module names, not paths.
- Dashboard scripts are interactive even with a script: `./run.sh demo.txt` resolves under `examples/` and waits for Enter before request batches and each simulated step. Do not treat stdin EOF as a protocol failure.
- Drive simulations with `Zhujiang.step()` or `run_until_idle()`, not `ring.step()` alone: system steps also advance RN request queues and Home retry grants. Ring emptiness is not completion; use `has_pending_work()` and `check_invariants()`.

## Architecture

- This is a packet-level teaching model, not a complete CHI implementation. Opcode presence alone does not imply full protocol compliance.
- `src/tinyzj/zj.py` owns scheduling and RN/Home/Storage state machines; `xj.py` is protocol-agnostic ring transport; `data.py` packetizes/reassembles DAT; `dj.py` is backing storage. Perimeter: `n00(CC0) → n01(HF) → n11(CC1) → n10(S) → n00`.

## Lifecycle constraints

- Routing chooses the shortest path, with ties forward in node-list order. Each ring step returns due credits, snapshots occupancy, delivers at most one message per `(node, channel)` subject to `can_receive()`, then advances initial messages one hop. Forwarding outranks injection; new messages created during delivery wait until a later step.
- Finite buffers use start-of-step occupancy plus link credits; source injection requires two free slots/credits to preserve a bubble. Packet link credits are separate from CHI retry P-Credits. `retry_enabled=True` enables RN→HF type-0 retry; default behavior is receive backpressure. Home resource/address gating applies to REQ, not RSP/DAT.
- `Ring.inject()` queues messages without assigning or translating IDs. Endpoints own independent RN TxnID, HF home/downstream/write-DBID/snoop, and SN DBID spaces. Keep mappings explicit and preserve received message history when translating or retrying. With `id_capacity`, IDs are reused; `Socket` response history is keyed by request object, not just numeric ID.
- RN may release a coherent-read or acknowledged dataless TxnID once CompAck is queued, while HF holds the home transaction/address lock until CompAck arrives. CompAck targets returned HomeNID and uses returned DBID as TxnID.
- Writes require both completion and data submission before resource release; atomic reads require both returned data and operand submission. Preserve legal response reordering and error-path cleanup (`tests/test_completion.py`, `tests/test_atomic.py`, `tests/test_ids.py`).

## Coherence and data gotchas

- `read()` is non-snooping; `read_shared()`/`read_unique()` can return `None` on a cache hit. `write()` aliases coherent `write_unique()` (WriteUniqueFull); explicit `write_no_snp()` does not invalidate caches.
- Dashboard `read` uses `read_shared`; Dashboard `write` uses `store`: a miss/full-line overwrite uses `MakeUnique` + `SnpMakeInvalid`, then becomes local `UD` without reading or updating Storage. A UC/UD hit is local only.
- `store_cached()` requires UC/UD ownership and no pending local request. RN `writeback()` transfers dirty ownership to Home without necessarily updating Storage; RN `write_clean()` leaves UC and Home-owned latest data; RN `evict()` is only for clean SC/UC. `hf.evict()` flushes idle, unshared Home dirty data. Preserve Home dirty data on failed flushes.
- Storage is keyed by the exact address; absent reads return `("read data", False)`, not a zero-filled line. Do not assume all APIs normalize integer addresses: atomics normalize to a 64-byte line plus offset, while partial writes require aligned integer addresses and bytes64 payloads.
- DAT splitting applies only to 64-byte `bytes` payloads; teaching strings remain single packets. `data_width` is in bytes (16/32/64), with DataIDs respectively `0,1,2,3` / `0,2` / `0`. Use `Endpoint.send()` for data and pass received packets through assembly before advancing protocol state; errors/poison must survive reassembly and forwarding.
