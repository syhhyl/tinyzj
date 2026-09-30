from copy import copy

from .chi import (
  Channel,
  DatOpcode,
  ReqOpcode,
  Resp,
  RespErr,
  RspOpcode,
  SnpOpcode,
)
from .dj import DongJiang
from .data import DataAssembly, packets
from .xj import Message, Ring, RingNode
from .atomic import OPERATIONS, LOAD_OPCODES, STORE_OPCODES, calculate


ATOMIC_OPCODES = (ReqOpcode.ATOMIC_SWAP, ReqOpcode.ATOMIC_COMPARE) + LOAD_OPCODES + STORE_OPCODES
MAINTENANCE_OPCODES = (ReqOpcode.CLEAN_INVALID, ReqOpcode.CLEAN_SHARED)
WRITE_DATA_OPCODES = (
  ReqOpcode.WRITE_NO_SNP_FULL, ReqOpcode.WRITE_NO_SNP_PTL,
  ReqOpcode.WRITE_UNIQUE_FULL, ReqOpcode.WRITE_BACK_FULL,
  ReqOpcode.DVM_OP,
) + ATOMIC_OPCODES


def allocate_id(endpoint, counter, active):
  candidate = getattr(endpoint, counter)
  capacity = endpoint.id_capacity
  if capacity is not None:
    candidate %= capacity
    for _ in range(capacity):
      if candidate not in active:
        break
      candidate = (candidate + 1) % capacity
    else:
      raise RuntimeError("ID reservation invariant violated")
  setattr(endpoint, counter, candidate + 1 if capacity is None else (candidate + 1) % capacity)
  return candidate


def validate_id_capacity(capacity):
  if capacity is not None and (type(capacity) is not int or capacity < 1):
    raise ValueError("id_capacity must be a positive integer")

class Zhujiang:
  
  def __init__(
    self,
    buffer_capacity=None,
    max_transactions=None,
    error_addresses=None,
    id_capacity=None,
    retry_enabled=False,
    credit_return_delay=1,
    data_width=16,
  ):
    validate_id_capacity(id_capacity)
    if type(data_width) is not int or data_width not in (16, 32, 64):
      raise ValueError("data_width must be 16, 32, or 64 bytes")
    self.ring = Ring([
      RingNode("n00", "CC0"),
      RingNode("n01", "HF"),
      RingNode("n11", "CC1"),
      RingNode("n10", "S"),
    ], buffer_capacity=buffer_capacity, credit_return_delay=credit_return_delay)

    self.cc0 = Socket(self.ring, "n00", "n01", id_capacity=id_capacity)
    self.hf = HomeWrapper(
      self.ring,
      "n01",
      "n10",
      max_transactions=max_transactions,
      id_capacity=id_capacity,
      retry_enabled=retry_enabled,
    )
    self.cc1 = Socket(self.ring, "n11", "n01", id_capacity=id_capacity)
    self.s = StorageWrapper(
      self.ring,
      "n10",
      error_addresses=error_addresses,
      id_capacity=id_capacity,
    )

    self.ring.connect("n00", self.cc0)
    self.ring.connect("n01", self.hf)
    self.ring.connect("n11", self.cc1)
    self.ring.connect("n10", self.s)
    for endpoint in (self.cc0, self.cc1, self.hf, self.s):
      endpoint.data_width = data_width
      endpoint.data_assembly = DataAssembly(data_width)

  def step(self):
    self.ring.step()
    self.cc0.advance_requests()
    self.cc1.advance_requests()
    self.hf.advance_credits()

  def run_until_idle(self, max_steps=100):
    if max_steps < 0:
      raise ValueError("max_steps must be non-negative")

    steps = 0
    while self.has_pending_work():
      if steps == max_steps:
        raise RuntimeError("system did not become idle")
      self.step()
      steps += 1
    return steps

  def has_pending_work(self):
    return bool(self.ring.in_flight or self.cc0.has_pending_requests() or self.cc1.has_pending_requests()
                or self.hf.pending_requests or self.hf.credit_waiters or self.hf.credit_reservations
                or self.s.pending_writes or any(endpoint.data_assembly.pending
                  for endpoint in (self.cc0, self.cc1, self.hf, self.s)))

  def check_invariants(self):
    ring = self.ring
    if ring.buffer_capacity is not None:
      for key, credit in ring.link_credits.items():
        node, direction, channel = key
        occupied = len(ring.ring_buffers[node][(direction, channel)])
        returning = sum(1 for _, candidate in ring.credit_returns if candidate == key)
        assert credit >= 0 and credit + occupied + returning == ring.buffer_capacity, key
    home = self.hf
    active = set(home.pending_requests)
    assert set(home.downstream_requests.values()) <= active
    assert set(home.write_dbids.values()) <= active
    assert {entry["home_id"] for entry in home.pending_snoops.values()} <= active
    assert set(home.pending_write_data) <= active
    assert set(home.pending_write_errors) <= active
    assert set(home.atomic_results) <= active
    for address, holders in home.address_busy.items():
      assert len(holders) == 1
      assert holders <= active
      assert all(home.pending_requests[index].address == address for index in holders)
    for cc in (self.cc0, self.cc1):
      ids = set(cc.active_requests)
      assert set(cc.pending_write_data) <= ids
      assert cc.write_data_sent <= ids
      assert set(cc.atomic_completions) <= ids
      assert set(cc.retry_requests) <= ids
      assert set(cc.pending_cached_stores) <= set(cc.active_requests.values()) | set(cc.waiting_requests)
      if cc.id_capacity is not None:
        assert len(ids) <= cc.id_capacity
    if not self.has_pending_work():
      for address, holders in home.directory.items():
        for node, state in holders.items():
          cache = ring.connections[node].cache
          assert address in cache, (node, address)
          assert cache[address][0] == state or (state == Resp.UC and cache[address][0] == Resp.UD)
        if any(state == Resp.UC for state in holders.values()):
          assert len(holders) == 1
      assert not home.pending_snoops and not home.address_busy
      assert not home.downstream_requests and not home.write_dbids


class Endpoint:

  def __init__(self):
    self.received_messages = []
    self.data_width = 16
    self.data_assembly = DataAssembly(self.data_width)

  def send(self, message):
    for packet in packets(message, self.data_width):
      self.ring.inject(packet)

  def receive(self, message):
    self.received_messages.append(message)
    return self.data_assembly.accept(message)


class Socket(Endpoint):

  def __init__(self, ring, node_name, home_name, id_capacity=None):
    super().__init__()
    validate_id_capacity(id_capacity)
    self.id_capacity = id_capacity
    self.active_requests = {}
    self.waiting_requests = []
    self.waiting_write_data = {}
    self.request_states = {}
    self.write_completions = {}
    self.atomic_completions = {}
    self.write_data_sent = set()
    self.retry_requests = {}
    self.protocol_credits = {}
    self.ring = ring
    self.node_name = node_name
    self.home_name = home_name
    self._responses = {}
    self.cache = {}
    self.tlb = {}
    self.dvm_parts = {}
    self.pending_write_data = {}
    self.next_transaction_id = 0
    self.pending_cached_stores = {}

  def read(self, address):
    if address is None:
      raise ValueError("read request needs an address")
    return self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_NO_SNP,
    )

  def read_shared(self, address):
    if address is None:
      raise ValueError("read request needs an address")
    if address in self.cache and not self._address_pending(address):
      return None
    return self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_SHARED,
    )

  def read_unique(self, address):
    if address is None:
      raise ValueError("read request needs an address")
    if address in self.cache and self.cache[address][0] in (Resp.UC, Resp.UD) and not self._address_pending(address):
      return None
    return self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_UNIQUE,
    )

  def store_cached(self, address, data):
    if self._address_pending(address):
      raise ValueError("cached store requires prior local requests to complete")
    if address not in self.cache or self.cache[address][0] not in (Resp.UC, Resp.UD):
      raise ValueError("cached store requires unique ownership")
    self.cache[address] = (Resp.UD, data)

  def store(self, address, data):
    request = self.read_unique(address)
    if request is None:
      self.store_cached(address, data)
    else:
      self.pending_cached_stores[request] = data
    return request

  def _address_pending(self, address):
    return any(request.address == address for request in self.active_requests.values()) or any(
      request.address == address for request in self.waiting_requests
    )

  def writeback(self, address):
    if address not in self.cache:
      raise ValueError("writeback requires a cached line")
    return self._write(address, self.cache[address][1], ReqOpcode.WRITE_BACK_FULL)

  def write(self, address, data):
    return self.write_unique(address, data)

  def clean_invalid(self, address):
    if address is None:
      raise ValueError("maintenance requires an address")
    return self._send_request(address, Channel.REQ, ReqOpcode.CLEAN_INVALID)

  def clean_shared(self, address):
    if address is None:
      raise ValueError("maintenance requires an address")
    return self._send_request(address, Channel.REQ, ReqOpcode.CLEAN_SHARED)

  def dvm_invalidate(self, page=None):
    return self._write(("dvm",), ("invalidate", page), ReqOpcode.DVM_OP)

  def dvm_sync(self):
    return self._write(("dvm",), ("sync", None), ReqOpcode.DVM_OP)

  def write_no_snp(self, address, data):
    return self._write(address, data, ReqOpcode.WRITE_NO_SNP_FULL)

  def read_exclusive_no_snp(self, address):
    request = self.read(address)
    request.excl = True
    return request

  def write_exclusive_no_snp(self, address, data):
    request = self.write_no_snp(address, data)
    request.excl = True
    return request

  def atomic_swap(self, address, value):
    return self._atomic_request(address, value, ReqOpcode.ATOMIC_SWAP)

  def atomic_compare(self, address, compare, replacement):
    if not isinstance(compare, bytes) or not isinstance(replacement, bytes) or len(compare) != len(replacement):
      raise ValueError("atomic compare operands must be equally sized bytes")
    return self._atomic_request(address, compare + replacement, ReqOpcode.ATOMIC_COMPARE,
                                operand_size=len(compare))

  def _atomic_request(self, address, value, opcode, operand_size=None):
    size = operand_size if operand_size is not None else len(value) if isinstance(value, bytes) else 0
    allowed = (1, 2, 4, 8, 16) if opcode == ReqOpcode.ATOMIC_COMPARE else (1, 2, 4, 8)
    if size not in allowed or not isinstance(value, bytes):
      raise ValueError("unsupported atomic operand size")
    if type(address) is not int or address < 0 or address % size:
      raise ValueError("atomic address must be naturally aligned")
    request = self._write(address - address % 64, value, opcode)
    request.byte_offset = address % 64
    request.size = len(value).bit_length() - 1
    return request

  def atomic_load(self, address, operation, value):
    return self._atomic_arithmetic(address, operation, value, "AtomicLoad")

  def atomic_store(self, address, operation, value):
    return self._atomic_arithmetic(address, operation, value, "AtomicStore")

  def _atomic_arithmetic(self, address, operation, value, prefix):
    if operation not in OPERATIONS:
      raise ValueError("unsupported atomic operation")
    return self._atomic_request(address, value, prefix + operation)

  def write_no_snp_partial(self, address, data, byte_enable):
    if type(address) is not int or address < 0 or address % 64:
      raise ValueError("partial line write requires a 64-byte aligned integer address")
    if not isinstance(data, bytes) or len(data) != 64:
      raise ValueError("partial line write requires 64 bytes")
    if type(byte_enable) is not int or not 0 <= byte_enable < (1 << 64):
      raise ValueError("byte_enable must be a 64-bit mask")
    request = self._write(address, data, ReqOpcode.WRITE_NO_SNP_PTL)
    request.byte_enable = byte_enable
    return request

  def write_unique(self, address, data):
    return self._write(address, data, ReqOpcode.WRITE_UNIQUE_FULL)

  def _write(self, address, data, opcode):
    if address is None:
      raise ValueError("write request needs an address")
    request = self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=opcode,
    )
    if request.transaction_id is None:
      self.waiting_write_data[request] = data
    else:
      self.pending_write_data[request.transaction_id] = data
    return request

  def _send_request(
    self,
    address,
    channel,
    opcode,
  ):
    request = Message(
      self.node_name,
      self.home_name,
      address=address,
      channel=channel,
      opcode=opcode,
      exp_comp_ack=opcode in (ReqOpcode.READ_SHARED, ReqOpcode.READ_UNIQUE),
    )
    self.waiting_requests.append(request)
    self.request_states[request] = "queued"
    self.advance_requests()
    return request

  def has_pending_requests(self):
    return bool(self.active_requests or self.waiting_requests)

  def advance_requests(self):
    for txn_id, credit_key in list(self.retry_requests.items()):
      if self.protocol_credits.get(credit_key, 0):
        self.protocol_credits[credit_key] -= 1
        if not self.protocol_credits[credit_key]:
          del self.protocol_credits[credit_key]
        request = self.active_requests[txn_id]
        retry = copy(request)
        retry.allow_retry = False
        retry.pcrd_type = credit_key[1]
        self.ring.inject(retry)
        self.retry_requests.pop(txn_id)
        self.request_states[request] = "await_dbid" if request.opcode in WRITE_DATA_OPCODES else "await_data"
    for request in list(self.waiting_requests):
      if self.id_capacity is not None and len(self.active_requests) >= self.id_capacity:
        break
      if any(active.address == request.address for active in self.active_requests.values()):
        continue
      self.waiting_requests.remove(request)
      txn_id = allocate_id(self, "next_transaction_id", self.active_requests)
      request.transaction_id = txn_id
      self.active_requests[txn_id] = request
      self.request_states[request] = "await_dbid" if request.opcode in WRITE_DATA_OPCODES else "await_data"
      if request in self.waiting_write_data:
        self.pending_write_data[txn_id] = self.waiting_write_data.pop(request)
      self.ring.inject(request)

  def read_response_for(self, request):
    return self._responses.get(
      (Channel.DAT, DatOpcode.COMP_DATA, request)
    )

  def write_response_for(self, request):
    return self._responses.get(
      (Channel.RSP, RspOpcode.COMP, request)
    )

  def _finish_write(self, txn_id):
    if txn_id not in self.write_data_sent or txn_id not in self.write_completions:
      return
    request = self.active_requests.pop(txn_id)
    self._responses[(Channel.RSP, RspOpcode.COMP, request)] = self.write_completions.pop(txn_id)
    self.write_data_sent.remove(txn_id)
    self.pending_write_data.pop(txn_id)
    self.request_states[request] = "complete"

  def _finish_atomic_read(self, txn_id):
    if txn_id not in self.write_data_sent or txn_id not in self.atomic_completions:
      return
    request = self.active_requests.pop(txn_id)
    response = self.atomic_completions.pop(txn_id)
    self._responses[(Channel.DAT, DatOpcode.COMP_DATA, request)] = response
    self.pending_write_data.pop(txn_id)
    self.write_data_sent.remove(txn_id)
    self.request_states[request] = "complete"

  def receive(self, message):
    message = super().receive(message)
    if message is None:
      return
    if message.channel == Channel.SNP and message.opcode == SnpOpcode.SNP_DVM_OP:
      part, operation, page = message.payload
      key = (message.source_name, message.transaction_id)
      parts = self.dvm_parts.setdefault(key, {})
      if part in parts or part not in (0, 1):
        raise ValueError("invalid or duplicate DVM snoop part")
      parts[part] = (operation, page)
      if len(parts) == 2:
        if parts[0] != parts[1]:
          raise ValueError("inconsistent DVM snoop parts")
        self.dvm_parts.pop(key)
        if operation == "invalidate":
          if page is None:
            self.tlb.clear()
          else:
            self.tlb.pop(page, None)
        self.send(Message(self.node_name, message.source_name,
                          transaction_id=message.transaction_id,
                          channel=Channel.RSP, opcode=RspOpcode.SNP_RESP, resp=Resp.I))
      return
    if message.channel == Channel.RSP and message.opcode == RspOpcode.RETRY_ACK:
      self.retry_requests[message.transaction_id] = (message.source_name, message.pcrd_type)
      self.request_states[self.active_requests[message.transaction_id]] = "await_credit"
      return
    if message.channel == Channel.RSP and message.opcode == RspOpcode.PCRD_GRANT:
      key = (message.source_name, message.pcrd_type)
      self.protocol_credits[key] = self.protocol_credits.get(key, 0) + 1
      return
    if (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.COMP_DATA
    ):
      request = self.active_requests[message.transaction_id]
      if request.opcode in ATOMIC_OPCODES and request.opcode not in STORE_OPCODES:
        self.atomic_completions[message.transaction_id] = message
        self._finish_atomic_read(message.transaction_id)
        return
      key = (message.channel, message.opcode, request)
      self._responses[key] = message
      if request.exp_comp_ack:
        self.request_states[request] = "send_comp_ack"
        if message.resp_err == RespErr.OK:
          self.cache[message.address] = (message.resp, message.payload)
        if request in self.pending_cached_stores:
          data = self.pending_cached_stores.pop(request)
          if message.resp_err == RespErr.OK:
            self.cache[message.address] = (Resp.UD, data)
        ack = Message(
          self.node_name,
          message.home_nid,
          transaction_id=message.dbid,
          channel=Channel.RSP,
          opcode=RspOpcode.COMP_ACK,
        )
        self.ring.inject(ack)
      self.request_states[request] = "complete"
      self.active_requests.pop(message.transaction_id)
      if request.opcode in ATOMIC_OPCODES:
        self.pending_write_data.pop(message.transaction_id)
        self.write_data_sent.discard(message.transaction_id)
    elif (
      message.channel == Channel.SNP
      and message.opcode in (SnpOpcode.SNP_SHARED, SnpOpcode.SNP_CLEAN_SHARED)
    ):
      dirty = self.cache.get(message.address, (None,))[0] == Resp.UD
      payload = self.cache[message.address][1] if dirty else None
      if message.address in self.cache:
        state, payload = self.cache[message.address]
        self.cache[message.address] = (Resp.SC, payload)
      self.send(
        Message(
          self.node_name,
          message.source_name,
          transaction_id=message.transaction_id,
          payload=payload,
          channel=Channel.DAT if dirty else Channel.RSP,
          opcode=DatOpcode.SNP_RESP_DATA if dirty else RspOpcode.SNP_RESP,
          resp=Resp.SC,
          pass_dirty=dirty,
        )
      )
    elif (
      message.channel == Channel.SNP
      and message.opcode in (SnpOpcode.SNP_UNIQUE, SnpOpcode.SNP_CLEAN_INVALID)
    ):
      dirty = self.cache.get(message.address, (None,))[0] == Resp.UD
      payload = self.cache[message.address][1] if dirty else None
      self.cache.pop(message.address, None)
      self.send(
        Message(
          self.node_name,
          message.source_name,
          transaction_id=message.transaction_id,
          payload=payload,
          channel=Channel.DAT if dirty else Channel.RSP,
          opcode=DatOpcode.SNP_RESP_DATA if dirty else RspOpcode.SNP_RESP,
          resp=Resp.I,
          pass_dirty=dirty,
        )
      )
    elif (
      message.channel == Channel.RSP
      and message.opcode in (RspOpcode.DBID_RESP, RspOpcode.COMP_DBID_RESP)
    ):
      data = self.pending_write_data[message.transaction_id]
      request = self.active_requests[message.transaction_id]
      copyback = request.opcode == ReqOpcode.WRITE_BACK_FULL
      if copyback:
        state, data = self.cache.pop(request.address, (Resp.I, None))
      write_data = Message(
        self.node_name,
        message.source_name,
        payload=data,
        transaction_id=message.dbid,
        channel=Channel.DAT,
        opcode=DatOpcode.COPY_BACK_WRITE_DATA if copyback else DatOpcode.NON_COPY_BACK_WRITE_DATA,
        resp=state if copyback else None,
        byte_enable=request.byte_enable,
      )
      self.send(write_data)
      self.write_data_sent.add(message.transaction_id)
      if message.opcode == RspOpcode.COMP_DBID_RESP:
        self.write_completions[message.transaction_id] = message
      request = self.active_requests[message.transaction_id]
      self.request_states[request] = "await_comp"
      self._finish_write(message.transaction_id)
      self._finish_atomic_read(message.transaction_id)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.COMP
    ):
      request = self.active_requests[message.transaction_id]
      if request.opcode in MAINTENANCE_OPCODES:
        self.active_requests.pop(message.transaction_id)
        self._responses[(Channel.RSP, RspOpcode.COMP, request)] = message
        self.request_states[request] = "complete"
        return
      self.write_completions[message.transaction_id] = message
      self._finish_write(message.transaction_id)


class HomeWrapper(Endpoint):
  
  def __init__(self, ring, node_name, storage_name, max_transactions=None, id_capacity=None, retry_enabled=False):
    super().__init__()
    validate_id_capacity(id_capacity)
    self.id_capacity = id_capacity
    self.retry_enabled = retry_enabled
    self.credit_waiters = []
    self.credit_reservations = {}
    self.ring = ring
    self.node_name = node_name
    self.storage_name = storage_name
    self.max_transactions = max_transactions
    if max_transactions is not None:
      if not isinstance(max_transactions, int) or max_transactions < 1:
        raise ValueError("max_transactions must be a positive integer")
    self.pending_requests = {}
    self.pending_comp_acks = {}
    self.directory = {}
    self.address_busy = {}
    self.pending_snoops = {}
    self.pending_write_data = {}
    self.next_home_id = 0
    self.pending_write_errors = {}
    self.atomic_results = {}
    self.exclusive_monitors = {}
    self.next_downstream_id = 0
    self.downstream_requests = {}
    self.next_dbid = 0
    self.write_dbids = {}
    self.next_snoop_id = 0
    self.dirty_data = {}
    self.dirty_errors = {}
    self.downstream_completions = {}
    self.downstream_data_sent = set()
    self.evictions = {}
    self.eviction_results = {}

  def evict(self, address):
    if address not in self.dirty_data:
      raise ValueError("home eviction requires home-owned dirty data")
    if self.directory.get(address) or self.address_busy.get(address):
      raise ValueError("home eviction requires an unshared idle line")
    limits = [n for n in (self.max_transactions, self.id_capacity) if n is not None]
    if limits and len(self.pending_requests) + sum(self.credit_reservations.values()) >= min(limits):
      raise ValueError("home eviction requires a free transaction slot")
    request = Message(self.node_name, self.storage_name, address=address,
                      channel=Channel.REQ, opcode=ReqOpcode.WRITE_NO_SNP_FULL)
    home_id = allocate_id(self, "next_home_id", self.pending_requests)
    self.pending_requests[home_id] = request
    self.evictions[home_id] = request
    self.address_busy[address] = {home_id}
    self.pending_write_data[home_id] = self.dirty_data[address]
    self.pending_write_errors[home_id] = self.dirty_errors.get(address, RespErr.OK)
    self._send_storage_request(home_id, ReqOpcode.WRITE_NO_SNP_FULL)
    return request

  def _finish_downstream_write(self, txn_id):
    if txn_id not in self.downstream_data_sent or txn_id not in self.downstream_completions:
      return
    completion = self.downstream_completions.pop(txn_id)
    self.downstream_data_sent.remove(txn_id)
    home_id = self.downstream_requests.pop(txn_id)
    request = self.pending_requests.pop(home_id)
    self.pending_write_data.pop(home_id)
    self.pending_write_errors.pop(home_id, None)
    if completion.resp_err == RespErr.OK:
      self.exclusive_monitors = {key: address for key, address in self.exclusive_monitors.items()
                                 if address != request.address}
    if request.opcode in ATOMIC_OPCODES:
      old = self.atomic_results.pop(home_id)
      if completion.resp_err == RespErr.OK:
        self.dirty_data.pop(request.address, None)
        self.dirty_errors.pop(request.address, None)
      self.address_busy.pop(request.address)
      self._send_atomic_result(request, old, completion.resp_err)
      return
    if completion.resp_err == RespErr.OK and request.opcode in (ReqOpcode.WRITE_UNIQUE_FULL,) + MAINTENANCE_OPCODES:
      self.dirty_data.pop(request.address, None)
      self.dirty_errors.pop(request.address, None)
    busy = self.address_busy.get(request.address)
    if busy is not None:
      busy.discard(home_id)
      if not busy:
        self.address_busy.pop(request.address, None)
    if home_id in self.evictions:
      self.evictions.pop(home_id)
      self.eviction_results[request] = completion
      if completion.resp_err == RespErr.OK:
        self.dirty_data.pop(request.address)
        self.dirty_errors.pop(request.address, None)
      return
    self.ring.inject(Message(
      self.node_name,
      request.source_name,
      transaction_id=request.transaction_id,
      resp_err=RespErr.EXOK if request.excl and completion.resp_err == RespErr.OK else completion.resp_err,
      channel=Channel.RSP,
      opcode=RspOpcode.COMP,
    ))

  def _send_atomic_result(self, request, old, error):
    store = request.opcode in STORE_OPCODES
    self.send(Message(self.node_name, request.source_name, address=request.address,
                      transaction_id=request.transaction_id, payload=None if store else old,
                      channel=Channel.RSP if store else Channel.DAT,
                      opcode=RspOpcode.COMP if store else DatOpcode.COMP_DATA,
                      resp_err=error, resp=Resp.I))

  def _send_write_dbid(self, home_id):
    request = self.pending_requests[home_id]
    dbid = allocate_id(self, "next_dbid", self.write_dbids)
    self.write_dbids[dbid] = home_id
    self.ring.inject(Message(
      self.node_name,
      request.source_name,
      transaction_id=request.transaction_id,
      dbid=dbid,
      channel=Channel.RSP,
      opcode=RspOpcode.COMP_DBID_RESP if request.opcode == ReqOpcode.WRITE_BACK_FULL else RspOpcode.DBID_RESP,
    ))

  def _complete_maintenance(self, home_id):
    request = self.pending_requests[home_id]
    if request.address in self.dirty_data:
      self.pending_write_data[home_id] = self.dirty_data[request.address]
      self.pending_write_errors[home_id] = self.dirty_errors.get(request.address, RespErr.OK)
      self._send_storage_request(home_id, ReqOpcode.WRITE_NO_SNP_FULL)
      return
    self.pending_requests.pop(home_id)
    self.address_busy.pop(request.address)
    self.send(Message(self.node_name, request.source_name,
                      transaction_id=request.transaction_id, channel=Channel.RSP,
                      opcode=RspOpcode.COMP))

  def _send_storage_request(self, home_id, opcode):
    request = self.pending_requests[home_id]
    if opcode == ReqOpcode.READ_NO_SNP and request.opcode != ReqOpcode.READ_NO_SNP and request.address in self.dirty_data:
      self._complete_read(home_id, self.dirty_data[request.address], request.address, True,
                          self.dirty_errors.get(request.address, RespErr.OK))
      return
    downstream_id = allocate_id(self, "next_downstream_id", self.downstream_requests)
    self.downstream_requests[downstream_id] = home_id
    self.ring.inject(Message(
      self.node_name,
      self.storage_name,
      address=self.pending_requests[home_id].address,
      transaction_id=downstream_id,
      channel=Channel.REQ,
      opcode=opcode,
      byte_enable=request.byte_enable,
    ))

  def advance_credits(self):
    if not self.retry_enabled or not self.credit_waiters:
      return
    limits = [n for n in (self.max_transactions, self.id_capacity) if n is not None]
    limit = min(limits) if limits else float("inf")
    while self.credit_waiters and len(self.pending_requests) + sum(self.credit_reservations.values()) < limit:
      source = self.credit_waiters.pop(0)
      self.credit_reservations[source] = self.credit_reservations.get(source, 0) + 1
      self.ring.inject(Message(self.node_name, source, transaction_id=0,
                               channel=Channel.RSP, opcode=RspOpcode.PCRD_GRANT))

  def can_receive(self, message):
    if self.retry_enabled and message.channel == Channel.REQ and message.allow_retry:
      return True
    return self._can_accept(message)

  def _can_accept(self, message):
    if message.channel != Channel.REQ:
      return True
    if self.id_capacity is not None and len(self.pending_requests) >= self.id_capacity:
      return False
    if self.address_busy.get(message.address):
      return False
    if self.max_transactions is None:
      return True
    return len(self.pending_requests) < self.max_transactions

  def receive(self, message):
    message = super().receive(message)
    if message is None:
      return
    if self.retry_enabled and message.channel == Channel.REQ:
      reserved = sum(self.credit_reservations.values())
      limits = [n for n in (self.max_transactions, self.id_capacity) if n is not None]
      full = bool(limits) and len(self.pending_requests) + reserved >= min(limits)
      if message.allow_retry and (full or not self._can_accept(message) or self.credit_waiters):
        self.credit_waiters.append(message.source_name)
        self.ring.inject(Message(self.node_name, message.source_name,
                                 transaction_id=message.transaction_id,
                                 channel=Channel.RSP, opcode=RspOpcode.RETRY_ACK))
        return
      if not message.allow_retry:
        self.credit_reservations[message.source_name] -= 1
        if not self.credit_reservations[message.source_name]:
          del self.credit_reservations[message.source_name]
    if (
      message.channel == Channel.REQ
      and message.opcode
      in (ReqOpcode.READ_NO_SNP, ReqOpcode.READ_SHARED, ReqOpcode.READ_UNIQUE)
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      home_id = allocate_id(self, "next_home_id", self.pending_requests)
      self.pending_requests[home_id] = message
      self.address_busy.setdefault(message.address, set()).add(home_id)
      holders = set(self.directory.get(message.address, {}))
      snoop_opcode = {
        ReqOpcode.READ_SHARED: SnpOpcode.SNP_SHARED,
        ReqOpcode.READ_UNIQUE: SnpOpcode.SNP_UNIQUE,
      }.get(message.opcode)
      if snoop_opcode and holders:
        snoop_id = allocate_id(self, "next_snoop_id", self.pending_snoops)
        self.pending_snoops[snoop_id] = {
          "home_id": home_id,
          "address": message.address,
          "awaiting": holders,
          "kind": "read",
        }
        for holder_name in holders:
          if snoop_opcode == SnpOpcode.SNP_UNIQUE:
            self.directory[message.address].pop(holder_name, None)
          else:
            self.directory[message.address][holder_name] = Resp.SC
          self.ring.inject(
            Message(
              self.node_name,
              holder_name,
              address=message.address,
              transaction_id=snoop_id,
              channel=Channel.SNP,
              opcode=snoop_opcode,
            )
          )
        if not self.directory[message.address]:
          del self.directory[message.address]
      else:
        self._send_storage_request(home_id, ReqOpcode.READ_NO_SNP)
    elif (
      message.channel == Channel.REQ
      and message.opcode in WRITE_DATA_OPCODES + MAINTENANCE_OPCODES
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      home_id = allocate_id(self, "next_home_id", self.pending_requests)
      self.pending_requests[home_id] = message
      self.address_busy.setdefault(message.address, set()).add(home_id)
      holders = set(self.directory.get(message.address, {})) if message.opcode in (ReqOpcode.WRITE_UNIQUE_FULL,) + MAINTENANCE_OPCODES + ATOMIC_OPCODES else set()
      if holders:
        snoop_id = allocate_id(self, "next_snoop_id", self.pending_snoops)
        self.pending_snoops[snoop_id] = {
          "home_id": home_id,
          "address": message.address,
          "awaiting": holders,
          "kind": "maintenance" if message.opcode in MAINTENANCE_OPCODES else "write",
        }
        for holder_name in holders:
          if message.opcode == ReqOpcode.CLEAN_SHARED:
            self.directory[message.address][holder_name] = Resp.SC
          else:
            self.directory.get(message.address, {}).pop(holder_name, None)
          self.ring.inject(
            Message(
              self.node_name,
              holder_name,
              address=message.address,
              transaction_id=snoop_id,
              channel=Channel.SNP,
              opcode={ReqOpcode.CLEAN_INVALID: SnpOpcode.SNP_CLEAN_INVALID,
                      ReqOpcode.CLEAN_SHARED: SnpOpcode.SNP_CLEAN_SHARED}.get(message.opcode, SnpOpcode.SNP_UNIQUE),
            )
          )
        if message.address in self.directory and not self.directory[message.address]:
          del self.directory[message.address]
      else:
        if message.opcode in MAINTENANCE_OPCODES:
          self._complete_maintenance(home_id)
        else:
          self._send_write_dbid(home_id)
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.COMP_DATA
    ):
      home_id = self.downstream_requests.pop(message.transaction_id)
      self._complete_read(home_id, message.payload, message.address, message.data_present, message.resp_err)
    elif (
      (message.channel == Channel.RSP and message.opcode == RspOpcode.SNP_RESP)
      or (message.channel == Channel.DAT and message.opcode == DatOpcode.SNP_RESP_DATA)
    ):
      entry = self.pending_snoops[message.transaction_id]
      if message.opcode == DatOpcode.SNP_RESP_DATA:
        self.dirty_data[entry["address"]] = message.payload
        self.dirty_errors[entry["address"]] = message.resp_err
      entry["awaiting"].discard(message.source_name)
      if not entry["awaiting"]:
        self.pending_snoops.pop(message.transaction_id)
        if entry["kind"] == "write":
          self._send_write_dbid(entry["home_id"])
        elif entry["kind"] == "maintenance":
          self._complete_maintenance(entry["home_id"])
        elif entry["kind"] == "dvm":
          home_id = entry["home_id"]
          self.pending_write_data.pop(home_id)
          self.pending_write_errors.pop(home_id, None)
          self._complete_maintenance(home_id)
        else:
          self._send_storage_request(entry["home_id"], ReqOpcode.READ_NO_SNP)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.COMP_ACK
    ):
      address, resp_err, state, home_id = self.pending_comp_acks.pop(
        (message.source_name, message.transaction_id)
      )
      self.pending_requests.pop(home_id, None)
      if resp_err == RespErr.OK:
        self.directory.setdefault(address, {})[message.source_name] = state
      busy = self.address_busy.get(address)
      if busy is not None:
        busy.discard(home_id)
        if not busy:
          self.address_busy.pop(address, None)
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.COPY_BACK_WRITE_DATA
    ):
      home_id = self.write_dbids.pop(message.transaction_id)
      request = self.pending_requests.pop(home_id)
      if message.resp == Resp.UD:
        self.dirty_data[request.address] = message.payload
        self.dirty_errors[request.address] = message.resp_err
      holders = self.directory.get(request.address, {})
      holders.pop(request.source_name, None)
      if not holders:
        self.directory.pop(request.address, None)
      self.address_busy[request.address].discard(home_id)
      if not self.address_busy[request.address]:
        del self.address_busy[request.address]
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.NON_COPY_BACK_WRITE_DATA
    ):
      home_id = self.write_dbids.pop(message.transaction_id)
      self.pending_write_data[home_id] = message.payload
      self.pending_write_errors[home_id] = message.resp_err
      request = self.pending_requests[home_id]
      if request.opcode == ReqOpcode.DVM_OP:
        if message.resp_err != RespErr.OK:
          self.pending_requests.pop(home_id)
          self.pending_write_data.pop(home_id)
          self.pending_write_errors.pop(home_id, None)
          self.address_busy.pop(request.address)
          self.send(Message(self.node_name, request.source_name, transaction_id=request.transaction_id,
                            channel=Channel.RSP, opcode=RspOpcode.COMP, resp_err=RespErr.NDERR))
          return
        operation, page = message.payload
        snoop_id = allocate_id(self, "next_snoop_id", self.pending_snoops)
        targets = {node.name for node in self.ring.nodes if node.role.startswith("CC")}
        self.pending_snoops[snoop_id] = {"home_id": home_id, "address": request.address,
                                        "awaiting": targets, "kind": "dvm"}
        for target in sorted(targets):
          for part in (0, 1):
            self.send(Message(self.node_name, target, transaction_id=snoop_id,
                              payload=(part, operation, page), channel=Channel.SNP,
                              opcode=SnpOpcode.SNP_DVM_OP))
        return
      if request.excl:
        key = (request.source_name, request.lpid)
        success = self.exclusive_monitors.pop(key, None) == request.address
        if not success:
          self.pending_requests.pop(home_id)
          self.pending_write_data.pop(home_id)
          self.pending_write_errors.pop(home_id, None)
          self.address_busy.pop(request.address)
          self.send(Message(self.node_name, request.source_name, transaction_id=request.transaction_id,
                            channel=Channel.RSP, opcode=RspOpcode.COMP, resp_err=RespErr.OK))
          return
      if request.opcode in ATOMIC_OPCODES:
        self._send_storage_request(home_id, ReqOpcode.READ_NO_SNP)
        return
      request_mask = request.byte_enable
      if message.byte_enable != request_mask:
        raise ValueError("write data byte enable differs from request metadata")
      self._send_storage_request(home_id, ReqOpcode.WRITE_NO_SNP_PTL if request.opcode == ReqOpcode.WRITE_NO_SNP_PTL else ReqOpcode.WRITE_NO_SNP_FULL)
    elif (
      message.channel == Channel.RSP
      and message.opcode in (RspOpcode.DBID_RESP, RspOpcode.COMP_DBID_RESP)
    ):
      write_data = Message(
        self.node_name,
        message.source_name,
        payload=self.pending_write_data[self.downstream_requests[message.transaction_id]],
        transaction_id=message.dbid,
        channel=Channel.DAT,
        opcode=DatOpcode.NON_COPY_BACK_WRITE_DATA,
        byte_enable=self.pending_requests[self.downstream_requests[message.transaction_id]].byte_enable,
        resp_err=self.pending_write_errors.get(self.downstream_requests[message.transaction_id], RespErr.OK),
      )
      self.send(write_data)
      self.downstream_data_sent.add(message.transaction_id)
      if message.opcode == RspOpcode.COMP_DBID_RESP:
        self.downstream_completions[message.transaction_id] = message
      self._finish_downstream_write(message.transaction_id)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.COMP
    ):
      self.downstream_completions[message.transaction_id] = message
      self._finish_downstream_write(message.transaction_id)

  def _complete_read(self, home_id, payload, address, data_present, resp_err):
      request = self.pending_requests[home_id]
      if request.excl and request.opcode == ReqOpcode.READ_NO_SNP:
        self.exclusive_monitors.pop((request.source_name, request.lpid), None)
        if resp_err == RespErr.OK:
          self.exclusive_monitors[(request.source_name, request.lpid)] = address
          resp_err = RespErr.EXOK
      if request.opcode in ATOMIC_OPCODES:
        operand_error = self.pending_write_errors.get(home_id, RespErr.OK)
        if resp_err != RespErr.OK or operand_error != RespErr.OK or not isinstance(payload, bytes) or len(payload) != 64:
          self.pending_requests.pop(home_id)
          self.pending_write_data.pop(home_id)
          self.pending_write_errors.pop(home_id, None)
          self.address_busy.pop(address)
          self._send_atomic_result(request, None, RespErr.DERR)
          return
        operand = self.pending_write_data[home_id]
        size = (1 << request.size) // (2 if request.opcode == ReqOpcode.ATOMIC_COMPARE else 1)
        offset = request.byte_offset
        old = payload[offset:offset + size]
        if request.opcode in LOAD_OPCODES + STORE_OPCODES:
          operation = request.opcode.removeprefix("AtomicLoad").removeprefix("AtomicStore")
          operand = calculate(operation, old, operand)
        if request.opcode == ReqOpcode.ATOMIC_COMPARE:
          if operand[:size] != old:
            self.pending_requests.pop(home_id)
            self.pending_write_data.pop(home_id)
            self.pending_write_errors.pop(home_id, None)
            self.address_busy.pop(address)
            self.send(Message(self.node_name, request.source_name, address=address,
                              transaction_id=request.transaction_id, payload=old,
                              channel=Channel.DAT, opcode=DatOpcode.COMP_DATA,
                              resp_err=RespErr.OK, resp=Resp.I))
            return
          operand = operand[size:]
        self.atomic_results[home_id] = old
        self.pending_write_data[home_id] = payload[:offset] + operand + payload[offset + size:]
        self._send_storage_request(home_id, ReqOpcode.WRITE_NO_SNP_FULL)
        return
      if request.opcode not in (ReqOpcode.READ_SHARED, ReqOpcode.READ_UNIQUE):
        self.pending_requests.pop(home_id)
        busy = self.address_busy.get(request.address)
        if busy is not None:
          busy.discard(home_id)
          if not busy:
            self.address_busy.pop(request.address, None)
      response = Message(
        self.node_name,
        request.source_name,
        payload=payload,
        address=address,
        transaction_id=request.transaction_id,
        data_present=data_present,
        resp_err=resp_err,
        resp=Resp.I,
        channel=Channel.DAT,
        opcode=DatOpcode.COMP_DATA,
      )
      if request.opcode == ReqOpcode.READ_SHARED:
        response.resp = Resp.SC
        response.dbid = home_id
        response.home_nid = self.node_name
        self.pending_comp_acks[
          (request.source_name, home_id)
        ] = (address, resp_err, Resp.SC, home_id)
      elif request.opcode == ReqOpcode.READ_UNIQUE:
        response.resp = Resp.UC
        response.dbid = home_id
        response.home_nid = self.node_name
        self.pending_comp_acks[
          (request.source_name, home_id)
        ] = (address, resp_err, Resp.UC, home_id)
      self.send(response)
    

class StorageWrapper(Endpoint):

  def __init__(self, ring, node_name, error_addresses=None, id_capacity=None):
    super().__init__()
    validate_id_capacity(id_capacity)
    self.id_capacity = id_capacity
    self.ring = ring
    self.node_name = node_name
    self.error_addresses = set(error_addresses or ())
    self.dj = DongJiang()
    self.pending_writes = {}
    self.next_dbid = 0

  def can_receive(self, message):
    if message.channel != Channel.REQ or message.opcode not in (ReqOpcode.WRITE_NO_SNP_FULL, ReqOpcode.WRITE_NO_SNP_PTL):
      return True
    return self.id_capacity is None or len(self.pending_writes) < self.id_capacity

  def receive(self, message):
    message = super().receive(message)
    if message is None:
      return
    if (
      message.channel == Channel.REQ
      and message.opcode == ReqOpcode.READ_NO_SNP
    ):
      payload, data_present = self.dj.read(message.address)
      response = Message(
        self.node_name,
        message.source_name,
        payload=payload,
        address=message.address,
        transaction_id=message.transaction_id,
        data_present=data_present,
        resp_err=(
          RespErr.DERR if message.address in self.error_addresses else RespErr.OK
        ),
        resp=Resp.UC,
        channel=Channel.DAT,
        opcode=DatOpcode.COMP_DATA,
      )
      self.send(response)
    elif (
      message.channel == Channel.REQ
      and message.opcode in (ReqOpcode.WRITE_NO_SNP_FULL, ReqOpcode.WRITE_NO_SNP_PTL)
    ):
      dbid = allocate_id(self, "next_dbid", self.pending_writes)
      self.pending_writes[dbid] = message
      dbid_response = Message(
        self.node_name,
        message.source_name,
        transaction_id=message.transaction_id,
        dbid=dbid,
        channel=Channel.RSP,
        opcode=RspOpcode.DBID_RESP,
      )
      self.ring.inject(dbid_response)
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.NON_COPY_BACK_WRITE_DATA
    ):
      request = self.pending_writes.pop(message.transaction_id)
      if request.address not in self.error_addresses and message.resp_err == RespErr.OK:
        if request.opcode == ReqOpcode.WRITE_NO_SNP_PTL:
          self.dj.write_partial(request.address, message.payload, message.byte_enable)
        else:
          self.dj.write(request.address, message.payload)
      response = Message(
        self.node_name,
        message.source_name,
        transaction_id=request.transaction_id,
        resp_err=(
          RespErr.NDERR
          if request.address in self.error_addresses or message.resp_err != RespErr.OK
          else RespErr.OK
        ),
        channel=Channel.RSP,
        opcode=RspOpcode.COMP,
      )
      self.ring.inject(response)
