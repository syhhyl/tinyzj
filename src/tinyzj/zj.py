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
from .xj import Message, Ring, RingNode


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
  ):
    validate_id_capacity(id_capacity)
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

  def step(self):
    self.ring.step()
    self.cc0.advance_requests()
    self.cc1.advance_requests()
    self.hf.advance_credits()

  def run_until_idle(self, max_steps=100):
    if max_steps < 0:
      raise ValueError("max_steps must be non-negative")

    steps = 0
    while self.ring.in_flight or self.cc0.has_pending_requests() or self.cc1.has_pending_requests():
      if steps == max_steps:
        raise RuntimeError("system did not become idle")
      self.step()
      steps += 1
    return steps


class Endpoint:

  def __init__(self):
    self.received_messages = []

  def receive(self, message):
    self.received_messages.append(message)


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
    self.write_data_sent = set()
    self.retry_requests = {}
    self.protocol_credits = {}
    self.ring = ring
    self.node_name = node_name
    self.home_name = home_name
    self._responses = {}
    self.cache = {}
    self.pending_write_data = {}
    self.next_transaction_id = 0

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

  def write_no_snp(self, address, data):
    return self._write(address, data, ReqOpcode.WRITE_NO_SNP_FULL)

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
        self.request_states[request] = "await_dbid" if request.opcode in (ReqOpcode.WRITE_NO_SNP_FULL, ReqOpcode.WRITE_UNIQUE_FULL, ReqOpcode.WRITE_BACK_FULL) else "await_data"
    for request in list(self.waiting_requests):
      if self.id_capacity is not None and len(self.active_requests) >= self.id_capacity:
        break
      if any(active.address == request.address for active in self.active_requests.values()):
        continue
      self.waiting_requests.remove(request)
      txn_id = allocate_id(self, "next_transaction_id", self.active_requests)
      request.transaction_id = txn_id
      self.active_requests[txn_id] = request
      self.request_states[request] = "await_dbid" if request.opcode in (ReqOpcode.WRITE_NO_SNP_FULL, ReqOpcode.WRITE_UNIQUE_FULL, ReqOpcode.WRITE_BACK_FULL) else "await_data"
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

  def receive(self, message):
    super().receive(message)
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
      key = (message.channel, message.opcode, request)
      self._responses[key] = message
      if request.exp_comp_ack:
        self.request_states[request] = "send_comp_ack"
        if message.resp_err == RespErr.OK:
          self.cache[message.address] = (message.resp, message.payload)
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
    elif (
      message.channel == Channel.SNP
      and message.opcode == SnpOpcode.SNP_SHARED
    ):
      dirty = self.cache.get(message.address, (None,))[0] == Resp.UD
      payload = self.cache[message.address][1] if dirty else None
      if message.address in self.cache:
        state, payload = self.cache[message.address]
        self.cache[message.address] = (Resp.SC, payload)
      self.ring.inject(
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
      and message.opcode == SnpOpcode.SNP_UNIQUE
    ):
      dirty = self.cache.get(message.address, (None,))[0] == Resp.UD
      payload = self.cache[message.address][1] if dirty else None
      self.cache.pop(message.address, None)
      self.ring.inject(
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
      )
      self.ring.inject(write_data)
      self.write_data_sent.add(message.transaction_id)
      if message.opcode == RspOpcode.COMP_DBID_RESP:
        self.write_completions[message.transaction_id] = message
      request = self.active_requests[message.transaction_id]
      self.request_states[request] = "await_comp"
      self._finish_write(message.transaction_id)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.COMP
    ):
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
    self.next_downstream_id = 0
    self.downstream_requests = {}
    self.next_dbid = 0
    self.write_dbids = {}
    self.next_snoop_id = 0
    self.dirty_data = {}
    self.downstream_completions = {}
    self.downstream_data_sent = set()

  def _finish_downstream_write(self, txn_id):
    if txn_id not in self.downstream_data_sent or txn_id not in self.downstream_completions:
      return
    completion = self.downstream_completions.pop(txn_id)
    self.downstream_data_sent.remove(txn_id)
    home_id = self.downstream_requests.pop(txn_id)
    request = self.pending_requests.pop(home_id)
    self.pending_write_data.pop(home_id)
    if completion.resp_err == RespErr.OK and request.opcode == ReqOpcode.WRITE_UNIQUE_FULL:
      self.dirty_data.pop(request.address, None)
    busy = self.address_busy.get(request.address)
    if busy is not None:
      busy.discard(home_id)
      if not busy:
        self.address_busy.pop(request.address, None)
    self.ring.inject(Message(
      self.node_name,
      request.source_name,
      transaction_id=request.transaction_id,
      resp_err=completion.resp_err,
      channel=Channel.RSP,
      opcode=RspOpcode.COMP,
    ))

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

  def _send_storage_request(self, home_id, opcode):
    request = self.pending_requests[home_id]
    if opcode == ReqOpcode.READ_NO_SNP and request.opcode != ReqOpcode.READ_NO_SNP and request.address in self.dirty_data:
      self._complete_read(home_id, self.dirty_data[request.address], request.address, True, RespErr.OK)
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
    super().receive(message)
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
      and message.opcode in (ReqOpcode.WRITE_NO_SNP_FULL, ReqOpcode.WRITE_UNIQUE_FULL, ReqOpcode.WRITE_BACK_FULL)
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      home_id = allocate_id(self, "next_home_id", self.pending_requests)
      self.pending_requests[home_id] = message
      self.address_busy.setdefault(message.address, set()).add(home_id)
      holders = set(self.directory.get(message.address, {})) if message.opcode == ReqOpcode.WRITE_UNIQUE_FULL else set()
      if holders:
        snoop_id = allocate_id(self, "next_snoop_id", self.pending_snoops)
        self.pending_snoops[snoop_id] = {
          "home_id": home_id,
          "address": message.address,
          "awaiting": holders,
          "kind": "write",
        }
        for holder_name in holders:
          self.directory.get(message.address, {}).pop(holder_name, None)
          self.ring.inject(
            Message(
              self.node_name,
              holder_name,
              address=message.address,
              transaction_id=snoop_id,
              channel=Channel.SNP,
              opcode=SnpOpcode.SNP_UNIQUE,
            )
          )
        if message.address in self.directory and not self.directory[message.address]:
          del self.directory[message.address]
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
      entry["awaiting"].discard(message.source_name)
      if not entry["awaiting"]:
        self.pending_snoops.pop(message.transaction_id)
        if entry["kind"] == "write":
          self._send_write_dbid(entry["home_id"])
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
      self._send_storage_request(home_id, ReqOpcode.WRITE_NO_SNP_FULL)
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
      )
      self.ring.inject(write_data)
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
      self.ring.inject(response)
    

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
    if message.channel != Channel.REQ or message.opcode != ReqOpcode.WRITE_NO_SNP_FULL:
      return True
    return self.id_capacity is None or len(self.pending_writes) < self.id_capacity

  def receive(self, message):
    super().receive(message)
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
      self.ring.inject(response)
    elif (
      message.channel == Channel.REQ
      and message.opcode == ReqOpcode.WRITE_NO_SNP_FULL
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
      if request.address not in self.error_addresses:
        self.dj.write(request.address, message.payload)
      response = Message(
        self.node_name,
        message.source_name,
        transaction_id=request.transaction_id,
        resp_err=(
          RespErr.NDERR
          if request.address in self.error_addresses
          else RespErr.OK
        ),
        channel=Channel.RSP,
        opcode=RspOpcode.COMP,
      )
      self.ring.inject(response)
