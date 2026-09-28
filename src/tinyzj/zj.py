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

class Zhujiang:
  
  def __init__(
    self,
    buffer_capacity=None,
    max_transactions=None,
    error_addresses=None,
  ):
    self.ring = Ring([
      RingNode("n00", "CC0"),
      RingNode("n01", "HF"),
      RingNode("n11", "CC1"),
      RingNode("n10", "S"),
    ], buffer_capacity=buffer_capacity)

    self.cc0 = Socket(self.ring, "n00", "n01")
    self.hf = HomeWrapper(
      self.ring,
      "n01",
      "n10",
      max_transactions=max_transactions,
    )
    self.cc1 = Socket(self.ring, "n11", "n01")
    self.s = StorageWrapper(
      self.ring,
      "n10",
      error_addresses=error_addresses,
    )

    self.ring.connect("n00", self.cc0)
    self.ring.connect("n01", self.hf)
    self.ring.connect("n11", self.cc1)
    self.ring.connect("n10", self.s)

  def step(self):
    self.ring.step()

  def run_until_idle(self, max_steps=100):
    if max_steps < 0:
      raise ValueError("max_steps must be non-negative")

    steps = 0
    while self.ring.in_flight:
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

  def __init__(self, ring, node_name, home_name):
    super().__init__()
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
    if address in self.cache:
      return None
    return self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_SHARED,
    )

  def read_unique(self, address):
    if address is None:
      raise ValueError("read request needs an address")
    if address in self.cache and self.cache[address][0] == Resp.UC:
      return None
    return self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_UNIQUE,
    )

  def write(self, address, data):
    if address is None:
      raise ValueError("write request needs an address")
    request = self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.WRITE_NO_SNP_FULL,
    )
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
      transaction_id=self.next_transaction_id,
    )
    self.next_transaction_id += 1
    self.ring.inject(request)
    return request

  def read_response_for(self, request):
    return self._responses.get(
      (Channel.DAT, DatOpcode.COMP_DATA, request.transaction_id)
    )

  def write_response_for(self, request):
    return self._responses.get(
      (Channel.RSP, RspOpcode.COMP, request.transaction_id)
    )

  def receive(self, message):
    super().receive(message)
    if (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.COMP_DATA
    ):
      key = (message.channel, message.opcode, message.transaction_id)
      self._responses[key] = message
      if message.resp in (Resp.SC, Resp.UC):
        if message.resp_err == RespErr.OK:
          self.cache[message.address] = (message.resp, message.payload)
        self.ring.inject(
          Message(
            self.node_name,
            message.source_name,
            transaction_id=message.transaction_id,
            channel=Channel.RSP,
            opcode=RspOpcode.COMP_ACK,
          )
        )
    elif (
      message.channel == Channel.SNP
      and message.opcode == SnpOpcode.SNP_SHARED
    ):
      if message.address in self.cache:
        state, payload = self.cache[message.address]
        self.cache[message.address] = (Resp.SC, payload)
      self.ring.inject(
        Message(
          self.node_name,
          message.source_name,
          transaction_id=message.transaction_id,
          channel=Channel.RSP,
          opcode=RspOpcode.SNP_RESP,
        )
      )
    elif (
      message.channel == Channel.SNP
      and message.opcode == SnpOpcode.SNP_UNIQUE
    ):
      self.cache.pop(message.address, None)
      self.ring.inject(
        Message(
          self.node_name,
          message.source_name,
          transaction_id=message.transaction_id,
          channel=Channel.RSP,
          opcode=RspOpcode.SNP_RESP,
        )
      )
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.DBID_RESP
    ):
      data = self.pending_write_data[message.transaction_id]
      write_data = Message(
        self.node_name,
        message.source_name,
        payload=data,
        transaction_id=message.dbid,
        channel=Channel.DAT,
        opcode=DatOpcode.NON_COPY_BACK_WRITE_DATA,
      )
      self.ring.inject(write_data)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.COMP
    ):
      self.pending_write_data.pop(message.transaction_id)
      key = (message.channel, message.opcode, message.transaction_id)
      self._responses[key] = message


class HomeWrapper(Endpoint):
  
  def __init__(self, ring, node_name, storage_name, max_transactions=None):
    super().__init__()
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

  def can_receive(self, message):
    if message.channel != Channel.REQ:
      return True
    if self.address_busy.get(message.address):
      return False
    if self.max_transactions is None:
      return True
    return len(self.pending_requests) < self.max_transactions

  def receive(self, message):
    super().receive(message)
    if (
      message.channel == Channel.REQ
      and message.opcode
      in (ReqOpcode.READ_NO_SNP, ReqOpcode.READ_SHARED, ReqOpcode.READ_UNIQUE)
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      home_id = self.next_home_id
      self.next_home_id += 1
      self.pending_requests[home_id] = message
      self.address_busy.setdefault(message.address, set()).add(home_id)
      holders = set(self.directory.get(message.address, {}))
      snoop_opcode = {
        ReqOpcode.READ_SHARED: SnpOpcode.SNP_SHARED,
        ReqOpcode.READ_UNIQUE: SnpOpcode.SNP_UNIQUE,
      }.get(message.opcode)
      if snoop_opcode and holders:
        self.pending_snoops[home_id] = {
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
              transaction_id=home_id,
              channel=Channel.SNP,
              opcode=snoop_opcode,
            )
          )
        if not self.directory[message.address]:
          del self.directory[message.address]
      else:
        storage_request = Message(
          self.node_name,
          self.storage_name,
          address=message.address,
          transaction_id=home_id,
          channel=Channel.REQ,
          opcode=ReqOpcode.READ_NO_SNP,
        )
        self.ring.inject(storage_request)
    elif (
      message.channel == Channel.REQ
      and message.opcode == ReqOpcode.WRITE_NO_SNP_FULL
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      home_id = self.next_home_id
      self.next_home_id += 1
      self.pending_requests[home_id] = message
      self.address_busy.setdefault(message.address, set()).add(home_id)
      holders = set(self.directory.get(message.address, {}))
      if holders:
        self.pending_snoops[home_id] = {
          "address": message.address,
          "awaiting": holders,
          "kind": "write",
          "request": message,
        }
        for holder_name in holders:
          self.directory.get(message.address, {}).pop(holder_name, None)
          self.ring.inject(
            Message(
              self.node_name,
              holder_name,
              address=message.address,
              transaction_id=home_id,
              channel=Channel.SNP,
              opcode=SnpOpcode.SNP_UNIQUE,
            )
          )
        if message.address in self.directory and not self.directory[message.address]:
          del self.directory[message.address]
      else:
        dbid_response = Message(
          self.node_name,
          message.source_name,
          transaction_id=message.transaction_id,
          dbid=home_id,
          channel=Channel.RSP,
          opcode=RspOpcode.DBID_RESP,
        )
        self.ring.inject(dbid_response)
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.COMP_DATA
    ):
      request = self.pending_requests[message.transaction_id]
      if request.opcode not in (ReqOpcode.READ_SHARED, ReqOpcode.READ_UNIQUE):
        self.pending_requests.pop(message.transaction_id)
        busy = self.address_busy.get(request.address)
        if busy is not None:
          busy.discard(message.transaction_id)
          if not busy:
            self.address_busy.pop(request.address, None)
      response = Message(
        self.node_name,
        request.source_name,
        payload=message.payload,
        address=message.address,
        transaction_id=request.transaction_id,
        data_present=message.data_present,
        resp_err=message.resp_err,
        resp=Resp.I,
        channel=Channel.DAT,
        opcode=DatOpcode.COMP_DATA,
      )
      if request.opcode == ReqOpcode.READ_SHARED:
        response.resp = Resp.SC
        self.pending_comp_acks[
          (request.source_name, request.transaction_id)
        ] = (message.address, message.resp_err, Resp.SC, message.transaction_id)
      elif request.opcode == ReqOpcode.READ_UNIQUE:
        response.resp = Resp.UC
        self.pending_comp_acks[
          (request.source_name, request.transaction_id)
        ] = (message.address, message.resp_err, Resp.UC, message.transaction_id)
      self.ring.inject(response)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.SNP_RESP
    ):
      entry = self.pending_snoops[message.transaction_id]
      entry["awaiting"].discard(message.source_name)
      if not entry["awaiting"]:
        self.pending_snoops.pop(message.transaction_id)
        if entry["kind"] == "write":
          dbid_response = Message(
            self.node_name,
            entry["request"].source_name,
            transaction_id=entry["request"].transaction_id,
            dbid=message.transaction_id,
            channel=Channel.RSP,
            opcode=RspOpcode.DBID_RESP,
          )
          self.ring.inject(dbid_response)
        else:
          storage_request = Message(
            self.node_name,
            self.storage_name,
            address=entry["address"],
            transaction_id=message.transaction_id,
            channel=Channel.REQ,
            opcode=ReqOpcode.READ_NO_SNP,
          )
          self.ring.inject(storage_request)
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
      and message.opcode == DatOpcode.NON_COPY_BACK_WRITE_DATA
    ):
      request = self.pending_requests[message.transaction_id]
      self.pending_write_data[message.transaction_id] = message.payload
      storage_request = Message(
        self.node_name,
        self.storage_name,
        address=request.address,
        transaction_id=message.transaction_id,
        channel=Channel.REQ,
        opcode=ReqOpcode.WRITE_NO_SNP_FULL,
      )
      self.ring.inject(storage_request)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.DBID_RESP
    ):
      write_data = Message(
        self.node_name,
        message.source_name,
        payload=self.pending_write_data[message.transaction_id],
        transaction_id=message.dbid,
        channel=Channel.DAT,
        opcode=DatOpcode.NON_COPY_BACK_WRITE_DATA,
      )
      self.ring.inject(write_data)
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.COMP
    ):
      request = self.pending_requests.pop(message.transaction_id)
      self.pending_write_data.pop(message.transaction_id)
      busy = self.address_busy.get(request.address)
      if busy is not None:
        busy.discard(message.transaction_id)
        if not busy:
          self.address_busy.pop(request.address, None)
      response = Message(
        self.node_name,
        request.source_name,
        transaction_id=request.transaction_id,
        resp_err=message.resp_err,
        channel=Channel.RSP,
        opcode=RspOpcode.COMP,
      )
      self.ring.inject(response)
    

class StorageWrapper(Endpoint):

  def __init__(self, ring, node_name, error_addresses=None):
    super().__init__()
    self.ring = ring
    self.node_name = node_name
    self.error_addresses = set(error_addresses or ())
    self.dj = DongJiang()
    self.pending_writes = {}
    self.next_dbid = 0

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
      dbid = self.next_dbid
      self.next_dbid += 1
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
