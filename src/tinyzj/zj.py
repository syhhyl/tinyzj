from .chi import Channel, DatOpcode, ReqOpcode, RspOpcode
from .dj import DongJiang
from .xj import Message, Ring, RingNode

class Zhujiang:
  
  def __init__(self):
    self.ring = Ring([
      RingNode("n00", "CC0"),
      RingNode("n01", "HF"),
      RingNode("n11", "CC1"),
      RingNode("n10", "S"),
    ])

    self.cc0 = Socket(self.ring, "n00", "n01")
    self.hf = HomeWrapper(self.ring, "n01", "n10")
    self.cc1 = Socket(self.ring, "n11", "n01")
    self.s = StorageWrapper(self.ring, "n10")

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
    self.pending_write_data = {}

  def read(self, address):
    if address is None:
      raise ValueError("read request needs an address")
    return self._send_request(
      address=address,
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_NO_SNP,
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
    )
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
    elif (
      message.channel == Channel.RSP
      and message.opcode == RspOpcode.DBID_RESP
    ):
      data = self.pending_write_data[message.transaction_id]
      write_data = Message(
        self.node_name,
        message.source_name,
        payload=data,
        transaction_id=message.transaction_id,
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
  
  def __init__(self, ring, node_name, storage_name):
    super().__init__()
    self.ring = ring
    self.node_name = node_name
    self.storage_name = storage_name
    self.pending_requests = {}
    self.pending_write_data = {}

  def receive(self, message):
    super().receive(message)
    if (
      message.channel == Channel.REQ
      and message.opcode == ReqOpcode.READ_NO_SNP
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      self.pending_requests[message.transaction_id] = message
      storage_request = Message(
        self.node_name,
        self.storage_name,
        address=message.address,
        transaction_id=message.transaction_id,
        channel=Channel.ERQ,
        opcode=ReqOpcode.READ_NO_SNP,
      )
      self.ring.inject(storage_request)
    elif (
      message.channel == Channel.REQ
      and message.opcode == ReqOpcode.WRITE_NO_SNP_FULL
    ):
      if message.address is None:
        raise ValueError("home request needs an address")
      self.pending_requests[message.transaction_id] = message
      dbid_response = Message(
        self.node_name,
        message.source_name,
        transaction_id=message.transaction_id,
        channel=Channel.RSP,
        opcode=RspOpcode.DBID_RESP,
      )
      self.ring.inject(dbid_response)
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.COMP_DATA
    ):
      request = self.pending_requests.pop(message.transaction_id)
      response = Message(
        self.node_name,
        request.source_name,
        payload=message.payload,
        address=message.address,
        transaction_id=message.transaction_id,
        data_present=message.data_present,
        channel=Channel.DAT,
        opcode=DatOpcode.COMP_DATA,
      )
      self.ring.inject(response)
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
        channel=Channel.ERQ,
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
        transaction_id=message.transaction_id,
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
      response = Message(
        self.node_name,
        request.source_name,
        transaction_id=message.transaction_id,
        channel=Channel.RSP,
        opcode=RspOpcode.COMP,
      )
      self.ring.inject(response)
    

class StorageWrapper(Endpoint):

  def __init__(self, ring, node_name):
    super().__init__()
    self.ring = ring
    self.node_name = node_name
    self.dj = DongJiang()
    self.pending_writes = {}

  def receive(self, message):
    super().receive(message)
    if (
      message.channel == Channel.ERQ
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
        channel=Channel.DAT,
        opcode=DatOpcode.COMP_DATA,
      )
      self.ring.inject(response)
    elif (
      message.channel == Channel.ERQ
      and message.opcode == ReqOpcode.WRITE_NO_SNP_FULL
    ):
      self.pending_writes[message.transaction_id] = message
      dbid_response = Message(
        self.node_name,
        message.source_name,
        transaction_id=message.transaction_id,
        channel=Channel.RSP,
        opcode=RspOpcode.DBID_RESP,
      )
      self.ring.inject(dbid_response)
    elif (
      message.channel == Channel.DAT
      and message.opcode == DatOpcode.NON_COPY_BACK_WRITE_DATA
    ):
      request = self.pending_writes.pop(message.transaction_id)
      self.dj.write(request.address, message.payload)
      response = Message(
        self.node_name,
        message.source_name,
        transaction_id=message.transaction_id,
        channel=Channel.RSP,
        opcode=RspOpcode.COMP,
      )
      self.ring.inject(response)
