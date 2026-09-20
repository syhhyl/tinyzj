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
    self.sent_messages = []
    self._responses = {}

  def read(self, address):
    if address is None:
      raise ValueError("read request needs an address")
    return self._send_request("read_request", address=address)

  def write(self, address, data):
    if address is None:
      raise ValueError("write request needs an address")
    return self._send_request(
      "write_request",
      payload=data,
      address=address,
    )

  def _send_request(self, message_type, payload=None, address=None):
    request = Message(
      self.node_name,
      self.home_name,
      payload=payload,
      address=address,
      message_type=message_type,
    )
    self.sent_messages.append(request)
    self.ring.inject(request)
    return request

  def read_response_for(self, request):
    return self._response_for("read_response", request)

  def write_response_for(self, request):
    return self._response_for("write_response", request)

  def _response_for(self, response_type, request):
    return self._responses.get((response_type, request.transaction_id))

  def receive(self, message):
    super().receive(message)
    if message.message_type in (
      "read_response",
      "write_response",
    ):
      key = (message.message_type, message.transaction_id)
      self._responses[key] = message


class HomeWrapper(Endpoint):
  
  def __init__(self, ring, node_name, storage_name):
    super().__init__()
    self.ring = ring
    self.node_name = node_name
    self.storage_name = storage_name
    self.sent_messages = []
    self.pending_requests = {}

  def receive(self, message):
    super().receive(message)
    if message.message_type in ("read_request", "write_request"):
      if message.address is None:
        raise ValueError("home request needs an address")
      self.pending_requests[message.transaction_id] = message
      storage_request = Message(
        self.node_name,
        self.storage_name,
        payload=message.payload,
        address=message.address,
        message_type=f"storage_{message.message_type}",
        transaction_id=message.transaction_id,
      )
      self.sent_messages.append(storage_request)
      self.ring.inject(storage_request)
    elif message.message_type in (
      "storage_read_response",
      "storage_write_response",
    ):
      request = self.pending_requests.pop(message.transaction_id)
      response_type = message.message_type.removeprefix("storage_")
      response = Message(
        self.node_name,
        request.source_name,
        payload=message.payload,
        address=message.address,
        message_type=response_type,
        transaction_id=message.transaction_id,
        data_present=message.data_present,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
    

class StorageWrapper(Endpoint):

  def __init__(self, ring, node_name):
    super().__init__()
    self.ring = ring
    self.node_name = node_name
    self.sent_messages = []
    self.dj = DongJiang()

  def receive(self, message):
    super().receive(message)
    if message.message_type == "storage_read_request":
      payload, data_present = self.dj.read(message.address)
      response = Message(
        self.node_name,
        message.source_name,
        payload=payload,
        address=message.address,
        message_type="storage_read_response",
        transaction_id=message.transaction_id,
        data_present=data_present,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
    elif message.message_type == "storage_write_request":
      self.dj.write(message.address, message.payload)
      response = Message(
        self.node_name,
        message.source_name,
        payload="write complete",
        address=message.address,
        message_type="storage_write_response",
        transaction_id=message.transaction_id,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
