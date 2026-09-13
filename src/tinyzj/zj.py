from .dj import DongJiang
from .xj import Message, Ring, RingNode

class Zhujiang:
  
  def __init__(self):
    nodes = []
    
    nodes.append(RingNode("cc", "CPU cluster"))
    nodes.append(RingNode("home", "coherent home"))
    nodes.append(RingNode("io", "memory and IO"))
    
    self.ring = Ring(nodes)
    
    self.socket = Socket(self.ring)
    self.home = HomeWrapper(self.ring)
    self.io_wrapper = IoWrapper(self.ring)

    self.ring.connect("cc", self.socket)
    self.ring.connect("home", self.home)
    self.ring.connect("io", self.io_wrapper)

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

  def __init__(self, ring):
    super().__init__()
    self.ring = ring
    self.sent_messages = []
    self._responses = {}

  def read(self, address):
    return self._send_request("home", "read_request", address=address)

  def io_request(self, payload=None):
    return self._send_request("io", "io_request", payload)

  def write(self, address, data):
    return self._send_request(
      "home",
      "write_request",
      payload=data,
      address=address,
    )

  def _send_request(self, target_name, message_type, payload=None, address=None):
    request = Message(
      "cc",
      target_name,
      payload=payload,
      address=address,
      message_type=message_type,
    )
    self.sent_messages.append(request)
    self.ring.inject(request)
    return request

  def read_response_for(self, request):
    return self._response_for("read_response", request)

  def io_response_for(self, request):
    return self._response_for("io_response", request)

  def write_response_for(self, request):
    return self._response_for("write_response", request)

  def _response_for(self, response_type, request):
    return self._responses.get((response_type, request.transaction_id))

  def receive(self, message):
    super().receive(message)
    if message.message_type in (
      "read_response",
      "io_response",
      "write_response",
    ):
      key = (message.message_type, message.transaction_id)
      self._responses[key] = message


class HomeWrapper(Endpoint):
  
  def __init__(self, ring):
    super().__init__()
    self.ring = ring
    self.sent_messages = []
    self.dj = DongJiang()
    self.data_by_address = {}

  def receive(self, message):
    super().receive(message)
    if message.message_type == "read_request":
      response = Message(
        "home",
        message.source_name,
        payload=self.data_by_address.get(message.address, "read data"),
        address=message.address,
        message_type="read_response",
        transaction_id=message.transaction_id,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
    elif message.message_type == "write_request":
      self.data_by_address[message.address] = message.payload
      response = Message(
        "home",
        message.source_name,
        payload="write complete",
        address=message.address,
        message_type="write_response",
        transaction_id=message.transaction_id,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
    

class IoWrapper(Endpoint):

  def __init__(self, ring):
    super().__init__()
    self.ring = ring
    self.sent_messages = []

  def receive(self, message):
    super().receive(message)
    if message.message_type == "io_request":
      response = Message(
        "io",
        message.source_name,
        payload="io data",
        message_type="io_response",
        transaction_id=message.transaction_id,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
