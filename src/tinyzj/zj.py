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
    if address is None:
      raise ValueError("read request needs an address")
    return self._send_request("home", "read_request", address=address)

  def io_request(self, payload=None):
    return self._send_request("io", "io_request", payload)

  def write(self, address, data):
    if address is None:
      raise ValueError("write request needs an address")
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
    return self._response_for("read_request", "read_response", request)

  def io_response_for(self, request):
    return self._response_for("io_request", "io_response", request)

  def write_response_for(self, request):
    return self._response_for("write_request", "write_response", request)

  def _response_for(self, request_type, response_type, request):
    if request not in self.sent_messages:
      raise ValueError("request was not sent by this socket")
    if request.message_type != request_type:
      raise ValueError(f"expected {request_type}")
    return self._responses.get((response_type, request.transaction_id))

  def receive(self, message):
    super().receive(message)
    response_requirements = {
      "read_response": ("read_request", "home"),
      "write_response": ("write_request", "home"),
      "io_response": ("io_request", "io"),
    }
    if message.message_type not in response_requirements:
      return

    request_type, source_name = response_requirements[message.message_type]
    matching_requests = [
      request
      for request in self.sent_messages
      if request.message_type == request_type
      and request.transaction_id == message.transaction_id
    ]
    if not matching_requests:
      raise ValueError("response has no matching request")
    if message.source_name != source_name:
      raise ValueError(f"expected response from {source_name}")

    key = (message.message_type, message.transaction_id)
    if key in self._responses:
      raise ValueError("response already received")
    self._responses[key] = message


class HomeWrapper(Endpoint):
  
  def __init__(self, ring):
    super().__init__()
    self.ring = ring
    self.sent_messages = []
    self.dj = DongJiang()

  def receive(self, message):
    super().receive(message)
    if message.message_type == "read_request":
      payload, data_present = self.dj.read(message.address)
      response = Message(
        "home",
        message.source_name,
        payload=payload,
        address=message.address,
        message_type="read_response",
        transaction_id=message.transaction_id,
        data_present=data_present,
      )
      self.sent_messages.append(response)
      self.ring.inject(response)
    elif message.message_type == "write_request":
      self.dj.write(message.address, message.payload)
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
