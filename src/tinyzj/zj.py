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
    self._read_responses = {}
    self._io_responses = {}

  def read(self, payload=None):
    request = Message(
      "cc",
      "home",
      payload=payload,
      message_type="read_request",
    )
    self.sent_messages.append(request)
    self.ring.inject(request)
    return request

  def io_request(self, payload=None):
    request = Message(
      "cc",
      "io",
      payload=payload,
      message_type="io_request",
    )
    self.sent_messages.append(request)
    self.ring.inject(request)
    return request

  def read_response_for(self, request):
    return self._read_responses.get(request.transaction_id)

  def io_response_for(self, request):
    return self._io_responses.get(request.transaction_id)

  def receive(self, message):
    super().receive(message)
    if message.message_type == "read_response":
      self._read_responses[message.transaction_id] = message
    if message.message_type == "io_response":
      self._io_responses[message.transaction_id] = message


class HomeWrapper(Endpoint):
  
  def __init__(self, ring):
    super().__init__()
    self.ring = ring
    self.sent_messages = []
    self.dj = DongJiang()

  def receive(self, message):
    super().receive(message)
    if message.message_type == "read_request":
      response = Message(
        "home",
        message.source_name,
        payload="read data",
        message_type="read_response",
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
