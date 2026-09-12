from .dj import DongJiang
from .xj import Message, Ring, RingNode

class Zhujiang:
  
  def __init__(self):
    nodes = []
    
    nodes.append(RingNode("cc", "CPU cluster"))
    nodes.append(RingNode("home", "coherent home"))
    nodes.append(RingNode("io", "memory and IO"))
    
    self.ring = Ring(nodes)
    
    self.socket = Socket()
    self.home = HomeWrapper(self.ring)
    self.io_wrapper = IoWrapper()

    self.ring.connect("cc", self.socket)
    self.ring.connect("home", self.home)
    self.ring.connect("io", self.io_wrapper)


class Endpoint:

  def __init__(self):
    self.received_messages = []

  def receive(self, message):
    self.received_messages.append(message)


class Socket(Endpoint):
  pass


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
  pass
