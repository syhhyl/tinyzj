from .dj import DongJiang
from .xj import Ring, RingNode

class Zhujiang:
  
  def __init__(self):
    nodes = []
    
    nodes.append(RingNode("cc", "CPU cluster"))
    nodes.append(RingNode("home", "coherent home"))
    nodes.append(RingNode("io", "memory and IO"))
    
    self.ring = Ring(nodes)
    
    self.socket = Socket()
    self.home = HomeWrapper()
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
  
  def __init__(self):
    super().__init__()
    self.dj = DongJiang()
    

class IoWrapper(Endpoint):
  pass
