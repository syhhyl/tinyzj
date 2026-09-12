from dj import DongJiang
from xj import Ring, RingNode

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



class Socket:
  pass


class HomeWrapper:
  
  def __init__(self):
    self.dj = DongJiang()
    

class IoWrapper:
  pass
